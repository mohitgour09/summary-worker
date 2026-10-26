-- Safe to re-apply: tables use IF NOT EXISTS and later columns are added by the
-- idempotent upgrade block at the bottom.

-- One row per encounter. patient_id / encounter_type are fixed for the encounter's lifetime
-- and are set by the first accepted event; later events must match (else 409 + quarantine).
CREATE TABLE IF NOT EXISTS encounters (
  encounter_id TEXT PRIMARY KEY,
  patient_id TEXT NOT NULL,
  encounter_type TEXT NOT NULL CHECK (encounter_type IN ('Appointment','MedicationRefill','TelephoneTriage')),
  current_version INTEGER NOT NULL DEFAULT 0 CHECK (current_version >= 0),
  -- Points at a ready summary only while it describes current_version (compare-and-set in the worker).
  current_summary_version INTEGER NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Immutable snapshot of every accepted version; this is the exact input a summary job reads.
-- PRIMARY KEY (encounter_id, version) enforces "one logical event per version";
-- UNIQUE (event_id) enforces event_id uniqueness for accepted events.
CREATE TABLE IF NOT EXISTS encounter_versions (
  encounter_id TEXT NOT NULL REFERENCES encounters(encounter_id),
  version INTEGER NOT NULL CHECK (version > 0),
  event_id TEXT NOT NULL UNIQUE,
  patient_id TEXT NOT NULL,
  encounter_type TEXT NOT NULL CHECK (encounter_type IN ('Appointment','MedicationRefill','TelephoneTriage')),
  transcription TEXT NOT NULL,
  payload_hash TEXT NOT NULL,
  received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (encounter_id, version)
);

-- Every event_id we have answered, so retries of duplicate/stale events get the same answer.
CREATE TABLE IF NOT EXISTS event_receipts (
  event_id TEXT PRIMARY KEY,
  encounter_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  payload_hash TEXT NOT NULL,
  outcome TEXT NOT NULL CHECK (outcome IN ('accepted_current','duplicate','stale','conflict')),
  received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Quarantine for events that violate a source invariant. Stores hashes and ids only, no transcription.
CREATE TABLE IF NOT EXISTS event_conflicts (
  conflict_id BIGSERIAL PRIMARY KEY,
  event_id TEXT NOT NULL,
  encounter_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  conflict_type TEXT NOT NULL CHECK (conflict_type IN ('event_id_conflict','identity_conflict','version_conflict')),
  payload_hash TEXT NOT NULL,
  received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Durable queue + workflow state. One job per accepted version.
CREATE TABLE IF NOT EXISTS summary_jobs (
  job_id UUID PRIMARY KEY,
  encounter_id TEXT NOT NULL,
  encounter_version INTEGER NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('queued','processing','ready','failed','superseded')),
  attempt_count INTEGER NOT NULL DEFAULT 0,
  -- Attempts that don't count toward the retry budget: those before a redrive, and circuit-breaker
  -- probes that failed because the provider was down. They remain in summary_attempts for cost.
  excluded_attempts INTEGER NOT NULL DEFAULT 0,
  next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  lease_until TIMESTAMPTZ NULL,
  accepted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  sla_deadline_at TIMESTAMPTZ NOT NULL,
  started_at TIMESTAMPTZ NULL,
  completed_at TIMESTAMPTZ NULL,
  last_error_code TEXT NULL,
  last_error_at TIMESTAMPTZ NULL,
  sla_breached_at TIMESTAMPTZ NULL,
  FOREIGN KEY (encounter_id, encounter_version)
    REFERENCES encounter_versions(encounter_id, version),
  UNIQUE (encounter_id, encounter_version)
);
CREATE INDEX IF NOT EXISTS summary_jobs_claim_idx ON summary_jobs(status, next_attempt_at, accepted_at);
CREATE INDEX IF NOT EXISTS summary_jobs_lease_idx ON summary_jobs(status, lease_until);

-- Results are keyed by the exact version they describe and never overwritten.
CREATE TABLE IF NOT EXISTS summary_results (
  encounter_id TEXT NOT NULL,
  encounter_version INTEGER NOT NULL,
  summary TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (encounter_id, encounter_version),
  FOREIGN KEY (encounter_id, encounter_version)
    REFERENCES encounter_versions(encounter_id, version)
);

-- One row per (potentially billable) provider call, including calls lost to crashes or timeouts.
CREATE TABLE IF NOT EXISTS summary_attempts (
  attempt_id UUID PRIMARY KEY,
  job_id UUID NOT NULL REFERENCES summary_jobs(job_id),
  attempt_number INTEGER NOT NULL,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ NULL,
  outcome TEXT NOT NULL CHECK (outcome IN ('started','succeeded','transient_error','permanent_error','timeout','worker_lost')),
  error_code TEXT NULL,
  UNIQUE (job_id, attempt_number)
);

-- Shared circuit breaker for the summary provider (single row). While tripped, workers stop
-- claiming jobs and only one probe call is allowed per cooldown, which caps spend during an outage.
CREATE TABLE IF NOT EXISTS provider_circuit (
  id SMALLINT PRIMARY KEY CHECK (id = 1),
  consecutive_failures INTEGER NOT NULL DEFAULT 0,
  open_until TIMESTAMPTZ NULL,
  cooldown_ms INTEGER NOT NULL DEFAULT 15000,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO provider_circuit(id) VALUES (1) ON CONFLICT (id) DO NOTHING;

-- Upgrade block for databases created from an earlier version of this schema.
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_name = 'summary_jobs' AND column_name = 'attempts_at_redrive') THEN
    ALTER TABLE summary_jobs RENAME COLUMN attempts_at_redrive TO excluded_attempts;
  END IF;
END $$;
ALTER TABLE summary_jobs ADD COLUMN IF NOT EXISTS excluded_attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE summary_jobs ADD COLUMN IF NOT EXISTS sla_deadline_at TIMESTAMPTZ;
UPDATE summary_jobs SET sla_deadline_at = accepted_at + interval '10 seconds' WHERE sla_deadline_at IS NULL;
ALTER TABLE summary_jobs ALTER COLUMN sla_deadline_at SET NOT NULL;
