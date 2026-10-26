# Reliable Encounter Updates & AI Summaries

Candidate implementation by Mohit Gour. Node.js, TypeScript, Express, PostgreSQL.

**The design document (API contract, data model, pseudocode, failure scenarios, verification) is [`DESIGN.md`](DESIGN.md).**

## Prerequisites
- Node.js 20+
- Docker Desktop / Docker Engine with Compose
- npm

## 1. Start PostgreSQL

```bash
docker compose up -d postgres
```

The schema is initialized from `sql/schema.sql` on the first database volume creation.
The script is idempotent and includes an upgrade block, so to update an existing database apply it again:

```bash
docker compose exec -T postgres psql -U postgres -d encounters < sql/schema.sql
```

## 2. Configure and install

```bash
cp .env.example .env
npm install
```

On Windows PowerShell, use `Copy-Item .env.example .env` instead of `cp`.

The defaults use the free local mock provider. To use OpenAI, set `SUMMARY_PROVIDER=openai` and `OPENAI_API_KEY`.

## Run the automated tests

With PostgreSQL from step 1 running:

```bash
npm test
```

The suite creates and uses a separate `encounters_test` database (your dev data is untouched) and covers the
ingestion contract, concurrency, out-of-order results, crash recovery and fencing, provider outage with the
circuit breaker, retry exhaustion and redrive, and SLA reporting. Set `TEST_PG_HOST` (`user:pass@host:port`) if
Postgres is not on `localhost:5432`.

## 3. Run API and worker in separate terminals

Terminal A:

```bash
npm run dev:api
```

Terminal B:

```bash
npm run dev:worker
```

Health check: `GET http://localhost:3000/health`

## 4. Submit a test event

```bash
curl -i -X POST http://localhost:3000/v1/encounter-events \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"evt-102","encounter_id":"enc-42","patient_id":"pat-77","encounter_type":"TelephoneTriage","version":12,"payload":{"transcription":"Nurse: Thanks for calling. Patient: I have had a cough for two days."}}'
```

Expected first response: HTTP 202 and `outcome: accepted_current`. The local mock summary provider is deterministic and free; it is only for plumbing verification, not clinical use.

Poll the status:

```bash
curl http://localhost:3000/v1/encounters/enc-42/summary
```

Repeat the POST unchanged to verify duplicate handling. Change only `event_id` and submit version 11 to verify stale handling. Change `patient_id` for `enc-42` to verify HTTP 409 identity conflict.

## Design notes
Full rationale is in [`DESIGN.md`](DESIGN.md). In short:
- Ingestion locks the encounter row, then commits the immutable version snapshot and its job together; the job table is the durable queue.
- `FOR UPDATE SKIP LOCKED` plus leases let multiple workers claim different jobs safely; expired leases are recovered, and a reclaimed worker is fenced by `attempt_count`.
- Jobs reference `(encounter_id, encounter_version)`; the worker reads exactly that immutable snapshot.
- Results are keyed by encounter/version. A compare-and-set ensures only a result for the current version can become current.
- Obsolete jobs are superseded before any paid call (at ingest, claim, and failure time).
- Provider errors are classified: permanent errors fail immediately, provider outages retry with capped exponential backoff and honour `Retry-After`, and a shared circuit breaker pauses all workers during an outage. Every call is recorded in `summary_attempts`.
- SLA breach is derived from timestamps and never changes `status`.
- Logs are structured JSON with ids, codes and timings only (no transcript, summary or patient_id).

## Important production hardening before real clinical use
1. Integrate an approved AI provider and classify provider errors explicitly; do not use the mock for real data.
2. Add authentication/authorization, audit trails, secrets management, TLS, encryption/retention policy, and request rate limits.
3. Add structured metrics/tracing and alerting; avoid logging patient content or identifiers.
4. Run the integration suite in CI against a disposable PostgreSQL container, and add a contract test of the provider error classification against the real provider.
5. Review HIPAA and other applicable legal/security requirements with the organization. This exercise is not a certified clinical system.
6. For ambiguous provider timeouts, an external paid call may have succeeded even though the client did not receive a response. Local transactions cannot guarantee exactly-once external execution.

## API summary
- `POST /v1/encounter-events`: 202 accepted/current; 200 duplicate or stale; 409 identity/event/version conflict (quarantined); 400 validation error.
- `GET /v1/encounters/:encounterId/summary`: current version by default; optional `?version=N` for a specific version. Returns `queued`, `processing`, `ready`, `failed` or `superseded`, plus `is_current` and SLA metadata.
- `POST /v1/encounters/:encounterId/summary/redrive`: operator action; re-queues a failed job for the current version with a fresh retry budget (202), 409 if not failed or not current.
