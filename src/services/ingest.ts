import { createHash, randomUUID } from 'node:crypto';
import { pool, withTransaction } from '../db/pool.js';
import { config } from '../config.js';
import { log } from '../log.js';
import type { EncounterEvent } from '../types.js';

export type IngestOutcome = 'accepted_current' | 'duplicate' | 'stale';
export type ConflictType = 'event_id_conflict' | 'identity_conflict' | 'version_conflict';

export interface IngestResult {
  outcome: IngestOutcome;
  encounter_id: string;
  received_version: number;
  current_version: number;
  summary_status?: 'queued';
  status_url: string;
}

export class IngestConflictError extends Error {
  readonly status = 409;
  constructor(
    readonly conflict: ConflictType,
    readonly encounterId: string,
    readonly receivedVersion: number,
    readonly currentVersion: number | null,
  ) {
    super(conflict);
  }
}

function fingerprint(event: EncounterEvent): string {
  const canonical = JSON.stringify({
    encounter_id: event.encounter_id,
    patient_id: event.patient_id,
    encounter_type: event.encounter_type,
    version: event.version,
    transcription: event.payload.transcription,
  });
  return createHash('sha256').update(canonical).digest('hex');
}

const statusUrl = (encounterId: string) => `/v1/encounters/${encodeURIComponent(encounterId)}/summary`;

export async function ingestEvent(event: EncounterEvent): Promise<IngestResult> {
  const hash = fingerprint(event);
  try {
    return await ingestInTransaction(event, hash);
  } catch (error) {
    if (error instanceof IngestConflictError) {
      // The ingest transaction rolled back; quarantine the event separately so it can be reviewed.
      await pool.query(
        `INSERT INTO event_conflicts(event_id, encounter_id, version, conflict_type, payload_hash)
         VALUES ($1,$2,$3,$4,$5)`,
        [event.event_id, event.encounter_id, event.version, error.conflict, hash],
      ).catch(() => undefined);
      log('warn', 'ingest_conflict', {
        conflict: error.conflict, event_id: event.event_id, encounter_id: event.encounter_id, version: event.version,
      });
    }
    throw error;
  }
}

async function ingestInTransaction(event: EncounterEvent, hash: string): Promise<IngestResult> {
  return withTransaction(async (client) => {
    // 1. Serialize all events for this encounter on its row lock. Every check below runs after
    //    the lock, so concurrent copies of the same event see each other's committed effects.
    await client.query(
      `INSERT INTO encounters(encounter_id, patient_id, encounter_type, current_version)
       VALUES ($1,$2,$3,0) ON CONFLICT (encounter_id) DO NOTHING`,
      [event.encounter_id, event.patient_id, event.encounter_type],
    );
    const encounterResult = await client.query(
      'SELECT patient_id, encounter_type, current_version FROM encounters WHERE encounter_id=$1 FOR UPDATE',
      [event.encounter_id],
    );
    const encounter = encounterResult.rows[0];
    const currentVersion = Number(encounter.current_version);
    const reply = (outcome: IngestOutcome): IngestResult => ({
      outcome, encounter_id: event.encounter_id, received_version: event.version,
      current_version: currentVersion, status_url: statusUrl(event.encounter_id),
    });

    // 2. Same event_id seen before (accepted, duplicate or stale): same content => duplicate.
    const eventExisting = await client.query(
      `SELECT encounter_id, version, payload_hash FROM event_receipts WHERE event_id = $1
       UNION ALL
       SELECT encounter_id, version, payload_hash FROM encounter_versions WHERE event_id = $1
       LIMIT 1`,
      [event.event_id],
    );
    if (eventExisting.rowCount) {
      const row = eventExisting.rows[0];
      if (row.encounter_id !== event.encounter_id || Number(row.version) !== event.version || row.payload_hash !== hash) {
        throw new IngestConflictError('event_id_conflict', event.encounter_id, event.version, currentVersion || null);
      }
      return reply('duplicate');
    }

    // 3. patient_id / encounter_type are fixed per encounter.
    if (encounter.patient_id !== event.patient_id || encounter.encounter_type !== event.encounter_type) {
      throw new IngestConflictError('identity_conflict', event.encounter_id, event.version, currentVersion);
    }

    // 4. (encounter_id, version) already stored under another event_id: same logical event.
    const versionExisting = await client.query(
      'SELECT payload_hash FROM encounter_versions WHERE encounter_id=$1 AND version=$2',
      [event.encounter_id, event.version],
    );
    if (versionExisting.rowCount) {
      if (versionExisting.rows[0].payload_hash !== hash) {
        throw new IngestConflictError('version_conflict', event.encounter_id, event.version, currentVersion);
      }
      await insertReceipt(client, event, hash, 'duplicate');
      return reply('duplicate');
    }

    // 5. Older than what we hold (including skipped versions that arrive late): acknowledge, don't apply.
    if (event.version <= currentVersion) {
      await insertReceipt(client, event, hash, 'stale');
      return reply('stale');
    }

    // 6. Newer: store immutable snapshot + job, retire queued older work, advance the pointer.
    //    All in this one transaction, so an acknowledged update always has a durable job.
    await client.query(
      `INSERT INTO encounter_versions(encounter_id, version, event_id, patient_id, encounter_type, transcription, payload_hash)
       VALUES ($1,$2,$3,$4,$5,$6,$7)`,
      [event.encounter_id, event.version, event.event_id, event.patient_id, event.encounter_type, event.payload.transcription, hash],
    );
    await client.query(
      `INSERT INTO summary_jobs(job_id, encounter_id, encounter_version, status, accepted_at, next_attempt_at, sla_deadline_at)
       VALUES ($1,$2,$3,'queued',now(),now(), now() + ($4 * interval '1 second'))`,
      [randomUUID(), event.encounter_id, event.version, config.slaSeconds],
    );
    await client.query(
      `UPDATE summary_jobs SET status='superseded', completed_at=now()
       WHERE encounter_id=$1 AND encounter_version < $2 AND status='queued'`,
      [event.encounter_id, event.version],
    );
    await client.query(
      `UPDATE encounters SET current_version=$2, current_summary_version=NULL, updated_at=now() WHERE encounter_id=$1`,
      [event.encounter_id, event.version],
    );
    await insertReceipt(client, event, hash, 'accepted_current');
    return { ...reply('accepted_current'), current_version: event.version, summary_status: 'queued' };
  });
}

async function insertReceipt(
  client: { query: (sql: string, params: unknown[]) => Promise<unknown> },
  event: EncounterEvent, hash: string, outcome: IngestOutcome,
) {
  await client.query(
    `INSERT INTO event_receipts(event_id, encounter_id, version, payload_hash, outcome)
     VALUES ($1,$2,$3,$4,$5) ON CONFLICT (event_id) DO NOTHING`,
    [event.event_id, event.encounter_id, event.version, hash, outcome],
  );
}
