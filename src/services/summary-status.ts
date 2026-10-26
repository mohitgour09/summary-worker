import { pool, withTransaction } from '../db/pool.js';

export async function getSummaryStatus(encounterId: string, requestedVersion?: number) {
  const encounterResult = await pool.query(
    'SELECT encounter_id, patient_id, encounter_type, current_version, current_summary_version FROM encounters WHERE encounter_id=$1',
    [encounterId],
  );
  if (!encounterResult.rowCount) return null;
  const encounter = encounterResult.rows[0];
  const currentVersion = Number(encounter.current_version);
  const version = requestedVersion ?? currentVersion;
  // sla_breached is derived from timestamps here, so it is correct even if no worker is alive
  // to run the sweep. It is informational and never changes status.
  const result = await pool.query(
    `SELECT j.status, j.attempt_count, j.accepted_at, j.started_at, j.completed_at, j.sla_deadline_at,
            j.next_attempt_at, j.last_error_code, j.last_error_at, r.summary, r.created_at AS summary_created_at,
            CASE WHEN j.status = 'superseded' THEN NULL
                 ELSE (j.sla_breached_at IS NOT NULL OR COALESCE(j.completed_at, now()) > j.sla_deadline_at) END AS sla_breached
     FROM summary_jobs j LEFT JOIN summary_results r
       ON r.encounter_id = j.encounter_id AND r.encounter_version = j.encounter_version
     WHERE j.encounter_id = $1 AND j.encounter_version = $2`,
    [encounterId, version],
  );
  if (!result.rowCount) {
    return { encounter_id: encounterId, current_version: currentVersion, requested_version: version, status: 'not_found' as const };
  }
  const job = result.rows[0];
  const ready = job.status === 'ready';
  return {
    encounter_id: encounterId,
    patient_id: encounter.patient_id,
    encounter_type: encounter.encounter_type,
    current_version: currentVersion,
    summary_version: version,
    // A summary is "current" only if it describes the encounter's current version.
    is_current: version === currentVersion,
    status: job.status as 'queued' | 'processing' | 'ready' | 'failed' | 'superseded',
    summary: ready ? job.summary : null,
    summary_created_at: ready ? job.summary_created_at : null,
    attempt_count: Number(job.attempt_count),
    accepted_at: job.accepted_at,
    started_at: job.started_at,
    completed_at: job.completed_at,
    next_attempt_at: job.status === 'queued' ? job.next_attempt_at : null,
    last_error_code: job.last_error_code,
    last_error_at: job.last_error_at,
    sla_deadline_at: job.sla_deadline_at,
    sla_breached: job.sla_breached,
  };
}

export type RedriveResult =
  | { kind: 'not_found' }
  | { kind: 'not_redrivable'; reason: 'not_failed' | 'not_current'; status: string; current_version: number }
  | { kind: 'requeued'; encounter_id: string; version: number; status: 'queued' };

// Operator action after an outage or config fix: give a failed job for the CURRENT version a
// fresh attempt budget. Previous attempts stay on record; obsolete versions are never redriven.
export async function redriveSummary(encounterId: string, requestedVersion?: number): Promise<RedriveResult> {
  return withTransaction(async (client) => {
    const selected = await client.query(
      `SELECT j.job_id, j.status, j.encounter_version, e.current_version
       FROM summary_jobs j JOIN encounters e ON e.encounter_id = j.encounter_id
       WHERE j.encounter_id = $1 AND j.encounter_version = COALESCE($2, e.current_version)
       FOR UPDATE OF j`,
      [encounterId, requestedVersion ?? null],
    );
    if (!selected.rowCount) return { kind: 'not_found' };
    const job = selected.rows[0];
    const currentVersion = Number(job.current_version);
    if (Number(job.encounter_version) < currentVersion) {
      return { kind: 'not_redrivable', reason: 'not_current', status: job.status, current_version: currentVersion };
    }
    if (job.status !== 'failed') {
      return { kind: 'not_redrivable', reason: 'not_failed', status: job.status, current_version: currentVersion };
    }
    await client.query(
      `UPDATE summary_jobs SET status = 'queued', excluded_attempts = attempt_count, next_attempt_at = now(),
         completed_at = NULL, lease_until = NULL
       WHERE job_id = $1`,
      [job.job_id],
    );
    return { kind: 'requeued', encounter_id: encounterId, version: Number(job.encounter_version), status: 'queued' };
  });
}
