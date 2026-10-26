import { randomUUID } from 'node:crypto';
import { pool, withTransaction } from '../db/pool.js';
import { config } from '../config.js';
import { log } from '../log.js';
import { classifyProviderError, generateSummary } from '../services/summary-provider.js';
import { acquirePermit, recordProviderFailure, recordProviderSuccess, releaseProbe } from './provider-circuit.js';

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export type ProcessResult = 'idle' | 'circuit_open' | 'superseded' | 'ready' | 'retry_scheduled' | 'failed' | 'lost';

interface ClaimedJob {
  job_id: string;
  encounter_id: string;
  encounter_version: number;
  attempt_number: number;
  excluded_attempts: number;
  attempt_id: string;
  transcription: string;
}

// Exponential backoff with "equal jitter": never retries instantly, never synchronises workers.
export function retryDelayMs(attemptsSinceRedrive: number): number {
  const exp = Math.min(config.retryMaxMs, config.retryBaseMs * 2 ** Math.max(0, attemptsSinceRedrive - 1));
  return Math.floor(exp / 2 + Math.random() * (exp / 2));
}

// Jobs whose worker died (lease expired) go back to the queue, or to superseded/failed.
// One statement, so the job transition and the lost attempt record cannot drift apart.
export async function recoverExpiredLeases() {
  const result = await pool.query(
    `WITH expired AS (
       UPDATE summary_jobs j SET
         status = CASE WHEN j.encounter_version < e.current_version THEN 'superseded'
                       WHEN j.attempt_count - j.excluded_attempts >= $1 THEN 'failed'
                       ELSE 'queued' END,
         completed_at = CASE WHEN j.encounter_version < e.current_version
                               OR j.attempt_count - j.excluded_attempts >= $1 THEN now() END,
         next_attempt_at = now(), lease_until = NULL,
         last_error_code = 'WORKER_LEASE_EXPIRED', last_error_at = now()
       FROM encounters e
       WHERE e.encounter_id = j.encounter_id AND j.status = 'processing' AND j.lease_until < now()
       RETURNING j.job_id, j.attempt_count, j.status
     ), lost AS (
       UPDATE summary_attempts a SET outcome = 'worker_lost', finished_at = now(), error_code = 'WORKER_LEASE_EXPIRED'
       FROM expired x
       WHERE a.job_id = x.job_id AND a.attempt_number = x.attempt_count AND a.outcome = 'started'
     )
     SELECT job_id, status FROM expired`,
    [config.maxSummaryAttempts],
  );
  for (const row of result.rows) log('warn', 'summary_lease_expired', { job_id: row.job_id, new_status: row.status });
}

// SLA breach is operational metadata only; it never changes workflow status.
export async function sweepSla() {
  const result = await pool.query(
    `UPDATE summary_jobs SET sla_breached_at = sla_deadline_at
     WHERE sla_breached_at IS NULL AND status IN ('queued','processing') AND sla_deadline_at < now()
     RETURNING job_id, encounter_id, encounter_version, status, attempt_count, last_error_code`,
  );
  for (const row of result.rows) {
    log('warn', 'summary_sla_breached', {
      job_id: row.job_id, encounter_id: row.encounter_id, version: row.encounter_version,
      status: row.status, attempt_count: row.attempt_count, last_error_code: row.last_error_code,
    });
  }
}

async function claimJob(isProbe: boolean): Promise<ClaimedJob | 'superseded' | null> {
  return withTransaction(async (client) => {
    // A probe prefers the job with the fewest attempts so an outage doesn't drain one job's budget.
    const order = isProbe ? 'j.attempt_count - j.excluded_attempts, j.accepted_at' : 'j.accepted_at';
    const selected = await client.query(
      `SELECT j.job_id, j.encounter_id, j.encounter_version, j.attempt_count, j.excluded_attempts, e.current_version
       FROM summary_jobs j JOIN encounters e ON e.encounter_id = j.encounter_id
       WHERE j.status = 'queued' AND j.next_attempt_at <= now()
       ORDER BY ${order}
       FOR UPDATE OF j SKIP LOCKED LIMIT 1`,
    );
    if (!selected.rowCount) return null;
    const job = selected.rows[0];

    // Obsolete work is retired before any paid call is made.
    if (Number(job.encounter_version) < Number(job.current_version)) {
      await client.query(
        `UPDATE summary_jobs SET status = 'superseded', completed_at = now() WHERE job_id = $1`, [job.job_id],
      );
      log('info', 'summary_superseded', { job_id: job.job_id, encounter_id: job.encounter_id, version: job.encounter_version });
      return 'superseded';
    }

    const attemptNumber = Number(job.attempt_count) + 1;
    await client.query(
      `UPDATE summary_jobs SET status = 'processing', attempt_count = $2, started_at = COALESCE(started_at, now()),
         lease_until = now() + ($3 * interval '1 millisecond')
       WHERE job_id = $1`,
      [job.job_id, attemptNumber, config.leaseMs],
    );
    const attemptId = randomUUID();
    await client.query(
      `INSERT INTO summary_attempts(attempt_id, job_id, attempt_number, outcome) VALUES ($1,$2,$3,'started')`,
      [attemptId, job.job_id, attemptNumber],
    );
    // The input is the immutable snapshot for exactly this (encounter_id, version).
    const source = await client.query(
      'SELECT transcription FROM encounter_versions WHERE encounter_id = $1 AND version = $2',
      [job.encounter_id, job.encounter_version],
    );
    return {
      job_id: job.job_id, encounter_id: job.encounter_id, encounter_version: Number(job.encounter_version),
      attempt_number: attemptNumber, excluded_attempts: Number(job.excluded_attempts),
      attempt_id: attemptId, transcription: source.rows[0].transcription as string,
    };
  });
}

async function callProvider(transcription: string): Promise<string> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), config.summaryTimeoutMs);
  try {
    return await Promise.race([
      generateSummary(transcription, controller.signal),
      new Promise<never>((_, reject) => controller.signal.addEventListener(
        'abort', () => reject(new Error('SUMMARY_TIMEOUT')), { once: true },
      )),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

async function completeJob(job: ClaimedJob, summary: string, providerMs: number): Promise<ProcessResult> {
  const row = await withTransaction(async (client) => {
    // A result for this exact version is valid whichever attempt produced it; first one wins.
    await client.query(
      `INSERT INTO summary_results(encounter_id, encounter_version, summary) VALUES ($1,$2,$3)
       ON CONFLICT (encounter_id, encounter_version) DO NOTHING`,
      [job.encounter_id, job.encounter_version, summary],
    );
    // Accept even if our lease was lost and the job was requeued/failed meanwhile: the call is
    // already paid for and its input was immutable. A superseded job keeps that status.
    const updated = await client.query(
      `UPDATE summary_jobs SET status = 'ready', completed_at = now(), lease_until = NULL, last_error_code = NULL,
         sla_breached_at = COALESCE(sla_breached_at, CASE WHEN now() > sla_deadline_at THEN sla_deadline_at END)
       WHERE job_id = $1 AND status IN ('queued','processing','failed')
       RETURNING EXTRACT(EPOCH FROM (completed_at - accepted_at)) * 1000 AS e2e_ms, sla_breached_at`,
      [job.job_id],
    );
    // Stale-result protection: only the current version may become the current summary.
    await client.query(
      `UPDATE encounters SET current_summary_version = $2, updated_at = now()
       WHERE encounter_id = $1 AND current_version = $2`,
      [job.encounter_id, job.encounter_version],
    );
    await client.query(
      `UPDATE summary_attempts SET outcome = 'succeeded', finished_at = now() WHERE attempt_id = $1`, [job.attempt_id],
    );
    return updated.rows[0];
  });
  await recordProviderSuccess();
  log('info', 'summary_ready', {
    job_id: job.job_id, encounter_id: job.encounter_id, version: job.encounter_version, attempt: job.attempt_number,
    provider_ms: providerMs, e2e_ms: row ? Math.round(Number(row.e2e_ms)) : undefined, sla_breached: Boolean(row?.sla_breached_at),
  });
  return 'ready';
}

async function failAttempt(job: ClaimedJob, error: unknown, providerMs: number, isProbe: boolean): Promise<ProcessResult> {
  const c = classifyProviderError(error);
  // A failed breaker probe measures provider health, not this job: it is recorded (and paid for)
  // but not charged to the job's retry budget, so a long outage doesn't exhaust the probed job.
  const excludeThisAttempt = isProbe && c.providerUnavailable ? 1 : 0;
  const chargedAttempts = job.attempt_number - job.excluded_attempts - excludeThisAttempt;
  const outcome = await withTransaction(async (client) => {
    const enc = await client.query('SELECT current_version FROM encounters WHERE encounter_id = $1', [job.encounter_id]);
    const superseded = job.encounter_version < Number(enc.rows[0].current_version);
    const exhausted = chargedAttempts >= config.maxSummaryAttempts;
    const status = superseded ? 'superseded' : c.permanent || exhausted ? 'failed' : 'queued';
    const nextDelayMs = Math.max(c.retryAfterMs ?? 0, retryDelayMs(Math.max(1, chargedAttempts)));
    // Fenced on attempt_count: a worker whose lease was reclaimed cannot clobber the new owner.
    const updated = await client.query(
      `UPDATE summary_jobs SET status = $2, next_attempt_at = now() + ($3 * interval '1 millisecond'),
         excluded_attempts = excluded_attempts + $6,
         lease_until = NULL, last_error_code = $4, last_error_at = now(),
         completed_at = CASE WHEN $2 IN ('failed','superseded') THEN now() END,
         sla_breached_at = COALESCE(sla_breached_at,
           CASE WHEN $2 = 'failed' AND now() > sla_deadline_at THEN sla_deadline_at END)
       WHERE job_id = $1 AND status = 'processing' AND attempt_count = $5`,
      [job.job_id, status, nextDelayMs, c.code, job.attempt_number, excludeThisAttempt],
    );
    await client.query(
      `UPDATE summary_attempts SET outcome = $2, finished_at = now(), error_code = $3 WHERE attempt_id = $1`,
      [job.attempt_id, c.timeout ? 'timeout' : c.permanent ? 'permanent_error' : 'transient_error', c.code],
    );
    return { status: updated.rowCount ? status : 'lost', nextDelayMs };
  });

  if (c.providerUnavailable) await recordProviderFailure(c.retryAfterMs);
  else await recordProviderSuccess(); // provider answered; the problem is this request, not provider health

  log(outcome.status === 'failed' ? 'error' : 'warn', 'summary_attempt_failed', {
    job_id: job.job_id, encounter_id: job.encounter_id, version: job.encounter_version,
    attempt: job.attempt_number, charged_attempts: chargedAttempts, max_attempts: config.maxSummaryAttempts,
    probe: isProbe, code: c.code, http_status: c.httpStatus,
    provider_request_id: c.requestId, provider_ms: providerMs, new_status: outcome.status,
    next_attempt_in_ms: outcome.status === 'queued' ? outcome.nextDelayMs : undefined,
  });
  switch (outcome.status) {
    case 'queued': return 'retry_scheduled';
    case 'failed': return 'failed';
    case 'superseded': return 'superseded';
    default: return 'lost';
  }
}

export async function processOneJob(): Promise<ProcessResult> {
  const permit = await acquirePermit();
  if (permit === 'open') return 'circuit_open';
  const claimed = await claimJob(permit === 'probe');
  if (!claimed) {
    if (permit === 'probe') await releaseProbe();
    return 'idle';
  }
  if (claimed === 'superseded') {
    if (permit === 'probe') await releaseProbe();
    return 'superseded';
  }
  const started = Date.now();
  try {
    const summary = await callProvider(claimed.transcription);
    return await completeJob(claimed, summary, Date.now() - started);
  } catch (error) {
    return failAttempt(claimed, error, Date.now() - started, permit === 'probe');
  }
}

let stopping = false;

export async function runWorker() {
  log('info', 'summary_worker_started');
  const stop = () => { stopping = true; };
  process.on('SIGINT', stop);
  process.on('SIGTERM', stop);
  let lastMaintenance = 0;
  while (!stopping) {
    try {
      if (Date.now() - lastMaintenance >= config.maintenanceIntervalMs) {
        lastMaintenance = Date.now();
        await recoverExpiredLeases();
        await sweepSla();
      }
      const result = await processOneJob();
      if (result === 'idle' || result === 'circuit_open') await delay(config.workerPollMs);
    } catch (error) {
      log('error', 'summary_worker_loop_error', { error: error instanceof Error ? error.name : 'unknown' });
      await delay(config.workerPollMs);
    }
  }
  log('info', 'summary_worker_stopped');
  await pool.end();
}
