import type { AddressInfo } from 'node:net';
import type { Server } from 'node:http';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { app } from '../src/app.js';
import { pool } from '../src/db/pool.js';
import { mockStats } from '../src/services/summary-provider.js';
import { processOneJob, recoverExpiredLeases, sweepSla } from '../src/workers/summary-worker.js';

let server: Server;
let base: string;
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

beforeAll(async () => {
  server = app.listen(0);
  base = `http://127.0.0.1:${(server.address() as AddressInfo).port}/v1`;
});
afterAll(async () => {
  server.close();
  await pool.end();
});
beforeEach(async () => {
  await pool.query(`TRUNCATE summary_attempts, summary_results, summary_jobs, encounter_versions,
                    event_receipts, event_conflicts, encounters, provider_circuit`);
  await pool.query('INSERT INTO provider_circuit(id) VALUES (1)');
  delete process.env.MOCK_PROVIDER_ERROR;
  mockStats.calls = 0;
});

function event(version: number, over: Record<string, unknown> = {}, transcription?: string) {
  return {
    event_id: `evt-${version}`, encounter_id: 'enc-42', patient_id: 'pat-77', encounter_type: 'TelephoneTriage',
    version, payload: { transcription: transcription ?? `Nurse: version ${version}. Patient: cough.` }, ...over,
  };
}
async function post(body: unknown) {
  const r = await fetch(`${base}/encounter-events`, {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body),
  });
  return { status: r.status, body: await r.json() };
}
async function summary(encounterId = 'enc-42', version?: number) {
  const r = await fetch(`${base}/encounters/${encounterId}/summary${version ? `?version=${version}` : ''}`);
  return { status: r.status, body: await r.json() };
}
async function job(version: number, encounterId = 'enc-42') {
  const r = await pool.query('SELECT * FROM summary_jobs WHERE encounter_id=$1 AND encounter_version=$2', [encounterId, version]);
  return r.rows[0];
}
async function waitForStatus(version: number, status: string) {
  for (let i = 0; i < 100; i++) {
    if ((await job(version))?.status === status) return;
    await sleep(10);
  }
  throw new Error(`job v${version} never reached ${status}`);
}
async function drain() {
  for (let i = 0; i < 50; i++) {
    const result = await processOneJob();
    if (result === 'idle') return;
  }
}

describe('ingestion contract', () => {
  it('distinguishes accepted/current (202), duplicate (200), stale (200) and conflicts (409)', async () => {
    let r = await post(event(12));
    expect(r).toMatchObject({ status: 202, body: { outcome: 'accepted_current', current_version: 12, summary_status: 'queued' } });

    r = await post(event(12));
    expect(r).toMatchObject({ status: 200, body: { outcome: 'duplicate', current_version: 12 } });

    r = await post(event(12, { event_id: 'evt-12-resent-by-source-defect' }));
    expect(r).toMatchObject({ status: 200, body: { outcome: 'duplicate', current_version: 12 } });

    r = await post(event(10));
    expect(r).toMatchObject({ status: 200, body: { outcome: 'stale', received_version: 10, current_version: 12 } });

    r = await post(event(13, { patient_id: 'pat-OTHER' }));
    expect(r).toMatchObject({ status: 409, body: { outcome: 'conflict', conflict: 'identity_conflict', current_version: 12 } });

    r = await post(event(12, { event_id: 'evt-12-different-payload' }, 'different content'));
    expect(r).toMatchObject({ status: 409, body: { conflict: 'version_conflict' } });

    r = await post({ ...event(14), version: 0 });
    expect(r.status).toBe(400);

    const enc = await pool.query('SELECT current_version, patient_id FROM encounters');
    expect(enc.rows).toEqual([{ current_version: 12, patient_id: 'pat-77' }]);
    const conflicts = await pool.query('SELECT conflict_type FROM event_conflicts ORDER BY conflict_id');
    expect(conflicts.rows.map((c) => c.conflict_type)).toEqual(['identity_conflict', 'version_conflict']);
    const jobs = await pool.query('SELECT count(*)::int AS n FROM summary_jobs');
    expect(jobs.rows[0].n).toBe(1);
  });
});

describe('concurrency', () => {
  it('concurrent copies of one event produce exactly one version and one job; stale copies never 500', async () => {
    const results = await Promise.all(Array.from({ length: 20 }, () => post(event(1))));
    expect(results.filter((r) => r.body.outcome === 'accepted_current')).toHaveLength(1);
    expect(results.filter((r) => r.body.outcome === 'duplicate')).toHaveLength(19);

    await post(event(5));
    const stale = await Promise.all(Array.from({ length: 20 }, () => post(event(3))));
    expect(stale.every((r) => r.status === 200)).toBe(true);
    expect(stale.filter((r) => r.body.outcome === 'stale')).toHaveLength(1);
    expect(stale.filter((r) => r.body.outcome === 'duplicate')).toHaveLength(19);

    const counts = await pool.query(
      `SELECT (SELECT count(*)::int FROM encounter_versions) AS versions, (SELECT count(*)::int FROM summary_jobs) AS jobs`,
    );
    expect(counts.rows[0]).toEqual({ versions: 2, jobs: 2 });
  });

  it('concurrent different versions converge on the highest; only its job stays queued', async () => {
    const versions = [7, 3, 10, 1, 9, 4, 8, 2, 6, 5];
    await Promise.all(versions.map((v) => post(event(v))));
    expect((await summary()).body.current_version).toBe(10);
    const queued = await pool.query(`SELECT encounter_version FROM summary_jobs WHERE status='queued'`);
    expect(queued.rows).toEqual([{ encounter_version: 10 }]);
  });
});

describe('worker', () => {
  it('generates a summary for the exact version with one paid call', async () => {
    await post(event(12));
    expect((await summary()).body).toMatchObject({ status: 'queued', summary: null });
    expect(await processOneJob()).toBe('ready');
    expect(await processOneJob()).toBe('idle');
    const r = await summary();
    expect(r.body).toMatchObject({ status: 'ready', summary_version: 12, is_current: true, attempt_count: 1, sla_breached: false });
    expect(r.body.summary).toContain('version 12');
    expect(mockStats.calls).toBe(1);
  });

  it('results out of order: v13 finishing before v12 never lets v12 become current', async () => {
    await post(event(12, {}, 'Nurse: version 12. [[mock-delay-ms:500]]'));
    const slowV12 = processOneJob();
    await waitForStatus(12, 'processing');

    expect((await post(event(13))).status).toBe(202);
    expect(await processOneJob()).toBe('ready'); // v13 done first
    expect(await slowV12).toBe('ready'); // v12 finishes later, kept as history

    const current = await summary();
    expect(current.body).toMatchObject({ current_version: 13, summary_version: 13, status: 'ready', is_current: true });
    expect(current.body.summary).toContain('version 13');
    const old = await summary('enc-42', 12);
    expect(old.body).toMatchObject({ summary_version: 12, status: 'ready', is_current: false });
    const enc = await pool.query('SELECT current_summary_version FROM encounters');
    expect(enc.rows[0].current_summary_version).toBe(13);
  });

  it('a newer version arriving while an older one fails retires the old job instead of retrying it', async () => {
    process.env.MOCK_PROVIDER_ERROR = '503';
    await post(event(1, {}, 'Nurse: version 1. [[mock-delay-ms:300]]'));
    const v1 = processOneJob();
    await waitForStatus(1, 'processing');
    await post(event(2));
    expect(await v1).toBe('superseded');
    expect((await job(1)).status).toBe('superseded');
    expect(mockStats.calls).toBe(1);
  });

  it('crash recovery: an expired lease is requeued, the lost call is recorded, and the job completes', async () => {
    await post(event(1));
    const j = await job(1);
    // Simulate a worker that claimed the job, started a paid call, and died.
    await pool.query(
      `UPDATE summary_jobs SET status='processing', attempt_count=1, lease_until=now() - interval '1 second' WHERE job_id=$1`,
      [j.job_id],
    );
    await pool.query(
      `INSERT INTO summary_attempts(attempt_id, job_id, attempt_number, outcome) VALUES (gen_random_uuid(), $1, 1, 'started')`,
      [j.job_id],
    );
    await recoverExpiredLeases();
    expect((await job(1))).toMatchObject({ status: 'queued', last_error_code: 'WORKER_LEASE_EXPIRED' });

    expect(await processOneJob()).toBe('ready');
    expect((await summary()).body).toMatchObject({ status: 'ready', attempt_count: 2 });
    const attempts = await pool.query('SELECT attempt_number, outcome FROM summary_attempts ORDER BY attempt_number');
    expect(attempts.rows).toEqual([
      { attempt_number: 1, outcome: 'worker_lost' },
      { attempt_number: 2, outcome: 'succeeded' },
    ]);
  });

  it('fencing: a worker whose lease was reclaimed cannot overwrite the new owner\'s state', async () => {
    process.env.MOCK_PROVIDER_ERROR = '503';
    await post(event(1, {}, 'Nurse: version 1. [[mock-delay-ms:400]]'));
    const workerA = processOneJob();
    await waitForStatus(1, 'processing');
    await pool.query(`UPDATE summary_jobs SET lease_until = now() - interval '1 second'`);
    await recoverExpiredLeases();
    const workerB = processOneJob(); // reclaims as attempt 2
    await waitForStatus(1, 'processing');

    expect(await workerA).toBe('lost');
    expect(await job(1)).toMatchObject({ status: 'processing', attempt_count: 2 });
    await workerB;
  });
});

describe('outage, retries and redrive', () => {
  it('circuit breaker stops spending during an outage, then recovers without failing jobs', async () => {
    process.env.MOCK_PROVIDER_ERROR = '503';
    for (let i = 1; i <= 5; i++) await post(event(1, { encounter_id: `enc-${i}`, event_id: `evt-${i}` }));

    // Three consecutive provider failures trip the breaker.
    for (let i = 0; i < 3; i++) expect(await processOneJob()).toBe('retry_scheduled');
    expect(await processOneJob()).toBe('circuit_open');
    expect(mockStats.calls).toBe(3);

    // After the cooldown exactly one probe call is allowed; it fails and re-opens the circuit.
    await sleep(350);
    expect(await processOneJob()).toBe('retry_scheduled');
    expect(await processOneJob()).toBe('circuit_open');
    expect(mockStats.calls).toBe(4);
    // The probe is recorded as an attempt but not charged to the job's retry budget.
    const budget = await pool.query(
      'SELECT sum(attempt_count)::int AS attempts, sum(excluded_attempts)::int AS excluded FROM summary_jobs',
    );
    expect(budget.rows[0]).toEqual({ attempts: 4, excluded: 1 });

    // Provider recovers; the next probe closes the circuit and the backlog drains.
    delete process.env.MOCK_PROVIDER_ERROR;
    await sleep(650);
    await drain();
    const statuses = await pool.query('SELECT status, count(*)::int AS n FROM summary_jobs GROUP BY status');
    expect(statuses.rows).toEqual([{ status: 'ready', n: 5 }]);
    expect(mockStats.calls).toBe(9);
  });

  it('permanent errors fail immediately; retry exhaustion fails; failed work can be redriven', async () => {
    process.env.MOCK_PROVIDER_ERROR = '400';
    await post(event(1));
    expect(await processOneJob()).toBe('failed');
    expect(mockStats.calls).toBe(1);
    expect((await summary()).body).toMatchObject({ status: 'failed', last_error_code: 'PROVIDER_REJECTED_REQUEST', attempt_count: 1 });

    process.env.MOCK_PROVIDER_ERROR = '503';
    await post(event(2));
    for (let i = 0; i < 3; i++) {
      await pool.query(`UPDATE summary_jobs SET next_attempt_at = now()`);
      await pool.query(`UPDATE provider_circuit SET consecutive_failures = 0, open_until = NULL`);
      await processOneJob();
    }
    expect((await summary()).body).toMatchObject({ status: 'failed', attempt_count: 3, last_error_code: 'PROVIDER_UNAVAILABLE' });

    // Obsolete versions are never redriven; the current one is.
    let r = await fetch(`${base}/encounters/enc-42/summary/redrive?version=1`, { method: 'POST' });
    expect(r.status).toBe(409);
    r = await fetch(`${base}/encounters/enc-42/summary/redrive`, { method: 'POST' });
    expect(r.status).toBe(202);
    r = await fetch(`${base}/encounters/enc-42/summary/redrive`, { method: 'POST' });
    expect(r.status).toBe(409); // already queued

    delete process.env.MOCK_PROVIDER_ERROR;
    await pool.query(`UPDATE provider_circuit SET consecutive_failures = 0, open_until = NULL`);
    expect(await processOneJob()).toBe('ready');
    expect((await summary()).body).toMatchObject({ status: 'ready', summary_version: 2, attempt_count: 4 });
  });
});

describe('SLA', () => {
  it('breach is reported even with no worker running and does not change the workflow status', async () => {
    await post(event(1));
    await sleep(1_200); // SLA_SECONDS=1 in tests
    expect((await summary()).body).toMatchObject({ status: 'queued', sla_breached: true });

    await sweepSla();
    expect((await job(1)).sla_breached_at).not.toBeNull();

    expect(await processOneJob()).toBe('ready');
    expect((await summary()).body).toMatchObject({ status: 'ready', sla_breached: true });
  });
});
