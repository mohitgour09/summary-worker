import { pool } from '../db/pool.js';
import { config } from '../config.js';
import { log } from '../log.js';

// Shared (DB-backed) circuit breaker, so every worker instance sees the same provider health.
//   closed  : consecutive_failures < threshold, all workers claim normally.
//   open    : tripped and open_until in the future, nobody calls the provider.
//   probe   : tripped and cooldown elapsed; exactly one worker wins the probe slot (atomic UPDATE)
//             and makes a single call. Success closes the circuit; failure re-opens it with a
//             doubled cooldown (capped). A crashed prober frees the slot after one lease.
export type Permit = 'closed' | 'probe' | 'open';

export async function acquirePermit(): Promise<Permit> {
  const probe = await pool.query(
    `UPDATE provider_circuit SET open_until = now() + ($2 * interval '1 millisecond'), updated_at = now()
     WHERE id = 1 AND consecutive_failures >= $1 AND (open_until IS NULL OR open_until <= now())
     RETURNING id`,
    [config.circuitFailureThreshold, config.leaseMs],
  );
  if (probe.rowCount) return 'probe';
  const state = await pool.query('SELECT consecutive_failures FROM provider_circuit WHERE id = 1');
  return Number(state.rows[0]?.consecutive_failures ?? 0) >= config.circuitFailureThreshold ? 'open' : 'closed';
}

// Called when a probe found no work, so the next worker may probe as soon as work appears.
export async function releaseProbe() {
  await pool.query('UPDATE provider_circuit SET open_until = now() WHERE id = 1');
}

export async function recordProviderSuccess() {
  const result = await pool.query(
    `UPDATE provider_circuit SET consecutive_failures = 0, open_until = NULL, cooldown_ms = $1, updated_at = now()
     WHERE id = 1 AND (consecutive_failures > 0 OR open_until IS NOT NULL)
     RETURNING id`,
    [config.circuitCooldownMs],
  );
  if (result.rowCount) log('info', 'provider_circuit_closed');
}

export async function recordProviderFailure(retryAfterMs = 0) {
  // First trip opens for the configured cooldown; each failed probe after that doubles it (capped).
  // A provider Retry-After longer than the cooldown wins.
  const result = await pool.query(
    `UPDATE provider_circuit SET
       consecutive_failures = consecutive_failures + 1,
       open_until = CASE
         WHEN consecutive_failures + 1 = $1 THEN now() + (GREATEST($4::int, $3::int) * interval '1 millisecond')
         WHEN consecutive_failures + 1 > $1 THEN now() + (GREATEST(cooldown_ms, $3::int) * interval '1 millisecond')
         ELSE open_until END,
       cooldown_ms = CASE
         WHEN consecutive_failures + 1 = $1 THEN LEAST($4::int * 2, $2::int)
         WHEN consecutive_failures + 1 > $1 THEN LEAST(cooldown_ms * 2, $2::int)
         ELSE cooldown_ms END,
       updated_at = now()
     WHERE id = 1
     RETURNING consecutive_failures, open_until`,
    [config.circuitFailureThreshold, config.circuitMaxCooldownMs, retryAfterMs, config.circuitCooldownMs],
  );
  const row = result.rows[0];
  if (row && Number(row.consecutive_failures) >= config.circuitFailureThreshold) {
    log('warn', 'provider_circuit_open', { consecutive_failures: Number(row.consecutive_failures), open_until: row.open_until });
  }
}
