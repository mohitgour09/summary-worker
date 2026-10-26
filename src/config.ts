import 'dotenv/config';

function numberFromEnv(name: string, fallback: number): number {
  const value = Number(process.env[name] ?? fallback);
  if (!Number.isFinite(value) || value <= 0) throw new Error(`${name} must be a positive number`);
  return value;
}

const summaryTimeoutMs = numberFromEnv('SUMMARY_TIMEOUT_MS', 15000);

export const config = {
  port: numberFromEnv('PORT', 3000),
  databaseUrl: process.env.DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/encounters',
  summaryProvider: process.env.SUMMARY_PROVIDER ?? 'mock',
  summaryTimeoutMs,
  // A lease must outlive the provider timeout, otherwise a healthy call could be reclaimed.
  leaseMs: numberFromEnv('WORKER_LEASE_MS', summaryTimeoutMs + 30_000),
  maxSummaryAttempts: numberFromEnv('MAX_SUMMARY_ATTEMPTS', 8),
  retryBaseMs: numberFromEnv('RETRY_BASE_MS', 2_000),
  retryMaxMs: numberFromEnv('RETRY_MAX_MS', 300_000),
  circuitFailureThreshold: numberFromEnv('CIRCUIT_FAILURE_THRESHOLD', 3),
  circuitCooldownMs: numberFromEnv('CIRCUIT_COOLDOWN_MS', 15_000),
  circuitMaxCooldownMs: numberFromEnv('CIRCUIT_MAX_COOLDOWN_MS', 300_000),
  slaSeconds: numberFromEnv('SLA_SECONDS', 10),
  workerPollMs: numberFromEnv('WORKER_POLL_MS', 500),
  maintenanceIntervalMs: numberFromEnv('MAINTENANCE_INTERVAL_MS', 1_000),
};
