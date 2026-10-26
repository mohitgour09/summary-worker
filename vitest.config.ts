import { defineConfig } from 'vitest/config';

const host = process.env.TEST_PG_HOST ?? 'postgres:postgres@localhost:5432';

export default defineConfig({
  test: {
    globalSetup: './tests/global-setup.ts',
    fileParallelism: false,
    testTimeout: 30_000,
    hookTimeout: 30_000,
    env: {
      DATABASE_URL: `postgres://${host}/encounters_test`,
      SUMMARY_PROVIDER: 'mock',
      LOG_SILENT: '1',
      SLA_SECONDS: '1',
      SUMMARY_TIMEOUT_MS: '5000',
      MAX_SUMMARY_ATTEMPTS: '3',
      RETRY_BASE_MS: '200',
      RETRY_MAX_MS: '400',
      CIRCUIT_FAILURE_THRESHOLD: '3',
      CIRCUIT_COOLDOWN_MS: '300',
      CIRCUIT_MAX_COOLDOWN_MS: '1000',
    },
  },
});
