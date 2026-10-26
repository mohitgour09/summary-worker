import { runWorker } from './workers/summary-worker.js';
import { pool } from './db/pool.js';

runWorker().catch(async (error) => {
  console.error('Worker failed to start', error instanceof Error ? error.message : 'unknown');
  await pool.end();
  process.exit(1);
});
