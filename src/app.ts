import express from 'express';
import { encountersRouter } from './routes/encounters.js';

export const app = express();
app.use(express.json({ limit: '3mb' }));
app.get('/health', (_req, res) => res.status(200).json({ status: 'ok' }));
app.use('/v1', encountersRouter);
app.use((error: Error & { status?: number }, _req: express.Request, res: express.Response, _next: express.NextFunction) => {
  const status = error.status ?? 500;
  if (status >= 500) console.error(JSON.stringify({ level: 'error', code: error.message || 'internal_error' }));
  return res.status(status).json({ error: status === 500 ? 'internal_error' : error.message });
});
