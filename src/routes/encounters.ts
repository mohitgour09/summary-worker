import { Router } from 'express';
import { z } from 'zod';
import { encounterTypes } from '../types.js';
import { IngestConflictError, ingestEvent } from '../services/ingest.js';
import { getSummaryStatus, redriveSummary } from '../services/summary-status.js';

export const encountersRouter = Router();
const eventSchema = z.object({
  event_id: z.string().min(1).max(200),
  encounter_id: z.string().min(1).max(200),
  patient_id: z.string().min(1).max(200),
  encounter_type: z.enum(encounterTypes),
  version: z.number().int().positive(),
  payload: z.object({ transcription: z.string().min(1).max(2_000_000) }),
});

function parseVersion(raw: unknown): number | undefined | null {
  if (raw === undefined) return undefined;
  const version = Number(raw);
  return Number.isInteger(version) && version > 0 ? version : null;
}

// 202 accepted_current: new state stored, summary work queued (not yet done).
// 200 duplicate / stale: valid and safely acknowledged, nothing changed; the body says why.
// 409 conflict: violates a source invariant; quarantined, never applied.
// 400 invalid body.
encountersRouter.post('/encounter-events', async (req, res, next) => {
  const parsed = eventSchema.safeParse(req.body);
  if (!parsed.success) return res.status(400).json({ error: 'invalid_request', details: parsed.error.flatten() });
  try {
    const result = await ingestEvent(parsed.data);
    return res.status(result.outcome === 'accepted_current' ? 202 : 200).json(result);
  } catch (error) {
    if (error instanceof IngestConflictError) {
      return res.status(409).json({
        outcome: 'conflict', conflict: error.conflict, encounter_id: error.encounterId,
        received_version: error.receivedVersion, current_version: error.currentVersion,
      });
    }
    return next(error);
  }
});

encountersRouter.get('/encounters/:encounterId/summary', async (req, res, next) => {
  try {
    const version = parseVersion(req.query.version);
    if (version === null) return res.status(400).json({ error: 'invalid_version' });
    const result = await getSummaryStatus(req.params.encounterId, version);
    if (!result) return res.status(404).json({ error: 'encounter_not_found' });
    if (result.status === 'not_found') return res.status(404).json({ error: 'summary_not_found', ...result });
    return res.status(200).json(result);
  } catch (error) { return next(error); }
});

// Operator endpoint (would sit behind admin auth in production).
encountersRouter.post('/encounters/:encounterId/summary/redrive', async (req, res, next) => {
  try {
    const version = parseVersion(req.query.version);
    if (version === null) return res.status(400).json({ error: 'invalid_version' });
    const result = await redriveSummary(req.params.encounterId, version);
    if (result.kind === 'not_found') return res.status(404).json({ error: 'summary_job_not_found' });
    if (result.kind === 'not_redrivable') return res.status(409).json({ error: result.reason, status: result.status, current_version: result.current_version });
    return res.status(202).json(result);
  } catch (error) { return next(error); }
});
