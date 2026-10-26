// Structured, PHI-free logging. Callers pass ids, versions, codes and timings only;
// never transcription text, summaries, patient_id or raw provider error messages.
export function log(level: 'info' | 'warn' | 'error', event: string, fields: Record<string, unknown> = {}) {
  if (process.env.LOG_SILENT === '1') return;
  const line = JSON.stringify({ ts: new Date().toISOString(), level, event, ...fields });
  if (level === 'error') console.error(line); else console.log(line);
}
