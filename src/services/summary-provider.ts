import OpenAI from 'openai';
import { config } from '../config.js';

// Error raised by this adapter itself (as opposed to the provider SDK).
export class ProviderError extends Error {
  constructor(readonly code: string, readonly permanent: boolean) {
    super(code);
  }
}

export interface ClassifiedError {
  code: string;
  // permanent: retrying the same input cannot succeed, so stop spending on it.
  permanent: boolean;
  // providerUnavailable: the provider (not this job) is unhealthy; feeds the shared circuit breaker.
  providerUnavailable: boolean;
  timeout: boolean;
  retryAfterMs?: number;
  httpStatus?: number;
  requestId?: string;
}

function header(headers: unknown, name: string): string | undefined {
  if (!headers) return undefined;
  if (typeof (headers as Headers).get === 'function') return (headers as Headers).get(name) ?? undefined;
  return (headers as Record<string, string>)[name];
}

function parseRetryAfterMs(headers: unknown): number | undefined {
  const ms = Number(header(headers, 'retry-after-ms'));
  if (Number.isFinite(ms) && ms > 0) return ms;
  const seconds = Number(header(headers, 'retry-after'));
  if (Number.isFinite(seconds) && seconds > 0) return seconds * 1000;
  return undefined;
}

export function classifyProviderError(error: unknown): ClassifiedError {
  if (error instanceof ProviderError) {
    return { code: error.code, permanent: error.permanent, providerUnavailable: false, timeout: false };
  }
  const e = (error ?? {}) as {
    name?: string; message?: string; status?: number; code?: string; headers?: unknown;
    request_id?: string; requestID?: string;
  };
  const requestId = e.request_id ?? e.requestID;
  if (e.message === 'SUMMARY_TIMEOUT' || e.name === 'APIConnectionTimeoutError' || e.name === 'APIUserAbortError') {
    return { code: 'SUMMARY_TIMEOUT', permanent: false, providerUnavailable: true, timeout: true, requestId };
  }
  if (e.name === 'APIConnectionError') {
    return { code: 'PROVIDER_CONNECTION_ERROR', permanent: false, providerUnavailable: true, timeout: false, requestId };
  }
  const status = typeof e.status === 'number' ? e.status : undefined;
  const base = { httpStatus: status, requestId, timeout: false };
  if (status === 429 && e.code === 'insufficient_quota') {
    return { ...base, code: 'PROVIDER_QUOTA_EXHAUSTED', permanent: true, providerUnavailable: false };
  }
  if (status === 429) {
    return { ...base, code: 'PROVIDER_RATE_LIMITED', permanent: false, providerUnavailable: true, retryAfterMs: parseRetryAfterMs(e.headers) };
  }
  if (status !== undefined && (status === 408 || status === 409 || status >= 500)) {
    return { ...base, code: 'PROVIDER_UNAVAILABLE', permanent: false, providerUnavailable: true, retryAfterMs: parseRetryAfterMs(e.headers) };
  }
  if (status === 401 || status === 403) {
    return { ...base, code: 'PROVIDER_AUTH_ERROR', permanent: true, providerUnavailable: false };
  }
  if (status !== undefined && status >= 400) {
    return { ...base, code: 'PROVIDER_REJECTED_REQUEST', permanent: true, providerUnavailable: false };
  }
  return { ...base, code: 'SUMMARY_PROVIDER_ERROR', permanent: false, providerUnavailable: false };
}

// ---- Development mock --------------------------------------------------------------------
// Free and local. Test hooks (mock mode only):
//   transcription containing [[mock-delay-ms:N]]  -> the call takes N ms (honours abort)
//   process.env.MOCK_PROVIDER_ERROR = "503"|"429"|"401"|"400" -> every call fails with that status
export const mockStats = { calls: 0 };

async function mockSummary(transcription: string, signal?: AbortSignal): Promise<string> {
  mockStats.calls += 1;
  const delayMs = Number(/\[\[mock-delay-ms:(\d+)\]\]/.exec(transcription)?.[1] ?? 0);
  if (delayMs) {
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(resolve, delayMs);
      signal?.addEventListener('abort', () => { clearTimeout(timer); reject(new Error('SUMMARY_TIMEOUT')); }, { once: true });
    });
  }
  const forcedStatus = Number(process.env.MOCK_PROVIDER_ERROR ?? 0);
  if (forcedStatus) {
    throw Object.assign(new Error(`mock provider error ${forcedStatus}`), {
      status: forcedStatus, headers: forcedStatus === 429 ? { 'retry-after': '1' } : {},
    });
  }
  const excerpt = transcription.replace(/\[\[mock-[^\]]*\]\]/g, '').replace(/\s+/g, ' ').trim().slice(0, 240);
  return `Development mock summary (not clinical advice): ${excerpt}${transcription.length > 240 ? '…' : ''}`;
}

export async function generateSummary(
  transcription: string,
  signal?: AbortSignal,
): Promise<string> {
  if (config.summaryProvider === 'mock') return mockSummary(transcription, signal);

  if (config.summaryProvider !== 'openai') {
    throw new ProviderError('PROVIDER_NOT_CONFIGURED', true);
  }

  const apiKey = process.env.OPENAI_API_KEY;

  if (!apiKey) {
    throw new ProviderError('PROVIDER_NOT_CONFIGURED', true);
  }

  if (!transcription.trim()) {
    throw new ProviderError('INVALID_TRANSCRIPTION', true);
  }

  const client = new OpenAI({
    apiKey,
    // Retries are owned by the job queue so every paid call is recorded and budgeted.
    maxRetries: 0,
    timeout: config.summaryTimeoutMs,
  });

  const response = await client.responses.create(
    {
      model: process.env.OPENAI_MODEL || 'gpt-4.1-mini',
      instructions: `
You summarize clinical encounter transcriptions for documentation.

Produce a concise, factual summary using these headings:
- Reason for encounter
- Symptoms and relevant history
- Medications mentioned
- Assessment or concerns discussed
- Plan and follow-up

Rules:
- Use only information explicitly present in the transcription.
- Do not invent diagnoses, medications, dosages, findings, or instructions.
- Preserve important negations, uncertainty, and relevant details.
- If information is not mentioned, say "Not stated".
- Distinguish what the patient reports from what the clinician says.
- Do not add independent medical advice.
- The output is a draft for qualified human review.
      `.trim(),
      input: transcription,
    },
    {
      signal,
      maxRetries: 0,
      timeout: config.summaryTimeoutMs,
    },
  );

  const summary = response.output_text.trim();

  if (!summary) {
    // Treated as transient: a fresh generation may well succeed.
    throw new ProviderError('EMPTY_SUMMARY', false);
  }

  return summary;
}
