# Reliable Encounter Updates & AI Summaries: Design

**Candidate:** Mohit Gour  
**Stack:** Node.js 20 + TypeScript, Express, PostgreSQL 16. Runnable code is in this repository (see `README.md`); pseudocode below mirrors it.

**In one paragraph:** Postgres is the single source of truth *and* the durable job queue. Ingestion takes a row lock on the encounter, decides accepted/duplicate/stale/conflict, and, if accepted, writes an **immutable version snapshot and its summary job in the same transaction**, so an acknowledged update can never lose its work. Workers claim jobs with `FOR UPDATE SKIP LOCKED` and a lease, call the AI provider **outside** any transaction, and commit the result keyed by `(encounter_id, version)`. A result can only become *current* through a compare-and-set against the encounter's current version. Paid calls are budgeted (classified errors, capped exponential backoff, a shared circuit breaker) and every call is recorded.

---

## 0. Assumptions

1. One Postgres primary; API and worker are separate, horizontally scalable stateless processes.
2. The partner retries on network errors and 5xx, and treats any 2xx as "do not resend".
3. `version` fits in a 32-bit integer; transcripts are at most ~2 MB of text (validated).
4. The summary is a **draft for clinician review**, not an autonomous clinical output.
5. A summary is useful only for the version it describes. Clients want the *current* version's summary, but older ones may be fetched as history.
6. `encounter_id` is an opaque internal identifier and may appear in logs; `patient_id`, transcripts and summaries may not.
7. An abandoned or timed-out provider call may still be billed; we cannot observe that, so we count it as a cost.

---

## 1. Design

### 1.1 Components

```
partner ──POST /v1/encounter-events──▶ API ──(1 transaction)──▶ Postgres
                                                     encounters / encounter_versions / summary_jobs / event_receipts
client  ──GET  /v1/encounters/:id/summary──▶ API ──read──▶ Postgres
worker(s) ──claim (SKIP LOCKED + lease)──▶ Postgres ──▶ generate_summary(transcription) ──▶ commit result (CAS)
```

### 1.2 API contract

#### `POST /v1/encounter-events`

| Outcome | HTTP | Why |
|---|---|---|
| `accepted_current` | **202 Accepted** | State advanced; summary work queued but not done. 202 is the standard "accepted for async processing" code; the body carries `status_url`. |
| `duplicate` | **200 OK** | Valid and already applied (same `event_id`, or same `(encounter_id, version)`). Idempotent success, so the partner must stop retrying. |
| `stale` | **200 OK** | Valid but older than current, so it is acknowledged and not applied. Still a success from the partner's view; a 4xx would make well-behaved retries look like errors and could trigger partner-side alerts or retry loops. The body says `stale` and gives `current_version`. |
| `conflict` | **409 Conflict** | Violates a source invariant (different `patient_id`/`encounter_type`, different content for a known version or `event_id`). Retrying can't fix it. Quarantined, never applied. |
| invalid body | **400** (413 if > 3 MB) | Schema validation. |
| server error | **500** | Partner retries; safe because ingestion is idempotent. |

Using 202 vs 200 means a partner that only reads status codes still learns whether *this* request moved state. The `outcome` field is the precise signal.

```http
POST /v1/encounter-events
{ "event_id": "evt-102", "encounter_id": "enc-42", "patient_id": "pat-77",
  "encounter_type": "TelephoneTriage", "version": 12,
  "payload": { "transcription": "Nurse: Thanks for calling, how can I help today? Patient: ..." } }
```

Accepted and current:
```http
HTTP/1.1 202 Accepted
{ "outcome": "accepted_current", "encounter_id": "enc-42", "received_version": 12, "current_version": 12,
  "summary_status": "queued", "status_url": "/v1/encounters/enc-42/summary" }
```

Duplicate (exact retry, or same version under a new `event_id`):
```http
HTTP/1.1 200 OK
{ "outcome": "duplicate", "encounter_id": "enc-42", "received_version": 12, "current_version": 12,
  "status_url": "/v1/encounters/enc-42/summary" }
```

Stale (version 11 arrives after 12):
```http
HTTP/1.1 200 OK
{ "outcome": "stale", "encounter_id": "enc-42", "received_version": 11, "current_version": 12,
  "status_url": "/v1/encounters/enc-42/summary" }
```

Conflict (same encounter, different patient):
```http
HTTP/1.1 409 Conflict
{ "outcome": "conflict", "conflict": "identity_conflict", "encounter_id": "enc-42",
  "received_version": 13, "current_version": 12 }
```
`conflict` is one of `identity_conflict`, `version_conflict` and `event_id_conflict`.

#### `GET /v1/encounters/{encounter_id}/summary[?version=N]`

Defaults to the **current** version. `?version=N` returns history. The response always names the exact `summary_version` and whether it `is_current`. A known job returns **200** whatever its state (`failed` is a state of the resource, not a failed request). An unknown encounter or version returns **404**.

Processing (also how `queued` looks, with `started_at: null`):
```json
{ "encounter_id": "enc-42", "patient_id": "pat-77", "encounter_type": "TelephoneTriage",
  "current_version": 12, "summary_version": 12, "is_current": true,
  "status": "processing", "summary": null, "attempt_count": 1,
  "accepted_at": "2026-10-26T10:00:00.000Z", "started_at": "2026-10-26T10:00:00.120Z", "completed_at": null,
  "next_attempt_at": null, "last_error_code": null, "last_error_at": null,
  "sla_deadline_at": "2026-10-26T10:00:10.000Z", "sla_breached": false }
```

Ready:
```json
{ "encounter_id": "enc-42", "current_version": 12, "summary_version": 12, "is_current": true,
  "status": "ready", "summary": "Reason for encounter: ...", "summary_created_at": "2026-10-26T10:00:04.910Z",
  "attempt_count": 1, "accepted_at": "2026-10-26T10:00:00.000Z", "completed_at": "2026-10-26T10:00:04.910Z",
  "sla_breached": false }
```

Retrying during a provider outage (still `queued`, so the client keeps polling):
```json
{ "summary_version": 12, "is_current": true, "status": "queued", "summary": null, "attempt_count": 2,
  "next_attempt_at": "2026-10-26T10:00:21.000Z", "last_error_code": "PROVIDER_UNAVAILABLE", "sla_breached": true }
```

Failed (retry policy exhausted or permanent error):
```json
{ "summary_version": 12, "is_current": true, "status": "failed", "summary": null, "attempt_count": 8,
  "completed_at": "2026-10-26T10:04:31.000Z", "last_error_code": "PROVIDER_UNAVAILABLE", "sla_breached": true }
```

Older version (history): `GET ...?version=11` gives `"summary_version": 11, "is_current": false` with `status` `ready` or `superseded`.

#### `POST /v1/encounters/{encounter_id}/summary/redrive[?version=N]` (operator)

Re-queues a **failed** job for the **current** version with a fresh retry budget: **202**. It returns **409** if the job isn't failed or the version isn't current, and **404** if unknown. In production this sits behind admin auth.

### 1.3 Data model

```
encounters (1) ──< encounter_versions (immutable, 1 per accepted version)
                        │ 1
                        ├──1 summary_jobs ──< summary_attempts (1 row per paid call)
                        └──0..1 summary_results
event_receipts   (every event_id answered: accepted / duplicate / stale)
event_conflicts  (quarantine for invariant violations, hashes only)
provider_circuit (single row, shared breaker state)
```

| Table | Key / constraints | Purpose |
|---|---|---|
| `encounters` | PK `encounter_id`; `patient_id`, `encounter_type` NOT NULL; `encounter_type` CHECK in the 3 types; `current_version`; `current_summary_version` | Latest state. **`patient_id` and `encounter_type` live here, fixed at first acceptance.** Every later event is checked against them. |
| `encounter_versions` | **PK `(encounter_id, version)`**; **UNIQUE `event_id`**; FK → encounters; `payload_hash` | Immutable snapshot of each accepted version: the exact summary input. Also carries `patient_id`/`encounter_type` as they were received (audit). |
| `event_receipts` | **PK `event_id`** | Answers retries of duplicate/stale events consistently. Together with `encounter_versions.event_id` this enforces event_id uniqueness. |
| `summary_jobs` | PK `job_id`; **UNIQUE `(encounter_id, encounter_version)`**; FK → `encounter_versions` | Durable queue and workflow state: `queued → processing → ready \| failed \| superseded`; `attempt_count`, `excluded_attempts`, `next_attempt_at`, `lease_until`, `accepted_at`, `sla_deadline_at`, `sla_breached_at`, `completed_at`, `last_error_code`. |
| `summary_results` | **PK `(encounter_id, encounter_version)`**; FK → `encounter_versions` | One result per version, never overwritten (first writer wins). |
| `summary_attempts` | PK `attempt_id`; UNIQUE `(job_id, attempt_number)` | Every provider call, written *before* the call, so even a crash leaves a cost record. |
| `event_conflicts` | PK serial | Quarantined events: ids, version, conflict type, payload hash. No transcript. |
| `provider_circuit` | PK `id = 1` | Shared circuit breaker. |

The summary record reaches `patient_id`/`encounter_type` through its `encounter_id` → `encounters`. They are not copied onto the result, because they can't change for an encounter.

**Retention choice: keep an immutable record of every accepted version.** A job references `(encounter_id, version)`, and that row is never updated. So a worker reads its input by primary key and *cannot* read a different version's transcript, even if newer versions arrive during processing. It also gives a free audit trail and makes `?version=N` trivial. The cost is storage (transcripts are large), handled with a retention job that drops `transcription` from superseded versions after N days while keeping the hash. Stale events (older than current when they arrive) are **not** stored; only a receipt is.

### 1.4 Processing logic

#### Ingestion (one transaction)

```
ingest(event):
  hash = sha256(encounter_id, patient_id, encounter_type, version, transcription)
  BEGIN
    INSERT encounters(event identity, current_version=0) ON CONFLICT DO NOTHING
    enc = SELECT ... FROM encounters WHERE encounter_id=? FOR UPDATE     -- serialises this encounter
    -- every check below runs *after* the lock, so concurrent copies see each other
    if receipt or version row exists for event.event_id:
        if same encounter/version/hash: return DUPLICATE(enc.current_version)
        else: raise CONFLICT(event_id_conflict)
    if enc.patient_id != event.patient_id or enc.encounter_type != event.encounter_type:
        raise CONFLICT(identity_conflict)
    if encounter_versions has (encounter_id, version):
        if hash equal: insert receipt(duplicate); return DUPLICATE
        else: raise CONFLICT(version_conflict)
    if version <= enc.current_version:
        insert receipt(stale) ON CONFLICT DO NOTHING; return STALE(enc.current_version)
    INSERT encounter_versions(..., transcription, hash)               -- immutable snapshot
    INSERT summary_jobs(queued, accepted_at=now, sla_deadline_at=now+10s)
    UPDATE summary_jobs SET superseded WHERE encounter_id=? AND version < new AND status='queued'
    UPDATE encounters SET current_version=new, current_summary_version=NULL
    INSERT event_receipts(accepted_current)
  COMMIT  → 202
on CONFLICT: ROLLBACK; INSERT event_conflicts(ids, type, hash); log; → 409
```

**Concurrency:** the encounter row lock serialises everything for one encounter. Different encounters never contend. The primary key, UNIQUE constraints and `ON CONFLICT` guards back this up, so even an unexpected interleaving fails safe (constraint error, then rollback, then the partner retries and gets `duplicate`) rather than storing two versions.

#### Worker

```
loop:
  every 1s: recover_expired_leases(); sweep_sla()
  permit = circuit.acquire()            -- closed | probe (exactly one worker wins) | open
  if open: sleep; continue

  -- Claim transaction (short; no external call inside)
  BEGIN
    job = SELECT j.*, e.current_version FROM summary_jobs j JOIN encounters e
          WHERE status='queued' AND next_attempt_at<=now()
          ORDER BY accepted_at FOR UPDATE OF j SKIP LOCKED LIMIT 1
    if job.version < e.current_version: mark superseded; COMMIT; continue   -- no paid call
    UPDATE job SET processing, attempt_count+=1, lease_until=now()+(timeout+30s)
    INSERT summary_attempts(attempt_number, 'started')                        -- cost trail before the call
    input = SELECT transcription FROM encounter_versions WHERE (encounter_id, version) = job's
  COMMIT

  try summary = generate_summary(input) with 15s timeout (SDK retries disabled)
  on success, one transaction:
    INSERT summary_results(enc, version, summary) ON CONFLICT DO NOTHING      -- first valid result wins
    UPDATE job SET ready, completed_at WHERE status IN (queued, processing, failed)
    UPDATE encounters SET current_summary_version=version
        WHERE encounter_id=? AND current_version=version                       -- stale-result CAS
    UPDATE attempt SET succeeded
    circuit.success()
  on error, one transaction:
    c = classify(error)        -- permanent? provider_unavailable? retry_after?
    status = superseded if newer version exists
           else failed if c.permanent or charged_attempts >= 8
           else queued (next_attempt_at = now + max(retry_after, backoff))
    UPDATE job ... WHERE job_id=? AND status='processing' AND attempt_count=my_attempt   -- fencing
    UPDATE attempt SET outcome, error_code
    circuit.failure() if c.provider_unavailable else circuit.success()
```

**Transaction boundaries:** (1) ingest; (2) claim; (3) complete *or* fail. The 2–15 s provider call is never inside a transaction or holding a lock.

**Stale-result protection (three layers):**
1. Results are keyed by version and never overwritten, so a v12 result can only ever describe v12.
2. `current_summary_version` moves only via the CAS `WHERE current_version = job.version`.
3. Reads resolve "current" from `encounters.current_version` and return that version's job. A finished v12 is visible only via `?version=12`, labelled `is_current: false`.

**At-least-once and cost.** The queue is at-least-once: a job may run twice if a lease expires while its call is still running, or if a worker dies after the provider answered but before commit. Exactly-once execution of an external, non-idempotent, paid call isn't possible with local transactions, so the design **minimises and records** duplicate cost instead:

- The lease (timeout + 30 s) always outlives a healthy call.
- SDK-level retries are disabled, so every call goes through our budget.
- Each attempt is written before the call.
- A late result from a reclaimed worker is still accepted, since it was paid for and its input is immutable.
- The failure path is fenced on `attempt_count`, so a stale worker can't overwrite the new owner's state.

**Retry policy:**

| Error class | Examples | Action |
|---|---|---|
| Permanent | 400/404/422 (`PROVIDER_REJECTED_REQUEST`), 401/403 (`PROVIDER_AUTH_ERROR`), 429 `insufficient_quota`, `INVALID_TRANSCRIPTION`, provider not configured | `failed` immediately, so no money is spent on a call that can't succeed. Redrive after the fix. |
| Provider unavailable | 5xx, 408, connection errors, timeouts, 429 rate limit | Retry with backoff; honour `Retry-After`; feeds the circuit breaker. |
| Other transient | `EMPTY_SUMMARY`, unknown | Retry with backoff. |

Backoff is exponential with equal jitter, base 2 s, cap 5 min: roughly 1–2 s, 2–4 s, 4–8 s … The budget is **8 charged attempts** per job, after which the job is `failed`.

**Circuit breaker (shared via Postgres):** 3 consecutive provider-unavailable failures, from any workers, open it. While open, **no worker claims jobs**, so a backlog of N jobs costs nothing extra. After a cooldown (15 s, doubling to a 5-minute cap) exactly one worker wins an atomic probe slot and makes a single call. Success closes the breaker; failure re-opens it with a longer cooldown. A failed probe is recorded as a paid attempt but **not charged to that job's budget**, because it measured provider health. A crashed prober frees the slot after one lease.

### 1.5 Obsolete work (newer version arrives)

| Where the old job is | What happens | Cost |
|---|---|---|
| `queued` | Marked `superseded` in the ingest transaction | 0 calls |
| Re-queued for retry, then claimed | Claim sees a newer version: `superseded` | 0 calls |
| Fails while a newer version exists | Becomes `superseded`, not re-queued | 0 extra calls |
| **In flight** | Allowed to finish; result stored as history (`ready`, `is_current: false`); CAS keeps it from becoming current | ≤ 1 call already paid |

**Trade-off:** cancelling an in-flight call would need a cross-process cancel signal (polling or `LISTEN/NOTIFY` plus `AbortController`). It would rarely save money, because the provider has usually already billed for input tokens, and it adds a race-prone path. Retiring work at every point *before* a call is cheap and saves the money that matters. Keeping the late result costs one row and gives an audit trail.

### 1.6 SLA (< 10 s acceptance → ready)

- `sla_deadline_at = accepted_at + 10 s` is stored per job.
- The GET response **derives** `sla_breached` from timestamps (`completed_at` or `now()` past the deadline). It is correct even when no worker is alive, which is exactly when it matters.
- A worker sweep (every 1 s) stamps `sla_breached_at` on open jobs and emits a `summary_sla_breached` log event for alerting. Completion and failure paths stamp late jobs too.
- A breach **never** changes `status`. Only the failure policy above moves a job to `failed`.
- The provider timeout (15 s) is deliberately longer than the SLA: a slow-but-successful 12 s call is better than paying for a timeout plus a retry.

---

## 2. Failure scenarios

| Scenario | Stored state | Client sees | Recovery |
|---|---|---|---|
| **Concurrent duplicate:** two instances receive the same event | Both transactions block on the encounter row lock. The first commits one version, one job and one receipt. The second, after the lock, finds the `event_id` and returns duplicate. | One `202 accepted_current`, one `200 duplicate` (either order). | None needed. The PK and UNIQUE constraints are a backstop: if violated, the request rolls back, returns 500, and the partner's retry returns `duplicate`. *(Tested with 20 parallel copies, for both new and stale events.)* |
| **Missing or late version:** v12 arrives while v10 is stored; v11 later or never | v12 accepted, current = 12, v10's queued job superseded. v11 gets a `stale` receipt only. | v12: `202`. v11: `200 stale, current_version: 12`. Summary of v12 when ready. | Gaps are allowed by the contract. Nothing waits for v11, because each payload is a full snapshot. |
| **Crash after saving:** encounter saved, crash before hand-off | Impossible to split. The version row and its job are inserted in the **same transaction**. If the commit happened, the job exists; if not, nothing exists. | If the 202 was sent, the job is durable. If the crash came before the response, the partner retries and gets `duplicate`, and the job already exists. | Any worker picks up the `queued` job. Because the job table *is* the queue there's no hand-off step; with an external broker we would use a transactional outbox. |
| **Worker interruption:** crash mid-call, or after saving the result but before ack | Mid-call: job stays `processing` until `lease_until`, then recovery (one statement) re-queues it and marks the attempt `worker_lost`. After saving: completion is one transaction (result + job ready + attempt), so there's no separate ack to lose. | `processing`, then `queued` with `last_error_code: WORKER_LEASE_EXPIRED`, then `ready`. | Retried with the same immutable input. The lost call is visible in `summary_attempts` for cost. A late result from the "dead" worker is accepted (first writer wins); its failure path is fenced. |
| **Newer version during processing:** v12 in flight, v13 arrives | v13 accepted with its own job. v12 continues. | GET (current) shows v13's state, never v12's summary. | v12 finishes and is stored as history. If v12 *fails*, it becomes `superseded` and is not retried. See §1.5. |
| **Results out of order:** v13 finishes before v12 | v13 result stored and `current_summary_version = 13`. When v12 completes, its result is stored and the job set `ready`, but the CAS (`current_version = 12`) matches nothing. | Current summary is v13. `?version=12` shows v12 with `is_current: false`. | Nothing to do. *(Tested.)* |
| **AI outage and retry exhaustion:** provider down for 20 min | The first failures re-queue jobs with backoff. After 3 consecutive failures the breaker opens and **all workers stop calling**. Probes run about 15 s, 45 s, 1.75 min, 3.75 min, 7.75 min, 12.75 min, 17.75 min and 22.75 min after the trip, then every 5 min. That's **≈ 10 paid calls for the whole outage regardless of backlog size**, versus N × 8 without a breaker (more if W workers had calls in flight when it started). Probes aren't charged to job budgets, so **no job fails because of a 20-minute outage**. | Status `queued` with `attempt_count`, `last_error_code: PROVIDER_UNAVAILABLE`, `next_attempt_at`, and `sla_breached: true` after 10 s. Never a misleading `failed` while recovery is still possible. | The first successful probe after recovery (≤ 5 min later, the cooldown cap) closes the breaker and the backlog drains at worker concurrency. **Exhaustion:** a job reaches 8 charged attempts when errors are intermittent (the breaker keeps closing) or input-specific. It becomes `failed` with the last error code. **Inspect:** `summary_jobs` (status, codes, timestamps), `summary_attempts` (each call's outcome, error code, timing) and logs with the provider request id. **Redrive:** `POST .../summary/redrive` re-queues only the *current* version with a fresh budget while keeping history. Bulk redrive is the same UPDATE filtered by `last_error_code` and time window. Obsolete versions are refused (409). |
| **Inconsistent identity fields:** existing `encounter_id`, different `patient_id` or `encounter_type` | Unchanged. The transaction rolls back; a quarantine row (ids, version, conflict type, payload hash, no transcript) goes into `event_conflicts`. | `409 conflict: identity_conflict` with `current_version`. | First-seen identity wins. We never re-attribute a transcript to a different patient automatically, because that is a patient-safety risk. Any conflict raises an alert; resolving it is a manual or partner process. The same policy covers `version_conflict` and `event_id_conflict`. |

---

## 3. Verification

### Test cases

These are automated in `tests/integration.test.ts` (Vitest against a disposable Postgres database; run with `npm test`). There are 11 tests in total; the headline six:

| # | Test | Expected outcome |
|---|---|---|
| 1 | **Contract:** post v12; resend; v12 under a new `event_id`; v10; v13 with another `patient_id`; v12 with different content | 202 accepted; 200 duplicate; 200 duplicate; 200 stale (current 12); 409 identity_conflict; 409 version_conflict. State stays at v12 with one job; two quarantine rows. |
| 2 | **Concurrency:** 20 identical new events in parallel; then 20 identical *stale* events in parallel; then v1–v10 in random order in parallel | Exactly 1 accepted + 19 duplicates; stale burst: all 200 (1 stale + 19 duplicate, no 500s); current version ends at 10 with only v10's job queued. |
| 3 | **Results out of order:** v12 summary slowed down; v13 accepted and summarised first; v12 completes later | Current version and summary = v13; `current_summary_version = 13`; v12 retained with `is_current: false`. |
| 4 | **Crash/retry:** a job is claimed, its attempt started, then the worker "dies" (lease expires) | Recovery re-queues it with `WORKER_LEASE_EXPIRED`; attempt 1 = `worker_lost`; the next run succeeds as attempt 2. Plus a fencing test: the stale worker's later failure cannot overwrite the new owner's `processing` state. |
| 5 | **Outage:** provider returns 503 for 5 queued jobs | 3 calls, then the breaker opens and further work returns `circuit_open` with 0 calls; after the cooldown exactly 1 probe (not charged to the job's budget); after recovery all 5 jobs `ready`, 9 calls in total, none failed. Separately: a 400 fails after 1 call; 503 × 3 exhausts → `failed`; redrive of an old version gives 409, of the current one 202, then `ready`. |
| 6 | **SLA:** accept with no worker running; wait past the SLA | GET shows `status: queued, sla_breached: true` (not failed); after processing, `ready` with `sla_breached: true`. |

### Observability (no patient content)

Logs are structured JSON (`src/log.ts`) carrying only job_id, encounter_id, version, attempt numbers, error codes, HTTP status, provider request id and timings. They **never** include the transcript, summary, `patient_id` or raw provider error text, which could echo input.

- **Timestamps per job:** `accepted_at`, `started_at`, `next_attempt_at`, `sla_deadline_at`, `sla_breached_at`, `completed_at`, `last_error_at`; per attempt: `started_at`, `finished_at`, `outcome`, `error_code`.
- **Metrics** (derived from the log events / tables):
  - acceptance→ready latency p50/p95 (`summary_ready.e2e_ms`)
  - SLA breach rate
  - queue depth by status and **age of oldest queued job**
  - provider latency and errors by code
  - **paid calls per ready summary** (waste ratio)
  - lease expiries
  - superseded count
  - circuit state
  - conflicts by type
- **Alerts:**
  - oldest queued job > 60 s
  - SLA breach rate > 5 % over 10 min
  - circuit open > 2 min
  - any `failed` job
  - any `ingest_conflict`
  - lease-expiry spike
  - calls per summary > 1.5
- **Investigating a stuck summary:** look up the job by `encounter_id`/version. Then:
  - `status`/`next_attempt_at`/`last_error_code` say whether it's waiting on backoff, the breaker or a worker.
  - `summary_attempts` shows every call with its outcome.
  - `provider_circuit` shows whether calls are paused.
  - The worker logs for the job_id show provider request ids to raise with the vendor.

---

## 4. Known limitations

- **Queue on Postgres:** simple and transactional. Suits moderate throughput (hundreds of jobs/s), but polling adds up to `WORKER_POLL_MS` of latency. At larger scale, use `LISTEN/NOTIFY` or move to a broker with a transactional outbox.
- **One job at a time per worker process;** scale by running more processes.
- **Breaker recovery latency:** after an outage ends, summaries may wait up to the cooldown cap (5 min).
- **Duplicate paid calls** remain possible on ambiguous timeouts or crashes after a provider response. They are recorded but cannot be prevented without provider-side idempotency keys.
- **Immutability of `encounter_versions` is by convention** (no code path updates it). Production should enforce it with a trigger or by revoking UPDATE.
- **Stale payloads are not content-checked:** versions older than current at arrival aren't stored, so a conflicting stale payload can't be detected.
- **Not implemented:**
  - authN/Z (including the redrive endpoint)
  - request rate limiting
  - encryption-at-rest and retention jobs
  - HIPAA review
- **Tests use a mock provider.** The OpenAI adapter's error classification follows the SDK's documented error shape but isn't exercised against the live API in CI.
- **Summaries are AI drafts** and need clinician review.
