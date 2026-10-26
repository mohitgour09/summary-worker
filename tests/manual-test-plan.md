# Manual integration test plan

Use a disposable local database and keep all data synthetic.

1. **Accept current**: POST event `evt-a`, encounter `enc-a`, version 12. Expect HTTP 202, `accepted_current`, current_version 12; then poll until ready.
2. **Duplicate retry**: POST the identical event again. Expect HTTP 200, `duplicate`, and no second summary job.
3. **Stale update**: POST event `evt-old`, same encounter, version 10. Expect HTTP 200, `stale`, current_version 12.
4. **Identity conflict**: POST version 13 for `enc-a` with a different patient_id. Expect HTTP 409; current_version remains 12.
5. **Concurrent duplicate**: issue the exact same POST simultaneously from two terminals. Expect one accepted response and one duplicate response (which response arrives first can vary), one logical version row, and one job.
6. **Out-of-order summary completion**: add artificial latency to the provider adapter for version 12, submit version 13, and let 13 finish first. When 12 later finishes, `current_version` must remain 13 and the summary endpoint must not present 12 as current.
7. **Crash recovery**: stop the worker after the API returns 202. The job remains queued in PostgreSQL. Restart the worker; it should eventually process the job. For the ambiguous external-call case, inject a crash after the provider call but before committing the result and verify that the retry is represented in `summary_attempts`.

All of the above, plus outage/circuit-breaker, retry exhaustion, redrive, fencing and SLA cases, are automated in `tests/integration.test.ts` (`npm test`). Use this plan for exploratory checks against running API and worker processes. With `SUMMARY_PROVIDER=mock`, a transcription containing `[[mock-delay-ms:3000]]` slows that summary down, and setting `MOCK_PROVIDER_ERROR=503` on the worker simulates an outage.
