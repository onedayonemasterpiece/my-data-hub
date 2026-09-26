# Voice Intake recovery correction — 2026-09-26

The September 21 schema-only retry correction left a second classifier in the
worker: the adapter correctly marked HTTP 5xx as retryable, but the worker
discarded that decision for any sent request except 429 or invalid schema.
A September 22 recording consequently remained stopped after one rejected
request. Old metadata retained only `provider_rejected_request`, so its exact
HTTP status cannot be established retrospectively. One explicitly authorized
recovery attempt completed transcription, summary, verified publication and
purge. No recording had to be recaptured.

The worker now follows the adapter's explicit retryable/non-ambiguous contract.
Definite transient HTTP failures retry through the shared limiter with durable
exponential backoff; provider downtime does not exhaust the separate budget
for malformed output. Permanent 4xx and unreceipted timeout/network outcomes
still require intervention. This supersedes the broad sent-request fence in
the September 5 report and the schema-only exception of September 21.

Definite failed responses now receive the same durable accounting protection
as successful results. A crash or limiter outage after the response no longer
turns a known failure into an unknown outcome. Sanitized attempt history in
SQLite retains exact HTTP status for future diagnosis.

The installed Android client also unconditionally treats
`response_schema_invalid` as manual reconciliation. Scheduled retries now expose
`provider_output_retry_pending` while retaining the original internal error.
This fixes polling compatibility without requiring an APK reinstall.

Regression coverage uses the actual adapter and worker with a fake transport:
six successive 503 responses and restart, summary-only recovery, permanent
4xx, HTML 503, accounting outage, SQLite failure after response, and crash
during a subsequent send. Existing publication/readback and audio-retention
tests remain required. These tests do not establish that an Android phone has
observed the restored status; the existing app needs "Повторить сейчас" for
an already terminal local session.
