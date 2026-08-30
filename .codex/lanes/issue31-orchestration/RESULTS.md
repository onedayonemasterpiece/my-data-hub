# Issue 31 orchestration lane results

- Lane: `issue31-orchestration`
- Base SHA: `a93c255`
- Implementation head SHA: `439d72520dabebd1ad28be70c54f7ddcdc7bbdff`
- Writable scope respected: `worker.py`, `test_worker.py`, and this evidence file only.

## Delivered

- Replaced aggregate transcription orchestration with ordered, bounded, one-source-chunk-at-a-time normalization and `transcribe_segment` calls.
- Reconciles every source file against its durable size, SHA-256, duration, index, and private spool location before content processing.
- Persists immutable accepted segment receipts and reuses them after an explicit retry/restart, avoiding replay of completed provider calls.
- Assembles the transcript only through `persist_content_verification`; legacy transcript state without a content receipt cannot bypass segment verification.
- Starts summary only after durable full-coverage verification, then publishes.
- Persists publication verification, creates a separate purge authorization while source files still exist, and only then performs/finishes physical purge.
- A publication receipt alone cannot authorize deletion. Purge-authorized/audio-purged restart paths repeat only the remaining purge/finalization work.

## Test evidence

Commands used the dependency-complete sibling lane virtual environment with `PYTHONPATH=src` so imports resolve this worktree.

- `pytest -q tests/voice_intake_v2/test_worker.py -o addopts=''`: **15 passed**.
- `pytest -q tests/voice_intake_v2 -o addopts=''`: **87 passed, 1 failed**. The sole failure is the pre-existing/stale `test_exact_retry_after_audio_purge_does_not_resurrect_server_audio` in forbidden `tests/voice_intake_v2/test_api.py`; it directly invokes `purge_audio` without content/publication/authorization receipts and now correctly gets `purge_not_authorized`. Parent integration must update that cross-lane expectation rather than weaken the gate.
- `ruff check src/my_data_hub/voice_intake_v2/worker.py tests/voice_intake_v2/test_worker.py`: passed.
- `mypy src/my_data_hub/voice_intake_v2/worker.py`: passed.
- `python -m compileall -q src/my_data_hub/voice_intake_v2 tests/voice_intake_v2`: passed.
- `git diff --check`: passed before commit.

Regression coverage includes physical chunk-file assertions for 20+ minute short-valid rejection, parseable/malformed MAX_TOKENS, missing segment coverage, GitHub-only readback, source tampering, full seven-segment flow, and actual store reopen after injected boundaries following segment/content/summary/publication/authorization/purge durability.

## Cross-lane integration gaps / risks

1. `SegmentInferenceReceipt.transcript_receipt_sha256` currently hashes rich provider/source evidence, while the store lane field with the same name validates the canonical transcript JSON hash. The worker preserves the rich hash in bounded `coverage.inference_receipt_sha256` and passes `None` for the store's content-hash field. Parent/store integration must give these two hashes distinct durable fields; no hash was fabricated or weakened.
2. `persist_content_verification` currently clears the aggregate `transcript_request_uid`, while `PublicationProjection` still types that property as non-null. Per-chunk provider UIDs cannot be collapsed into a fabricated aggregate provider UID. Parent/store/markdown integration must project a separate content receipt identifier or nullable legacy field.
3. Provider failures that do not expose a request identity are not written as fabricated failed-receipt rows. They remain fail-closed through `StageFailure`; ambiguous outcomes are fenced and audio is retained. A provider/limiter request identity must be supplied by the inference/ledger lanes before durable failed-attempt mapping can be added safely.
4. A true process death after a provider succeeds but before its returned receipt is persisted remains intentionally reconciliation-required through the existing in-flight lease fence; it is never silently replayed. Tests prove no replay after each durable receipt boundary.

No private audio, transcript, terminology, token, Authorization header, or API key was added.
