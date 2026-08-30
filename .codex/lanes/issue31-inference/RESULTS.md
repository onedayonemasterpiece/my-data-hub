# issue31-inference lane results

## Scope

- Lane ID: `issue31-inference`
- Requirements: bounded per-source-segment transcription; exact provider-success
  semantics; source/input hashing; duration/coverage/plausibility evidence;
  fail-closed public status and old-client purge compatibility.
- Base SHA: `edc382f93157376532abc008a16440be652d0070`
- Implementation SHA: `b0ea67c053e2b372e774f488ba3218eadc2aa2e5`
- Evidence SHA: this file's enclosing follow-up commit.

## Changed files

- `src/my_data_hub/voice_intake_v2/contracts.py`
- `src/my_data_hub/voice_intake_v2/inference.py`
- `tests/voice_intake_v2/test_inference.py`
- `.codex/lanes/issue31-inference/RESULTS.md`

## Implemented evidence

- Replaced aggregate transcription entry point with a bounded, single-source-
  chunk request and a durable `SegmentInferenceReceipt` contract.
- Verifies the original source file against its manifest SHA-256 before quota
  reservation/provider send. Separately hashes the exact normalized MP3 bytes
  sent to Gemini and binds both hashes into the receipt.
- Persists the source interval, full-coverage interval, exact `STOP` finish
  reason, request identity, bounded token usage, transcript receipt hash and
  content-free duration-normalized plausibility evidence.
- Accepts only one candidate, one text part and exact uppercase `STOP`. Missing,
  unknown, non-success, `MAX_TOKENS`, malformed and ambiguous responses fail
  closed. No raw provider response is placed in diagnostics.
- A schema-valid but implausibly short response for a long VAD speech interval
  fails with `segment_content_incomplete`; mismatched coverage/language fails
  with `segment_coverage_invalid`.
- Added dynamic non-negative segment/request progress fields and validation that
  completed counts never exceed their totals.
- Separates content, publication, purge authorization and physical purge state.
  Client deletion permission requires all durable gates and verified physical
  server deletion.
- Adds additive `legacy_unverified_purge`. It represents only truthfully migrated
  already-purged history: both physical purge flags must be true while content
  verification, purge authorization and client purge permission remain false.
  It cannot be used as a new deletion bypass.

## Commands and results

Environment was an isolated, ignored `.venv` created with:

```text
uv venv .venv
uv pip install --python .venv/bin/python -e '.[dev]'
```

Targeted regression tests:

```text
.venv/bin/pytest tests/voice_intake_v2/test_inference.py -o addopts='' -q
29 passed
```

Pre-integration Voice v2 suite:

```text
.venv/bin/pytest tests/voice_intake_v2 -o addopts='' -q --tb=no
69 passed, 6 failed
```

The six failures are the expected integration boundary, all in the old
`test_worker.py` aggregate/purge implementation. That old store emits
`server_audio_purged=true` while omitting the new `audio_purged`, content
verification, authorization and legacy-migration evidence. `StatusResponse`
correctly rejects that unsafe projection with `physical audio purge flags must
agree`. The ledger/worker migration lane must be integrated before the complete
Voice v2 suite can pass; relaxing this validation would reintroduce issue #31.

Static and repository validation:

```text
.venv/bin/ruff check <three lane files>
All checks passed!

.venv/bin/ruff format --check <three lane files>
3 files already formatted

.venv/bin/mypy src/my_data_hub/voice_intake_v2/contracts.py \
  src/my_data_hub/voice_intake_v2/inference.py
Success: no issues found in 2 source files

.venv/bin/python -m compileall -q src tests
PASS

.venv/bin/python scripts/validate_repository.py
4554 checks, 0 errors, ok=true

.venv/bin/python scripts/scan_tracked_secrets.py
tracked-secret scan: PASS

git diff --check
PASS
```

## Risks and integration requirements

- Plausibility thresholds intentionally prefer retaining audio on uncertain or
  silence-heavy input. This can create safe false negatives, not unsafe purge.
- A `MAX_TOKENS` segment requires an explicit retry/recovery decision; it never
  yields a successful segment receipt.
- The store/worker lane must persist and reuse each segment receipt before
  starting summary, assemble only contiguous ordered coverage, and create the
  independent durable content-verification and purge-authorization receipts.
- Existing purged rows must receive `legacy_unverified_purge=true` only in the
  idempotent migration. New processing must never set it.
- Final full-suite and filesystem/crash-path acceptance evidence belongs to the
  integrated branch because this lane is intentionally restricted from editing
  `store.py`, `worker.py`, API, renderer or migration tests.
