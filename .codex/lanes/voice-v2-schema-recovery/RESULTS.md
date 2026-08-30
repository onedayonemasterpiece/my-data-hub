# Lane voice-v2-schema-recovery results

## Scope

- Lane: `voice-v2-schema-recovery`
- Requirements: R7-R8, with sanitized diagnostic support for R3 and explicit-retry semantics for R6.
- Base SHA: `804ec60d5da12790bd9ae9b016270e417169eff8`
- Implementation head SHA: `d00c6d8c8c205c292d08a19d2e5fd899b170b36a`
- Branch: `agent/voice-v2-schema-recovery/core`
- Production/state/secrets were not accessed or modified by this lane.

## Root cause and implementation

The aggregate v2 transcription request configured `maxOutputTokens=8192`. The incident evidence supplied to
the lane showed 8,178 output tokens immediately before `response_schema_invalid`, which is consistent with a
completion truncated at that bound. Gemini 3.1 Flash-Lite's official model page documents a 65,536-token output
limit; the lane uses the more conservative bounded value 32,768:
https://ai.google.dev/gemini-api/docs/models/gemini-3.1-flash-lite

The implementation:

- keeps one aggregate `generateContent` POST per transcription attempt and one summary POST;
- raises only the aggregate transcription output bound from 8,192 to 32,768;
- checks provider `finishReason` before parsing/validating response content;
- classifies explicit `MAX_TOKENS` as `response_schema_invalid`, `sent=true`, `retryable=true`, without a hidden
  retry or public error-code/schema change;
- keeps malformed responses with `STOP` fail-closed and nonretryable;
- attaches and logs only structural diagnostics: schema/version, JSON path, expected constraint, actual
  type/shape, missing/extra field names, finish reason, usage counts, configured max, and truncation flag;
- never stores or logs provider response text through this diagnostic path;
- leaves a sent retryable failure in `retryable_error` with `retry_at=null`, so only the existing explicit
  identical-complete resume signal can start another aggregate attempt.

The reviewer follow-up strengthened the end-to-end worker/store regression to use seven contiguous synthetic
chunk receipts totaling exactly 1,207,620 ms. It proves chunk recording makes zero inference calls, restart-safe
processing normalizes all seven inputs once, successful processing makes one aggregate transcription plus one
summary, all seven chunks remain present through verified publication readback, and purge happens only after the
durable verified receipt.

## Red tests recorded before implementation

Command:

```text
uv run --with pytest --with pytest-asyncio pytest -q tests/voice_intake_v2/test_inference.py \
  -k 'twenty_minute or max_tokens or malformed_stop'
```

Result: 3 failures, proving the prior 8,192 limit, nonretryable truncation, and absent diagnostics.

Command:

```text
uv run --with pytest --with pytest-asyncio pytest -q tests/voice_intake_v2/test_worker.py \
  -k sent_truncation
```

Result: 1 failure because `StageFailure` did not accept sanitized diagnostics.

## Final validation

```text
.venv/bin/python -m pytest -q tests/voice_intake_v2/test_inference.py \
  tests/voice_intake_v2/test_worker.py
```

Result: `16 passed`.

```text
.venv/bin/python -m pytest -q tests/voice_intake_v2
```

Result: `53 passed`.

Focused reviewer follow-up:

```text
.venv/bin/python -m pytest -q tests/voice_intake_v2/test_worker.py \
  -k seven_chunks_and_twenty_minutes
```

Result: `1 passed`.

```text
.venv/bin/python -m compileall -q src/my_data_hub/voice_intake_v2 tests/voice_intake_v2
git diff --check
uvx ruff check src/my_data_hub/voice_intake_v2/inference.py \
  src/my_data_hub/voice_intake_v2/worker.py tests/voice_intake_v2/test_inference.py \
  tests/voice_intake_v2/test_worker.py
.venv/bin/python -m mypy src/my_data_hub/voice_intake_v2/inference.py \
  src/my_data_hub/voice_intake_v2/worker.py
```

Results: all successful. A whole-package mypy run still reports three pre-existing issues in unowned
`markdown.py` and `publisher.py`; both changed files pass strict targeted mypy.

## Changed files

- `src/my_data_hub/voice_intake_v2/inference.py`
- `src/my_data_hub/voice_intake_v2/worker.py`
- `tests/voice_intake_v2/test_inference.py`
- `tests/voice_intake_v2/test_worker.py`
- `.codex/lanes/voice-v2-schema-recovery/RESULTS.md`

## Risks and handoff

- This lane does not mutate the already-failed production session. Its existing `retryable=false` row still
  requires the parent's bounded operator recovery decision.
- A `MAX_TOKENS` attempt is known-failed rather than ambiguous, so explicit resume may issue exactly one new
  aggregate transcription call. It never fans out calls per chunk.
- A malformed `STOP` completion remains nonretryable because there is no provider evidence that truncation was
  the cause.
- Head SHA: `d00c6d8c8c205c292d08a19d2e5fd899b170b36a` (implementation commit; this evidence file is
  committed immediately afterward without changing implementation code).
