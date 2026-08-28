# Lane PUBLISH-V2 Results

## Status

committed

## Requirement IDs

- R07

## Branch

`agent/voice-intake-v2/publish-v2`

## Worktree

`/home/dev/.codex/worktrees/my-data-hub/voice-intake-v2-publish`

## Base SHA

`56783c6502f3453567c517894ff09e7190a243a8`

## Head SHA

Implementation head before this lane-receipt commit:
`0df91a589b5b737b3477b29ed49adc98cd603cf0`

## Files changed

- `src/my_data_hub/voice_intake/runtime.py`
- `src/my_data_hub/voice_intake_v2/__init__.py`
- `src/my_data_hub/voice_intake_v2/markdown.py`
- `src/my_data_hub/voice_intake_v2/publisher.py`
- `src/my_data_hub/voice_intake_v2/runtime.py`
- `src/my_data_hub/voice_intake_v2/worker.py`
- `tests/voice_intake_v2/test_markdown.py`
- `tests/voice_intake_v2/test_publisher.py`
- `tests/voice_intake_v2/test_runtime.py`
- `tests/voice_intake_v2/test_worker.py`
- `.codex/lanes/PUBLISH-V2/RESULTS.md`

## Commands run

- `.venv/bin/pytest -q tests/voice_intake tests/voice_intake_v2`
- `.venv/bin/ruff check src/my_data_hub/voice_intake/runtime.py src/my_data_hub/voice_intake_v2 tests/voice_intake_v2`
- `.venv/bin/mypy --strict src/my_data_hub/voice_intake_v2/markdown.py src/my_data_hub/voice_intake_v2/publisher.py src/my_data_hub/voice_intake_v2/runtime.py src/my_data_hub/voice_intake_v2/worker.py`
- `.venv/bin/python -m compileall -q src/my_data_hub/voice_intake src/my_data_hub/voice_intake_v2 tests/voice_intake tests/voice_intake_v2`
- `git diff --check`
- Rendered a representative v2 projection into the checked-out IdeaHub registry at
  `70b24b52bcfa6dac0909d169a991755a8805f9e5` and validated the resulting registry with
  `schemas/intake-session.schema.json` using `Draft202012Validator` and `FormatChecker`.

## Tests / verification

- Voice v1 and v2 targeted suite: all tests passed (46 tests at final lane run).
- Ruff: passed.
- Strict focused mypy: passed.
- Focused compileall: passed.
- IdeaHub registry validation: 0 schema errors.
- Tests prove one four-path Git tree, exact candidate-commit readback for all four
  artifacts, immutable current-main source/detail, current-main registry/index markers,
  recovery of the original atomic publication commit from source-path history, explicit
  ambiguous-outcome fencing, durable verified receipt before purge, and GitHub retry
  without replaying either inference stage.
- Production assembly now calls `attach_configured_voice_intake_v2`; the existing v1
  `attach_voice_intake_routes(create_app(settings))` assembly remains byte-for-byte in
  the same nesting position.

## Risks

- No live GitHub write was performed in this isolated lane. Deployment/live acceptance
  remains the integrator's responsibility.
- Publication-history reconciliation intentionally requires evidence that the source
  creation commit changed exactly source, detail, registry and voice index. An older or
  externally created partial publication is fenced as `reconciliation_required` rather
  than accepted optimistically.
- `registered_at` is the durable session `ended_at` value so retries after restart render
  byte-identical artifacts; it is not a nondurable worker clock timestamp.

## Merge notes

- Commits:
  - `a251649d71791a6a8ddce01651469abd1e10db4e` — v2 Markdown, publisher, readback,
    receipt/retry handling and tests.
  - `0df91a589b5b737b3477b29ed49adc98cd603cf0` — configured production assembly and
    disabled-spool guard.
- Reuses the existing v1 bounded GitHub transport, registry insertion/schema validator,
  voice-index renderer and path helper without modifying v1 GitHub or Markdown modules.
