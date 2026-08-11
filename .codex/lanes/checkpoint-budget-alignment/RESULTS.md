# Lane checkpoint-budget-alignment Results

## Status

Committed by the integration owner.

## Requirement IDs

- F3-R integration dependency: declared checkpoint-stage maxima fit one admitted attempt.

## Branch

`integration/operational-mvp`

## Base SHA

`295aceb8e3dc5da834fdb1e1a41fb078c7cce82c`

## Delivered behavior

- One 5,400-second checkpoint attempt is now the explicit sum of two sequential
  1,200-second archive-command allocations, one 1,800-second independent verifier
  allocation, and 1,200 seconds reserved for provider/control upload, exact readback,
  metadata, and callback overhead.
- Production checkpoint construction uses those exact archive and verifier limits.
- Verifier runtime or polling policies above the allocation fail closed.
- The 10,800-second reserve remains two complete attempts.
- These values are conservative admission allocations. They are not live Kaggle timing
  evidence and do not claim that an already-running third-party SDK call can be
  interrupted at the deadline.

## Files changed

- `src/my_data_hub/runtime_sdk/lifetime.py`
- `src/my_data_hub/runtime_sdk/__init__.py`
- `src/my_data_hub/checkpoints/kaggle_runtime.py`
- `tests/master/test_notebook_entrypoint.py`
- `tests/provider/test_checkpoint_runtime_wiring.py`

## Verification

- Focused master/checkpoint provider tests: passed (32 tests).
- Ruff for changed modules/tests: passed.
- `python -m compileall -q src tests`: passed.

## Risks

- No live Kaggle checkpoint duration was measured. Real provider runs must demonstrate
  that the allocations are adequate; a timeout fails closed and preserves the last
  verified HEAD rather than proving zero RPO.
