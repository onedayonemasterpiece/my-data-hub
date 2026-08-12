# Lane broker-live-canary Results

## Status

committed

## Requirement IDs

- R01 — disposable live orchestration
- R02 — credential-free producer and direct signed PUT
- R03 — central exact Dataset finalization
- R04 — independent exact-version verifier
- R05 — claim-bound cleanup and inventory absence
- R06 — secret-free, fake-proof receipt
- R07 — one repository Kaggle adapter and established custom-state pattern
- R08 — schema/example, focused tests, and operator documentation

## Branch

`agent/operational-mvp/broker-live-canary`

## Worktree

`/home/dev/.codex/worktrees/my-data-hub/broker-live-canary`

## Base SHA

`1b481b37a6d88e0a3659f7159987c9e665343b34`

## Head SHA

Final lane commit (`git rev-parse HEAD`); this Results file is part of that commit.

## Files changed

- `scripts/provider/broker_live_canary.py`
- `tests/provider/test_broker_live_canary.py`
- `schemas/broker-live-canary-receipt.v1.schema.json`
- `examples/contracts/broker-live-canary-receipt.v1.example.json`
- `docs/operations/brokered-checkpoint-upload.md`
- `.codex/lanes/broker-live-canary/RESULTS.md`

## Commands run

- `python3 -m compileall -q src tests scripts/provider/broker_live_canary.py`
- `.venv/bin/pytest -q tests/provider/test_broker_live_canary.py`
- `.venv/bin/pytest -q tests/provider/test_broker_live_canary.py tests/provider/test_kaggle_brokered_adapter.py tests/control/test_brokered_checkpoint_upload.py`
- `.venv/bin/pytest -q`
- `.venv/bin/ruff check scripts/provider/broker_live_canary.py tests/provider/test_broker_live_canary.py`
- `.venv/bin/python scripts/validate_repository.py`
- `git diff --check`

## Tests / verification

- Focused canary tests: 3 passed.
- Broker/adapter focused set: 24 passed.
- Full repository suite: passed (3 skipped; 2 pre-existing `jsonschema.RefResolver` deprecation warnings).
- Repository/schema/notebook validation: 4,020 checks, 0 errors.
- Compileall, Ruff, and whitespace gates passed.
- No live mutation was performed in this lane, as requested.

## Risks

- The provider's signed upload capability is intentionally embedded in the private
  producer source until that disposable Notebook is deleted. It is never written to
  custom state, status, stdout, or the public receipt.
- If a provider mutation becomes ambiguous before the repository adapter can return its
  exact claim, the runner fails closed rather than performing an unclaimed slug delete.
  The secret-free state and append-only provider journal remain the recovery evidence.
- Operational acceptance remains pending the root-owned deploy and live execution.

## Merge notes

- Cherry-pick the single lane commit onto the newer integration head.
- Source scope is isolated; no `src/`, migrations, notebooks, deployment configuration,
  or provider adapter code changed.
- Run only from a clean commit with private ledger/state/receipt paths outside the
  checkout and the existing central `provider.env` loaded. Use the documented
  `uv run --no-project --with-editable . --with kaggle==2.2.4` form: it supplies the
  pinned optional SDK without generating an untracked `uv.lock` that would defeat the
  clean-commit gate.
