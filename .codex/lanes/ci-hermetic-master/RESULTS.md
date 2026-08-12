# Lane CI-HERMETIC-MASTER Results

## Status

committed

## Requirement IDs

- CI-CONTRACTS

## Branch

`agent/operational-mvp/ci-hermetic-master`

## Worktree

`/home/dev/.codex/worktrees/my-data-hub/ci-hermetic-master`

## Base SHA

`000d64486e12336d92f4ef82ae8618b2fbacc28f`

## Files changed

- `tests/master/test_notebook_entrypoint.py`
- `.codex/lanes/ci-hermetic-master/RESULTS.md`

## Commands run

- Reproduced the three hosted-CI failures locally against the live public control endpoint.
- Ran the exact three failing test cases after the fix.
- Ran repository validation, compileall, Ruff, diff check and the full pytest suite.

## Tests / verification

- Exact previously failing cases: 3 passed.
- Full pytest: passed (3 expected skips; existing jsonschema deprecation warnings only).
- Repository validator: 4,154 checks, zero errors or notes.
- Compileall, Ruff and `git diff --check`: passed.

## Risks

None. Production runtime behavior is unchanged. The tests now stub every optional
control poll that precedes the outcome under test, so a live public deployment
cannot make the unit suite non-hermetic.

## Merge notes

Cherry-pick the single lane commit onto `integration/operational-mvp`, push PR #5,
and require hosted `contracts` plus `postgres-integration` to pass.
