# Lane acceptance-matrix-runtime Results

## Status

committed

## Requirement IDs

- N1
- N2
- N3

## Branch

`agent/operational-mvp/acceptance-matrix-runtime`

## Worktree

`/home/dev/.codex/worktrees/my-data-hub/acceptance-matrix-runtime`

## Base SHA

`6b1cebdd1e81541669b66f63e6369905c58dcc11`

## Head SHA

Implementation commit: `24b5d4fcfbbb888e1553e3ee9283ef08df2841d0`

The final branch also contains the documentation-only RESULTS commit created after this
implementation SHA was known.

## Files changed

- `.github/workflows/provider-real.yml`
- `docs/operations/operational-kaggle-matrix.md`
- `scripts/provider/operational_kaggle_driver.py`
- `scripts/provider/operational_kaggle_matrix.py`
- `tests/provider/test_operational_kaggle_driver.py`
- `tests/provider/test_operational_kaggle_matrix.py`
- `.codex/lanes/acceptance-matrix-runtime/RESULTS.md`

## Commands run

- `python -m compileall -q scripts/provider/operational_kaggle_driver.py scripts/provider/operational_kaggle_matrix.py`
- `pytest -q tests/provider/test_operational_kaggle_driver.py tests/provider/test_operational_kaggle_matrix.py`
- `pytest -q tests/provider/test_operational_kaggle_driver.py tests/provider/test_operational_kaggle_matrix.py tests/provider/test_scheduled_acceptance.py tests/test_architecture_invariants.py`
- `ruff check scripts/provider/operational_kaggle_driver.py scripts/provider/operational_kaggle_matrix.py tests/provider/test_operational_kaggle_driver.py tests/provider/test_operational_kaggle_matrix.py`
- `python -m compileall src tests scripts`
- `python scripts/validate_repository.py`
- `ruff check .`
- `pytest`
- `pytest -q tests/provider/test_real_kaggle_matrix.py::test_provider_real_workflow_does_not_run_smoke_as_operational_acceptance tests/provider/test_operational_kaggle_matrix.py::test_provider_workflow_runs_operational_matrix_not_smoke_surrogate`
- YAML parse of `.github/workflows/provider-real.yml` with `yaml.safe_load`
- `git diff --check`

The Python/pytest/ruff commands used the shared environment at
`/home/dev/.codex/worktrees/my-data-hub/operational-mvp/.venv` because the isolated
lane worktree intentionally has no duplicate virtual environment.

## Tests / verification

- Focused operational driver/matrix tests: PASS after the final implementation changes.
- Focused operational + scheduled acceptance + architecture tests: PASS.
- Repository validator: PASS, 3,714 checks and zero errors.
- Full Ruff: PASS.
- Compileall: PASS.
- First full pytest run: 1,076 passed, 2 skipped, 1 failed. The sole failure was an
  existing workflow assertion requiring the literal upload path
  `artifacts/operational-kaggle-scenarios/`; that upload path was restored.
- The exact two workflow tests, including the formerly failing test: PASS after the fix.
- A second full pytest run was attempted, but concurrent test work exhausted the shared
  filesystem (`OSError: [Errno 28] No space left on device`, `/dev/vda2` at 100%) and
  the run was stopped. This is recorded as an environmental full-suite blocker rather
  than represented as a product failure.

## Risks

- No live provider acceptance run was executed. Typed receipts are required at runtime;
  unit fixtures and examples are not live evidence.
- The staged GitHub controller requires the configured provider owner, workload plan
  JSON, workload production config JSON, MCP/control credentials, and (only for an
  authorized FM16 continuation) the owner envelope secret.
- Continuation intentionally rejects commit/config drift and requires an exact prior
  run ID plus run attempt. Operators must retain/select that immutable artifact.
- The full post-fix suite could not complete only because the shared runner had zero
  free disk. Focused regression coverage, repository validation, compileall, and Ruff
  all passed after the final changes.

## Merge notes

- Cherry-pick implementation commit `24b5d4fcfbbb888e1553e3ee9283ef08df2841d0`
  and the following RESULTS commit.
- N1 routes FM04/FM07/FM08/FM09/FM12 through the same typed
  `acceptance.scenario.request/status` execute/reconcile path used by the closed master
  protocol. PASS is derived only from validated typed evidence and exact carrier/master/
  checkpoint projections.
- N2 restores durable matrix/controller artifacts, validates materialized evidence and
  data-workload inputs, preserves FM16 pause state, and re-probes completed zero-mutation
  blockers without turning a launch fence into a false resume.
- N3 adds no Kaggle token/client or matrix-local adapter. Provider reconciliation remains
  behind the existing MCP/control-owned central adapter; reader catalogs are unchanged.
