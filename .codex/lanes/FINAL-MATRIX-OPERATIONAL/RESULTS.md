# Lane FINAL-MATRIX-OPERATIONAL Results

## Status
committed

## Requirement IDs
- FM01–FM24: exact ordered operational scenario contracts and typed receipts
- LC01: at least 15 distinct exact numeric provider run refs and kernel IDs
- LC02: at least 3 boots, 2 clean rotations, abrupt termination, control restart, host reboot, and 60–90 minute soak
- SAFE01: modern-token gate exits 78 before plan/ledger/adapter/driver/provider mutation
- EVID01: fake/injected paths cannot emit live PASS; task UUIDs and platform smoke are never counted

## Branch
agent/operational-mvp/final-matrix-operational

## Worktree
`/home/dev/.codex/worktrees/my-data-hub/final-matrix-operational`

## Base SHA
`97c40e4e0ba28b38c69a5f028748432d3475092f`

## Head SHA
This file is part of the lane head; the exact commit SHA is reported in the handoff.

## Files changed
- `.github/workflows/provider-real.yml`
- `scripts/provider/operational_kaggle_matrix.py`
- `schemas/provider/operational-kaggle-*.v1.schema.json`
- `examples/provider/operational-kaggle-*.v1.example.json`
- `tests/provider/test_operational_kaggle_matrix.py`
- `tests/provider/test_real_kaggle_matrix.py`
- `docs/operations/operational-kaggle-matrix.md`
- `docs/operations/real-kaggle-matrix.md`
- `.codex/lanes/FINAL-MATRIX-OPERATIONAL/RESULTS.md`

## Commands run
- `uv sync --extra dev --extra kaggle`
- `uv run pytest -q tests/provider/test_operational_kaggle_matrix.py`
- `uv run pytest -q tests/provider`
- `uv run pytest -q`
- `uv run python scripts/validate_repository.py`
- `uv run python -m compileall -q src tests scripts`
- `uv run ruff check .`
- `uv run mypy`
- `.venv/bin/python -m pytest -q tests/provider/test_operational_kaggle_matrix.py tests/provider/test_real_kaggle_matrix.py`
- `.venv/bin/ruff check scripts/provider/operational_kaggle_matrix.py tests/provider/test_operational_kaggle_matrix.py tests/provider/test_real_kaggle_matrix.py`
- credential-free CLI preflight (observed exit 78)
- `git diff --check`

## Tests / verification
- Full pytest: PASS (one pre-existing skipped test; two jsonschema deprecation warnings).
- Repository validator: PASS, 3,122 checks, zero errors.
- Compileall: PASS.
- Ruff: PASS.
- Configured strict mypy target set: PASS.
- Provider suite: PASS.
- Operational contract examples validate against strict Draft 2020-12 schemas.
- No live Kaggle run was attempted or claimed in this lane.

## Requirement closure

| Requirement | Implementation status | Live evidence status |
|---|---|---|
| FM01–FM24 | Done: exact plan entries, assertion sets, per-scenario typed receipts, driver request/result and exact Notebook output contracts | BLOCKED: trusted privileged production operational driver is not present/configured |
| LC01 | Done: PASS counts only unique numeric `provider_run_ref` and `provider_kernel_id`; minimum 15 | BLOCKED with FM01–FM24 |
| LC02 | Done: executable aggregate gates and fail-closed validators | BLOCKED with FM01–FM24 |
| SAFE01 | Done | Verified locally without credentials (exit 78, no adapter/ledger/plan) |
| EVID01 | Done | Verified by fake/injection tests |

## Risks
- The exact privileged callable for host reboot, process termination, control-plane restart, checkpoint corruption, full YDB import, and E5/BGE-M3 submissions is absent from this repository. The runner therefore emits concrete per-scenario BLOCKED dependencies and exits 78 until `MY_DATA_HUB_OPERATIONAL_DRIVER_JSON` points to that trusted executable.
- A driver PASS is only a locator. Acceptance still requires reconciliation via the single real `KaggleProviderAdapter`, exact terminal numeric run identity, and exact downloaded `operational-result.json` assertions.
- No live credentials or production mutations were available/used; no live readiness claim is made.

## Merge notes
- Cherry-pick the lane head as one commit.
- The old platform-smoke script remains available only as a local diagnostic, but `provider-real.yml` no longer runs it as acceptance.
- Do not mark the operational acceptance complete until the trusted driver is configured and all 24 live receipts plus lifecycle aggregates PASS.
