# H6-EVIDENCE contract correction

- Base SHA: `764ffc31f58bad015f28d3a5fefc784e91f67ca6`
- Tested implementation SHA: `6b562e7646b5fff653c4bfa4495227ecb1193176`
- Branch: `agent/operational-mvp/h6-evidence-correction`

## Result

- `NotebookLifecycleRequest` now admits FM01 and FM03 evidence Notebooks in addition to FM02/FM06/FM22/FM23.
- `AcceptanceCleanupRequest` admits the same scenarios, preserving exact two-phase cleanup through `COMPLETE`.
- `claim_get` uses the shared complete evidence-scenario allowlist, including FM01 and FM03.
- FM01 and FM22 tests derive distinct deterministic Dataset and Notebook `task_id` values under the same scenario and prove both terminal claims remain independently readable.
- FM03 is tested through Notebook lifecycle, exact claim read, and exact cleanup.
- Reuse of one `(scenario_id, task_id)` with a changed request remains an `IdempotencyConflict`.
- No driver files were changed.

## Gates

Passed from the correction worktree:

- Ruff over the full repository.
- `python -m compileall -q src tests scripts`.
- `scripts/validate_repository.py`: 3289 checks, zero errors.
- `scripts/create_notebooks.py --check`: zero drift.
- Full `pytest -q`: 100%, only the two pre-existing `jsonschema.RefResolver` deprecation warnings.
- `git diff --check`.

## Changed files

- `src/my_data_hub/control_plane/acceptance_evidence.py`
- `tests/control/test_acceptance_evidence.py`
- `docs/operations/acceptance-evidence-control-plane.md`
- `.codex/lanes/H6-EVIDENCE-CORRECTION/RESULTS.md`
