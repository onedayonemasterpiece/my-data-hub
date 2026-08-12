# Lane checkpoint-acceptance-mount-discovery Results

## Status
committed

## Requirement IDs
- R01: Remove fixed Kaggle input slug paths from FM05/FM14/FM15 production launch.
- R02: Remove the fixed verifier slug path from the central FM15 broker.
- R03: Add executable normalized-mount and fail-closed negative coverage.

## Branch
`agent/checkpoint-acceptance-mount-discovery/oauth-matrix-refresh`

## Worktree
`/home/dev/.codex/worktrees/my-data-hub/checkpoint-acceptance-mount-discovery`

## Base SHA
`fc7e2d90426bbc74bf2c04eef95998ca61c51a3d`

## Head SHA
Recorded in the final handoff after commit.

## Files changed
- `src/my_data_hub/acceptance/checkpoint_launcher.py`
- `src/my_data_hub/checkpoints/acceptance_broker.py`
- `tests/acceptance/test_checkpoint_launcher.py`
- `tests/acceptance/test_checkpoint_acceptance_broker.py`
- `docs/operations/checkpoint-acceptance-production.md`
- `.codex/lanes/checkpoint-acceptance-mount-discovery/RESULTS.md`

## Commands run
- focused checkpoint acceptance pytest suites
- Ruff on changed Python files
- `python -m compileall src tests`
- repository/schema/notebook validators
- full `pytest`

## Tests / verification
Executable generated-source tests cover normalized mount names for FM05, FM14,
and FM15. Negative cases cover ambiguity, wrong claim set, symlinks and oversized
files before status bootstrap/action. The central FM15 broker test proves the
real provider intent keeps exact numeric Dataset sources and no fixed mount path.

## Risks
The rendered provider script intentionally performs bounded tree hashing before
any acceptance action. Large checkpoint template trees can add preflight time,
but bounds match the existing 100 caller files plus two provider metadata files
and the 20 GiB checkpoint ceiling.

## Merge notes
Cherry-pick the final lane commit onto the integration branch. No app, deploy,
embedding, master-runtime, or YDB files were changed.
