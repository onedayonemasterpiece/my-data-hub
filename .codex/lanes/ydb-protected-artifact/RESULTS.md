# Lane ydb-protected-artifact Results

## Status
committed

## Requirement IDs
- R01 versioned Pydantic manifest contract and generated JSON Schema
- R02 detached metadata-only sealing receipt and generated JSON Schema
- R03 deterministic batch/snapshot/source-revision binding
- R04 viewer-only principal/access-binding and write-denial metadata
- R05 strict positive inventory reconciliation without a source-count literal in the exporter contract
- R06 two independent exact ordered file/logical/accounting proofs
- R07 owner-only filesystem validation and tamper/cleanup tests
- R08 ACTIVE-master-only protected artifact loader/import adapter
- R09 existing live YDB direct path and source read-only behavior preserved
- R10 operations documentation, repository validation and full gates

## Branch
agent/operational-mvp/ydb-protected-artifact

## Worktree
/home/dev/.codex/worktrees/my-data-hub/ydb-protected-artifact

## Base SHA
5589629b3449998cdc1459855f2bbabe19927378

## Head SHA
d239e0d (content commit; this RESULTS update is included by amend)

## Files changed
- `src/my_data_hub/workloads/bloggers/protected_artifact.py`
- `scripts/provider/read_only_ydb_blogger_export.py`
- `src/my_data_hub/workloads/bloggers/master_stage.py`
- `src/my_data_hub/master_runtime/notebook_entrypoint.py`
- two generated schemas, tests, validator wiring, `.env.example`, and operations documentation

## Commands run
- `uv run pytest` targeted protected-artifact/exporter/reader/import/master tests
- `uv run ruff check ...`
- `uv run python -m compileall src tests`
- `uv run python scripts/validate_repository.py`
- full `uv run pytest`

## Tests / verification
- Targeted artifact/exporter/reader/import tests pass.
- Repository validator passes 3838 checks before final additions; rerun recorded in final handoff.
- Full `pytest -q` passes with 3 skips and only the two pre-existing
  `jsonschema.RefResolver` deprecation warnings.

## Risks
- No live export/import was executed. The source remains externally STOPPED with effective RCU 0.
- `notebook_entrypoint.py` conflicts with independently advancing Gate K work; root integration order is broker -> Gate K -> this lane and must preserve both semantics.
- Protected bytes are permitted only under `/kaggle/working`, removed after successful external metadata-receipt delivery, and never enter a devstand/control database.

## Merge notes
Cherry-pick the lane commit after Gate K. Resolve only the small notebook entrypoint import/helper/call-site overlap; do not replace Gate K lifecycle changes.
