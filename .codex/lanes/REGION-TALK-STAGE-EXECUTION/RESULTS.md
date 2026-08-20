# REGION-TALK-STAGE-EXECUTION results

## Identity

- Lane: `REGION-TALK-STAGE-EXECUTION`
- Base SHA: `660b06abb1ec22825e3c5c2ca497cfa88804d9ee`
- Implementation head SHA: `bd29401f40620ef91435ac1e2db13538abda144e`
- Implementation commit: `bd29401 feat(region-talk): execute typed post-import stage DAG`
- Live mutation: none

## Delivered

- Added the separate `RegionTalkPostImportSupervisor` with a fixed two-phase
  PREPARE -> pure transform -> COMMIT protocol.
- Added the exact fixed-function PostgreSQL port. Each call executes
  `SET LOCAL ROLE mdh_region_talk_pipeline` immediately before
  `migration.execute_region_talk_post_import_stages(uuid,uuid,jsonb)`; no generic SQL or DML
  port is exposed.
- Defined the deterministic eight-node DAG, UUIDv5 identities, exact stage contracts,
  dependencies, retry limits, timeouts, terminal statuses, immutable fingerprints, typed work
  requests, and typed terminal receipt.
- Added an executable pure queue-formation call site using `rank_review_queue`:
  imported legacy-selected candidates remain visible for operator review, while future missing
  or stale evidence is queued and cannot be accepted.
- Kept publication and notification dispatch false in every request, work item, queue outcome,
  and receipt.
- Replaced the directly required E5, BGE-M3, image, final-verifier, and writer notebook
  `NotImplementedError` shells with exact contract validation and honest
  `HEAVY_RUNTIME_NOT_ATTACHED` failures. The E5/BGE notebooks use existing exact repository
  model revisions and remain `production_ready=false`.
- Added operational documentation with the exact remaining heavyweight blockers.

## Verification evidence

Commands run from `/home/dev/.codex/worktrees/my-data-hub/operational-mvp`:

```text
.venv/bin/python -m compileall -q src tests
PASS

.venv/bin/pytest -q tests/region_talk
PASS (one environment-dependent skip)

.venv/bin/pytest -q tests/region_talk/test_stage_execution.py
9 passed

.venv/bin/ruff check \
  src/my_data_hub/workloads/region_talk/stage_execution.py \
  src/my_data_hub/workloads/region_talk/notebook_stages.py \
  tests/region_talk/test_stage_execution.py scripts/create_notebooks.py
All checks passed

.venv/bin/python scripts/create_notebooks.py --check
drift: []

.venv/bin/python scripts/validate_repository.py
5067 checks, 0 errors

git show --check --oneline bd29401
PASS
```

Focused tests prove:

- imported legacy-selected rows form and read back a typed review-queue receipt;
- missing E5/BGE evidence produces deterministic independent work requests;
- stale evidence is refreshed rather than accepted;
- retryable evidence and exhausted retry limits have distinct durable statuses;
- a future candidate reaches the review queue only with all exact-current evidence;
- required notebook shells cannot emit a fabricated heavyweight success;
- the SQL port calls only the fixed security-definer seam, in the required role order.

## Changed files

- `src/my_data_hub/workloads/region_talk/stage_execution.py`
- `src/my_data_hub/workloads/region_talk/notebook_stages.py`
- `tests/region_talk/test_stage_execution.py`
- `scripts/create_notebooks.py`
- `notebooks/README.md`
- `notebooks/20-region-talk-e5-enrichment/{worker.ipynb,kernel-metadata.example.json}`
- `notebooks/30-region-talk-bge-m3-enrichment/{worker.ipynb,kernel-metadata.example.json}`
- `notebooks/40-region-talk-image-diagnostic/{worker.ipynb,kernel-metadata.example.json}`
- `notebooks/50-region-talk-final-verifier/{worker.ipynb,kernel-metadata.example.json}`
- `notebooks/70-region-talk-writer/{worker.ipynb,kernel-metadata.example.json}`
- `docs/operations/region-talk-stage-execution.md`
- `.codex/lanes/REGION-TALK-STAGE-EXECUTION/RESULTS.md`

## Residual risks and blockers

1. Root integration must call the supervisor immediately after `DirectSnapshotRunner.run()` and
   require its typed receipt before terminal success. The shared `direct_pipeline.py` surface was
   explicitly outside this lane and remained owned by the root/A integration.
2. Migration 0026 owns the fixed function, durable run/stage receipt tables, work-item insertion,
   and queue persistence. This lane was tested against its coordinated JSON/UUID contract; the
   disposable PostgreSQL integration remains the migration owner's evidence.
3. No real Region Talk E5/BGE semantic-bank, image, final-verifier, or writer receipt exists.
   Image/final/writer exact donor model revisions and shadow-equivalence evidence remain absent.
4. Migration 0026 conservatively prepares heavyweight evidence as `MISSING`; production
   reconciliation of genuinely current evidence remains future work. The Python contract and
   tests support `CURRENT`, but no production PASS is claimed.
5. No live YDB, Kaggle, PostgreSQL, checkpoint, publication, or notification operation was run.
6. The shared integration worktree contained concurrent uncommitted A/B lane edits after this
   lane committed. None of the files listed above remained dirty; those sibling edits were not
   staged, reverted, or included by this lane.
