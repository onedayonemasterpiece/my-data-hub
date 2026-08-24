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

## R15 heavy-dispatch follow-up (2026-08-20)

### Identity

- Follow-up base SHA: `91e22ce6b43c421dc8a8b6f05c206c82a118f444`
- Integrated prerequisite SHA at commit time: `9373f2f1d86a437adbae4f0c454eeabb1c41da47`
- Follow-up implementation SHA: `7812f2a`
- Live/provider mutation: none

### Delivered

- Added `RegionTalkStageDispatcher`, a task/master/epoch-bound controller for migration 0027's
  fixed claim, submit, and status functions. Every PostgreSQL call performs
  `SET LOCAL ROLE mdh_region_talk_pipeline`; no generic SQL surface exists.
- Added database-selected, dependency-ready claims; deterministic database UUIDv5 effect
  verification; fixed stage-to-private-Notebook mapping; one injected provider-adapter seam;
  and a mode-0600, fsync/rename metadata-only dispatch journal.
- Persisted the exact claim/launch before provider effects. Restart observes the same effect,
  ambiguous observation fails closed, response loss replays the identical result submission,
  and an expired lease remains nonterminal for the database-owned next attempt.
- Added bounded common worker-result metadata and generic Notebook result reconciliation.
  Subject, revision, input fingerprint, result hash, producer identity, attempt, lease, task,
  master, and epoch must all agree before result landing.
- Preserved `WAITING_WORK`, `EMPTY`, and `WAITING_DEPENDENCY` as nonterminal. Only explicit
  database `COMPLETE` can produce the dispatch `COMPLETE` receipt.
- Implemented the pure vector-fusion worker using `fuse_vector_evidence` and exact-current E5/BGE
  upstream receipts. Added generated private Notebook 35 for this stage.
- Added a fixed attached-runtime seam for E5/BGE/image/final-verifier/writer. Exact input hashes
  are checked before invocation; missing runtime or private image artifact stays retryable and
  cannot become successful evidence.
- Kept publication and notification false and left every stage template
  `production_ready=false`; no schedule/provider action was enabled.

### Verification evidence

```text
.venv/bin/pytest -q \
  tests/region_talk/test_stage_dispatch.py \
  tests/region_talk/test_stage_execution.py
19 passed

.venv/bin/ruff check \
  src/my_data_hub/workloads/region_talk/stage_dispatch.py \
  src/my_data_hub/workloads/region_talk/notebook_stages.py \
  tests/region_talk/test_stage_dispatch.py \
  tests/region_talk/test_stage_execution.py scripts/create_notebooks.py
All checks passed

.venv/bin/python -m compileall -q \
  src/my_data_hub/workloads/region_talk \
  tests/region_talk/test_stage_dispatch.py \
  tests/region_talk/test_stage_execution.py
PASS

.venv/bin/python scripts/create_notebooks.py --check
drift: []

.venv/bin/python scripts/validate_repository.py
5100 checks, 0 errors

git diff --cached --check
PASS before implementation commit
```

Focused tests cover nonterminal waiting/dependency states, exact Notebook mapping, cross-epoch
rejection, database-receipt tamper rejection, provider restart reconciliation, result response
loss/replay, lease timeout and next-attempt effect identity, generic Notebook output tamper
rejection, attached-runtime success, unavailable-runtime retry, pure vector fusion, and the three
fixed PostgreSQL functions in the required role order.

### Follow-up changed files

- `src/my_data_hub/workloads/region_talk/stage_dispatch.py`
- `src/my_data_hub/workloads/region_talk/notebook_stages.py`
- `tests/region_talk/test_stage_dispatch.py`
- `tests/region_talk/test_stage_execution.py`
- `scripts/create_notebooks.py`
- `notebooks/20-region-talk-e5-enrichment/worker.ipynb`
- `notebooks/30-region-talk-bge-m3-enrichment/worker.ipynb`
- `notebooks/35-region-talk-vector-fusion/{worker.ipynb,kernel-metadata.example.json}`
- `notebooks/40-region-talk-image-diagnostic/worker.ipynb`
- `notebooks/50-region-talk-final-verifier/worker.ipynb`
- `notebooks/70-region-talk-writer/worker.ipynb`
- `docs/operations/region-talk-stage-execution.md`
- `.codex/lanes/REGION-TALK-STAGE-EXECUTION/RESULTS.md`

### Exact residual heavy-worker blockers

1. No live Region Talk semantic-bank E5 or BGE-M3 result exists. Although the exact model
   revisions are pinned, the Region Talk semantic-bank runtime/assets and an exact private-run
   receipt are not attached.
2. No verified private image artifact exists in the migrated canonical rows observed by
   migration 0027. Image work therefore returns a retryable unavailable result rather than
   fabricating analysis.
3. The image diagnostic, final verifier, and writer donor implementations, exact model/runtime
   revisions, and shadow-equivalence receipts remain unavailable. Their attached-runtime seams
   cannot be marked production ready.
4. Vector fusion is executable but remains dependency-blocked until both exact-current E5 and
   BGE results land.
5. The production central assembly still must inject the concrete private-Kaggle stage adapter
   and drive the dispatcher while the post-import stage run is waiting. A `WAITING_WORK` receipt
   must not be mapped to cycle completion or terminal success.
6. No live Kaggle/YDB/PostgreSQL/checkpoint cycle was performed. Scheduling remains disabled;
   no production row count, provider run, checkpoint, or readiness claim is made.

## R15 private-payload and rotation correction (final lane state)

### Identity

- Correction base: `a550c2e`
- Migration 0028 prerequisite: `ab470cc`
- Migration 0029 prerequisite: `2983b22`
- Lane commits: `a02b86d`, `5adb112`, `49a2348`
- Final implementation head before this evidence receipt: `49a2348`
- Live/provider mutation: none

### Delivered

- Corrected the control/data split after review found that the 0027 claim payload contained
  canonical text. The supervisor now has exact 0028 metadata-only claim/bind/status ports, while
  private child workers alone have exact payload-fetch/result-submit ports. Every fixed call
  executes `SET LOCAL ROLE mdh_region_talk_pipeline` in the same transaction.
- The central `StageWorkerLaunch` and dispatch journal contain only bounded identities, hashes,
  stage/attempt/timeout fields and provider references. They reject payload, input data, text,
  raw lease and database URL keys. Receipt hashes and task/work/effect/dispatch UUIDs are verified
  before any adapter effect.
- Added direct private-worker execution: payload is fetched from the ACTIVE master using the
  separately registered child task; exact subject/revision/input hashes are validated; attached
  runtime output is hashed and submitted directly. Missing runtime/artifact lands
  `FAILED_RETRYABLE`; invalid exact input lands `FAILED_TERMINAL`; neither is success.
- Added the child credential command/registration handshake in the private capability directory.
  Command replay is deterministic and control-visible status contains only credential IDs,
  generations and hashes, never the credential or DB URL.
- Added exact migration 0029 rotation models/port and N -> N+1 command replay. Private execution
  exposes bounded checkpoints before fetch and before submit so a replacement tunnel/connection
  can be installed and the final result submitted with the current binding. Prior-generation
  fencing remains database-owned.
- Added `PrivateSupervisorStageCoordinator`, which replays one deterministic `claim_request_id`
  while the child registration is pending, binds only the exact registered child, and advances
  the sequence only after the bound dispatch callback.
- Fixed the direct supervisor lifecycle: the snapshot imports once, `WAITING_WORK` remains
  `RETRYABLE`, later cycles only re-run post-import PREPARE/COMMIT, DB `FAILED` is terminal failed,
  and only DB `COMPLETE` can return complete. A metadata reconciler is called only while waiting.
- Publication/notification stay false; Region Talk scheduling stays disabled.

### Verification evidence

```text
.venv/bin/ruff check <changed Region Talk runtime/tests>
All checks passed

.venv/bin/python -m compileall -q \
  src/my_data_hub/workloads/region_talk \
  tests/region_talk/test_stage_dispatch.py \
  tests/region_talk/test_long_run_authority.py \
  tests/region_talk/test_direct_pipeline.py
PASS

.venv/bin/pytest -q \
  tests/region_talk/test_stage_dispatch.py \
  tests/region_talk/test_long_run_authority.py \
  tests/region_talk/test_direct_pipeline.py \
  tests/region_talk/test_stage_execution.py \
  tests/region_talk/test_pipeline_core.py
46 passed

git diff --check
PASS

.venv/bin/python scripts/create_notebooks.py --check
drift: []

.venv/bin/python scripts/validate_repository.py
5108 checks, 0 errors
```

Tests prove exact 0028/0029 fixed-function names and role order, metadata receipt tamper
rejection, deterministic restart without duplicate launch, absence of business/capability bytes
from the journal and child command, direct result success/retry semantics, child registration ACK
gating, successive command generations, rotation binding use before submit, repeated claim request
identity while pending, import-once polling, nonterminal waiting, and terminal DB failure.

### Correction changed files

- `src/my_data_hub/workloads/region_talk/stage_dispatch.py`
- `src/my_data_hub/workloads/region_talk/notebook_stages.py`
- `src/my_data_hub/workloads/region_talk/production_assembly.py`
- `src/my_data_hub/workloads/region_talk/direct_pipeline.py`
- `src/my_data_hub/workloads/region_talk/pipeline_runtime.py`
- `src/my_data_hub/control_plane/app.py`
- `tests/region_talk/test_stage_dispatch.py`
- `tests/region_talk/test_long_run_authority.py`
- `tests/region_talk/test_direct_pipeline.py`
- `docs/operations/region-talk-stage-execution.md`
- `.codex/lanes/REGION-TALK-STAGE-EXECUTION/RESULTS.md`

### Exact residual heavy-worker blockers

1. **Concrete central provider assembly is not implemented in this lane.** The existing injected
   `KaggleProviderAdapter` is not yet wrapped by a `CentralRegionTalkStageNotebookAdapter`, and
   the supervisor claim-ready/bound plus worker attestation/rotation callbacks are not installed
   in the control app. Therefore this lane does not claim end-to-end R15 closure.
2. The required adapter must persist only original metadata intents/hashes, reconcile before
   launch after response loss, create a unique private disposable orchestrator-protected
   Dataset/Notebook for the exact dispatch, attest exact source/image/wheel before releasing the
   child capability, and revoke every child credential/resource at terminal cleanup. A second
   Kaggle client/adapter is forbidden.
3. No real Region Talk semantic-bank E5/BGE, private image, final-verifier or writer runtime and
   exact producer receipt is attached. Vector fusion is executable only after both current
   embeddings land. Notebook templates remain `production_ready=false`.
4. No live Kaggle/YDB/PostgreSQL/checkpoint proof was run. Schedule, publication and notification
   remain disabled. `WAITING_WORK` is deliberately nonterminal until the missing adapter and live
   heavy-worker evidence exist.
