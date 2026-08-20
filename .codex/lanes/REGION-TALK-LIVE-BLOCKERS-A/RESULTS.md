# REGION-TALK-LIVE-BLOCKERS-A results

## Scope and revisions

- Lane: `REGION-TALK-LIVE-BLOCKERS-A`
- Assigned base SHA: `660b06abb1ec22825e3c5c2ca497cfa88804d9ee`
- Integrated sibling baseline used for final validation: `d67ec57`
- Lane implementation HEAD SHA: `a99ace8`
- Live mutation/deployment: **not performed**
- Publication dispatch: hard-pinned to `false`

## Requirement evidence

| ID | Result | Evidence |
|---|---|---|
| A1 | Done | Central journal v3 persists the original Dataset, Notebook, and cleanup intents before their provider effects. Dataset/Notebook response-loss tests restart the central adapter, retain original `requested_at`, reconcile exact provider versions/source, rebuild claims/receipt, and prove one physical mutation. |
| A2 | Done (code/config gate) | Endpoint/database and the non-secret Kaggle User Secret label are typed launch pins. The stable private `mdh-region-talk-supervisor` Notebook must be pre-provisioned with the reviewed User Secret attachment. Bootstrap reads it with `UserSecretsClient`, writes a mode-0600 service-account file, and exposes only its path to the YDB SDK. No credential is put in Dataset/status, SQLite, source, or logs. |
| A3 | Done | `DirectSnapshotRunner` refreshes at cycle, begin, pass/page, landing, and finalize boundaries. A replacement tunnel/DB connection is activated before the old one closes, while pass/table/cursor/page state stays in the runner. Focused test proves continuation from the exact cursor after a page-boundary rotation. |
| A4 | Done | `region_talk.pipeline.status` was removed from remote provider routing and is served by a read-only SQLite `mode=ro` query. Focused test proves no write gateway and no master resolution/wake. |
| A5 | Done | Activation now persists the revocation mailbox, repeats the idempotent certificate revocation after a crash, and records the new active generation last. Crash-injection replay test passes. |
| A6 | Done | Generated terminal posting retries the exact serialized receipt. The terminal endpoint accepts exact terminal/cleanup replay before active-master fencing and rejects conflicting replay. Focused HTTP replay test passes. |

The runtime continues to use one Kaggle adapter. The permanent, orchestrator-protected supervisor Notebook is versioned rather than task-deleted so its reviewed User Secret attachment remains bound. The task-specific status Dataset remains disposable.

## Validation

All commands ran from `/home/dev/.codex/worktrees/my-data-hub/operational-mvp` after sibling migration/stage commits were integrated.

- `.venv/bin/python -m pytest -q tests/region_talk tests/mcp/test_region_talk_contracts.py tests/provider/test_kaggle_adapter.py tests/test_control_plane.py` — PASS (one expected skip).
- `.venv/bin/python -m pytest -q` — PASS (full suite; four expected skips, only pre-existing `jsonschema.RefResolver` deprecation warnings).
- `.venv/bin/ruff check .` — PASS.
- `.venv/bin/python -m compileall -q src tests scripts` — PASS.
- `.venv/bin/python scripts/create_notebooks.py --check` — PASS, `drift: []`.
- `.venv/bin/python scripts/validate_repository.py` — PASS, 5,073 checks, zero errors/notes.
- `git diff --check` — PASS before implementation commit.

## Changed files

- `.env.example`
- `compose.control-plane.yaml`
- `docs/operations/region-talk-supervised-runtime.md`
- `src/my_data_hub/control_plane/{adapters.py,app.py}`
- `src/my_data_hub/mcp/control_gateway.py`
- `src/my_data_hub/providers/kaggle/adapter.py`
- `src/my_data_hub/workloads/region_talk/{central_launcher.py,direct_pipeline.py,direct_snapshot.py,pipeline_contracts.py,pipeline_runtime.py,production_assembly.py}`
- `tests/mcp/test_region_talk_contracts.py`
- `tests/provider/test_kaggle_adapter.py`
- `tests/region_talk/{test_direct_snapshot.py,test_long_run_authority.py,test_pipeline_core.py,test_production_assembly_response_loss.py}`
- `tests/test_control_plane.py`

## Remaining operational prerequisites and risks

- No production Kaggle/YDB/PostgreSQL action was run, so this is not live readiness or data-integrity evidence.
- Before enablement, an owner must provision the exact private `<owner>/mdh-region-talk-supervisor` Notebook, attach the reviewed viewer-only Kaggle User Secret, and supply observed production YDB endpoint/database pins. Missing or changed provider identity fails closed.
- The integration owner must invoke the sibling `RegionTalkPostImportSupervisor.execute_after_import()` seam immediately after a successful direct snapshot and require its typed receipt before terminal success; this lane intentionally did not edit the sibling-owned stage implementation or add a competing call.

---

# Follow-up audit A — offline closure, terminal revocation, database snapshots

## Scope and revisions

- Follow-up requested base SHA: `91e22ce`
- Concurrent stage-dispatch integration parent observed before commit: `d6f9ed5`
- Follow-up implementation SHA: `588438bfb4c11a0cad852bed0072cde23b74faf5`
- Results receipt SHA: recorded by the following commit
- Live mutation/deployment: **not performed**
- Publication dispatch: remains hard-pinned to `false`

## Requirement evidence

| ID | Result | Evidence |
|---|---|---|
| A1 | Done | Replaced the root-only YDB lock with canonical v2 containing the exact 14-wheel CPython 3.12/manylinux closure for `ydb==3.31.2`. Builder and independent verifier reject missing, extra, symlinked, malformed, or SHA-mismatched files, bind the manifest SHA into deployment environment and Region Talk launch metadata, and retain the exact root wheel assertion. Both generated master bootstrap paths SHA-locate every wheel in the same private Dataset, install each with `--no-index --no-deps` before application/YDB import, and attest installed versions. Local evidence verified all 14 committed hashes against the resolver-produced wheelhouse. No runtime index/image fallback is used. |
| A2 | Done | Terminal revocation now treats the durable mailbox as intent/delivery state, never as proof of the broker KRL/certificate effect. Each generation is broker-revoked idempotently before a terminal completion marker is atomically persisted. Crash tests prove exact replay both after mailbox persistence and after broker commit but before task persistence. |
| A3 | Done | `DirectYdbReader.run_snapshot_pass()` owns one `QuerySnapshotReadOnly` transaction for all five tables in Pass A and a second transaction for all five in Pass B. Table/page cursors remain bounded inside that pass. A mutation introduced between table reads is invisible to Pass A and visible to Pass B; the test asserts exactly two database transactions. |

## Validation

Commands ran from `/home/dev/.codex/worktrees/my-data-hub/operational-mvp`.

- `.venv/bin/python -m pytest -q tests/provider/test_build_master_assets.py tests/provider/test_master_runtime_bridge.py tests/region_talk/test_direct_pipeline.py tests/region_talk/test_direct_snapshot.py tests/region_talk/test_long_run_authority.py tests/region_talk/test_pipeline_core.py tests/region_talk/test_production_assembly_response_loss.py tests/test_control_plane.py -x` — PASS.
- `.venv/bin/python -m pytest -q tests/region_talk/test_snapshot_integrity_postgres.py -x` — expected skip without disposable PostgreSQL URL.
- `.venv/bin/python -m pytest -q tests/test_control_plane_deployment.py tests/control/test_control_runtime_wiring.py -x` — PASS (47 tests).
- `.venv/bin/python -m pytest -q tests/region_talk/test_long_run_authority.py -k 'terminal_revocation_crash'` — PASS (2 tests).
- `.venv/bin/python -m pytest -q tests/region_talk/test_direct_pipeline.py tests/region_talk/test_direct_snapshot.py -x` — PASS (10 tests).
- `.venv/bin/ruff check <all changed Python files>` — PASS.
- `.venv/bin/python -m compileall -q src tests scripts` — PASS.
- `.venv/bin/python scripts/create_notebooks.py --check` — PASS, `drift: []`.
- `.venv/bin/python scripts/validate_repository.py` — PASS, 5,100 checks, zero errors/notes.
- Local lock-to-wheelhouse SHA check — PASS, 14/14 exact files.
- `git diff --cached --check` — PASS before implementation commit.

A broader `pytest -q tests/region_talk -x` reached the concurrent stage-dispatch lane and
failed in `test_stage_dispatch.py::test_launch_restart_reconciles_one_effect_and_submits_exact_result`
because that lane's synthetic claim receipt hash was invalid before adapter execution. Root confirmed
that failure is C-owned. Per root's disk/concurrency direction, this lane did not run the final full
suite; root will run it after C settles.

## Changed files

- `.env.example`
- `compose.control-plane.yaml`
- `docs/operations/{master-asset-bundle.md,region-talk-supervised-runtime.md}`
- `examples/contracts/master-asset-bundle.v1.example.json`
- `schemas/master-asset-bundle.v1.schema.json`
- `scripts/provider/assets/master-ydb-wheel-lock.v1.json` (removed)
- `scripts/provider/assets/master-ydb-wheel-lock.v2.json` (added)
- `scripts/provider/{build_master_assets.py,verify_master_assets.py}`
- `src/my_data_hub/control_plane/app.py`
- `src/my_data_hub/providers/kaggle/master_runtime.py`
- `src/my_data_hub/workloads/region_talk/{central_launcher.py,direct_pipeline.py,direct_snapshot.py,pipeline_contracts.py,pipeline_runtime.py,production_assembly.py}`
- `tests/provider/{test_build_master_assets.py,test_master_runtime_bridge.py}`
- `tests/region_talk/{test_direct_pipeline.py,test_direct_snapshot.py,test_long_run_authority.py,test_pipeline_core.py,test_production_assembly_response_loss.py,test_snapshot_integrity_postgres.py}`
- `tests/test_control_plane.py`

## Remaining risks

- No production Kaggle/YDB/PostgreSQL action was performed; this is code/contract evidence only.
- The exact closure is pinned to the reviewed CPython 3.12/manylinux artifacts. Changing the runtime
  image ABI/platform requires a new lock and review rather than compatibility guessing.
- Final full-suite evidence is delegated to root after the concurrent stage-dispatch fixture is fixed.

---

# R15 provider assembly handoff

## Scope and revisions

- Requested integration base: `49a2348` / migration 0029.
- Actual clean integration base at edit start: `f7d6ea74fcd63bbaeca08e86882f1aa5ac3f4aee`.
- Implementation HEAD: recorded by the implementation commit containing this section.
- Live deployment/provider/YDB/PostgreSQL mutation: **not performed**.
- One provider adapter: the supervisor and stage assembly share the injected
  `KaggleProviderAdapter`; no second client is constructed.
- Schedule/publication/notification: remain `false`.

## Requirement evidence

| ID | Result | Evidence |
|---|---|---|
| R15-01 | Done | `create_app` assembles `CentralRegionTalkStageCredentialBroker`, `CentralRegionTalkStageNotebookAdapter`, and `RegionTalkStageDispatcher` from the same injected KPA and `DirectoryRegionTalkTaskAuthority`. |
| R15-02 | Done | Every dispatch uses unique protected/disposable capability Dataset and worker Notebook refs. The stage provider journal fsyncs original Dataset, Notebook, and delete intents plus original `requested_at` and source SHA before effects. A three-restart lost-response test proves exactly one Dataset create and Notebook push with exact readback reconciliation. |
| R15-03 | Done for transport; runtime matrix remains partial | Generated worker compiles, verifies source/image/commit/project wheel/canonical dependency manifest/every dependency wheel/model identity, installs offline with `--no-index --no-deps`, opens the task tunnel, and uses `PostgresStageWorkerFunctions` for direct migration 0028 fetch/submit. Rotation checkpoints use migration 0029. |
| R15-04 | Done | Supervisor claim-ready/bound/rotation callbacks and child attestation/rotation/terminal callbacks use strict typed metadata. Provider and dispatch journals fail closed on payload, input data, text, lease, database URL, task token, or lease-token hash. Private rotated access is a no-store capability response, not a status callback. |
| R15-05 | Done | App endpoints fence active master/epoch before stage capability dispatch. Exact claim/binding/source pins are revalidated before provider mutation. A focused authority test proves generation N remains active and unrevoked until the exact N+1 database receipt, then only N is revoked; exact replay is idempotent. |
| R15-06 | Done | Exact terminal replay is retained. Child credential revocation works for stage `claim` mailboxes, and the two exact task-owned provider resources are deleted with persisted cleanup intents. Focused test proves two deletes, one revocation, and no repeated cleanup on terminal replay. |
| R15-07 | Done | Unique resources are private, disposable and `ORCHESTRATOR_PROTECTED`; launch/callback models pin publication and notification false. Region Talk schedule remains disabled. |
| R15-08 | Done | Existing direct-cycle contract maps database `WAITING_WORK` to nonterminal `RETRYABLE`, dispatches one bounded child reconciliation, and maps only database `COMPLETE` to terminal success. Focused direct-pipeline tests pass. |

## Validation

Commands ran from `/home/dev/.codex/worktrees/my-data-hub/operational-mvp`:

- `.venv/bin/pytest -q tests/test_control_plane.py tests/region_talk/test_pipeline_core.py tests/region_talk/test_direct_pipeline.py tests/region_talk/test_stage_execution.py tests/region_talk/test_stage_dispatch.py tests/region_talk/test_long_run_authority.py tests/region_talk/test_private_stage_payload_v7.py tests/region_talk/test_stage_worker_rotation_v8.py tests/region_talk/test_production_assembly_response_loss.py` — PASS, 94 tests.
- `.venv/bin/ruff check src/my_data_hub/control_plane/app.py src/my_data_hub/workloads/region_talk/{production_assembly.py,central_launcher.py,stage_dispatch.py} tests/test_control_plane.py tests/region_talk/{test_stage_dispatch.py,test_long_run_authority.py}` — PASS.
- `.venv/bin/python -m compileall -q src tests` — PASS.
- `.venv/bin/python scripts/validate_repository.py` — PASS, 5,108 checks, zero errors/notes.
- `git diff --check` — PASS.
- Final full `pytest` intentionally left to root per disk/concurrency direction.

## Changed files

- `src/my_data_hub/control_plane/app.py`
- `src/my_data_hub/workloads/region_talk/{central_launcher.py,production_assembly.py,stage_dispatch.py}`
- `tests/region_talk/{test_long_run_authority.py,test_stage_dispatch.py}`
- `tests/test_control_plane.py`
- `docs/operations/region-talk-stage-execution.md`
- `.codex/lanes/REGION-TALK-LIVE-BLOCKERS-A/RESULTS.md`

## Honest runtime blockers

- `vector_fusion` is executable in-repository when exact current E5 and BGE receipts exist.
- `e5_embedding` and `bge_m3_embedding` authenticate their reviewed model IDs/revisions, but the
  Region Talk semantic-bank/model assets and attached runtime factories are not supplied by the
  current runtime Dataset. They return `FAILED_RETRYABLE`, not success.
- `image_scoring`, `final_verifier`, and `writer` likewise lack reviewed attached runtime/model
  assets and return `FAILED_RETRYABLE`.
- Consequently, the provider/credential/direct-DB assembly is implemented, but no claim of a live
  end-to-end `COMPLETE`, model quality, row count, or production readiness is made.
