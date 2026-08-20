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
