# REGION-TALK-ASSEMBLY results

- Lane: `REGION-TALK-ASSEMBLY`
- Requirements: R03, R04, R05, R06, R07
- Base SHA: `2dcd7797a9600c412e6d7a4b6040aba025466433`
- Implementation head SHA: `9237220b448dd607f90ab1f39f8fe89e55c4b7cb`

## Implemented

- Production-only Region Talk coordinator wiring uses the exact shared control SQLite ledger and the one injected central `KaggleProviderAdapter`.
- Separate deterministic private Kaggle status Dataset + pending-attestation supervisor Notebook launcher with exact numeric runtime Dataset ref, immutable image/source/wheel pins, provider-effect idempotency, persisted secret-free launch/claim journal, and revocation-before-delete cleanup.
- Generic task-worker credential GET/POST routes implement the exact `task_credentials.py` schemas for the dedicated `region_talk` worker kind; task token, command hash, generation, ACTIVE master and epoch remain bound.
- Attestation/running/terminal callbacks validate the private task token and exact task/request/master/epoch/source/image bindings before the Notebook materializes DB access; callbacks carry metadata only.
- Lightweight 30-second reconciler: requests a master for WAITING_MASTER, launches only on exact ACTIVE binding, schedules only behind a separate default-false flag, and hard-disables publication dispatch.
- Remote MCP routes `region_talk.pipeline.status/run` through the authenticated internal gateway; run is present only when the Region Talk assembly flag/controller are enabled. Operator scope is `region-talk:operate`, reads use `region-talk:read`.
- Private direct executor scans exactly five allowlisted YDB tables. Each pass/table is captured in one ordered `SnapshotReadOnly` transaction (in memory, no source-row files/control callbacks), rejects NULL/non-monotonic keys, and lands through fixed v2 migration functions under the dedicated role.
- Deploy profile adds the central-only private state mount, default-off pipeline/schedule flags, runtime pin inputs, OpenCode/ChatGPT OAuth scopes, and keeps provider-only profile unchanged.

## Safety and current limitation

- `publication_dispatch` is always false in request, launch, execution, callback and MCP results.
- The first supervised canary is bounded to 180 seconds and requires the credential to outlive that bound by 15 seconds. The generic master supports credential generations, but an in-run generation refresh/reconnect is not yet wired. Therefore this lane does **not** claim that the full 58k-row live migration or autonomous scheduled operation has completed; schedule remains disabled until a supervised run proves completion or refresh/continuation is added.
- Direct snapshot pages/functions are idempotent for an exact task/export batch, but the current control state machine does not automatically relaunch a cleaned/timed-out task. A timeout needs an explicitly supervised continuation decision; do not claim it as autonomous recovery.

## Evidence / commands

- `bash -n deploy/control-plane/install.sh`
- `.venv/bin/python -m compileall -q src tests`
- `.venv/bin/pytest -q tests/region_talk/test_direct_pipeline.py tests/region_talk/test_pipeline_core.py tests/mcp/test_region_talk_contracts.py tests/mcp/test_remote_runtime.py tests/control/test_control_runtime_wiring.py` -> passed
- `.venv/bin/pytest -q` -> reached 99%; all owned-lane tests passed. Remaining failures were concurrent uncommitted 0024 snapshot-hardening tests (4) and its pending post-deploy tool schema update (1). Those files are explicitly outside this lane and were not committed here.
- `git diff --check`

## Changed files owned by this lane

- `compose.control-plane.yaml`
- `deploy/control-plane/install.sh`
- `src/my_data_hub/control_plane/app.py`
- `src/my_data_hub/mcp/control_gateway.py`
- `src/my_data_hub/mcp/runtime.py`
- `src/my_data_hub/workloads/region_talk/direct_pipeline.py`
- `src/my_data_hub/workloads/region_talk/production_assembly.py`
- `tests/region_talk/test_direct_pipeline.py`
- `tests/test_control_plane_deployment.py`
- `.codex/lanes/REGION-TALK-ASSEMBLY/RESULTS.md`

## Shared concurrent files explicitly not owned/committed

- `src/my_data_hub/workloads/region_talk/direct_snapshot.py`
- `src/my_data_hub/workloads/region_talk/reader.py`
- `docs/migrations/region-talk/direct-snapshot-v2.md`
- `docs/migrations/region-talk/mapping.md`
- `sql/migrations/0024_region_talk_snapshot_integrity_and_canonicalize.sql`
- `tests/region_talk/test_reader.py`
- `tests/region_talk/test_snapshot_integrity_v3.py`
