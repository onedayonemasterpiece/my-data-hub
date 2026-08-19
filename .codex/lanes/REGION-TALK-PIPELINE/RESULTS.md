# REGION-TALK-PIPELINE results

## Lane contract

- Lane ID: `REGION-TALK-PIPELINE`
- Requirement IDs: `R03`, `R04`, `R05`
- Base SHA: `1068d103ec261a37dd31e1f6d11265e1e238c168`
- Implementation commit: `1199e906e338debeca7cda50321d56ad5f59746b`
- Final lane head: the commit containing this result file; exact SHA is supplied in the parent handoff because a Git commit cannot contain its own hash.

## Delivered

- Added a separate Region Talk private Kaggle supervisor launch contract and generated bootstrap.
- Added deterministic scheduled and supervised requests with durable SQLite state and append-only transition hashes.
- Added atomic singleton/replay lifecycle:
  `WAITING_MASTER -> LAUNCHING -> PENDING_ATTESTATION -> ATTESTED -> RUNNING -> TERMINAL -> CLEANUP_PENDING -> CLEANED`, plus `TIMED_OUT` and `FENCED`.
- Bound every advanced state to the exact ACTIVE master run, attempt, instance, epoch, and deterministic task run ID.
- Added response-loss reconciliation, ambiguity fail-closed behavior, lease-safe concurrent ticks, bounded timeout handling, exact cleanup, and restart replay.
- Kept the control ledger metadata-only. It has no database URL, password, key, token, article/post body, source row, or generic payload column. Only credential/token hashes and exact revocation identities are persisted.
- Aligned task access contracts exactly to the generic task-access lane:
  - `my-data-hub-task-credential-command.v1`
  - `my-data-hub-task-credential-batch.v1`
  - `my-data-hub-task-credential-registration.v1`
  - `my-data-hub-task-credential-revocation.v1`
  - `worker_kind=region_talk`, role `region_talk_pipeline`
  - canonical command hash, generation, token hash, credential ID, and SSH certificate serial.
- Exposed workload-neutral task credential and provider launch/cleanup Protocols; nothing reuses the embedding credential poller or embedding role.
- Generated bootstrap verifies one exact capability input, one exact wheel hash, runtime image source commit, task/master/epoch identity, and posts source/image/epoch attestation before materializing PostgreSQL/SSH access.
- Added a finite cycle runner (bounded cycles, bounded runtime, bounded retry backoff, idle completion) with `publication_dispatch=false` enforced by model and source.
- Added control migration `035_region_talk_pipeline_runs.sql` and its packaged twin.

## Evidence and commands

All commands used the existing project virtualenv at
`/home/dev/.codex/worktrees/my-data-hub/operational-mvp/.venv`.

- `python -m pytest tests/region_talk/test_pipeline_core.py -q`
  - `13 passed`
- `python -m ruff check` on all lane Python files
  - passed
- `python -m compileall -q src tests`
  - passed
- `python scripts/validate_repository.py`
  - `ok: true`, `4593` checks, no errors or notes
- migration discovery for repository and packaged directories
  - both contiguous `1..35`; twins byte-identical
- full suite excluding the one shared hard-coded migration-range assertion:
  - `python -m pytest -q --deselect tests/control/test_ledger_master.py::test_sqlite_pragmas_permissions_and_append_only_logs`
  - passed (three pre-existing skips; only pre-existing jsonschema deprecation warnings)
- unmodified full suite:
  - one failure only: `tests/control/test_ledger_master.py::test_sqlite_pragmas_permissions_and_append_only_logs`
  - reason: shared test hard-codes `range(1, 35)` although migration 035 now exists
  - parent explicitly retained ownership of that shared one-line integration update.

## Focused test coverage

- duplicate supervised idempotency key
- duplicate scheduled slot
- two concurrent scheduler ticks
- missing ACTIVE master
- lost provider response and restart reconciliation without a second push
- ambiguous provider identity fail-closed
- partial-launch timeout and discovered exact-credential cleanup
- source/image/epoch attestation mismatch
- ACTIVE epoch fencing
- timeout, cleanup, and subsequent-slot ordering
- terminal and cleanup replay after restart
- exact task credential hash/batch/registration/revocation contracts
- private capability separation and attestation-before-DB ordering
- bounded idle completion and publication dispatch prohibition
- fixed metadata-only journal columns and secret absence

## Root-owned integration gaps (forbidden in this lane)

1. Wire `RegionTalkPipelineStore`/`RegionTalkPipelineCoordinator` through the shared `ControlLedger`, control runtime, and internal callback routes.
2. Bind a concrete central Kaggle provider launch/observe/cleanup adapter to the supplied Protocols.
3. Bind task-access command/registration storage and SSH certificate broker issuance/revocation to the supplied exact identities.
4. Supply `my_data_hub.workloads.region_talk.direct_pipeline:build_cycle_executor` (or a pinned alternate factory) from the data/transform integration lane.
5. Ensure credential refresh is completed between bounded cycles, or cap a run below the issued credential expiry, before production enablement. The generic generation/refresh/revoke contracts are present; shared callback/status Dataset rotation is intentionally not implemented in this forbidden lane.
6. Keep the existing Region Talk pipeline paused and publication dispatch disabled until those integrations, the supervised first run, and checkpoint gates pass.
7. Update the shared migration-range assertion from versions `1..34` to `1..35` during integration.

## Risks

- This is control-core, not evidence that a real Kaggle supervisor or the canonical Region Talk transformation ran.
- A run must not outlive its task credential unless the root-owned refresh/status handoff is wired.
- Provider ambiguity deliberately blocks replay rather than risking a duplicate Notebook.
- Cleanup of a partially observed launch relies on the concrete cleanup port reconciling the deterministic task identity and returning the exact discovered credential/certificate binding.

## Changed files

- `control_migrations/035_region_talk_pipeline_runs.sql`
- `src/my_data_hub/control_plane/ledger/sql/035_region_talk_pipeline_runs.sql`
- `src/my_data_hub/workloads/region_talk/central_launcher.py`
- `src/my_data_hub/workloads/region_talk/pipeline_contracts.py`
- `src/my_data_hub/workloads/region_talk/pipeline_runtime.py`
- `tests/region_talk/test_pipeline_core.py`
- `.codex/lanes/REGION-TALK-PIPELINE/RESULTS.md`
