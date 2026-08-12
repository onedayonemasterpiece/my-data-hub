# Lane H6-FM24-CHECKPOINT-RECOVERY Results

## Status
committed

## Requirement IDs
- FM24

## Branch
`agent/operational-mvp/h6-fm24-checkpoint-recovery`

## Worktree
`/home/dev/.codex/worktrees/my-data-hub/h6-fm24-checkpoint-recovery`

## Base SHA
`85321972d30cb39a82a3436406e7f9a90d333674`

## Head SHA
Implementation commit: `9146285997e135359cf2c63f5740b0faf2c360dd` (this results-only commit follows it).

## Files changed
- `src/my_data_hub/acceptance/soak_session.py`
- `src/my_data_hub/acceptance/master_lifecycle.py`
- `src/my_data_hub/master_runtime/fm24_checkpoint_recovery.py`
- `tests/acceptance/test_soak_session.py`
- `tests/acceptance/test_master_lifecycle.py`
- `tests/master/test_fm24_checkpoint_recovery.py`
- `schemas/fm24-soak-state.v1.schema.json`
- `schemas/acceptance/master-lifecycle-receipt.v1.schema.json`
- `examples/contracts/fm24-soak-state.v1.example.json`
- `examples/acceptance/master-lifecycle-receipt-fm24.v1.example.json`
- `docs/operations/fm24-session-rotation-soak.md`
- `.codex/lanes/H6-FM24-CHECKPOINT-RECOVERY/RESULTS.md`

## Evidence
- Twelve contiguous, fully ACKed soak steps no longer make state `COMPLETE`; a persisted checkpoint/recovery intent and ACK are also required.
- `RuntimeCheckpointRecoveryAdapter` assigns a deterministic intent-derived checkpoint UUID, invokes the existing `RuntimeCheckpointCoordinator.create_and_publish`, requires its independent restore-verifier receipt, then performs a separate exact durable-HEAD `resolve_boot_checkpoint` recovery/readback.
- Durable evidence contains exact checkpoint ID, numeric version ref, manifest digest, full checkpoint receipt digest and fixed recovery receipt digest.
- Durable completion projection derives `heartbeats_continuous`, exact heartbeat count and 12 receipt hashes, plus `reads_succeeded`, exact bounded-read count and 12 receipt hashes from the persisted ordered action ACKs.
- `MasterAcceptanceReceipt` rejects FM24 live success unless all continuity, read, checkpoint and recovery fields are present and successful.
- The effect method accepts binding plus intent only: no caller bytes, clock, SQL, paths, timeout or database URL.
- Checkpoint response loss leaves `INTENT_COMMITTED`; retry reuses the exact intent. The deterministic checkpoint UUID lets a reconstructed coordinator reconcile the same promoted HEAD.

## Commands run
- `uv run --extra dev ruff check src/my_data_hub/acceptance/soak_session.py src/my_data_hub/acceptance/master_lifecycle.py src/my_data_hub/master_runtime/fm24_checkpoint_recovery.py tests/acceptance/test_soak_session.py tests/acceptance/test_master_lifecycle.py tests/master/test_fm24_checkpoint_recovery.py`
- `uv run --extra dev pytest -q tests/acceptance/test_soak_session.py tests/acceptance/test_master_lifecycle.py tests/acceptance/test_master_production.py tests/master/test_fm24_checkpoint_recovery.py`
- `uv run --extra dev python -m compileall -q src tests`
- `uv run --extra dev python scripts/validate_repository.py`
- `uv run --extra dev pytest -q`
- `git diff --check`

## Tests / verification
- Focused acceptance/runtime tests: PASS (`51 passed`).
- Compileall: PASS.
- Repository validation: PASS (`3624` checks, `0` errors).
- Full pytest: PASS through 100%; 3 skipped; only the existing `jsonschema.RefResolver` deprecation warnings.
- Ruff and diff check: PASS.

## Risks
- This lane does not fabricate production execution. There is no real FM24 `LIVE_PASS` receipt in the repository; one must be produced by a real 60–90 minute Kaggle execution.
- Parent integration must wire `RuntimeCheckpointRecoveryAdapter(coordinator=runtime_checkpoint_coordinator, database_url=runtime_private_database_url)` into `ProductionSoakSessionPort(checkpoint_recovery=...)` and copy `checkpoint_recovery_evidence(binding)` fields into `RotationSoakEvidence`.
- The fixed adapter archive bound is 1,800 seconds; the existing coordinator/verifier owns its other internal bounded provider operations, while the original 5,400-second soak deadline is rechecked before ACK persistence.

## Merge notes
Parent-owned `src/my_data_hub/acceptance/master_production.py` hook contract:
1. Extend `SoakSessionPort.checkpoint_recovery_evidence(binding) -> FM24SoakCompletionEvidence`.
2. After `exact_service_active(binding)` succeeds, fetch that evidence.
3. Copy `heartbeats_continuous`, `heartbeat_count`, `heartbeat_receipt_sha256s`, `reads_succeeded`, `read_query_count`, `bounded_read_receipt_sha256s`, `checkpoint_verified`, `recovery_succeeded`, `checkpoint_id`, `exact_version_ref`, `manifest_sha256`, `checkpoint_receipt_sha256`, and `recovery_receipt_sha256` into `RotationSoakEvidence`.
4. Missing/unacked projection must fail closed as `FM24_CHECKPOINT_RECOVERY_UNVERIFIED`.
