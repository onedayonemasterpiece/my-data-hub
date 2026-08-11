# FINAL-M1 results

- Base SHA: `46aafc3813b400f70a1bbeb8e040125ff96da5ce`.
- Validated implementation SHA: `cb06d07f2a7300eed3569a1b9eb7ea52ab2776a6`.
- Scope: Stage N control-ledger producers, real denial paths, restore/rotation consumers.

## Delivered

- Connector heartbeat producer callable authenticates the exact ACTIVE runtime token/run/attempt/epoch before persisting bounded metadata; no business rows enter SQLite.
- Stale epoch probe first proves the current real broker credential/session path works, then submits the stale epoch through that same read-only PostgreSQL admission/fencing path. Infrastructure failure remains BLOCKED rather than becoming synthetic denial evidence.
- Protected-resource probe invokes `ProviderPolicy.authorize(..., ProviderAction.DELETE)` and proves `PROTECTED_RESOURCE_DENIED` before any provider adapter call.
- Restore requests persist only when provider configuration and modern credentials prove a consumer can exist. The consumer launches the real protected isolated verifier through the same Kaggle adapter and downloads only its bounded receipt to devstand.
- Checkpoint package SHA and full manifest metadata are retained in the control ledger through append-only migration 010; checkpoint bytes remain in Kaggle.
- Rotation requests bind active epoch, exact verified checkpoint version and canonical revision. The consumer invokes the real master coordinator and terminalizes only after the replacement becomes ACTIVE.
- Acceptance operations expire to FAILED after their bounded timeout and `DURABLE_COMPLETE` is terminal, preventing immortal rows.
- Default reader catalog remains unchanged.

## Validation

- `python3 -m compileall -q src tests scripts` — PASS.
- Focused provider/control/MCP/checkpoint suites — PASS.
- `uv run --python 3.12 --no-project --with-editable '.[dev]' pytest -q` — PASS, two skips.
- `scripts/create_notebooks.py --check` — PASS, no drift.
- `scripts/validate_repository.py` — PASS, 2,979 checks.
- Ruff and `git diff --check` — PASS.
- Migration 010 mirror SHA-256: `e9e78e060415ba6709b4c5e79b4cc870a5480d979812aa2b12efff68642076f8`.

## Integration requirements communicated

`control_plane/app.py` is owned by FINAL-BLOGGER and intentionally untouched. That lane was given exact contracts to:

1. call `ControlPlaneMasterRuntime.reconcile_acceptance_once()` in its durable lifecycle loop;
2. expose an authenticated connector heartbeat endpoint calling `record_connector_heartbeat(...)`;
3. accept remote checkpoint stage `package-identity` and call `record_checkpoint_package_sha256(...)`.

The Blogger lane must trigger its real drain/checkpoint path before requesting rotation so the new verified HEAD manifest binds the imported canonical revision.

## Changed files

- `control_migrations/010_checkpoint_restore_metadata.sql`
- `src/my_data_hub/control_plane/ledger/sql/010_checkpoint_restore_metadata.sql`
- `src/my_data_hub/control_plane/{adapters.py,runtime.py}`
- `src/my_data_hub/control_plane/ledger/store.py`
- `src/my_data_hub/mcp/{runtime.py,service.py,postgres_broker.py}`
- `src/my_data_hub/checkpoints/{registry.py,kaggle_runtime.py}`
- `scripts/provider/scheduled_acceptance.py`
- focused tests under `tests/control`, `tests/mcp`, and `tests/provider`
- this results file
