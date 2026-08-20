# REGION-TALK-LONG-RUN-AUTHORITY results

## Scope and revisions

- Requested base: `132891f`
- Integrated prerequisite before this commit: `bc0324ddf6680e4413ee54afbafa03a0154d2923`
  (includes append-only migration 0025 and its reviewed task-binding contract)
- Implementation head: `6c5cd95`
- Live deployment: not performed; the schedule remains disabled by default.

## Delivered evidence

- Region Talk obtains successive PostgreSQL LOGIN and SSH certificate generations
  with a maximum four-minute database credential lifetime.
- The private worker proves the replacement tunnel and ACTIVE-epoch database
  session before the prior exact generation is revoked.
- PostgreSQL task binding is registered before the private database URL is handed
  to the worker. An uncertain binding response replays the same credential and
  never publishes first.
- Registration delivery and revocation delivery use explicit, exact,
  response-loss-safe acknowledgements. GET is non-destructive.
- Provider/control restart reuses the pre-effect task token, source/image binding,
  deterministic effect identities, and private capability bytes.
- Task token comparison is constant-time. Refresh and activation bind the exact
  request, task, source hash, image/source commit, master instance, epoch, previous
  generation, credential and SSH certificate serial.
- Private capability responses are `no-store`; secret capability sidecars are
  mode `0600` under a mode `0700` root, atomically file- and directory-fsynced,
  size-bounded, symlink-rejecting, and purged on exact terminal ACK or expiry.
- Expired unactivated generations are tombstoned and never reissued against the
  append-only database task-binding uniqueness constraint.
- Stale terminal callbacks resolve the current ACTIVE master, fence, and return
  `409` before any terminal state is recorded.
- Enabled-but-incomplete Region Talk assembly makes `/health/ready` return `503`.
- Remote post-deploy verification contracts include the exact Region Talk tool set.

## Validation

All commands ran from the integration worktree.

```text
.venv/bin/pytest -q tests/master/test_database_gate.py \
  tests/master/test_task_credentials.py tests/master/test_notebook_entrypoint.py \
  tests/region_talk tests/mcp/test_control_gateway.py \
  tests/mcp/test_region_talk_contracts.py tests/mcp/test_remote_runtime.py \
  tests/test_control_plane.py tests/test_control_plane_deployment.py \
  tests/test_post_deploy_acceptance.py tests/test_region_talk_migration.py
PASS (one environment-dependent skip)

.venv/bin/pytest -q
PASS at 100% (four expected skips; two jsonschema deprecation warnings)

.venv/bin/ruff check <all changed Python files>
PASS

.venv/bin/python -m compileall -q src tests
PASS

.venv/bin/python scripts/validate_repository.py
PASS: 5002 checks, 0 errors

git diff --check
PASS
```

## Changed files

- `examples/contracts/post-deploy-verification.v2.example.json`
- `schemas/post-deploy-verification.v2.schema.json`
- `src/my_data_hub/control_plane/app.py`
- `src/my_data_hub/master_runtime/database_gate.py`
- `src/my_data_hub/master_runtime/notebook_entrypoint.py`
- `src/my_data_hub/master_runtime/task_credentials.py`
- `src/my_data_hub/workloads/region_talk/central_launcher.py`
- `src/my_data_hub/workloads/region_talk/pipeline_contracts.py`
- `src/my_data_hub/workloads/region_talk/production_assembly.py`
- `tests/master/test_database_gate.py`
- `tests/master/test_task_credentials.py`
- `tests/region_talk/test_long_run_authority.py`
- `tests/region_talk/test_pipeline_core.py`
- `tests/test_control_plane.py`

## Remaining risks / operator gate

- No live Kaggle supervisor, production PostgreSQL, YDB, or 58,554-row Region Talk
  cycle was executed in this lane. Static/disposable coverage is not live proof.
- The scheduled mode remains default-off. Enablement requires a successful manual
  supervised run, exact terminal/cleanup receipts, and production reconciliation.
- A database session and established SSH tunnel intentionally remain usable for a
  single bounded cycle after login/certificate expiry; refresh occurs before a
  new cycle or when launch delay consumes the initial generation. PostgreSQL
  migration 0025 still enforces the exact task and ACTIVE epoch at session begin.
- Publication dispatch is fixed to `false`; this lane does not publish content.
