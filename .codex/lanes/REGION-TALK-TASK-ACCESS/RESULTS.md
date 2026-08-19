# REGION-TALK-TASK-ACCESS results

## Scope and status

- Lane: `REGION-TALK-TASK-ACCESS`
- Requirements: `R03`, `R05`
- Base SHA: `1068d103ec261a37dd31e1f6d11265e1e238c168`
- Tested implementation head SHA: `7acadf95e2110d9f9b5d9e5ce9e7efce2cda0ce8`
- Status: implemented and committed; this evidence file is a documentation-only successor.
- Effort: high because this lane changes epoch-bound PostgreSQL credentials and root-owned SSH certificate authorization.

## Outcome

- Added an exact workload-neutral task credential command/batch/registration/revocation contract for `embedding` and `region_talk` workers. Commands bind worker kind, task UUID, ACTIVE epoch, monotonic generation, token hash, and canonical command hash; unknown/extra fields are rejected.
- Added continuous task credential polling independent of the embedding stage. It starts after ACTIVE setup, uses a dedicated PostgreSQL connection per nonempty batch, continues through foreground stages, rotates at generation changes, and fails/fences before checkpointing on contract/authority failure.
- Limited issued task LOGINs to four minutes (and SSH task certificates to at most five minutes), exact allowlisted groups `mdh_embedding_worker` and `mdh_region_talk_pipeline`, two connections, bounded timeouts, and the current acknowledged lease.
- Preserved the legacy embedding credential reconciler and legacy broker IPC method signatures.
- Upgraded the tunnel broker to state v3 with kind/generation/hash/account/principal-version bindings and v1/v2 state compatibility.
- Added a dedicated locked/nologin `mdh-region-talk-worker`, its own authorized-principals file, certificate principal namespace, and OpenSSH `Match User` block restricted to local forwarding and exact `PermitOpen 127.0.0.1:25432`.
- Removed account-wide `pkill` from exact individual worker-certificate revocation. KRL revocation and PostgreSQL credential termination remove access without terminating unrelated embedding/Region Talk sessions; epoch loss still terminates both dedicated accounts and revokes every epoch certificate.
- Added worker-kind-aware broker IPC issue/revoke operations while keeping legacy embedding IPC responses unchanged.
- No credential, private key, database URL, business row, provider run, or production deployment was persisted or logged by this lane.

## Evidence and commands

Passed:

```text
python -m compileall -q src tests
bash -n deploy/control-plane/install_master_tunnel_broker.sh
ruff check <all changed Python/test files>
pytest -q tests/master tests/control/test_master_tunnel_broker.py
pytest -q tests/control/test_master_tunnel_broker.py tests/master/test_task_credentials.py tests/master/test_embedding_credential_reconcile.py
python scripts/validate_repository.py
  {"checks": 4585, "errors": [], "notes": [], "ok": true}
python scripts/create_notebooks.py --check
  {"drift": [], "mode": "check", "written": []}
git diff --check
```

The focused tests cover concurrent embedding/Region Talk certificates, revoke isolation, wrong kind/task/epoch/generation/hash, five-minute SSH bounds, refresh rotation, exact replay/restart behavior, v2 broker-state upgrade, continuous polling, and rejection of secret-shaped command fields.

The full repository pytest run reached 100%. Fifteen provider-upload tests failed for one shared-host environmental reason only: `/` had less free space than the pre-existing upload staging reserve, so every upload fixture was rejected with `ProviderUploadError: provider upload staging disk reserve would be violated`. No lane-related test failed. Root integration will rerun the full suite after shared disk cleanup.

## Integration requirements and risks

1. The control/pipeline lane must expose `GET/POST /internal/runtime/task-worker-credentials/{run_id}/{attempt_id}` using the exact schemas in `master_runtime/task_credentials.py`. GET 404 is intentionally an empty fail-safe during rolling upgrade; it never mints access.
2. Generic registration is a transient secret handoff to the private worker-status publisher. The control ledger must retain only hashes/identities/receipts, never `database_url` or password material.
3. Region Talk certificate issuance must use the new broker IPC action with `worker_kind=region_talk`, the exact generation, and `binding_sha256=command_sha256`; revocation must repeat the same tuple plus serial.
4. Migration `0023` (separate data lane) must create/grant the bounded NOLOGIN group `mdh_region_talk_pipeline` before a Region Talk command can be admitted.
5. Re-run the root installer to create the dedicated account/principals file and atomically validate/reload sshd before launching a Region Talk worker.
6. This lane did not launch or deploy the Region Talk Notebook and does not claim live pipeline readiness.

## Changed files

- `deploy/control-plane/install_master_tunnel_broker.sh`
- `src/my_data_hub/master_runtime/credentials.py`
- `src/my_data_hub/master_runtime/notebook_entrypoint.py`
- `src/my_data_hub/master_runtime/task_credentials.py`
- `src/my_data_hub/tunnel_broker.py`
- `src/my_data_hub/tunnel_broker_ipc.py`
- `tests/control/test_master_tunnel_broker.py`
- `tests/master/test_task_credentials.py`
- `.codex/lanes/REGION-TALK-TASK-ACCESS/RESULTS.md`
