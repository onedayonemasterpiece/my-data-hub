# MCP-CHUNKED-UPLOAD lane result

## Scope and base

- Base: `c0d2a8d82278fab658fe8f9b79e81f4b7a14f06a`
- Branch: `agent/provider-file-upload/mcp-chunked-upload`
- Scope: provider MCP file upload, provider gateway error propagation, and exact remote-provider routing only.
- No deployment or live provider mutation was performed.
- No master Notebook, PostgreSQL runtime, canonical-data, blogger, or checkpoint-runtime code was changed.

## Requirement closure

| ID | Status | Result |
|---|---|---|
| R01 typed provider gateway failures | Done | The control endpoint maps policy, contract, not-found, ambiguous, provider, upload-conflict and internal failures to an allowlisted code plus UUID correlation. The remote client preserves only allowlisted code/correlation/status and redacts raw HTTP/provider/secret text. `provider.inventory.live` now routes through the authenticated central provider gateway. |
| R04 chunked private Dataset create | Done | Added `provider.upload.start`, `provider.upload.put_chunk`, `provider.upload.status`, `provider.upload.finalize`, and `provider.upload.abort`; finalization uses the existing single injected Kaggle adapter and durable intent/claim/idempotency path. Existing create/version and download tools remain compatible. |
| Exact identity and replay binding | Done | Upload ID, task, resource, OAuth subject/client, effect, idempotency request, manifest, offsets and hashes are exact-bound. Start/chunk/finalize/abort response-loss replays are deterministic; changed replays quarantine or conflict. `FINALIZING` is restart-reconcilable through the same provider intent. A valid terminal receipt is authoritative over an orphan active directory after any crash window. |
| Bounded private staging | Done | Maximum 100 files, 64 MiB/file, 256 MiB/upload, 24 KiB/chunk, TTL 5 minutes..24 hours; global active quota 32 uploads/1 GiB declared, per subject+client quota 8 uploads/512 MiB, and a 512 MiB filesystem reserve at admission and chunk write. Staging is outside SQLite at 0700/0600, strictly rejects no-follow ancestor symlinks/traversal/reserved provider/checkpoint/PostgreSQL paths, and is removed after success/abort/expiry/tamper. Terminal metadata receipts expire after seven days and contain no raw bytes. |
| Deployment boundary | Done | Installer creates a private host staging directory and mounts it at `/uploads` only in central control for provider-only/operator profiles. The remote MCP receives neither the staging mount nor Kaggle credentials. |
| Documentation | Done | `docs/17-kaggle-control-plane.md` documents that Dataset file upload is provider-only and does not require the master Notebook/PostgreSQL, plus exact lifecycle, limits, replay and cleanup semantics. |

## Evidence

Focused tests cover:

- ten files of approximately 250 KiB each through 24 KiB chunks;
- process restart, persisted `FINALIZING` recovery and lost-success-response replay;
- FINALIZED/ABORTED/QUARANTINED receipt-before-cleanup crash windows without terminal-state overwrite;
- exact chunk replay, conflict/tamper quarantine, abort and expiry cleanup;
- concurrent/restart-safe global and per-principal session/declared-byte quotas plus disk reserve;
- replacement of chunk/assembly ancestors by symlinks without any external write;
- task/principal/client binding, traversal/symlink rejection and size/count/TTL bounds;
- real gateway finalization through a fake injected adapter with durable content manifest and no raw ledger bytes;
- typed/redacted HTTP errors with correlation propagation;
- `provider.inventory.live` and upload status routing to the central provider authority;
- central-only deployment mount.

Validation at the final commit:

- `python -m compileall -q src tests`: PASS
- `python scripts/validate_repository.py`: PASS (`4442` checks, zero errors)
- `python scripts/create_notebooks.py --check`: PASS (zero drift)
- `ruff check .`: PASS
- `bash -n deploy/control-plane/install.sh`: PASS
- `pytest -q`: PASS
