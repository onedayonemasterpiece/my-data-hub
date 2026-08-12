# MCP-KAGGLE-BATCH-GATEWAY results

## Scope and revisions

- Lane: `mcp-kaggle-batch-gateway`
- Requirement IDs: R01 lifecycle coverage; R02 binary JSON round-trip; R03 one central adapter; R04 durable ownership/idempotency; R05 safety denials; R06 bounded JSON/no secrets; R07 compatibility.
- Base SHA: `1e699908fefd6c25e1bb13aa77a987c507f0d2e6`
- Implementation head SHA: `3805b80df1c50f09536920a77e2727e7ad4bb968`
- No deployment or live provider mutation was performed.

## Delivered

- Backward-compatible UTF-8 create/version plus exact canonical-base64 file objects with declared raw size and SHA-256.
- New read-only-hint `provider.resources.list` and `provider.resources.download` MCP tools, registered through the public server, service, authenticated internal gateway, control authority and `KaggleMCPProviderGateway`.
- Exact private numeric-version Dataset metadata preflight and single-file download through the repository's sole injected, official `kaggle==2.2.4` adapter.
- Durable per-version content manifests contain only paths, sizes and hashes. Existing provider effect intents, receipts, task claims, fenced mutation leases and idempotent delete reconciliation remain authoritative.
- `mcp_managed` read/version/delete authority is bound to creating task and principal; `mcp_exchange` preserves manifest recipient/creator/TTL rules.
- Strict traversal, symlink, provider-owned metadata, canonical/checkpoint filename, file count, request size, dataset size, per-file size, hash, unexpected-file and mid-read tamper denials.
- ChatGPT-compatible JSON chunk responses contain canonical base64, exact file/chunk hashes, byte offsets and deterministic `next_offset`; no signed URL, blob token or credential is returned.

## Exact limits

- At most 100 upload files.
- Legacy text-only content: 256 KiB UTF-8 total.
- Binary object: 256 KiB declared per-file maximum and 320 KiB mixed raw validation ceiling; create/version semantic/internal JSON is capped at 512 KiB, so base64/manifest overhead lowers the practical raw maximum and 320 KiB is not claimed usable for every envelope.
- Dataset list: 50 content rows per page; exact metadata preflight is bounded to 102 provider files and 64 MiB total.
- Download: one exact file no larger than 64 MiB; at most 128 KiB raw per JSON result, continued by exact offset.
- MCP response: existing 2 MiB maximum.

## Evidence and commands

- Focused suite (89 tests):
  - `PYTHONPATH=$PWD/src /home/dev/.codex/worktrees/my-data-hub/operational-mvp/.venv/bin/pytest -q tests/control/test_mcp_operator_provider.py tests/control/test_control_runtime_wiring.py tests/mcp/test_dynamic_contracts.py tests/mcp/test_control_gateway.py tests/provider/test_kaggle_adapter.py tests/provider/test_kaggle_contracts.py`
  - Result: PASS.
- Full test suite:
  - `PYTHONPATH=$PWD/src /home/dev/.codex/worktrees/my-data-hub/operational-mvp/.venv/bin/pytest -q`
  - Result: PASS (repository-reported two existing `jsonschema.RefResolver` deprecation warnings).
- Compile:
  - `python3 -m compileall -q src tests`
  - Result: PASS.
- Lint:
  - `/home/dev/.codex/worktrees/my-data-hub/operational-mvp/.venv/bin/ruff check .`
  - Result: PASS.
- Repository/schema/notebook validation:
  - `PYTHONPATH=$PWD/src /home/dev/.codex/worktrees/my-data-hub/operational-mvp/.venv/bin/python scripts/validate_repository.py`
  - Result: PASS, `4252` checks, zero errors/notes.
- Whitespace:
  - `git diff --check`
  - Result: PASS.
- Optional targeted mypy invocation was not a release gate and was not green because the shared validation venv does not expose typed Pydantic to mypy and the selected pre-existing modules already contain broad baseline type errors. No mypy success is claimed.

## Risks and residual external evidence

- No live Kaggle provider token was used. The exact official SDK method signature is locked by contract tests, but a real private Dataset create/list/single-file-download/version/delete receipt remains an external deployment acceptance prerequisite.
- `dataset_download_file` downloads and verifies the complete selected file for each chunk call. This is bounded to 64 MiB and leaves no durable cache, but repeated chunks trade provider bandwidth for restart-safe statelessness.
- Dataset versions registered before this contract lack a durable content manifest. Metadata-only `provider.resources.read` remains compatible; list/download fail closed until a new exact version establishes the manifest.
- Deployment/install, checkpoints, embeddings and YDB were not modified.

## Changed files

- `src/my_data_hub/control_plane/adapters.py`
- `src/my_data_hub/control_plane/app.py`
- `src/my_data_hub/mcp/catalog.py`
- `src/my_data_hub/mcp/control_gateway.py`
- `src/my_data_hub/mcp/provider_schemas.py`
- `src/my_data_hub/mcp/server.py`
- `src/my_data_hub/mcp/service.py`
- `src/my_data_hub/providers/kaggle/__init__.py`
- `src/my_data_hub/providers/kaggle/adapter.py`
- `src/my_data_hub/providers/kaggle/contracts.py`
- `tests/control/test_control_runtime_wiring.py`
- `tests/control/test_mcp_operator_provider.py`
- `tests/mcp/test_control_gateway.py`
- `tests/mcp/test_dynamic_contracts.py`
- `tests/provider/test_kaggle_adapter.py`
- `tests/provider/test_kaggle_contracts.py`
- `docs/17-kaggle-control-plane.md`
- `docs/18-mcp-operator-and-database-access.md`
- `.codex/lanes/mcp-kaggle-batch-gateway/RESULTS.md`
