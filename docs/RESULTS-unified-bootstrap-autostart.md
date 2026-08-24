# Unified bootstrap/autostart implementation result

## Implemented

- Added an exclusive unified MCP profile: bounded canonical reads plus private provider
  resources, with provider-only write permits and no canonical operator/write authority.
- Made cold canonical reads return a durable `WAITING_FOR_MASTER` continuation and made
  `operation.get` project a not-yet-consumed durable master request.
- Preserved provider-only behavior while allowing unified provider mutations in any master
  state without calling `ensure_master`.
- Added control readiness for the concrete master runtime and central provider gateway;
  unified app construction fails closed when either is unavailable.
- Added the explicit unified installer action, exact OpenCode static-client validation,
  ChatGPT CIMD scope configuration, release-pointer rollback gate, and autostart compose
  wiring.
- Filtered protected-resource metadata so acceptance scenario tools/scopes are not
  advertised without their executor.

## Not claimed

No live devstand deploy, root tunnel-broker installation, browser authorization, Kaggle
launch, ACTIVE PostgreSQL callback, data read/write, verified checkpoint, or security-gate
receipt was produced by this code-only lane. Those are operator/live evidence steps in
`docs/operations/unified-bootstrap-mcp-deploy.md`.

## Local verification

- `bash -n deploy/control-plane/install.sh`
- `uv run ruff check src tests`
- `python3 -m compileall -q src tests`
- `uv run pytest` — 1432 passed, 4 skipped; two pre-existing jsonschema deprecation warnings
- `uv run python scripts/validate_repository.py` — 4536 checks, zero errors
