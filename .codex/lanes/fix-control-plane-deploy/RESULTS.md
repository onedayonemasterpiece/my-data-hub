# Lane fix-control-plane-deploy Results

## Status
committed

## Requirement IDs
- R-DEPLOY
- D1 explicit, side-effect-safe PREPARE and exact INSTALL gates
- D2 build/autostart control-plane, remote-mcp and oauth-server
- D3 exact opt-in profile, split environment/secret boundaries and loopback upstreams
- D4 process failure and host reboot reconciliation
- D5 preserve database-free devstand and permanently forbidden legacy same-host path

## Branch
agent/operational-mvp/fix-control-plane-deploy

## Worktree
/home/dev/.codex/worktrees/my-data-hub/fix-control-plane-deploy

## Base SHA
751febf21477cfc6b4fae720c5797756204db05d

## Head SHA
7288cb4a220646fc0884397bca62986b15522494 (implementation commit)

## Files changed
- compose.control-plane.yaml
- deploy/control-plane/install.sh
- tests/test_control_plane_deployment.py

## Required INSTALL inputs
- `MY_DATA_HUB_APPROVED_CONTROL_COMMIT` must equal the exact clean source commit.
- The systemd user must already have `Linger=yes` so the enabled user unit starts after reboot.
- `MY_DATA_HUB_CONTROL_PROVIDER_ENV_FILE` (default `<runtime>/env/provider.env`): private regular non-symlink file.
- `MY_DATA_HUB_MCP_ENV_FILE` (default `<runtime>/env/mcp-reader.env`): private regular non-symlink file.
- `MY_DATA_HUB_OAUTH_ENV_FILE` (default `<runtime>/env/oauth.env`): private regular non-symlink file.
- `MY_DATA_HUB_OAUTH_SIGNING_KEY_FILE` (default `<runtime>/secrets/oauth-signing-key.pem`): private regular non-symlink file.
- `MY_DATA_HUB_MASTER_TLS_CA_FILE` (default `<runtime>/master-tls/ca.pem`): regular non-symlink file.
- `MY_DATA_HUB_MASTER_ASSET_DIR` (default `<runtime>/master-assets`): non-symlink directory.
- Optional runtime/release/state roots remain overrideable by their existing `MY_DATA_HUB_*_DIR` variables.
- Static env files are rejected if they contain master data-plane connection variables or cross the provider/MCP/OAuth secret boundaries.

## Commands run
- `bash -n deploy/control-plane/install.sh`
- `uv run --extra dev pytest -q`
- `uv run python -m compileall -q src tests`
- `uv run python scripts/validate_repository.py`
- `uv run --extra dev ruff check tests/test_control_plane_deployment.py`
- `docker compose --env-file <temporary> --profile remote-mcp -f compose.control-plane.yaml config --quiet`
- Fake-command PREPARE and INSTALL end-to-end shell exercises in isolated temporary roots
- `git diff --check`

## Tests / verification
- Full pytest suite passed: 488 passed, 2 skipped (490 collected).
- Repository validation passed: 2852 checks, zero errors.
- Compileall, Ruff, Bash syntax and whitespace validation passed.
- Compose config validation passed with all three required split env files and exact `remote-mcp` profile.
- Fake PREPARE proved no unit/current pointer/state dirs were created and only the immutable release/image was prepared.
- Fake INSTALL generated and enabled one foreground reconciliation unit for all three services, wrote only path references to its compose env, checked all three loopback health endpoints and advanced the release pointer.
- No real host service, external DNS, Yandex Cloud or VPN action was executed.

## Risks
- A real deployment still depends on operator-provisioned values inside the three env files, a valid OAuth signing key/TLS CA/provider asset set, Docker availability and pre-enabled user lingering.
- The installer intentionally refuses to change lingering automatically because disabling/restoring a shared user's lingering state could affect unrelated user services.
- External edge routing remains outside this lane.

## Deployment documentation integration requests
- Update `docs/operations/devstand-deployment.md` from “one process/control-only” to the explicit `remote-mcp` opt-in three-process unit.
- Document the exact approved-commit, `Linger=yes`, env-file/key/CA/asset path and permission gates above.
- Document loopback upstreams `127.0.0.1:8080`, `:8765` and `:8780`, plus foreground systemd/Docker restart reconciliation.
- Update `docs/operations/first-deploy.md` so remote MCP/OAuth are no longer described as a separate later installer, while retaining the separate external DNS/VPN/443 gate.
- State explicitly that this lane did not perform a real deployment or external DNS/YC/VPN change.

## Merge notes
- Cherry-pick the implementation commit, then this results commit.
- Integrator owns the requested deployment documentation updates after merge.
