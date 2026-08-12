# Lane gates-i-m-readiness Results

## Status
committed

## Requirement IDs
- R01 — exact 15-tool reader catalog
- R02 — independent post-deploy OAuth/Host/Origin negatives
- R03 — `datahub-owner` bootstrap and mandatory provider rotation path
- R04 — exact three-step ChatGPT reader/owner connection and credential lifecycle commands
- R05 — obsolete canonical-PostgreSQL OAuth revocation helper disposition
- R06 — DNS/TLS documentation correction

## Requirement closure

| ID | Status | Evidence |
|---|---|---|
| R01 | Done | `verify_remote_mcp.py`, v2 report schema/example, and reader security catalog use the same exact 15 runtime reader tools; an invariant test compares the verifier set to `TOOL_CONTRACTS`. |
| R02 | Done (implementation) | v2 verifier requires seven distinct private canaries and separately probes invalid, expired, control-ledger-revoked, wrong issuer, wrong audience, wrong resource, wrong scope, missing auth, wrong Host and wrong Origin. Workflow materializes the secret only in the verification step. No live run was claimed. |
| R03 | Done (internal path), externally blocked | Yandex Identity Hub owns the one-time password and mandatory first-login change. Runtime pins an opaque provider `sub` and maps it only to local `datahub-owner`; a private-file verifier produces sanitized evidence. External OIDC application, accepted login portal, user assignment, provider subject and first-login rotation do not yet have observed receipts. |
| R04 | Done | `docs/20-remote-mcp-endpoint.md` contains one exact three-step sequence for both static public clients plus non-emitting Lockbox retrieval, provider rotation, refresh/client revocation and signing-key rotation commands. |
| R05 | Done | Deleted `scripts/record_oauth_canary_revocation.py`; replacement canary preparation records revocation in the lightweight control ledger. Repository search finds no executable/reference dependency on the deleted helper. |
| R06 | Done | Runbooks now distinguish the observed Yandex DNS/TLS edge from the still-uninstalled MCP/OAuth backends and last observed `502`; no endpoint/application success is claimed. |

## Branch
`agent/gates-i-m-readiness`

## Worktree
`/home/dev/.codex/worktrees/my-data-hub/gates-i-m-readiness`

## Base SHA
`6b1cebdd1e81541669b66f63e6369905c58dcc11`

## Implementation head SHA
`d38f4cb5e500afe502957d67de81f38d52c98fc5`

The final lane head additionally contains this results-only commit; use `git rev-parse HEAD`
at integration time for that commit identity.

## Files changed
- `.github/workflows/post-deploy.yml`
- `docs/08-security.md`
- `docs/11-deployment.md`
- `docs/20-remote-mcp-endpoint.md`
- `docs/operations/devstand-deployment.md`
- `docs/operations/first-deploy-template.md`
- `docs/operations/first-deploy.md`
- `docs/operations/secrets.md`
- `examples/contracts/post-deploy-verification.v2.example.json`
- `examples/mcp/reader-security-catalog.v1.json`
- `schemas/post-deploy-verification.v2.schema.json`
- `scripts/prepare_oauth_negative_canaries.py`
- `scripts/record_oauth_canary_revocation.py` (deleted)
- `scripts/verify_owner_oidc_bootstrap.py`
- `scripts/verify_post_deploy.py`
- `scripts/verify_remote_mcp.py`
- `src/my_data_hub/oauth_server/owner_oidc.py`
- `src/my_data_hub/oauth_server/runtime.py`
- `tests/oauth_server/test_oauth_server_runtime.py`
- `tests/test_oauth_negative_canaries.py`
- `tests/test_owner_oidc_bootstrap.py`
- `tests/test_post_deploy_acceptance.py`
- `tests/test_remote_mcp_verifier.py`

## Commands run
- `uv sync --extra dev`
- `.venv/bin/ruff check ...` (lane Python files and tests)
- `.venv/bin/python -m compileall -q src tests`
- `.venv/bin/pytest -q tests/test_remote_mcp_verifier.py tests/test_post_deploy_acceptance.py tests/test_oauth_negative_canaries.py tests/test_owner_oidc_bootstrap.py tests/oauth_server/test_oauth_server_runtime.py`
- `.venv/bin/pytest -q tests/test_control_plane_deployment.py tests/test_yandex_edge_deployment.py tests/test_same_host_deployment.py tests/test_config.py tests/test_mcp_oauth.py tests/test_mcp_oauth_runtime.py tests/mcp/test_remote_runtime.py tests/oauth_server/test_authorization_server.py tests/oauth_server/test_control_store.py`
- `.venv/bin/pytest -q`
- `.venv/bin/python scripts/validate_repository.py`
- `git diff --check`
- targeted official-documentation research for current ChatGPT Developer-mode static OAuth/CIMD/DCR behavior and Yandex Identity Hub OIDC/local-user password rotation.

## Tests / verification
- Focused readiness suite: **40 passed**.
- Adjacent OAuth/deployment suite: **111 passed**.
- Full repository suite: **passed**, exit 0; three pre-existing skipped tests and two `jsonschema.RefResolver` deprecation warnings.
- Repository validator: **3720 checks, 0 errors, 0 notes**.
- Compileall: passed.
- Ruff focused scope: passed.
- Exact reader cross-check: `exact-reader-catalog=15`.

## Risks / external blockers
- No deployment, DNS mutation, Kaggle action, live OAuth login, ChatGPT connection or live post-deploy run was performed in this lane.
- Yandex DNS/TLS edge existence is observed historical evidence; application routes were last recorded as `502`.
- A live owner ceremony requires the owner-created Identity Hub user/OIDC application, exact provider subject, accepted login portal and secret reference. The closure proof is the documented `verify_owner_oidc_bootstrap.py` command after mandatory first-login rotation.
- Live R02 proof requires a freshly generated private canary bundle, an unexpired reader credential, the exact deployed merge SHA and signed v2 host evidence. Synthetic tests are not production evidence.
- The v1 post-deploy schema/example remain historical. The active v2 contract carries the expanded negative matrix and exact 15-tool catalog.

## Merge notes
Cherry-pick the implementation commit followed by the results commit. Do not restore the
deleted PostgreSQL revocation helper. When combining with deployment work, retain OAuth-only
workflow/env names and avoid replacing fresh checkpoint-broker settings from other lanes.
