# Lane oauth-security-corrections Results

## Status

committed

## Requirement IDs

- R-H4: fail-closed OAuth Host/Origin/admission boundary
- R-H5: bounded request controls and trusted owner-login return contract
- R-M: preserve disabled static clients across runtime restart/upsert
- R-K: reconcile signing-key rotation runbook with overlapping JWKS support

## Branch

`agent/operational-mvp/oauth-security-corrections`

## Worktree

`/home/dev/.codex/worktrees/my-data-hub/oauth-security-corrections`

## Base SHA

`751febf`

## Head SHA

Implementation commit: `b16c0d7`

## Files changed

- `src/my_data_hub/oauth_server/__init__.py`
- `src/my_data_hub/oauth_server/app.py`
- `src/my_data_hub/oauth_server/models.py`
- `src/my_data_hub/oauth_server/owner_oidc.py`
- `src/my_data_hub/oauth_server/runtime.py`
- `src/my_data_hub/oauth_server/service.py`
- `src/my_data_hub/oauth_server/tokens.py`
- `tests/oauth_server/test_authorization_server.py`
- `tests/oauth_server/test_oauth_server_runtime.py`
- `.codex/lanes/oauth-security-corrections/RESULTS.md`

## Commands run

- Reproduced hostile Host/Origin acceptance, untrusted `request.url` return propagation, and disabled-client re-enable with focused failing tests before implementation.
- `uv run --extra dev python -m compileall -q src tests`
- `uv run --extra dev ruff check src tests`
- `uv run --extra dev pytest`
- `uv run --extra dev python scripts/validate_repository.py`
- `git diff --check`

## Tests / verification

- Full suite: `489 passed, 2 skipped`.
- Repository validator: `2851` checks, no errors or notes.
- OAuth tests prove exact Host/Origin rejection, trusted-proxy defaults, bounded body/query/rate/concurrency/queue/request-time behavior, and loopback health admission.
- Owner-login `return_to` is rebuilt from the configured HTTPS issuer and already validated client/redirect/resource/scope/PKCE/state/nonce fields; request scheme/Host and unknown query values cannot influence it.
- A disabled ledger client remains disabled after a new runtime object is built and authorization stays denied.
- JWKS tests prove one active private signing key, up to four validated public-only RS256 overlap JWKs, unique `kid` values, active-key-only issuance, and runtime publication from the bounded overlap file.

## Risks

- This lane does not claim deployment or live ChatGPT verification.
- Admission rate/concurrency state is process-local; an edge limiter remains additive for a multi-replica deployment.
- Operators must stage the next public JWK before switching the active private key, then retain the retired public JWK through the maximum issued-token lifetime as already required by the runbook.
- Trusted proxy IPs are empty by default. A deployment that emits forwarding headers must explicitly set the exact proxy IP allowlist.

## Merge notes

- Runtime reads optional exact allowlists from `MY_DATA_HUB_OAUTH_ALLOWED_HOSTS`, `MY_DATA_HUB_OAUTH_ALLOWED_ORIGINS`, and `MY_DATA_HUB_OAUTH_TRUSTED_PROXY_IPS`. Safe defaults admit the configured issuer plus the existing loopback healthcheck host/port.
- Runtime reads optional public-only overlap keys from `MY_DATA_HUB_OAUTH_OVERLAP_JWKS_FILE`; absence retains single-key behavior.
- The owner OIDC JWKS network/verification path and synchronous control-ledger operations now run outside the ASGI event loop so the admission request timeout remains effective.
- No checkpoint, master, control-plane implementation, compose, workflow, config, or documentation file was edited.
