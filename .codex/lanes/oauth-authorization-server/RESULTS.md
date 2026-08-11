# Lane oauth-authorization-server Results

## Status

committed

## Requirement IDs

- R-OAUTH

## Branch

`agent/operational-mvp/oauth-authorization-server`

## Worktree

`/home/dev/.codex/worktrees/my-data-hub/oauth-authorization-server`

## Base SHA

`b095f28`

## Head SHA

Implementation commit: `5f4dc60`

## Files changed

- `src/my_data_hub/oauth_server/__init__.py`
- `src/my_data_hub/oauth_server/app.py`
- `src/my_data_hub/oauth_server/models.py`
- `src/my_data_hub/oauth_server/service.py`
- `src/my_data_hub/oauth_server/stores.py`
- `src/my_data_hub/oauth_server/tokens.py`
- `tests/oauth_server/test_authorization_server.py`
- `.codex/lanes/oauth-authorization-server/RESULTS.md`

## Commands run

- Reviewed the official PyJWT usage source and RFC Editor primary texts for RFC 7636, RFC 8414 and RFC 8707, plus the final OpenID Connect Discovery specification.
- `uv run --extra dev python -m compileall -q src tests`
- `uv run --extra dev ruff check src tests`
- `uv run --extra dev pytest`
- `uv run --extra dev python scripts/validate_repository.py`
- `git diff --check`

## Tests / verification

- Full suite: `435 passed, 2 skipped`.
- Repository validator: `2793` checks, no errors or notes.
- OAuth tests cover discovery/JWKS, owner-auth challenge wiring, exact static redirects, S256 PKCE omissions and mismatch, exact resource/audience/scope policy, authorization-code replay, JWT claims, refresh rotation/replay-family revocation, explicit refresh revocation, disabled clients, duplicate form fields and rejected client secrets.
- PyJWT with its cryptography backend performs RS256 signing/JWK serialization; the implementation contains no custom JWT signing or password database.

## Risks

- This is not deployed and is not claimed to be proven against a live ChatGPT MCP connection.
- Production wiring must supply a durable, atomic control-plane `OAuthGrantStore`; `MemoryOAuthGrantStore` is explicitly a local/conformance implementation only.
- Production wiring must supply an `OwnerAuthenticator` backed by a passkey, upstream OIDC, or hardened owner session. No permissive/default authenticator is included.
- Static clients and exact redirect allowlists are used rather than dynamic registration.

## Merge notes

- No dependency change is required because `PyJWT[crypto]>=2.10,<3`, FastAPI and HTTPX test support are already pinned in `pyproject.toml`.
- Construct `AuthorizationServerSettings`, pass the existing `ControlLedgerOAuthAuthority` as `control_ledger`, provide a durable control-ledger adapter implementing `OAuthGrantStore`, and provide an `OwnerAuthenticator`; then create the ASGI app with `create_authorization_app(...)` and mount/serve it at the configured issuer origin.
- The access JWT claims (`iss`, exact `aud`, exact `resource`, `client_id`, `scope`, `jti`, bounded dates) are compatible with the existing MCP bearer validator and control-ledger client/revocation checks.
- No control-plane app, MCP module, compose/workflow/config/doc file, canonical database or migration was edited.
