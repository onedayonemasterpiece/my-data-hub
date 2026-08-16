# Local owner OAuth results

## Scope

- Exact base: `6838ad42d77dc42b2d83ab0c3b02847bf31b04c1`.
- Replace the deployed owner-login dependency on external Identity Hub with the
  owner-controlled browser-token ceremony already proven by eventsBot MCP.
- Preserve two independent OAuth clients: local OpenCode (static public PKCE
  client with loopback callback) and ChatGPT (CIMD public PKCE client with an
  HTTPS ChatGPT callback).

## Delivered

- Production `local_token` owner mode renders and accepts a bounded form on the
  authorization-server HTTPS origin.  The high-entropy operator token remains
  in one owner-mode `0600` devstand file and never enters a URL, cookie, grant,
  repository setting, MCP request, Kaggle resource, log, or receipt.
- Encrypted form state expires after five minutes.  The resulting owner session
  is `Secure`, `HttpOnly`, `SameSite=Lax`, and expires after one hour.
- OAuth authorization codes remain bound to exact client, redirect, resource,
  scopes, nonce, and S256 PKCE.  OpenCode and ChatGPT receive distinct rotating
  refresh-token families and can be revoked independently.
- The installer creates the private operator-token file atomically when absent,
  validates owner/mode/size, mounts it only into the OAuth container, and rejects
  raw token leakage through provider, MCP, or OAuth environment files.
- The prior OIDC implementation remains available only as an explicit
  compatibility mode; the production compose profile selects `local_token`.

## Validation before deployment

- Focused OAuth/runtime/deployment suite: PASS (`119` tests).
- Full `pytest -q`: PASS (three expected skips; two existing
  `jsonschema.RefResolver` deprecation warnings).
- Repository validator: PASS (`4496` checks, zero errors/notes).
- Ruff, compileall, installer `bash -n`, and `git diff --check`: PASS.
- Tests cover OpenCode loopback PKCE, ChatGPT static callback compatibility,
  ChatGPT CIMD dynamic public-client authorization, wrong-token denial, and
  tampered-state denial.

## Honest live boundary

No deployment or production token rotation is claimed by this implementation
receipt.  Live closure requires deploying the reviewed commit on this same
devstand, verifying public discovery/form redirects, and completing one browser
authorization from the owner's local OpenCode plus one from ChatGPT.
