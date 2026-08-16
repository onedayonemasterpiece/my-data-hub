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

## Live deployment follow-up

- Merged implementation: `c74e369d015e653b8fe2bf8f91af5cc2e00abd11`.
- A live probe then found that remote MCP rejected the correctly issued protocol
  scopes `openid offline_access`; the narrow resource-server correction was
  merged as `26a5e1c510278d7a80044df2cf532563d049d5ed` and deployed through the reviewed
  provider-only installer on the same devstand.
- All three local containers are healthy at that exact commit.  Public OAuth
  discovery and protected-resource metadata are HTTP 200; unauthenticated MCP is
  the expected HTTP 401 at remote IP `188.227.84.107`.
- A live static-client proof completed owner form, authorization-code exchange,
  MCP initialize, tools/list, and `platform.status`.  It observed the exact
  deployed commit and all batch provider tools.  The proof refresh/access family
  was revoked immediately afterward.
- The owner operator token was delivered as a document to the approved Telegram
  Saved Messages session (message id `34684`); no token value is recorded here.

Owner interaction remains for the two durable user connections: authorize local
OpenCode and, independently, ChatGPT CIMD.  Server-side readiness is proven; those
client-held refresh families are not fabricated or claimed before the owner
completes each browser flow.
