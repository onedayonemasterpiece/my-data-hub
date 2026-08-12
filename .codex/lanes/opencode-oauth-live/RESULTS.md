# OPENCODE-OAUTH-LIVE results

## Outcome

- Added an exact native-public-client redirect contract for pre-registered OpenCode:
  `http://127.0.0.1:19876/mcp/oauth/callback`.
- Kept issuer, resource, audience, owner login, HTTPS static clients, and ChatGPT CIMD on
  their existing HTTPS validation paths.
- Kept token authentication method `none`, PKCE S256, authorization-code and rotating
  refresh-token behavior. No DCR endpoint or client secret was added.
- Updated the provider-only installer's nonsecret client readiness probe to recognize the
  same exact IPv4 loopback contract.
- Documented the additive static-client JSON and secret-free OpenCode configuration.

## Negative security coverage

The model and installer tests reject `localhost`, wildcard or alternate IP addresses,
implicit ports, noncanonical port spellings, user information, and fragments. Existing
tests continue to reject Basic/client-secret token authentication and refresh replay.

## Deployment evidence and boundary

Before integration, the inspected devstand OAuth environment was a private mode-`0600`
regular file with one existing static reader client and no OpenCode client. The then-active
deployment predated this change and would have rejected the HTTP loopback URI, so no live
mutation was attempted until review, integration, and explicit approval. The subsequent
deployment is recorded below.

The installed OpenCode version was `1.18.15`. Its bundled schema confirms support for a
pre-registered `clientId`, space-delimited `scope`, `callbackPort`, and exact
`redirectUri`, with the required callback as its default. No stored OAuth credentials were
read or emitted.

## Live deployment

After integration as `fe46224da025c2a895da31f3d3e1713a52291a32`, the owner approved
the provider-only install. The private mode-`0600` OAuth environment was updated
additively: `bootstrap-reader` remains present and `opencode-my-data-hub` matches the exact
redirect and scope contract. The approved installer then made that exact release current;
the control plane, OAuth server, and remote MCP containers are healthy, autostart is
enabled, and readiness still proves the provider gateway with master `ABSENT`.

Public discovery proves authorization code plus refresh, PKCE S256, token authentication
method `none`, the required scopes, and no DCR endpoint. A noninteractive authorization
probe using the exact client/redirect/scope/resource was accepted and redirected to the
HTTPS owner login portal. OpenCode `1.18.15` debug discovered the protected-resource
challenge and reported the pre-registered client ID. No owner login was completed and no
tokens were created or recorded. Sanitized evidence is in
`docs/operations/evidence/2026-08-12-operational-mvp/opencode-oauth-live.json`.

The only remaining owner action is:

```bash
opencode mcp auth my-data-hub
```

## Validation

- Focused OAuth/runtime/deployment suite: PASS (77 tests).
- `python -m compileall -q src tests`: PASS.
- `ruff check .`: PASS.
- `python scripts/validate_repository.py`: PASS.
- `python scripts/create_notebooks.py --check`: PASS.
- `bash -n deploy/control-plane/install.sh`: PASS.
- `git diff --check`: PASS.
- Full `pytest`: 1350 passed, 2 skipped, 3 unrelated failures in
  `tests/master/test_notebook_entrypoint.py`. This lane changes neither that test nor
  `src/my_data_hub/master_runtime`; two failures make an unmocked live callback request
  and receive HTTP 401, while the third is the base blogger-receipt/checkpoint behavior.
