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

The inspected devstand OAuth environment is a private mode-`0600` regular file with one
existing static reader client and no OpenCode client. The active deployment predates this
change and would reject the HTTP loopback URI. Therefore this lane did not mutate the live
environment or restart/deploy services. Registration must be applied additively only after
this exact commit is reviewed, integrated, and installed; the existing client must be
preserved.

The installed OpenCode version was `1.18.15`. Its bundled schema confirms support for a
pre-registered `clientId`, space-delimited `scope`, `callbackPort`, and exact
`redirectUri`, with the required callback as its default. No stored OAuth credentials were
read or emitted.

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
