# OAuth matrix refresh lane results

## Status

Committed; ready for integration after the coordinated repository-validator update.

## Scope and base

- Base: `21f9160d7e986dc53e8b19560866964b97c7704e`
- Branch: `agent/operational-mvp/oauth-matrix-refresh`
- Implementation commits: `9cb0679`, `23f01a4`
- No live OAuth, GitHub runner, control-plane, provider, or deploy mutation was performed.

## Root cause

`provider-real.yml` was a six-hour GitHub-hosted job populated with fixed MCP
access-token secrets. The issuer intentionally limits those tokens to roughly
300 seconds. The repository already had restart-safe public-client refresh-token
rotation, but its owner-only mode-0600 state was deliberately local to the
devstand and therefore could not safely be copied to an ephemeral runner or an
immutable GitHub secret.

The independent scheduled-acceptance pass also captured one bearer in each
long-lived MCP HTTP client, so its operator polling could outlive that bearer
even when a rotating source was available elsewhere in the driver.

## Implemented

- Routed the real provider workflow only to
  `[self-hosted, linux, my-data-hub-devstand]`.
- Removed all six static MCP/data-MCP bearer secret bindings from that workflow.
- Added a checked-in fail-closed devstand bootstrap that requires:
  - `RUNNER_ENVIRONMENT=self-hosted`;
  - the existing absolute owner-owned non-symlink mode-0600 OAuth file;
  - `reader`, `operator`, and `provider` public-client refresh families;
  - the file to remain outside `GITHUB_WORKSPACE` and `RUNNER_TEMP`;
  - absence of inherited static MCP bearer copies.
- Preserved the existing `RotatingOAuthBearerSource` and public-client
  refresh-token rotation; no replacement authorization mechanism was added.
- Changed scheduled acceptance to use request-scoped asynchronous HTTP auth.
  Every MCP HTTP request asks the shared `BearerSource` for a current bearer,
  including status polls after the initial action request.
- Preserved nightly's static bearer compatibility fallback for bounded probes.
- Kept OAuth credentials out of argv, workflow variables/secrets, controller
  artifacts, receipts, and logs.

## Verification

- Focused OAuth/scheduled/operational provider suite: PASS.
- Full pytest: PASS (two existing environment-gated skips).
- Python compileall: PASS.
- Ruff on touched Python/tests: PASS.
- Generated notebook drift check: PASS.
- Tracked-secret scan: PASS.
- `git diff --check`: PASS.

The request-auth test advances a deterministic clock beyond one 300-second
access-token lifetime and proves a second HTTP request atomically persists and
uses the successor refresh family without writing either access token or the
initial refresh token to captured logs.

## Coordinated integration requirement

The current repository validator still hardcodes every scheduled job to
`runs-on: ubuntu-latest`. It therefore reports exactly one expected error for
the new safe topology:

`provider-real.yml:private-notebook-canary is not GitHub-hosted and bounded`

Per root coordination, this lane does not edit that validator. The integration
owner must update the invariant to require the exact three self-hosted labels,
the private-file preflight, and absence of static MCP/Kaggle credential bindings.
After that update, rerun the repository validator as an integration gate.

## Operational prerequisite

The workflow remains queued until an owner-managed devstand runner with the
exact `my-data-hub-devstand` label is registered and its service environment
provides `MY_DATA_HUB_MCP_OAUTH_CREDENTIAL_FILE`. This lane did not install or
register that runner and did not create/rotate live OAuth families.
