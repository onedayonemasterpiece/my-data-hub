# Provider-only MCP deployment

## Purpose and boundary

`INSTALL_MY_DATA_HUB_PROVIDER_MCP` brings up the useful provider control surface before
the Kaggle master and canonical PostgreSQL acceptance gates are complete. It starts the
same three long-running services as the normal remote profile: `control-plane`,
`remote-mcp`, and `oauth-server`. The default and full install actions are unchanged.

This mode constructs exactly one central `KaggleProviderAdapter` in the control process
from the existing private `provider.env` and its durable control-ledger journal. Kaggle
credentials never enter the MCP or OAuth containers. The MCP process reaches the adapter
only through the authenticated internal gateway.

The exposed tools are limited to platform status, private provider resource operations,
live provider inventory, and bounded provider acceptance-claim status/cleanup. OAuth is
restricted to `platform:read`, `provider:read`, and `provider:write`. Master, data,
blogger, migration, checkpoint, generic-write, and `acceptance:operate` authority are not
available. The provider write gate accepts only private `mcp_managed` or `mcp_exchange`
resources while master state is `ABSENT`; it does not invent a canonical revision or a
checkpoint.

## Prerequisites

- A clean checkout of the reviewed commit and an image built from that exact commit.
- `MY_DATA_HUB_APPROVED_CONTROL_COMMIT` equal to the checkout commit.
- Docker with Compose support for the `!override` tag. The installer runs `compose
  config --quiet` before changing the active release.
- User lingering enabled and the normal free-space/runtime ownership gates satisfied.
- Private regular files (no symlinks; no group/world access):
  - `provider.env`, containing exactly one central Kaggle credential mode:
    `KAGGLE_API_TOKEN`, or the legacy `KAGGLE_USERNAME` plus `KAGGLE_KEY` pair;
  - `mcp-reader.env` and `oauth.env`, without static bearer tokens or Kaggle credentials;
  - OAuth signing key, owner OIDC client secret, owner portal state key, and overlap JWKS;
  - 32--256 byte printable MCP write-gate key and provider gateway token.
- Existing external DNS/TLS routing to the three loopback upstreams.

No root tunnel-broker socket, master asset bundle, master TLS material, checkpoint upload
key, PostgreSQL/PGDATA, connector runtime, or acceptance supervisor/scenario is required
or permitted by this action.

## Exact command

Prepare/review the release first, then run from the exact clean checkout:

```bash
MY_DATA_HUB_APPROVED_CONTROL_COMMIT="$(git rev-parse HEAD)" \
  deploy/control-plane/install.sh INSTALL_MY_DATA_HUB_PROVIDER_MCP
```

Do not replace `provider.env` with per-run or session credentials. This profile deliberately
reuses the central credential and rotating OAuth implementation already used by the control
plane.

## Readiness and OAuth follow-up

Installation succeeds only when all three loopback health endpoints respond and the
control receipt proves `provider_only_mode=true`, `provider_gateway_ready=true`,
`master_state=ABSENT`, and `data_plane_ready=false`. The unit is enabled for autostart;
any failure invokes the existing rollback of the unit and release pointer.

The MCP protected-resource metadata advertises only the provider-only scopes. If
`oauth.env` already contains a bounded static client with the required provider scopes,
`openid`, `offline_access`, and exact HTTPS redirects, the installer reports its nonsecret
client ID. Otherwise it reports `CHATGPT_OAUTH_CLIENT_CONFIGURATION_REQUIRED` without
misclassifying the healthy local services as failed. Configure the exact callback shown on
the ChatGPT app management page (`https://chatgpt.com/connector/oauth/{callback_id}` for
new apps); no wildcard redirect or static bearer fallback is allowed. A legacy callback is
appropriate only for an already-published legacy app.

No live install or restart was performed while implementing this profile.
