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

The MCP protected-resource metadata advertises only the provider-only scopes. Provider-only
deployment enables bounded ChatGPT Client ID Metadata Document (CIMD) discovery and reports
`chatgpt_oauth_client_mode=cimd-public`. ChatGPT supplies its stable HTTPS client metadata
URL and exact MCP-specific `https://chatgpt.com/connector/oauth/{callback_id}` redirect;
the authorization server validates both on every cache miss. There is no wildcard redirect,
client secret, DCR endpoint, or static bearer fallback. Existing exact static clients remain
compatible and their nonsecret client ID is also reported when configured.

### Pre-registered OpenCode public client

OpenCode is a native public client: it has no client secret and listens only for the
authorization response on its local loopback interface. Add this exact entry to the
existing `MY_DATA_HUB_OAUTH_CLIENTS_JSON` array in the private `oauth.env`; do not replace
the existing static or ChatGPT configuration:

```json
{
  "client_id": "opencode-my-data-hub",
  "redirect_uris": ["http://127.0.0.1:19876/mcp/oauth/callback"],
  "allowed_scopes": [
    "openid",
    "offline_access",
    "platform:read",
    "provider:read",
    "provider:write"
  ]
}
```

This is the only HTTP static redirect form accepted: the IPv4 loopback literal and an
explicit port are mandatory. `localhost`, other IP addresses, user information,
fragments, implicit ports, and noncanonical port spellings fail closed. HTTPS static
clients and ChatGPT CIMD continue to use their existing validation paths. The token
endpoint accepts no HTTP Basic authorization or `client_secret`; OpenCode must use PKCE
S256, authorization code, and rotating refresh tokens.

For the currently deployed OpenCode 1.x configuration, add the following server entry
without putting tokens in the file:

```json
{
  "$schema": "https://opencode.ai/config.json",
  "mcp": {
    "my-data-hub": {
      "type": "remote",
      "url": "https://mcp-datahub.kenigevents.ru/mcp",
      "oauth": {
        "clientId": "opencode-my-data-hub",
        "scope": "openid offline_access platform:read provider:read provider:write",
        "callbackPort": 19876,
        "redirectUri": "http://127.0.0.1:19876/mcp/oauth/callback"
      }
    }
  }
}
```

After installing the exact reviewed server commit and preserving mode `0600` on
`oauth.env`, verify discovery before starting an interactive login:

```bash
curl --fail --silent --show-error \
  https://identity.kenigevents.ru/.well-known/oauth-authorization-server
opencode mcp debug my-data-hub
opencode mcp auth my-data-hub
```

The final command requires the owner browser login ceremony. It stores credentials in
OpenCode's credential storage, not in repository configuration, installer arguments,
artifacts, or logs. Do not run it with command tracing enabled.

No live install or restart was performed while implementing this profile.

### Owner browser authorization for both clients

The installed provider-only profile uses `MY_DATA_HUB_OWNER_AUTH_MODE=local_token`.
OpenCode remains the pre-registered public PKCE client with its exact loopback callback;
ChatGPT remains the CIMD-discovered public PKCE client with its exact ChatGPT HTTPS
callback.  Both are authorized by the same HTTPS form at `identity.kenigevents.ru`, using
the owner-only token file mounted only into the OAuth container.  This deliberately matches
the working eventsBot MCP browser-token ceremony and does not redirect to Yandex Identity
Hub.  The operator token is not an MCP bearer token and must never be added to either
client configuration.
