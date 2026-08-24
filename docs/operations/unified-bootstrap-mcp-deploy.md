# Unified MCP bootstrap and Kaggle master autostart

## Boundary

`INSTALL_MY_DATA_HUB_UNIFIED_BOOTSTRAP` is the bounded production profile that combines
one central Kaggle adapter, the private provider Dataset tools, the canonical Kaggle
master lifecycle, and reader tools in the same devstand control deployment. It does not
grant `master:ensure`, `data:write`, `migration:operate`, `bloggers:write`, `acceptance:operate`, an
operator PostgreSQL role, DDL, or generic canonical write authority.

Provider Dataset operations remain independent of canonical master state. A private
`mcp_managed` or `mcp_exchange` Dataset can therefore be created while the master is
`ABSENT`, starting, or `ACTIVE`; that operation never starts the PostgreSQL notebook.
Conversely, a canonical read such as `bloggers.search` against `ABSENT` durably records
one ensure request and returns `outcome=WAITING_FOR_MASTER`, its operation ID, the
`operation.get` status tool, and the instruction to retry the original request after the
master becomes `ACTIVE`. The devstand reconcile loop consumes that request through the
existing Kaggle master provider. No request is converted into an untracked background
session.

## Exact authority

The MCP resource scopes are exactly:

```text
platform:read,master:read,operation:read,checkpoint:read,embedding:read,provider:read,bloggers:read,region-talk:read,provider:write
```

Acceptance scenario request/status tools are not advertised unless their concrete
executor is configured. This profile deliberately never configures that executor.

OpenCode remains a separate static public PKCE client. Its `allowed_scopes` must equal
`openid`, `offline_access`, plus the exact resource scopes above, and it must include an
exact `http://127.0.0.1:<port>/...` loopback callback. ChatGPT remains a CIMD-discovered
public PKCE client; the deployment publishes the same exact scope set through
`MY_DATA_HUB_OAUTH_CHATGPT_CIMD_SCOPES`. Do not reuse OpenCode's client ID for ChatGPT.
After changing from provider-only to unified, disconnect/re-authorize both clients so old
provider-only grants and refresh families are not mistaken for unified authorization.

The corresponding OpenCode server entry keeps the same endpoint and callback but requests
the unified scope string:

```json
{
  "type": "remote",
  "url": "https://mcp-datahub.kenigevents.ru/mcp",
  "oauth": {
    "clientId": "opencode-my-data-hub-unified",
    "scope": "openid offline_access platform:read master:read operation:read checkpoint:read embedding:read provider:read bloggers:read region-talk:read provider:write",
    "callbackPort": 19876,
    "redirectUri": "http://127.0.0.1:19876/mcp/oauth/callback"
  }
}
```

The client ID is an example name; the installer prints the exact eligible nonsecret ID
selected from private `oauth.env`. Do not copy a token or client secret into this JSON.

## Installation gates

Before installation, the exact clean commit must have:

1. a master asset bundle rebuilt and verified for that same commit;
2. the root-installed epoch tunnel broker socket and master TLS/session directories;
3. checkpoint upload broker key (this proves only configuration, not a checkpoint);
4. exactly one central Kaggle credential mode in `provider.env`;
5. private provider gateway and provider-only write-permit signing keys;
6. an eligible exact-scope OpenCode client in private `oauth.env`.

Run only after the reviewed commit is approved:

```bash
MY_DATA_HUB_APPROVED_CONTROL_COMMIT="$(git rev-parse HEAD)" \
  deploy/control-plane/install.sh INSTALL_MY_DATA_HUB_UNIFIED_BOOTSTRAP
```

The installer does not advance the `current` release link until `/health/ready` proves
`master_runtime_ready=true`, `master_provider_status=available`, and
`provider_gateway_ready=true` with `unified_bootstrap_mode=true`. A failure remains under
the existing ERR trap and restores the previous unit and release pointer. Readiness does
not claim a verified checkpoint, ACTIVE master, successful data read, or any canonical
write.

## Live evidence required after an operator deploy

A release is operationally proven only after retaining sanitized receipts for:

1. exact deployed commit and installer readiness JSON;
2. OpenCode and ChatGPT catalogs with the exact bounded scopes and no generic SQL, acceptance, or canonical write tools;
3. provider Dataset create while master is `ABSENT`, with no master ensure request;
4. a cold bounded read returning a durable `WAITING_FOR_MASTER` operation;
5. the reconcile bridge consuming that operation and making one Kaggle master launch;
6. an ACTIVE callback followed by a successful retry of the original read.

Implementation of this profile performs no live deploy, root installation, OAuth login,
Kaggle run, or checkpoint mutation.
