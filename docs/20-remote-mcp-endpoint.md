# Remote MCP endpoint and OAuth operations

Status: `PUBLIC SERVICE LIVE / LOCAL-EDGE CUTOVER PENDING`

The canonical resource is exactly:

```text
https://mcp-datahub.kenigevents.ru/mcp
```

The Yandex Cloud edge was observed historically on
2026-08-11 as recorded in
[`operations/yandex-edge-deployment.md`](operations/yandex-edge-deployment.md). ADR-0019 retires that topology after a guarded local cutover. That historical
observation does **not** prove that the reviewed remote-MCP/OAuth containers are installed:
the application routes last returned `502`. Do not describe the endpoint, OAuth, ChatGPT,
process recovery, reboot recovery, or master cold start as live until the signed v2
post-deploy verifier passes against the deployed merge commit.

## Exact reader surface

`chatgpt-reader` must discover these 15 tools and no others:

```text
bloggers.get
bloggers.list
bloggers.migration.accounting
bloggers.provenance
bloggers.search
bloggers.statistics
checkpoint.status
data.change.status
data.query
embedding.coverage
embedding.production.capabilities
master.status
operation.get
platform.status
provider.resources.status
```

`chatgpt-owner-operator` is a separate static public OAuth client. It receives the reader
scopes plus guarded operator scopes only when the server-side write gate is enabled. Merely
connecting the owner client never enables writes.

## Owner authorization: local devstand browser token

The fixed local principal is `datahub-owner`.  Production authorization uses the same
owner-controlled pattern already proven by eventsBot MCP: OpenCode or ChatGPT opens the
issuer's HTTPS authorization page, the owner enters one high-entropy operator token, and
the issuer sets a short-lived `Secure; HttpOnly; SameSite=Lax` session cookie.  There is no
Yandex Identity Hub redirect, external identity VM, MCP client secret, or manually copied
access/refresh token.

The raw operator token exists only in the owner-mode `0600` file named by
`MY_DATA_HUB_OWNER_OPERATOR_TOKEN_FILE`.  The separate exact 32-byte
`MY_DATA_HUB_OWNER_PORTAL_STATE_KEY_FILE` encrypts the five-minute form state and one-hour
browser session.  Neither value enters `oauth.env`, repository configuration, URLs, logs,
MCP messages, Kaggle resources, or receipts.  OAuth authorization codes remain exact
client/resource/redirect/PKCE bound and refresh tokens remain rotating and replay resistant.

Use the same browser token for the initial OpenCode and ChatGPT connections, then rotate
the file after both clients have durable refresh grants.  Rotation does not invalidate
already-issued refresh families; revoke those separately when a client must be disconnected.

## Exact three-step ChatGPT connection

ChatGPT is a dynamic public client discovered through its HTTPS Client ID Metadata Document
(CIMD).  There is no ChatGPT client secret or manually registered callback to retrieve.

1. In ChatGPT, enable Developer mode and add a custom MCP connection whose server URL is
   exactly `https://mcp-datahub.kenigevents.ru/mcp`.
2. Choose OAuth/Connect.  The server fetches and validates ChatGPT's exact HTTPS client
   metadata and callback, then displays the `my-data-hub` owner form.
3. Enter the owner operator token, approve the requested scopes, and refresh the connection.
   Provider-only mode exposes only `platform:read`, `provider:read`, and `provider:write`;
   canonical-data/operator scopes remain unavailable until their independent gates pass.

OpenCode is an independent static public PKCE client named `opencode-my-data-hub`; it uses
`http://127.0.0.1:<port>/mcp/oauth/callback` on the computer where OpenCode runs.  It opens
the same owner form and receives a distinct refresh-token family.  Thus two connections are
expected and independently revocable even though the owner verifies with the same browser
token.

Official references: [ChatGPT Developer mode](https://developers.openai.com/api/docs/guides/developer-mode)
and [building MCP servers](https://developers.openai.com/api/docs/mcp).

## Secret retrieval, rotation and revocation

Never print the operator token in a shell transcript or store it in Git.  Read it only from
the private file for direct delivery to the owner-approved secret channel.  To rotate it,
atomically replace `MY_DATA_HUB_OWNER_OPERATOR_TOKEN_FILE` with a newly generated
mode-`0600` value of at least 32 bytes, restart only the OAuth service, and verify the public
metadata before removing the previous private backup.

Rotate the OAuth signing key by publishing its public JWK first, moving the old public JWK
to the bounded overlap file, atomically replacing the private `0600` key, restarting the
OAuth service, and retaining the old public key for at least the maximum access-token
lifetime. Never print either private key.

Revoke one refresh family without displaying the token:

```bash
curl --fail-with-body --silent --show-error \
  --data-urlencode client_id=chatgpt-reader \
  --data-urlencode token@"$HOME/.local/state/my-data-hub-control-plane/secrets/reader-refresh-token" \
  https://identity.kenigevents.ru/revoke >/dev/null
```

Disable a compromised client in the control ledger (startup reconciliation does not
re-enable it):

```bash
python3 - <<'PY'
import os
from pathlib import Path
from my_data_hub.control_plane.ledger import ControlLedger
ledger = ControlLedger(Path(os.environ['MY_DATA_HUB_CONTROL_LEDGER_PATH']))
record = ledger.oauth_client('https://identity.kenigevents.ru', os.environ['CLIENT_ID'])
if record is None:
    raise SystemExit('client is absent')
ledger.register_oauth_client(
    issuer=record['issuer'], client_id=record['client_id'],
    principal_id=record['principal_id'], allowed_scopes=record['allowed_scopes'],
    profile_kind=record['profile_kind'], enabled=False,
)
PY
```

Revocation state lives only in the lightweight control ledger or issuer. The deleted
`record_oauth_canary_revocation.py` canonical-PostgreSQL helper must not be restored.

### Multi-hour acceptance credentials

The issuer intentionally gives access tokens a short lifetime. A 60--90 minute soak or
the full 24-scenario matrix must therefore not copy one access token into a six-hour job.
The trusted driver accepts `MY_DATA_HUB_MCP_OAUTH_CREDENTIAL_FILE`, an absolute,
owner-owned, non-symlink `0600` JSON file. It contains one independently scoped refresh
family for each required `reader`, `operator`, or `provider` profile. The driver takes a
cross-process lock, reuses a still-fresh access token, otherwise performs the public-client
`refresh_token` exchange, and atomically records the successor refresh token. A failed
exchange leaves the prior file unchanged and its exception contains no token value.

This file belongs in the existing owner-only devstand secret root and is reused by the
devstand acceptance controller across process restarts. It must not be copied to GitHub
Actions secrets, variables, artifacts or the workspace: refresh-token rotation would make
an immutable repository secret stale after the first exchange. Existing short-lived bearer
environment values remain a compatibility fallback for bounded probes, but are not
sufficient evidence for the multi-hour matrix.

`provider-real.yml` consequently targets only a self-hosted Linux runner carrying the
dedicated `my-data-hub-devstand` label. The runner service supplies only the absolute local
file path through `MY_DATA_HUB_MCP_OAUTH_CREDENTIAL_FILE`; the workflow does not define that
variable and does not receive any MCP access or refresh token from GitHub. Before the
acceptance controller performs any live action, the checked-in preflight requires the
self-hosted runner identity, validates the owner-owned
mode-0600 file outside both the workspace and runner temporary directory, requires the
`reader`, `operator`, and `provider` refresh families, and rejects inherited static MCP
bearer variables. A GitHub-hosted dispatch cannot run this job because no matching runner
is eligible.

Both the operational driver and scheduled acceptance create authorization at request time.
The latter uses asynchronous HTTP authentication backed by the same `BearerSource`, so each
MCP HTTP request rechecks the cached expiry and atomically rotates the refresh family when
needed. A token is never placed in command arguments, receipts, controller artifacts, or
logs. Nightly's short bounded probes retain the static-source compatibility path.

Devstand preflight (it prints only its typed outcome):

```bash
python scripts/provider/devstand_acceptance_controller.py preflight
```

The private file uses this exact shape (placeholders are not credentials):

```json
{
  "schema_version": "my-data-hub-mcp-oauth-credentials.v1",
  "token_endpoint": "https://identity.kenigevents.ru/token",
  "resource": "https://mcp-datahub.kenigevents.ru/mcp",
  "profiles": {
    "reader": {
      "client_id": "acceptance-reader",
      "refresh_token": "<owner-private refresh grant>",
      "access_token": null,
      "access_expires_at": null
    }
  }
}
```

Do not put this file below `artifacts/`, a status Dataset, a Notebook input, GitHub, runner
temporary storage, or an MCP payload. A continuation uses the atomically persisted successor
in the same devstand file; an expired/replayed family fails closed rather than falling back
to a broad token.

## Post-deploy negative proof

On the devstand, immediately before verification, create a short-lived private bundle:

```bash
umask 077
python3 scripts/prepare_oauth_negative_canaries.py \
  --signing-key-file "$MY_DATA_HUB_OAUTH_SIGNING_KEY_FILE" \
  --control-ledger "$MY_DATA_HUB_CONTROL_LEDGER_PATH" \
  --key-id "$MY_DATA_HUB_OAUTH_SIGNING_KEY_ID" \
  --output "$HOME/.local/state/my-data-hub-control-plane/secrets/post-deploy-negative.json"
```

The bundle contains seven independent bearer values for invalid, expired, revoked, wrong
issuer, wrong audience, wrong resource and wrong scope cases. The revoked canary is written
to the control ledger before the file is published. Pass the private file as
`--negative-credentials-file`; the verifier additionally checks missing auth, wrong Host
and wrong Origin. Delete the bundle after the run. A synthetic test receipt is not live
evidence.
