# Remote MCP endpoint and OAuth operations

Status: `PUBLIC DNS/TLS EDGE OBSERVED / APPLICATION AND OWNER LOGIN NOT DEPLOYED`

The canonical resource is exactly:

```text
https://mcp-datahub.kenigevents.ru/mcp
```

The dedicated Yandex Cloud edge and hostname-valid certificate were observed on
2026-08-11 as recorded in
[`operations/yandex-edge-deployment.md`](operations/yandex-edge-deployment.md). That
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

## Owner bootstrap: Yandex Identity Hub, not a local password

Use an existing Yandex Identity Hub user pool and Web OIDC application. Identity Hub owns
the password/passkey policy, brute-force controls, one-time password ceremony and mandatory
first-login password change; the authorization service stores no owner password and uses
PyJWT's asymmetric issuer/JWKS verification. The local principal is always
`datahub-owner`; the opaque provider `sub` is pinned separately in
`MY_DATA_HUB_OWNER_OIDC_SUBJECT` and is mapped to that fixed local principal only after
signature, issuer, audience, expiry and `auth_time` validation.

The issuer now includes only the narrow OIDC authorization-code callback portal needed
to complete that upstream ceremony. It stores no password, access token or server-side
browser session. An encrypted five-minute PKCE/state cookie uses a persistent owner-only
32-byte key, so a process restart does not lose an in-flight login; the completed browser
session is the provider-signed ID token and remains independently verifiable after a
restart. This is unrelated to Kaggle provider authorization and does not add a Kaggle
client or move `KAGGLE_USERNAME`/`KAGGLE_KEY` out of the central lifecycle adapter.

One-time bootstrap sequence:

1. Create or identify the Identity Hub local user and Web OIDC application, assign only that
   user to the app, and register
   `https://identity.kenigevents.ru/owner/callback`. The integrated portal sets a
   `Secure; HttpOnly; SameSite=Lax` provider-signed JWT cookie on
   `identity.kenigevents.ru`; the repository does not accept a plaintext/basic-auth
   substitute. Store the OIDC application secret in a task-owned Lockbox entry or the
   service-owned `0600` file named by `MY_DATA_HUB_OWNER_OIDC_CLIENT_SECRET_FILE`.
   Store the separate random 32-byte restart-safe state key in the `0600` file named by
   `MY_DATA_HUB_OWNER_PORTAL_STATE_KEY_FILE`.
2. Generate/reset the one-time provider credential with
   `yc organization-manager idp user reset-password <USER_ID>`. The command emits the
   value once, so run it only in an owner-private terminal; never paste it into Git,
   shell history, a receipt, or this runbook. The first Identity Hub login must change it.
3. Export the resulting provider session to a temporary `0600` file and run
   `python3 scripts/verify_owner_oidc_bootstrap.py --token-file <FILE> --issuer <ISSUER> --audience <OIDC_CLIENT_ID> --jwks-url <JWKS_URL> --provider-subject <SUB>`.
   Delete the temporary file after the sanitized verifier returns
   `local_principal=datahub-owner`.

The accepted provider/portal coordinates and exact provider subject are external inputs;
none are fabricated in repository examples. Until the portal, OIDC app, user assignment,
first-login rotation and verifier receipt exist, owner OAuth remains an explicit external
blocker and the application must stay fail-closed.

## Exact three-step ChatGPT connection

Perform the following sequence once for `chatgpt-reader`, then repeat it for
`chatgpt-owner-operator` only after the write gate has passed. OpenAI's current Developer
mode supports streaming HTTP and OAuth with supplied static credentials; these profiles
use public-client token exchange (`none`), so there is no ChatGPT client secret to retrieve.

1. In ChatGPT on the web, enable **Settings → Security and login → Developer mode**.
2. Open **ChatGPT Plugins**, choose **+**, create a developer-mode app with server URL
   `https://mcp-datahub.kenigevents.ru/mcp`, OAuth, and the static client ID
   `chatgpt-reader` (or `chatgpt-owner-operator`). Copy the callback URI displayed by
   ChatGPT into that client's exact `redirect_uris` allowlist before retrying connection.
3. Complete the `datahub-owner` Identity Hub login and consent, then refresh the app. The
   reader app must show exactly the 15 tools above; the owner app must show writes only
   when the independently signed write-gate receipt is active.

Official references: [ChatGPT Developer mode](https://developers.openai.com/api/docs/guides/developer-mode)
and [building MCP servers](https://developers.openai.com/api/docs/mcp).

## Secret retrieval, rotation and revocation

Commands below name references and files, never secret values.

Retrieve a Lockbox entry directly into a private file (do not omit the redirection):

```bash
umask 077
yc lockbox payload get --id "$MY_DATA_HUB_OWNER_OIDC_LOCKBOX_ID" \
  --key owner-oidc-client-secret > "$HOME/.local/state/my-data-hub-control-plane/secrets/owner-oidc-client-secret"
chmod 0600 "$HOME/.local/state/my-data-hub-control-plane/secrets/owner-oidc-client-secret"
```

Rotate the Identity Hub bootstrap credential and force the provider's next-login change:

```bash
yc organization-manager idp user reset-password "$MY_DATA_HUB_OWNER_IDP_USER_ID"
```

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
