# Devstand deployment: lightweight control plane

Status: `CONTROL RUNTIME IMPLEMENTED / OPERATIONAL INSTALL BLOCKED`

DevCoveer is the permanent lightweight control-plane host. It must not run production
PostgreSQL, PGDATA, master migrations, canonical committer or master backup.

`compose.control-plane.yaml` defines a DB-free stack of three loopback-only processes:
the lifecycle control plane, remote MCP resource server and OAuth authorization server.
The control readiness is healthy at `master_state=ABSENT`; data methods fail closed. The
implementation branch adds the durable ledger, one concrete Kaggle adapter, master
lifecycle coordinator, metadata-only runtime callbacks/checkpoint registry, epoch
credential handoff, OAuth components and bounded MCP runtime. None of those code paths is
evidence that the current host stack is installed or that the public application routes
are ready.

The legacy same-host token is permanently disabled. The replacement installer has a new
explicit token and may enable only `my-data-hub-control-plane.service`; that foreground
unit reconciles exactly the three containers through the opt-in `remote-mcp` Compose
profile. It must deploy the reviewed implementation merge commit; this branch has not run
it. A separate Yandex ALB, issued certificate and DNS records now terminate public TLS
without changing DevCoveer's VPN/Xray listeners. The restricted tunnel reaches the old
control callback on loopback, but MCP and OAuth remain `502` until the reviewed
three-process stack is installed; see
[`yandex-edge-deployment.md`](yandex-edge-deployment.md).

Disposable database work uses root `compose.yaml` only, tmpfs only, and ends with
`docker compose down -v`.

## Provider launch configuration

The control process accepts a provider configuration only when the complete set below is
present. Partial configuration remains healthy but reports the provider unavailable and
makes no Kaggle call.

```text
KAGGLE_API_TOKEN or KAGGLE_USERNAME + KAGGLE_KEY (control process only)
MY_DATA_HUB_KAGGLE_MASTER_SOURCE_IDENTITY
MY_DATA_HUB_KAGGLE_MASTER_SOURCE_VERSION
MY_DATA_HUB_KAGGLE_MASTER_CHECKPOINT_REF
MY_DATA_HUB_KAGGLE_MASTER_DATASET_REF
MY_DATA_HUB_KAGGLE_MASTER_NOTEBOOK_REF
MY_DATA_HUB_KAGGLE_MASTER_DATASET_DIR
MY_DATA_HUB_KAGGLE_MASTER_NOTEBOOK_SOURCE
MY_DATA_HUB_CALLBACK_URL
MY_DATA_HUB_KAGGLE_CHECKPOINT_VERIFIER_REF
MY_DATA_HUB_KAGGLE_CHECKPOINT_VERIFIER_SOURCE_FILE
MY_DATA_HUB_KAGGLE_CHECKPOINT_PROBE_RELATIONS_JSON
MY_DATA_HUB_MASTER_TUNNEL_GATEWAY_HOST
MY_DATA_HUB_MASTER_TUNNEL_GATEWAY_PORT
MY_DATA_HUB_MASTER_TUNNEL_GATEWAY_USER
MY_DATA_HUB_MASTER_TUNNEL_REMOTE_PORT
MY_DATA_HUB_KAGGLE_MASTER_SECRET_BINDINGS_JSON
```

`MY_DATA_HUB_CALLBACK_URL` is not an arbitrary deployment input. Production
assembly accepts only
`https://mcp-datahub.kenigevents.ru/internal/runtime/events`, without userinfo,
custom port, query or fragment, before a per-attempt runtime token can be
delivered to a Notebook. The control process creates a random per-attempt
callback token, stores only its SHA-256, and places the raw value only in an
exact private `ORCHESTRATOR_PROTECTED` status Dataset attached by numeric
version. There is no runtime-token root or callback-token Kaggle User Secret.

The source/version/ref/path values above come from the independently verified
`master-assets.env`; the provider environment may not override them. Compose loads that
file from the reviewed asset bundle. The four tunnel values are deployment inputs in the
private provider environment.

The control process generates a distinct operation/epoch-bound self-signed PostgreSQL
certificate and private key for every attempt. Both travel only in the same exact private
status Dataset as the callback token; the control ledger retains the public certificate
and hashes, never the key. The Notebook validates the hashes and writes both to fixed
mode-`0600` files below `/kaggle/working`. Control atomically publishes the public
certificate in the shared mode-`0700` TLS directory before launch/activation; remote MCP
mounts that directory read-only so atomic rotation is visible by pathname rather than a
stale bind-mounted inode.

`MY_DATA_HUB_KAGGLE_MASTER_SECRET_BINDINGS_JSON` may contain only optional
`YDB_ACCESS_TOKEN_CREDENTIALS`. It cannot include TLS material, `KAGGLE_API_TOKEN`,
`KAGGLE_USERNAME`, `KAGGLE_KEY`, arbitrary environment names, or the
per-attempt callback token: the control-owned automated Kaggle credential remains
central and is never copied into the PostgreSQL master Notebook.

Production checkpoint publication uses the central lifecycle adapter as a split-upload
broker. The master sends only fixed names, lengths and hashes; control starts a Kaggle blob
upload and returns only the short-lived HTTPS `create_url`. The opaque blob token remains
encrypted in the mode-`0600` control ledger. The master streams each artifact directly to
Kaggle blob storage, and only the central adapter finalizes the private Dataset version and
launches the exact-version restore verifier. Neither checkpoint bytes nor provider
credentials cross the devstand metadata APIs.

The dataset directory and Notebook/verifier source paths are bounded regular files read
from the reviewed release. The checkpoint and verifier provider references are permanent
`orchestrator_protected` resources. Names alone never authorize them.

## Checkpoint data boundary

Checkpoint creation, exact numeric-version readback and isolated restore verification run
inside Kaggle. Backup packages and restored PostgreSQL directories stay below
`/kaggle/working`; only bounded manifests, hashes, provider version references and state
transitions cross the callback API. A later master boots only from the numeric version in
the durable verified HEAD, never from provider `latest`.

The broker encryption key is generated by the installer at
`$MY_DATA_HUB_SECRET_ROOT/checkpoint-upload-broker.key`, must be a root-owned regular
mode-`0600` file containing exactly 32 random bytes, and is mounted only into the control
container. It is never mounted into remote MCP or a Kaggle Notebook.

## Installation gates

Do not install or enable the unit until all of these have evidence:

1. the implementation PR exact head is XHigh-reviewed and merged;
2. the pinned official Kaggle SDK authenticates through exactly one automated
   control-side credential mode, a private master runtime-attestation canary passes,
   and any ordinary exact-GetKernel worker flow has its separately required API-token proof;
3. task-owned permanent protected resources and their exact claims exist;
4. the full real provider/checkpoint/fencing matrix is green;
5. the edge is provisioned without changing the existing VPN and the owner OAuth
   configuration is available;
6. the merge commit passes control process-kill and host-reboot recovery.

The installation command additionally refuses to proceed unless:

- `MY_DATA_HUB_APPROVED_CONTROL_COMMIT` equals the clean checkout `HEAD`;
- `loginctl show-user` reports `Linger=yes` for the service user;
- provider, MCP-reader and OAuth environment files are distinct regular non-symlink
  files with no group/world permissions;
- the OAuth signing key has the same private-file constraint;
- the overlap JWKS is a bounded regular public-key file (`{"keys":[]}` before the first
  rotation, then up to four previous public verification keys);
- the master TLS directory is a private non-symlink directory and its `ca.pem` target is
  a regular mode-`0600` file;
- the bounded master-asset root is a real non-symlink directory;
- the asset bundle contains the reviewed exact-hash portable PostgreSQL 18.4/pgvector
  0.8.6 archive, its pinned build recipe/provenance, and a hashed tunnel host-key asset;
- the private provider environment contains exact tunnel coordinates but no TLS values or
  asset identity override; the shared TLS directory is private and atomically writable by
  control/read-only to remote MCP;
- none of the static environments contains a PostgreSQL data-plane URL or crosses the
  provider/MCP/OAuth secret boundary.

Default paths are below `$HOME/.local/state/my-data-hub-control-plane`; operators may
override them only with the installer variables documented by `deploy/control-plane/install.sh`:

```text
MY_DATA_HUB_CONTROL_PROVIDER_ENV_FILE
MY_DATA_HUB_MCP_ENV_FILE
MY_DATA_HUB_OAUTH_ENV_FILE
MY_DATA_HUB_OAUTH_SIGNING_KEY_FILE
MY_DATA_HUB_OAUTH_OVERLAP_JWKS_FILE
MY_DATA_HUB_MASTER_TLS_DIR
MY_DATA_HUB_MASTER_ASSET_DIR
MY_DATA_HUB_CONTROL_LEDGER_DIR
MY_DATA_HUB_MASTER_SESSION_DIR
```

The generated non-secret Compose environment records only these paths, UID/GID and exact
image commit. Upstreams bind only to `127.0.0.1:8080`, `:8765` and `:8780`. Docker
`restart: unless-stopped` handles process recreation, while the enabled foreground user
systemd unit plus lingering reconciles all three after login-independent host boot.

`PREPARE_CONTROL_PLANE` builds an immutable release/image without reading secrets,
changing the current pointer or enabling a unit. Only the separately gated
`INSTALL_MY_DATA_HUB_CONTROL_PLANE` action reads the private inputs and installs the stack.
Neither action provisions DNS, Yandex Cloud, certificates, VPN or a local database.

Before even preparing an image, the installer requires at least 4 GiB free on the
runtime/release filesystem (bounded override: `MY_DATA_HUB_CONTROL_MIN_FREE_KIB`). Each
container uses Docker `json-file` rotation capped at five 10 MiB files; durable ledger
retention is enforced separately by the control-ledger policy.

## Signed host evidence closure

After an authorized install, process-failure exercise and host reboot, follow
[`deployment-evidence.md`](deployment-evidence.md). The collector derives the exact clean
source/installed-release hash, immutable running image IDs, systemd/Compose recovery,
DB-free boundary and loopback inventory before using a separate external Ed25519 key. It
does not initiate the reboot and no repository example is deployment evidence.

Post-deploy acceptance is restricted to the owner MCP resource
`https://mcp-datahub.kenigevents.ru/mcp` and authorization server
`https://identity.kenigevents.ru`. It runs trusted default-branch verifier code only after
the requested deployment SHA equals the approved repository variable and is proven to be
a reachable merge commit. Reader credentials are scoped to the verification step.
