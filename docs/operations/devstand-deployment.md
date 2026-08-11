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
evidence that the current host is installed or that a public endpoint exists.

The legacy same-host token is permanently disabled. The replacement installer has a new
explicit token and may enable only `my-data-hub-control-plane.service`; that foreground
unit reconciles exactly the three containers through the opt-in `remote-mcp` Compose
profile. It must deploy the reviewed implementation merge commit; this branch has not run
it. No DNS/VPN/443 change has been made.

Disposable database work uses root `compose.yaml` only, tmpfs only, and ends with
`docker compose down -v`.

## Provider launch configuration

The control process accepts a provider configuration only when the complete set below is
present. Partial configuration remains healthy but reports the provider unavailable and
makes no Kaggle call.

```text
KAGGLE_API_TOKEN
MY_DATA_HUB_KAGGLE_MASTER_SOURCE_IDENTITY
MY_DATA_HUB_KAGGLE_MASTER_SOURCE_VERSION
MY_DATA_HUB_KAGGLE_MASTER_CHECKPOINT_REF
MY_DATA_HUB_KAGGLE_MASTER_DATASET_REF
MY_DATA_HUB_KAGGLE_MASTER_NOTEBOOK_REF
MY_DATA_HUB_KAGGLE_MASTER_DATASET_DIR
MY_DATA_HUB_KAGGLE_MASTER_NOTEBOOK_SOURCE
MY_DATA_HUB_CALLBACK_URL
MY_DATA_HUB_KAGGLE_RUNTIME_TOKEN_SECRET_NAME
MY_DATA_HUB_KAGGLE_CHECKPOINT_VERIFIER_REF
MY_DATA_HUB_KAGGLE_CHECKPOINT_VERIFIER_SOURCE_FILE
MY_DATA_HUB_KAGGLE_CHECKPOINT_PROBE_RELATIONS_JSON
MY_DATA_HUB_MASTER_RUNTIME_TOKEN_ROOT
```

`MY_DATA_HUB_KAGGLE_MASTER_SECRET_BINDINGS_JSON` is an optional exact mapping from
Notebook environment names to Kaggle User Secret names. It may include the modern
`KAGGLE_API_TOKEN` binding needed by the in-Notebook instance of the same official adapter.
It must not bind the derived per-attempt runtime token back to itself.

The dataset directory and Notebook/verifier source paths are bounded regular files read
from the reviewed release. The checkpoint and verifier provider references are permanent
`orchestrator_protected` resources. Names alone never authorize them.

## Checkpoint data boundary

Checkpoint creation, exact numeric-version readback and isolated restore verification run
inside Kaggle. Backup packages and restored PostgreSQL directories stay below
`/kaggle/working`; only bounded manifests, hashes, provider version references and state
transitions cross the callback API. A later master boots only from the numeric version in
the durable verified HEAD, never from provider `latest`.

## Installation gates

Do not install or enable the unit until all of these have evidence:

1. the implementation PR exact head is XHigh-reviewed and merged;
2. a modern Kaggle access token passes private Notebook source/status/output readback;
3. task-owned permanent protected resources and their exact claims exist;
4. the full real provider/checkpoint/fencing matrix is green;
5. the edge/OAuth configuration is provisioned without changing the existing VPN;
6. the merge commit passes control process-kill and host-reboot recovery.

The installation command additionally refuses to proceed unless:

- `MY_DATA_HUB_APPROVED_CONTROL_COMMIT` equals the clean checkout `HEAD`;
- `loginctl show-user` reports `Linger=yes` for the service user;
- provider, MCP-reader and OAuth environment files are distinct regular non-symlink
  files with no group/world permissions;
- the OAuth signing key has the same private-file constraint;
- the master TLS CA is a regular non-symlink file;
- the bounded master-asset root is a real non-symlink directory;
- none of the static environments contains a PostgreSQL data-plane URL or crosses the
  provider/MCP/OAuth secret boundary.

Default paths are below `$HOME/.local/state/my-data-hub-control-plane`; operators may
override them only with the installer variables documented by `deploy/control-plane/install.sh`:

```text
MY_DATA_HUB_CONTROL_PROVIDER_ENV_FILE
MY_DATA_HUB_MCP_ENV_FILE
MY_DATA_HUB_OAUTH_ENV_FILE
MY_DATA_HUB_OAUTH_SIGNING_KEY_FILE
MY_DATA_HUB_MASTER_TLS_CA_FILE
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
