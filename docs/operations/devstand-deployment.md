# Devstand deployment: lightweight control plane

Status: `CONTROL RUNTIME IMPLEMENTED / OPERATIONAL INSTALL BLOCKED`

DevCoveer is the permanent lightweight control-plane host. It must not run production
PostgreSQL, PGDATA, master migrations, canonical committer or master backup.

`compose.control-plane.yaml` defines one loopback-only DB-free process. Its readiness is
healthy at `master_state=ABSENT`; its data methods fail closed. The implementation branch
adds the durable ledger, one concrete Kaggle adapter, master lifecycle coordinator,
metadata-only runtime callbacks/checkpoint registry, epoch credential handoff, OAuth
resource/authorization components and the bounded MCP runtime. None of those code paths is
evidence that the current host is installed or that a public endpoint exists.

The legacy same-host token is permanently disabled. The replacement installer has a new
explicit token and may enable only `my-data-hub-control-plane.service`. It must deploy the
reviewed implementation merge commit; this branch has not run it. No DNS/VPN/443 change
has been made.

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
