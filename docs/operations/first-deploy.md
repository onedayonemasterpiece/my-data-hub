# First deployment receipt — R1

Status: **BLOCKED — no devstand/backend is discoverable in the authorized cloud**
Receipt observation time: `2026-08-09T22:46:15Z`
Repository branch under test: `integration/r1-infrastructure-workflow`

This document deliberately separates observations from the current Codex runner and
observations from the actual devstand. The runner is **not** asserted to be the devstand.
No output below is inferred from Compose or systemd configuration.

## Exact deployment blocker

- Blocker ID: `DEVSTAND_BACKEND_IDENTITY_MISSING`.
- The initially expired `yc` user session was successfully re-authenticated at
  `2026-08-09T22:52Z`; a real API inventory then succeeded.
- Observed cloud: `b1ghfk15fpug7mn5439l`.
- Authorized folders: `b1g5tck18cgqtjb7rn3s` (`default`) and
  `b1g0v4ur96gis5kot6ku` (`kenigevents-email-prod`).
- Observed result: **zero Compute instances in both folders**, zero ALBs, and no
  `mcp-datahub.kenigevents.ru` record in Cloud DNS. The current runner has no
  `/opt/my-data-hub/current`, `/etc/my-data-hub/my-data-hub.env`, project systemd unit,
  or project container. Public DNS lookup for the required endpoint returns NXDOMAIN.
- Required permission/input: the exact existing devstand host/resource ID plus an
  allowlisted SSH identity and pinned host key, **or** an explicit owner decision to
  provision new billable compute/edge resources. The latter is not inferred from the
  instruction to avoid duplicates.
- Verification after the devstand identity is supplied:

  ```bash
  yc compute instance get "$DEVSTAND_INSTANCE_ID" --folder-id "$FOLDER_ID"
  yc dns zone list --folder-id "$FOLDER_ID"
  gh workflow run devstand-deploy.yml -f commit="$(git rev-parse origin/main)"
  ```

Until those commands succeed, the endpoint, devstand OS, firewall, listeners, volumes,
service state, reboot recovery, TLS certificate, and image digests remain **unverified**.

### Existing Yandex Cloud conventions observed read-only

- Shared network resources live in the `default` folder: network
  `enpefopfcgibi1igmjt2` with the three default regional subnets.
- Public `kenigevents.ru.` DNS is zone `dnsbhbtvj0l1lf8jpefb` in that folder.
- The existing managed certificate `fpqi91sau05ifdvfsft4` covers only
  `kenigevents.ru` and `www.kenigevents.ru`; it does not cover the required MCP host.
- The email production folder is labeled `project=kenigevents`, `purpose=email`,
  `environment=prod` and contains no network/compute/ALB/DNS resources. It must not be
  repurposed silently as the data-hub runtime folder.
- No duplicate network, DNS zone, certificate, ALB, or instance was created.

## Repository identity observed on the runner

| Fact | Observation | Command |
|---|---|---|
| commit at receipt start | `e3d269b0704e4d7c9451634d616d2b58ac7a8682` | `git rev-parse HEAD` |
| dirty paths | `20` (expected implementation work; not a deploy receipt) | `git status --porcelain=v1 \| wc -l` |
| expected main ancestry | `0fc9c8a… -> 0b6b731…` | verified earlier with `git log --reverse` |

The final deploy receipt must replace the commit with the merged `main` commit and show
zero dirty paths.

## Local disposable PostgreSQL evidence (not devstand)

The following evidence was produced in a disposable Docker test container named
`mdh-r1-pg-3472`. It proves application contracts but not deployment completion.

| Fact | Observed value |
|---|---|
| runner OS | Ubuntu 24.04.4 LTS, kernel `6.8.0-107-generic` |
| Docker | `29.4.1` build `055a478` |
| Docker Compose | `v5.1.3` |
| image | `pgvector/pgvector:0.8.6-pg18-bookworm` |
| image digest | `sha256:691673308c99d2161ba298736f3147f1f22d79de2fb7ec93ae9b4afcab870b62` |
| PostgreSQL | `18.4 (Debian 18.4-1.pgdg12+1)` |
| pgvector | `0.8.6` |
| database mount | Docker volume at `/var/lib/postgresql` |
| listener | `127.0.0.1:55432 -> 5432/tcp` |
| container restart policy | `no` (test-only; not deployment policy) |
| runner disk | `/dev/vda2`, 79 GiB total, 15 GiB available, 81% used |

The runner also has unrelated public listeners and Docker volumes. They are not copied
into this project receipt as if they belonged to my-data-hub. `sudo -n ufw status` was
denied, so runner firewall state is also unverified.

## PostgreSQL contract receipts

- Clean migration 1–10: PASS.
- Repeated migration: PASS, zero migrations applied.
- Upgrade from released revision 9 to 10: PASS; disposable database dropped.
- Bootstrap invariants: PASS.
- Role provisioning: PASS, twelve password-free group roles.
- PostgreSQL positive/adversarial/ownership probes: PASS, 89 probes. All application
  objects in the fresh audit database were owned by `mdh_owner`; direct canonical
  singleton mutation was denied outside the dedicated bounded committer function.
- OAuth revocation before/after durable row: PASS (`false -> true`).
- Process-kill recovery: PASS in a separate disposable container; an immediate server
  stop caused Docker restart count `1` and PostgreSQL WAL recovery.
- Host reboot recovery: **BLOCKED** pending devstand access. A container restart is not
  reported as a host reboot.

## Synthetic connector receipt

Latest restricted-login disposable flow at `2026-08-09T23:56:09Z`:

| Evidence | Identifier/value |
|---|---|
| accepted batch | `44855fdd-7bb7-5b00-b25d-b04b47aac8c7` |
| acceptance receipt | `f6454369-f9b6-4d32-a1f5-045c006ba5c6` |
| conflict quarantine | `92975ca4-714a-470a-a94f-a3af1b34d35b` |
| first semantic outbox | `38e15e90-7f1d-46ec-93ea-a35e5f0a14be` |
| outage/restart eventual batch | `7e20e6f1-d140-51c5-90ef-8cbd05e44ae1` |
| canonical revision after both latest commits | `4` |
| durable producer receipt count | `1` |

The producer spool first deferred delivery during a synthetic outage, was reopened as a
new spool instance, delivered the exact bytes once, retained one durable receipt, and
replayed the canonical commit without a duplicate. The semantic MCP connector status
projection observed four committed batches across repeatable canary runs. This remains disposable CI evidence, not a
production events-bot canary.

## Safety gates required at deployment

The deploy and nightly workflows reject any environment that does not contain all three
exact values:

```text
MY_DATA_HUB_SCHEDULER_ENABLED=false
MY_DATA_HUB_PRODUCTION_PUBLISH_ENABLED=false
MY_DATA_HUB_MCP_WRITE_ENABLED=false
```

Region Talk remains `paused`; `publication_dispatch` remains disabled. The remote tool
catalog is read-only and contains no operator or provider mutation tools.

## Evidence still required from the actual devstand

1. exact merged commit and clean tree;
2. OS/kernel, Docker/Compose, image IDs/digests and PostgreSQL/pgvector versions;
3. project-owned volumes, mount points, capacity and backup directory permissions;
4. `ss` listeners plus Cloud firewall/security-group observations proving that only TCP
   443 is public and PostgreSQL/API/MCP are private;
5. exact systemd/Compose supervision model and service/restart state;
6. process-kill **and host-reboot** timestamps with service recovery;
7. encrypted backup, private off-host readback, isolated restore and MCP-read receipt;
8. DNS/TLS/edge/backend identifiers for `mcp-datahub.kenigevents.ru`;
9. OAuth negative/revocation probe identifiers and authorized MCP client trace;
10. GitHub Actions run IDs for deploy, nightly, restore and Kaggle canary.

No item in this section is marked complete merely because its desired configuration is
present in the repository.
