# First deployment receipt — R1

Status: **CURRENT PERMANENT HOST IDENTIFIED / INSTALLATION NOT YET EXECUTED**
Receipt correction time: `2026-08-10`
Target host: `DevCoveer` (`188.227.84.107`)

The earlier receipt incorrectly treated this Codex host as a temporary runner and searched
for another devstand. The owner clarified that this machine is the permanent execution
host. The cloud inventory remains useful only to prove that no duplicate Compute/ALB was
created.

## Observed permanent-host baseline

| Fact | Observation |
|---|---|
| host / OS | `DevCoveer`, Ubuntu 24.04.4 LTS, kernel `6.8.0-107-generic` |
| public IPv4 | `188.227.84.107` |
| Docker / Compose | Docker `29.4.1`; Compose `v5.1.3`; Docker enabled at boot |
| capacity | 3.8 GiB RAM, 4 GiB swap, 18 GiB disk available |
| existing edge | nginx owns TCP 80; Xray REALITY owns TCP 443 |
| PostgreSQL / my-data-hub | no permanent service or volume at observation time |
| privilege boundary | `dev` may use Docker; unattended sudo is unavailable |
| user supervision | systemd user manager is running with `Linger=yes` |

The existing VPN containers already recover at boot through `restart: unless-stopped`.
The same-host deployment therefore uses `compose.same-host.yaml`, a stable project/volume
identity, distinct service database credentials, loopback-only ports, Docker restart
policies and an enabled user-systemd reconciliation unit. Native root systemd remains an
optional stronger profile, not a prerequisite for this host.

## Current exact blockers

- The prepared same-host installer has not yet been allowed to run its PostgreSQL
  bootstrap/migrations and start new services. No permanent-runtime receipt is claimed.
- Public HTTPS cannot be added by a path-only nginx edit: TCP 443 is currently Xray. The
  safe canonical design is SNI multiplexing for `mcp-datahub.kenigevents.ru`, which needs a
  controlled Xray restart and regression test.
- Production remote MCP also requires a real OAuth issuer/JWKS. Development-token mode
  remains loopback-only and will not be exposed publicly.
- Off-host backup storage and a recovery runner remain absent.

## Corrected Yandex Cloud interpretation

Cloud `b1ghfk15fpug7mn5439l` contains no Compute instance or ALB because the permanent
runtime is this existing server. No duplicate billable resource is required. Cloud DNS
zone `dnsbhbtvj0l1lf8jpefb` may point the canonical MCP hostname to `188.227.84.107`; the
certificate must then be issued locally at the existing edge.

## Repository identity

R1 was merged to `main` as `cbea2a43ffa430e0d2c82b82db6198d30c362f65`. The supplied
data-scope documentation is integrated separately and the final installation receipt must
record the exact later merged commit deployed from a clean release archive.

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
- PostgreSQL positive/adversarial/ownership probes: PASS, 90 probes. All application
  objects in the fresh audit database were owned by `mdh_owner`; direct canonical
  singleton mutation was denied outside the dedicated bounded committer function.
- OAuth revocation before/after durable row: PASS (`false -> true`).
- Process-kill recovery: PASS in a separate disposable container; an immediate server
  stop caused Docker restart count `1` and PostgreSQL WAL recovery.
- Host reboot recovery: **NOT YET EXECUTED** on the identified permanent host. Access is
  available, but reboot remains a separately approved disruptive operation; a container
  restart is not reported as a host reboot.

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

A later restricted-role poison-batch proof accepted generic envelope evidence, rejected
the invalid product counters, wrote terminal semantic quarantine
`2d676076-e238-4c23-8e8b-fd731bd3e72c`, replayed that terminal result idempotently and
then committed two later valid batches. The supervised committer subsequently drained
four older accepted/noncommittable test remnants into terminal quarantine without
blocking or returning a false PASS.

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
