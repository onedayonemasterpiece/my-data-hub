# Permanent same-host deployment and auto-start

`DevCoveer` is the permanent execution host, not an SSH target to discover elsewhere.
This document defines the evidence needed before that installation is considered verified
and ready for broad writes or Region Talk migration.

## Host layout

```text
~/.local/opt/my-data-hub/releases/<commit>  immutable user-owned release
~/.local/opt/my-data-hub/current            atomic release symlink
~/.local/state/my-data-hub/releases/<commit>/*.env  mode-0600 split environments
my-data-hub-postgres-data                   stable Docker PostgreSQL volume
my-data-hub-artifacts                       private Docker artifact volume
~/.local/state/my-data-hub/backups          encrypted local staging
```

## Immediate safety state

```text
scheduler=false
production publication=false
remote MCP=false until OAuth/TLS gate
MCP writes=false
Region Talk pipeline=paused
```

Record these observed values after service start and host reboot.

## Verification sequence

1. Record commit, clean tree, image digests and runtime versions.
2. Record listening sockets/firewall; PostgreSQL/API/MCP upstreams must be private.
3. Apply migrations, run status/verify, apply again and prove idempotency.
4. Create split PostgreSQL roles and run positive/negative grant probes.
5. Run repository tests and live PostgreSQL verification scripts.
6. Prove PostgreSQL/API/orchestrator recovery after process failure and host reboot.
7. Create encrypted backup, off-host copy, readback hash and isolated restore.
8. Configure CI/post-deploy/nightly/restore/provider workflows.
9. Publish remote MCP read-only at the accepted hostname.
10. Prove synthetic connector and Kaggle sandbox controls.
11. Prove operator access in a disposable schema.
12. Only then begin Region Talk inventory/export.

Prepare without database mutation:

```bash
deploy/same-host/install.sh PREPARE
```

The reviewed installation command is:

```bash
deploy/same-host/install.sh INSTALL_MY_DATA_HUB_SAME_HOST
```

It stages the exact clean commit, provisions distinct PostgreSQL LOGINs, applies the
append-only migrations and grant probes, starts PostgreSQL/API/orchestrator/committer and
encrypted local backup containers, and enables `my-data-hub-compose.service` in the
lingering user systemd manager. Docker ports 5432 and 8080 remain loopback-only. Remote MCP
is not started until TLS and OAuth are complete. Write observed results to
`docs/operations/first-deploy.md`.

## Required services

- PostgreSQL: restart on failure/boot, local/private only.
- API/intake: private upstream with bounded health/result/connector routes.
- Orchestrator: one active scheduler identity; DB fencing; plan-only until gate.
- MCP: local stdio plus private HTTP upstream; public only via TLS/OAuth edge.
- Backup: scheduled local/off-host generation and restore drill.
- Reverse proxy/edge: public 443 only for `mcp-datahub.kenigevents.ru`.

## Public endpoint on the same host

Canonical URL:

```text
https://mcp-datahub.kenigevents.ru/mcp
```

The DNS record points to `188.227.84.107`; no new Compute/ALB is needed. TCP 443 is already
used by Xray REALITY, so nginx must SNI-multiplex the MCP hostname to a separate internal
TLS listener and default traffic to the relocated loopback Xray listener. This is a
controlled VPN change with rollback and client regression proof, not a blind nginx path
edit. Do not expose port 8765 or PostgreSQL publicly.

Connector `/intake/v1` may share the edge temporarily with separate service auth and
upstream policy.

## Database roles before remote writes

At minimum:

- owner/migrator;
- app;
- orchestrator/committer;
- connector intake;
- MCP reader;
- MCP editor;
- migration operator;
- backup;
- monitor.

No remote role gets superuser, ownership, `BYPASSRLS`, role/database creation,
replication, extension, server file or program execution rights.

## Availability

PostgreSQL remains supervised/always-on. If the host is down, connector producers retain
batches in durable local spools. Kaggle is not a master database. An optional future
Yandex host-start controller must be external to the orchestrator and use minimal IAM.

## Acceptance evidence

- deployed commit/image/schema receipt;
- service process-kill and reboot recovery;
- open-port/firewall evidence;
- role/grant matrix and negative probes;
- backup/readback/restore receipt;
- remote MCP OAuth/Host/Origin negative tests;
- synthetic connector round trip/replay/outage;
- Kaggle protected-resource denials;
- operator disposable-schema canary;
- explicit dangerous-gate states.
