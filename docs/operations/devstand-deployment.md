# Devstand deployment and auto-start

The development host is the initial production host. The user reports that the project
has been deployed there; this document defines the evidence needed before that deployment
is considered verified and ready for broad writes or Region Talk migration.

## Host layout

```text
/opt/my-data-hub/repo             checked-out release
/var/lib/my-data-hub/postgres     Docker volume or dedicated data directory
/var/lib/my-data-hub/artifacts    private bounded cache
/var/lib/my-data-hub/connectors   private staging/spool where required
/etc/my-data-hub/env              root-readable environment file
/var/backups/my-data-hub          encrypted local staging
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

Write observed commands/results to `docs/operations/first-deploy.md`.

## Required services

- PostgreSQL: restart on failure/boot, local/private only.
- API/intake: private upstream with bounded health/result/connector routes.
- Orchestrator: one active scheduler identity; DB fencing; plan-only until gate.
- MCP: local stdio plus private HTTP upstream; public only via TLS/OAuth edge.
- Backup: scheduled local/off-host generation and restore drill.
- Reverse proxy/edge: public 443 only for `mcp-datahub.kenigevents.ru`.

## Yandex public endpoint

Canonical URL:

```text
https://mcp-datahub.kenigevents.ru/mcp
```

Use the existing Yandex CLI/deployment conventions to create Cloud DNS record,
certificate and HTTPS listener/reverse proxy. Record non-secret resource IDs and
certificate fingerprint. Do not expose port 8765 or PostgreSQL publicly.

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
