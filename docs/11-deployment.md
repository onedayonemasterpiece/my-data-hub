# Deployment profile: local devstand = initial production

## Services

- `postgres`: canonical database, private network only;
- `my-data-hub-api`: health/internal worker ingress;
- `my-data-hub-orchestrator`: scheduler/reconciler;
- `my-data-hub-mcp`: stdio locally; remote service only after OAuth port;
- backup timer/job;
- optional reverse proxy for API/MCP, never PostgreSQL.

## Filesystem

```text
/opt/my-data-hub/current          # checkout/release
/etc/my-data-hub/my-data-hub.env # root-readable secrets
/var/lib/my-data-hub/artifacts   # immutable bundles
/var/lib/my-data-hub/backups     # encrypted backups
/var/log/my-data-hub             # journald preferred
```

## Database roles

- `mdh_owner`: migrations only;
- `mdh_app`: normal DML on app schemas;
- `mdh_backup`: pg_dump/backup read permissions;
- `mdh_diagnostics`: read-only bounded diagnostics.

Compose bootstrap uses one role for convenience. Code agent must create split
roles before production migration.

## Autostart

Systemd units are provided under `deploy/systemd/`. The agent must install
units with absolute paths, restricted environment file, restart policy,
network ordering and hardening appropriate to the host.

## Backup

Initial minimum:

- nightly encrypted logical dump;
- manifest with database/schema/tool versions and SHA-256;
- retention policy;
- monthly restore into isolated DB and invariant checks;
- backup age/restore status surfaced in health.

Before YDB cutover, perform and record a real restore.
