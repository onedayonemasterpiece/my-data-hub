# Deployment profile: local devstand = initial production

## Services

- `postgres`: supervised canonical database, private/loopback only;
- `my-data-hub-api`: health, connector intake and internal worker ingress;
- `my-data-hub-orchestrator`: scheduler/reconciler/canonical committer;
- `my-data-hub-mcp`: stdio locally and Streamable HTTP privately behind TLS/OAuth;
- reverse proxy/edge for `mcp-datahub.kenigevents.ru`, never PostgreSQL;
- backup/readback/restore jobs;
- optional future external host availability controller.

Kaggle is compute/private-artifact storage, not a database service or automatic failover.

## Filesystem

```text
/opt/my-data-hub/current          # checkout/release
/etc/my-data-hub/my-data-hub.env # root-readable secrets
/var/lib/my-data-hub/artifacts   # immutable bundles
/var/lib/my-data-hub/connectors  # local intake/spool staging where required
/var/lib/my-data-hub/backups     # encrypted backups
/var/log/my-data-hub             # journald preferred
```

## Database roles

- schema/migration owner;
- application runtime;
- orchestrator/committer;
- connector intake;
- MCP reader;
- MCP editor;
- migration operator;
- backup/restore;
- monitoring.

Compose bootstrap uses one role only as a temporary convenience. Split roles and
negative grant tests are required before remote writes or Region Talk import.

## Public endpoint

```text
https://mcp-datahub.kenigevents.ru/mcp
```

Only TCP 443 is public. The edge handles DNS/TLS and coarse limits; MCP still validates
OAuth resource/audience, Host, Origin, scopes, profiles and dynamic targets. Development
token mode remains loopback-only.

Connector `/intake/v1` may share the hostname initially but uses a separate service
authentication policy and upstream.

## Autostart and availability

Systemd units under `deploy/systemd/` or equivalent Compose supervision must prove:

- dependency ordering;
- restart after process failure;
- restart after host reboot;
- publication and write gates preserve expected disabled state;
- one active scheduler identity;
- PostgreSQL/internal ports remain private.

The orchestrator cannot wake its own stopped host. A future Yandex availability
controller must be independent and narrowly scoped.

## Backup

Before broad writes or Region Talk migration:

- encrypted local logical backup;
- encrypted off-host generation (private protected Kaggle dataset or approved storage);
- manifest with versions/revision/hash;
- readback SHA-256 verification;
- isolated restore and integrity checks;
- multiple retained generations;
- freshness surfaced as an operator gate.

See [`operations/backup-and-recovery.md`](operations/backup-and-recovery.md).

## Deployment order

Follow [`15-infrastructure-first-plan.md`](15-infrastructure-first-plan.md): verify
infrastructure and test workflows before any heavy migration.
