# Deployment profile: `DevCoveer` same-host production

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

The immediately deployable non-root profile uses immutable releases under
`~/.local/opt/my-data-hub`, immutable per-release mode-0600 environments under
`~/.local/state/my-data-hub/releases/<commit>`, stable receipts/backups under
`~/.local/state/my-data-hub`, plus fixed Docker volumes
`my-data-hub-postgres-data` and `my-data-hub-artifacts`.

`compose.same-host.yaml` is production-specific and does not reuse the development
catch-all `.env`. The native root-owned units under `deploy/systemd/` remain an optional
harder isolation profile when interactive sudo is available.

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

The same-host installer uses the bootstrap login only for one-shot administration. Every
long-running process receives only its required restricted URL(s); distinct LOGIN,
membership and negative grant tests run before services start.

## Public endpoint

```text
https://mcp-datahub.kenigevents.ru/mcp
```

Only TCP 443 is public. On this host it is shared with the existing Xray service through
SNI routing; MCP receives only the exact hostname and `/mcp` resource. The edge handles DNS/TLS and coarse limits; MCP still validates
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
