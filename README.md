# my-data-hub

`my-data-hub` is a PostgreSQL-first personal data platform with a stable devstand control
plane and a dynamically started Kaggle master Notebook.

## Canonical architecture

- **Kaggle master Notebook:** the only ACTIVE writable PostgreSQL-primary. It contains
  canonical catalog/pipeline state, FTS/pgvector, restricted roles, transactional queues,
  write gate, lease watchdog and checkpoint agent.
- **Private Kaggle Datasets:** current and previous verified checkpoints plus portable
  logical backup, manifests, hashes and restore receipts.
- **Devstand:** lightweight control/status endpoint, future stable MCP gateway, lifecycle
  adapter, callbacks, registry, leases/fencing, checkpoint metadata, security and audit.
  It holds no canonical business data, production PostgreSQL or PGDATA.
- **Data plane:** workers/connectors call ensure/resolve, receive short-lived epoch-bound
  access, then connect directly to the ACTIVE master. External agents use stable devstand
  MCP, which resolves that master.

The exact source is
[`docs/source-material/idea-hub/idea-20260809-content-platform-current-design.md`](docs/source-material/idea-hub/idea-20260809-content-platform-current-design.md).
ADR-0016 records the owner-approved correction after a local-database architecture drift.

## PR-A state

PR-A is safety and contract work only. The database-free control-plane health surface is
healthy with `master_state=ABSENT`; master lifecycle, real Kaggle calls and data tools are
not implemented by this PR. Region Talk and publication remain disabled. DNS/VPN/443 and
remote MCP writes are frozen.

## Development

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -e '.[dev]'
make validate test lint notebooks
```

The root `compose.yaml` is **disposable integration-test infrastructure only**. Its
PostgreSQL uses tmpfs and no named volume:

```bash
cp .env.example .env                 # integration-only credentials
make integration-up
make integration-verify
make integration-down                # always executes docker compose down -v
```

`make up` intentionally fails: devstand must never start a local master database.

Production/control-plane shape can be inspected without starting it:

```bash
make control-config
```

The legacy command below is permanently forbidden and exits before side effects:

```text
deploy/same-host/install.sh INSTALL_MY_DATA_HUB_SAME_HOST
```

The replacement control-plane installer has a separate explicit token and must not be
run without a later owner approval. It contains no database URL, migration or backup path.

## Preserved contracts

Append-only migrations, schema/role contracts, connector receipts, recovery tooling,
MCP semantic/operator boundaries, Kaggle control classes and Region Talk accounting are
kept. They are rebound to an ACTIVE Kaggle master in later PRs; keeping code does not mean
that runtime already exists.

## Documentation

- [Source of truth](docs/00-source-of-truth.md)
- [Target architecture](docs/02-target-architecture.md)
- [Corrective ADR](docs/adr/0016-kaggle-postgresql-master-architecture-reset.md)
- [Architecture invariants](architecture/invariants.yaml)
- [Incident record](docs/incidents/2026-08-10-local-postgres-architecture-drift.md)
- [Preservation map](docs/architecture/work-preservation-map.md)
- [Ordered roadmap](docs/roadmap-architecture-reset.md)
- [Region Talk migration](docs/migrations/region-talk/README.md)

For historical YDB export tooling, set `MY_DATA_HUB_REGION_TALK_YDB_TABLE`; the pipeline
remains paused and no real export/import is authorized by PR-A.
