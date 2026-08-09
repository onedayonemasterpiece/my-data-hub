# Project status

Date: 2026-08-09
Status: `DEVSTAND_DEPLOYED_USER_REPORTED / VERIFICATION_PENDING`

## Reported runtime state

The owner reports that the project has been deployed on the devstand. This repository
snapshot does not independently prove the running commit, services, database revision,
backup/restore state or public endpoint. Those facts must be captured by the
infrastructure-first deployment receipt.

Until verified:

- scheduler and production publication remain disabled;
- Region Talk pipeline remains paused;
- remote MCP mutation profiles remain disabled;
- no full YDB migration/cutover begins.

## Bootstrap implemented before this supplement

- final name `my-data-hub`; historical `content-platform` alias;
- canonical target-vision provenance gate;
- Region Talk as first migration workload;
- PostgreSQL 18 + pgvector canonical storage;
- core, analysis, orchestration, sync, Region Talk, migration and Joplin schemas;
- append-only migrations `0001`–`0009` and live verification scripts;
- bounded semantic MCP v0.1;
- plan-only orchestrator, durable queue, leases and retries;
- typed notebook contracts and immutable result inbox;
- lossless YDB export/landing/mapping/reconciliation/cutover scaffold;
- Docker/systemd/backup/CI handoff;
- production publication and remote MCP disabled by default.

## Accepted documentation supplement

The following architecture is now accepted but not yet claimed as implemented runtime:

- supervised canonical PostgreSQL; Kaggle is not master DB/failover;
- data connector registry/intake/receipt/quarantine and durable producer spool;
- `events-bot.daily-statistics.v1` as first real connector candidate;
- remote endpoint `https://mcp-datahub.kenigevents.ru/mcp` through Yandex TLS/OAuth;
- Kaggle resource control classes:
  `orchestrator_protected`, `mcp_managed`, `mcp_exchange`, `external_read_only`;
- provider inventory and protected-resource mutation denial;
- broad bounded MCP database reader;
- preview/apply MCP data editor under restricted PostgreSQL role;
- typed migration-operator tools for agent-driven Region Talk migration;
- PR, post-deploy, nightly, weekly Kaggle and restore workflows;
- infrastructure-first release ordering.

Canonical documents:

- [`docs/15-infrastructure-first-plan.md`](docs/15-infrastructure-first-plan.md)
- [`docs/16-data-connectors.md`](docs/16-data-connectors.md)
- [`docs/17-kaggle-control-plane.md`](docs/17-kaggle-control-plane.md)
- [`docs/18-mcp-operator-and-database-access.md`](docs/18-mcp-operator-and-database-access.md)
- [`docs/19-test-first-rollout.md`](docs/19-test-first-rollout.md)
- [`docs/20-remote-mcp-endpoint.md`](docs/20-remote-mcp-endpoint.md)

## Current repository proof

The documentation/contracts snapshot was revalidated after this supplement:

```text
pytest -ra                                    90 passed, 1 skipped
python scripts/validate_repository.py         1280 checks / 0 errors
python -m compileall -q src tests scripts     PASS
python scripts/create_notebooks.py --check    PASS / no drift
git diff --check                              PASS
relative Markdown link check                  PASS
```

The skipped test requires the MCP Python SDK; PostgreSQL AST validation also requires
`pglast`. CI/devstand must install the complete `.[dev]` dependencies and rerun all gates.
These repository checks do not prove the user-reported devstand runtime.

## Next release gate: R1 infrastructure and workflow baseline

Required:

1. observed deployment receipt with commit/images/versions/ports/services;
2. clean and upgrade-path migration/idempotency evidence;
3. split PostgreSQL roles and negative grant tests;
4. process-kill and host-reboot recovery;
5. encrypted local/off-host backup, readback and isolated restore;
6. PR/post-deploy/nightly/restore workflows;
7. read-only OAuth MCP at the accepted hostname;
8. synthetic connector accept/commit/read/replay/outage proof;
9. Kaggle inventory and protected-resource authorization proof;
10. disposable MCP-managed notebook/dataset lifecycle;
11. database reader/editor canary in disposable schema;
12. explicit confirmation that scheduler/publication remain off and Region Talk paused.

Only after this gate should Region Talk YDB inventory/export and migration proceed.
