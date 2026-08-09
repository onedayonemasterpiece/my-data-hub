# Roadmap

## R0 — repository bootstrap

- target vision and ADRs;
- PostgreSQL schemas/migration runner;
- orchestrator and semantic command skeleton;
- MCP and notebook contracts;
- Region Talk migration package;
- Joplin boundary;
- CI/local deployment scaffold.

## R1 — infrastructure and workflow baseline

- verify deployed devstand and split PostgreSQL roles;
- clean/upgrade migration gates;
- service/reboot supervision;
- local/off-host backup, readback and isolated restore;
- PR, post-deploy, nightly and restore workflows;
- dangerous gates remain off.

## R2 — connector and remote read plane

- TLS/OAuth MCP at `mcp-datahub.kenigevents.ru`;
- semantic read-only tools;
- connector registry/intake/receipt/quarantine;
- synthetic round trip and outage/replay proof;
- events-bot daily statistics as first real data product.

## R3 — Kaggle provider plane

- complete inventory and control classes;
- protected vs MCP-managed authorization;
- private notebook/dataset lifecycle canary;
- private exchange packages;
- protected encrypted backup/checkpoint resources.

## R4 — agent data operator

- broad bounded reader;
- preview/apply editor under restricted roles;
- backup/revision/impact/audit gates;
- migration operator tools;
- no remote DDL/superuser/publication.

## R5 — Region Talk migration and shadow

- YDB inventory/export;
- baseline landing/mapping/accounting;
- reconciliation and incremental catch-up;
- donor worker/finalizer adapters;
- shadow cycles/private canary;
- controlled cutover and rollback window.

## R6 — Joplin bridge

- Windows bridge/plugin PoC;
- selected notebook mapping and read-only sync;
- conflict/evidence review;
- optional bounded outbound note creation.

## Later workloads

New projects reuse shared identities, connector/provider planes and operator controls.
A workload schema is added only for genuinely project-specific policy and projections.
