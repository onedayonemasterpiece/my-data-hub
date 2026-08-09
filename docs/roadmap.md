# Roadmap

## R0 — repository bootstrap (this commit)

- target vision and ADRs;
- PostgreSQL schemas/migration runner;
- orchestrator and semantic command skeleton;
- MCP and notebook contracts;
- Region Talk migration package;
- Joplin integration boundary;
- CI/validation and local deployment scaffold.

## R1 — devstand runtime

- install and harden PostgreSQL;
- apply migrations and verify roles/backups;
- deploy orchestrator/MCP with auto-start;
- pin tested dependencies/images;
- establish private artifact storage.

## R2 — Region Talk data migration

- complete YDB inventory/export adapter;
- baseline import and mapping completion;
- reconciliation and incremental catch-up;
- adapt donor workers and finalizer;
- shadow cycles and private canary;
- cutover and rollback window.

## R3 — agent operation

- production OAuth/scopes for remote MCP;
- richer bounded search/provenance tools;
- conflict review surfaces and operational reports;
- deliberate publication tool after canary evidence.

## R4 — Joplin bridge

- Windows bridge/plugin PoC;
- selected notebook mapping and read-only sync;
- conflict/evidence review;
- optional bounded outbound note creation.

## Later workloads

New projects should reuse shared actor/content/analysis identities and add a workload schema
only for genuinely project-specific policies and projections.
