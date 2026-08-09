# Release plan

## R0 — repository bootstrap

- docs/ADR/schemas/code skeleton;
- clean CI;
- push to `main`;
- no external credentials or workloads.

## R1 — single-node PostgreSQL

- apply all migrations; migration `0009` installs idempotent bootstrap rows, views and claim functions;
- app/migrator/backup roles;
- API and orchestrator systemd/Docker autostart;
- backup + restore proof;
- MCP stdio read tools.

## R2 — Region Talk inventory and export rehearsal

- import exact source implementation/docs manifest;
- enumerate YDB tables and row kinds;
- read-only deterministic bounded export;
- immutable raw bundle + row/table hashes;
- no writes to target domain tables yet.

## R3 — PostgreSQL migration rehearsal

- raw landing;
- explicit mapping/dedup/quarantine;
- queue sequence repair;
- 100% accounting report;
- product semantic diffs;
- no cutover.

## R4 — Worker/orchestrator shadow

- port Candidate/E5, BGE, image, writer stages;
- separate notebooks and exact result contracts;
- at least three shadow runs;
- compare queue, candidates, eligibility and review readiness;
- fix unexplained drift.

## R5 — MCP/operator canary

- remote OAuth boundary if required;
- Region Talk health/candidate/review tools;
- private review channel canary;
- exact revision and idempotency tests;
- production publisher still disabled.

## R6 — controlled cutover

- freeze legacy writes;
- final delta export/import/reconciliation;
- switch scheduler to PostgreSQL orchestrator;
- monitor rollback window;
- remove YDB write credentials;
- retain read-only source until explicit owner approval.

## R7 — Joplin read-only projection

- adapter on Windows desktop;
- one test notebook;
- exact mapping and conflict evidence;
- Android delivery through Joplin sync.

## Release blockers

- unexplained source row loss;
- duplicate/mutable `queue_seq`;
- unknown result/schema accepted;
- stale approval authorizes action;
- untested restore;
- secret in artifact/log;
- automatic publish enabled before owner gate.
