# Release plan

This plan supersedes the earlier migration-first ordering. Region Talk remains the first
migration workload, but infrastructure, workflow and access-control evidence come first.

## R0 — repository bootstrap

- target vision, ADRs, schemas and code skeleton;
- PostgreSQL, orchestrator, semantic MCP and Region Talk migration scaffold;
- no external credentials or production workloads.

## R1 — devstand infrastructure baseline

- record deployed commit, images, services, ports and schema revision;
- split PostgreSQL roles and prove negative grants;
- repeat clean and upgrade-path migrations;
- supervised PostgreSQL/API/orchestrator restart and host reboot test;
- local plus encrypted off-host backup, readback and isolated restore proof;
- scheduler/publication/write profiles false; Region Talk paused;
- pull-request and post-deploy workflows green.

Gate: [`15-infrastructure-first-plan.md`](15-infrastructure-first-plan.md).

## R2 — shared scope foundation, remote MCP and connector flow

- add stable logical pipeline identity plus `platform`, `project`, `pipeline` and
  `project_pipeline` scopes;
- backfill catalog objects and `hub.project_content` into generic scope relations without
  creating a second authority;
- implement namespaced scoped state, append-only usage and versioned policy decisions;
- prove independent states in two scopes and platform hard-deny precedence over local allow;
- publish `https://mcp-datahub.kenigevents.ru/mcp` through TLS/OAuth;
- port/test resource/audience, Host/Origin and admission controls;
- expose semantic read-only tools, including independent relation/usage/state/policy views;
- implement connector registry/intake/receipt/quarantine plus many-to-many consumer routing;
- prove one synthetic batch is accepted once, applied independently to at least two
  consumers, read through MCP and exactly replayed;
- register non-sensitive `events-bot.daily-statistics.v1` connector;
- nightly auth/scope/connector/backup runtime checks.

## R3 — Kaggle control plane and exchange

- inventory all visible notebooks/private datasets;
- registry control classes and protected-resource filtering;
- disposable MCP-managed private dataset lifecycle;
- disposable MCP-managed notebook run/status/output lifecycle;
- protected notebook/dataset mutation denial;
- private TTL/hash exchange package flow;
- backup datasets classified `orchestrator_protected`;
- weekly provider canary.

## R4 — MCP database operator

- restricted reader/editor/migration roles;
- broad bounded read query with AST/timeout/row/byte controls;
- preview/apply DML with idempotency, expected effects and audit;
- backup freshness and pre-change checkpoint gates;
- disposable-schema positive and adversarial negative tests;
- allowlisted application targets only after canary;
- no remote DDL, roles, ownership, superuser or publication.

## R5 — Region Talk inventory and export rehearsal

- import exact source implementation/docs manifest;
- register stable Region Talk project, logical pipeline and exact project-pipeline scopes;
- enumerate YDB tables and row kinds;
- read-only deterministic bounded export;
- immutable raw bundle, row/table hashes, Region Talk scope attestation and repeat hash proof;
- version the export/report contract instead of silently changing an accepted v1 schema;
- agent-operable typed inventory/export tools;
- no writes to target domain tables yet.

## R6 — PostgreSQL migration rehearsal

- raw landing with mandatory Region Talk batch scope;
- explicit mapping/dedup/quarantine;
- create or preserve the required Region Talk relation for every normalized/deduplicated
  shared target in the same canonical transaction;
- queue sequence repair;
- 100% row accounting plus scope-completeness and identity reconciliation;
- product semantic diffs and scoped-state isolation checks;
- agent-operated bounded partitions and resolutions;
- no cutover.

## R7 — Worker/orchestrator shadow

- port Candidate/E5, BGE, image, verifier and writer stages;
- separate notebooks and exact result contracts;
- at least three representative shadow runs;
- compare queue, candidates, eligibility, exact scoped states and review readiness;
- evaluate publication policy from exact decision revisions and archive the receipt;
- fix unexplained drift or cross-scope overwrite;
- protected Kaggle resources remain orchestrator-controlled.

## R8 — private canary and controlled cutover

- Region Talk health/candidate/review operator tools;
- exact revision/idempotency private review canary;
- freeze legacy writers;
- final delta export/import/reconciliation, including Region Talk scope completeness;
- prove no applicable platform hard deny can be bypassed by a narrower project/pipeline state;
- fresh backup and rollback rehearsal;
- switch scheduler to PostgreSQL orchestrator;
- monitor rollback window;
- remove YDB write credentials;
- retain read-only source until explicit owner approval;
- production publication remains independently gated.

## R9 — Joplin read-only projection

- Windows adapter/plugin;
- one test notebook;
- exact mapping and conflict evidence;
- Android delivery through normal Joplin sync.

## Release blockers

- unverified devstand runtime or exposed internal port;
- failed/untested restore;
- stale backup gate for broad writes;
- database role can exceed documented profile;
- remote MCP without exact OAuth resource/audience;
- connector replay can duplicate or lose data;
- one connector batch collapses several consumer applications into a shared mutable status;
- required object/batch/execution scope is absent or ambiguous;
- one pipeline overwrites another pipeline's namespaced object state;
- local/project allow can bypass an applicable platform hard deny;
- a Region Talk normalized/deduplicated target lacks its explicit Region Talk relation;
- public or unclassified Kaggle resource created by platform;
- mutation of `orchestrator_protected` resource through MCP;
- unknown provider outcome retried without reconciliation;
- unexplained source row loss or nonzero migration quarantine at cutover;
- unknown result/schema accepted;
- stale approval authorizes action;
- secret in artifact/log;
- automatic publication enabled before owner gate.
