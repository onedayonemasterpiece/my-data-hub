# PR-A work preservation map

| Classification | Existing work | PR-A treatment |
|---|---|---|
| KEEP AS-IS | append-only SQL migrations and schemas; Joplin boundary; contract schemas; security parsers | No migration/history rewrite |
| KEEP BUT REBIND TO KAGGLE MASTER | DB roles, recovery tools, connector landing/idempotency/receipts, semantic outbox, bounded MCP/operator services, Region Talk accounting | Executed only inside/against an ACTIVE epoch-bound master |
| KEEP BUT REBIND TO CONTROL PLANE | Kaggle control classes, operation identities, leases, audit, OAuth policy | Host stores operational metadata only, never canonical content |
| SUPERSEDE | ADR-0009; local-runtime clauses of ADR-0002/0006/0010/0011/0012/0014; same-host deployment claims | ADR-0016 and architecture invariants govern |
| DELETE/TEST-ONLY | persistent same-host Compose, DB-coupled systemd units/workflows, local backup/committer loops | Deleted; root Compose is disposable tmpfs integration only |
| DEFER | real Kaggle adapter/master, DNS/VPN/443, remote MCP writes, checkpoint dataset promotion, model services | Later ordered PRs; absent from PR-A runtime claims |
| DEFER | Region Talk inventory/migration/cutover/publication | Pipeline stays paused; publication stays disabled |
