# Source material and provenance

## Canonical target vision

```text
Source repository: onedayonemasterpiece/idea-hub
Source path: ideas/portfolio.inbox/idea-20260809-content-platform-current-design.md
Source commit reported by owner: 0c3fcf7
Historical project name: content-platform
Final project name: my-data-hub
```

The bootstrap may be committed with the source gate explicitly marked pending, but the exact file must be imported from the immutable commit before the architecture is marked verified or the deployment/migration gate is closed. Its full original text is authoritative evidence; the documents in this repository normalize it for implementation without downgrading it to an ordinary inbox idea.

## Architecture research

Research record: “Offline-first транзакционные БД для личных конвейеров данных: модели, реализации и ADR для idea-hub”, created 2026-08-09.

Accepted conclusions used here:

- PostgreSQL + FTS + pgvector;
- semantic transactional outbox;
- single canonical committer;
- deterministic conflict policy and quarantine;
- versioned encrypted checkpoints;
- no PostgreSQL logical replication as disconnected merge protocol;
- no SQLite canonical layer for `my-data-hub`.

## Region Talk donor

Repository: `onedayonemasterpiece/region-talk`.

Preserve behavior and evidence from:

- `docs/architecture.md`;
- `docs/orchestrator.md`;
- `docs/state-history-observability.md`;
- `docs/ydb-migration.md`;
- `docs/review-publishing.md`;
- `schemas/region-talk-delta-v1.schema.json`;
- Region Talk workers, fixtures and tests.

Superseded target decisions:

- SQLite canonical state;
- YDB → SQLite intermediate migration;
- GitHub Actions as the only possible live committer.

Retained patterns:

- immutable typed worker outputs;
- one reconciler/committer;
- exact base/input fingerprints;
- short catch-up ticks;
- E5/BGE separation;
- exact review revision and publication idempotency;
- full run/evidence history.

## events-bot MCP donor

Repository: `onedayonemasterpiece/events-bot-new`.

Reuse after exact source review:

- explicit scope/access policy;
- bounded tools;
- secure auth-store/crypto patterns where still appropriate;
- audit and privilege tests;
- no secret disclosure.

Do not copy application-specific access rules or legacy SDK APIs without adapting them to the current MCP Python SDK v2.
