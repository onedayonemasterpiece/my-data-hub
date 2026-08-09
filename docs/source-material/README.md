# Source material and provenance

## Canonical target vision

```text
Source repository: onedayonemasterpiece/idea-hub
Source path: ideas/portfolio.inbox/idea-20260809-content-platform-current-design.md
Source commit: 0c3fcf71b2ee8ba8afa49624bef4b779873802f7
SHA-256: c7efb28231223caa6fd02fcc001a38e0f16bcc3fa4c4cd53e744721b2eac0852
Import status: verified_import
Canonical project name: my-data-hub
Historical alias (not a separate project): content-platform
```

The exact file has been imported from the immutable commit through
`scripts/import_source_material.py`. Its full original bytes are authoritative evidence;
the documents in this repository normalize it for implementation without downgrading it
to an ordinary inbox idea. The source import verifies provenance only; it does not by
itself close deployment, migration or publication gates.

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
