# ADR-0002: PostgreSQL is the only canonical server-side database

- Status: Accepted
- Date: 2026-08-09

## Decision

A live PostgreSQL instance on the devstand is the sole canonical relational
runtime. Core content, orchestration state, semantic commands, migration
accounting, audit and transactional outbox live there. PostgreSQL FTS,
`pgvector`, `pgcrypto` and `citext` are approved extensions.

YDB is a temporary read-only migration and rollback source. SQLite, Supabase,
Kaggle Dataset, GitHub artifacts and Joplin are not alternate canonical state
stores. Files/object stores may hold immutable artifacts and encrypted backups.

## Rationale

A single transactional boundary removes distributed queue/state drift while
retaining ACID, Russian FTS and vector search. It also gives MCP and Region Talk
a dependable online boundary on the already planned supervised host.

## Consequences

Workers return immutable results and never write canonical tables directly.
Availability depends explicitly on a supervised PostgreSQL service, tested
backup/readback and auto-start. The database may later move to another host
without changing domain ownership.
