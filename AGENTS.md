# Agent operating contract

## Authority order

1. `docs/00-source-of-truth.md`, `docs/01-project-charter.md` and accepted ADRs.
2. Append-only database migrations and JSON schemas.
3. Runtime evidence and migration receipts.
4. Other documentation.
5. Historical Region Talk implementation.

The historical `content-platform` name is an alias of `my-data-hub`, not a separate project.

## Hard invariants

- Do not introduce SQLite, YDB, Supabase or another database as canonical application state.
- Joplin's internal local database is outside this boundary; never read or mutate it directly.
- Do not expose arbitrary SQL, shell, filesystem or secret-reading tools through MCP.
- Every business write that must leave a producer session must record semantic outbox operations in the same PostgreSQL transaction.
- A notebook/worker emits a typed result or semantic changeset; it does not mutate canonical state directly.
- Only one canonical committer may advance a revision.
- External publication requires a canonical, exact approved revision and an idempotency key.
- Never discard an unknown migration row. Land it and classify it.
- Never fabricate YDB row counts, table names, model versions, hashes, IDs or migration completeness.
- Do not silently merge duplicate people, accounts, materials or publications. Record a duplicate group and explicit decision.
- Migrations are append-only after merge. Never edit an applied migration.
- The public repository must contain no credentials, decrypted exports, personal notes, Telegram sessions or production data.

## Definition of done for code changes

- `python -m compileall src tests` passes.
- `pytest` passes.
- JSON schemas validate their examples.
- SQL migration filenames remain strictly ordered and checksummed by the migration runner.
- New MCP write tools declare and enforce a scope.
- New pipeline stages define retry, timeout, terminal outcome and result contract.
- Migration changes include accounting and rollback implications.
- Documentation states what was actually proven, not merely scaffolded.
