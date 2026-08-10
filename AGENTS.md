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
- Do not use Kaggle notebooks/datasets as master database, automatic failover or canonical pointer.
- Joplin's internal local database is outside this boundary; never read or mutate it directly.
- The default MCP must not expose generic SQL. A separately enabled operator profile may expose bounded reads and preview/apply DML only under ADR-0012 restricted roles, limits, backup and audit gates; never owner/superuser/DDL.
- Every business write that must leave a producer session must record semantic outbox operations in the same PostgreSQL transaction.
- A notebook/worker emits a typed result or semantic changeset; it does not mutate canonical state directly.
- Data connectors use versioned idempotent intake/landing contracts; they do not write shared canonical tables directly or self-assign authoritative project/platform scope.
- Never copy a shared actor/account/content/asset merely because another project or pipeline uses it. Preserve explicit scope relations and shared identity.
- Entity lifecycle, project/scope relation, scoped workflow state, pipeline usage and policy decision are distinct. `orchestration.work_item.status` is execution state only.
- A platform-wide hard deny/blacklist cannot be weakened by a project/pipeline allow; external effects require a fresh exact policy-evaluation receipt whose input fingerprint still matches at dispatch.
- Orchestrator-protected Kaggle resources are status-only through remote MCP and cannot be reclassified by name.
- Only one canonical committer may advance a revision.
- External publication requires a canonical, exact approved revision and an idempotency key.
- Never discard an unknown migration row. Land it and classify it.
- Every Region Talk raw row must resolve Region Talk batch scope; every normalized/deduplicated shared target must have the required Region Talk relation before cutover.
- Never fabricate YDB row counts, table names, model versions, hashes, IDs or migration completeness.
- Do not silently merge duplicate people, accounts, materials or publications. Record a duplicate group and explicit decision; merge aliases, provenance and the union of all project/pipeline relations.
- Migrations are append-only after merge. Never edit an applied migration.
- The public repository must contain no credentials, decrypted exports, personal notes, Telegram sessions or production data.

## Definition of done for code changes

- `python -m compileall src tests` passes.
- `pytest` passes.
- JSON schemas validate their examples.
- SQL migration filenames remain strictly ordered and checksummed by the migration runner.
- New MCP write tools declare and enforce a scope.
- New pipeline stages define retry, timeout, terminal outcome, exact logical pipeline/project scope and result contract.
- New shareable object types define catalog registration, scope-resolution rules, state namespace owner and policy applicability.
- New connector consumers define target scope, routing predicate, contract version, required/optional behavior and independent application receipt.
- Migration changes include accounting, scope-completeness, dedupe-relation and rollback implications.
- Documentation states what was actually proven, not merely scaffolded.
- Infrastructure-first gates (roles, backup/restore, workflows, remote read-only MCP, scope/policy foundation, multi-consumer connector and provider controls) precede full Region Talk migration.
