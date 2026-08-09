# ADR-0012: MCP has segregated semantic and database-operator profiles

- Status: Accepted; amends ADR-0005
- Date: 2026-08-09

## Context

The owner needs an agent to inspect and correct a broad part of the canonical data,
including managing the Region Talk migration. The original bootstrap prohibited all
arbitrary SQL through MCP. Keeping that rule without qualification would force a large
catalogue of one-off tools and prevent legitimate operator work. Exposing a database
owner or superuser through a public MCP endpoint would be an unacceptable opposite
extreme.

## Decision

The default MCP remains the bounded semantic surface from ADR-0005. A separate,
explicitly enabled database-operator profile is added with distinct OAuth scopes,
PostgreSQL roles, process configuration and audit.

Profiles:

1. `semantic_default` — current typed domain tools; no arbitrary SQL;
2. `data_reader` — broad bounded `SELECT`/catalog access in allowlisted application
   schemas through a read-only PostgreSQL role;
3. `data_editor` — single-transaction `INSERT`/`UPDATE`/`DELETE` in allowlisted business
   schemas, using preview then apply, expected effects and strict limits;
4. `migration_operator` — typed landing, mapping, quarantine, reconciliation, shadow,
   cutover-readiness and rollback tools for approved workloads;
5. `break_glass_admin` — DDL, roles, extensions and ownership changes only from the
   devstand/local administrative channel with short-lived credentials; it is not a
   normal remote ChatGPT profile.

The database role is the primary enforcement boundary. SQL parsing, allowlists,
timeouts, row/byte caps and preview receipts are defense in depth. No remote profile
receives superuser, `BYPASSRLS`, role administration, secret access, server file access,
`COPY ... PROGRAM`, untrusted procedural execution or ownership of canonical schemas.

A data-editor apply requires a recent successful backup/restore evidence state and a
short-lived preview receipt tied to principal, SQL fingerprint, parameters, expected
row bounds and canonical revision. High-impact operations require an elevated scope and
a pre-change checkpoint.

## Consequences

- The owner receives practically broad data access without turning the public MCP into
  a PostgreSQL superuser proxy.
- Backups are recovery controls, not the authorization model.
- Region Talk migration can be driven by an agent, but typed reconciliation and cutover
  gates cannot be bypassed with raw DML.
- Existing implementations remain read-only/semantic until the new roles, parser,
  audit, backup gates and negative tests are complete.
