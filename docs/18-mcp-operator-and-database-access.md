# MCP operator profiles and broad PostgreSQL access

Status: `ACCEPTED DESIGN / IMPLEMENTATION PENDING`
Date: 2026-08-09
Related decision: ADR-0012

## 1. Product requirement

The owner must be able to connect ChatGPT or another trusted agent to `my-data-hub` and
work with the canonical data broadly:

- inspect schemas, rows, provenance and operational state;
- correct business data;
- prepare and execute controlled bulk transformations;
- operate the Region Talk migration;
- inspect and manage supported Kaggle resources;
- retain enough evidence to understand and reverse mistakes.

This is a real operator capability, not merely a collection of read-only dashboard
tools. At the same time, remote access must not expose a PostgreSQL owner/superuser,
server filesystem or secrets.

## 2. Two MCP surfaces, one endpoint

The public endpoint can host multiple authorization profiles, but they are distinct
surfaces:

```text
semantic_default
  typed product/orchestration tools

data_operator
  broad bounded SQL reads and controlled DML

migration_operator
  typed migration lifecycle tools

kaggle_operator
  resource-class-aware provider tools
```

A principal sees only tools enabled for its profile and scopes. Disabling the operator
profile removes operator tools from discovery rather than leaving them callable with a
runtime error.

The existing semantic tools remain preferred for recurring operations because they
encode product invariants and produce stable receipts. The operator profile handles
legitimate exploration, repair and one-off data work that would otherwise require a
large artificial tool catalogue.

## 3. Database roles are the primary boundary

Create non-login group roles and short-lived/login service roles around them. Proposed
capabilities:

| Role/profile | Database capability |
|---|---|
| `mdh_mcp_reader` | connect, usage on allowlisted application schemas, bounded SELECT |
| `mdh_mcp_editor` | reader plus DML on explicitly granted business tables/sequences |
| `mdh_migration_operator` | migration landing/mapping/reconciliation procedures and approved target writes |
| `mdh_break_glass_admin` | local-only schema/role/extension maintenance |

Remote roles must not have:

- superuser or `BYPASSRLS`;
- `CREATEDB`, `CREATEROLE` or replication;
- ownership of canonical schemas/tables;
- server file or program execution;
- extension installation;
- arbitrary role membership or `SET ROLE` into owner roles;
- access to PostgreSQL password hashes, server settings containing secrets or OS files;
- direct rights to protected credential/token tables.

Grants are generated and tested from an allowlist. New schemas/tables are not
implicitly writable merely because they were created later.

## 4. `data_reader`: broad but bounded reads

### 4.1 Intended capability

The agent may issue arbitrary read queries over allowlisted application schemas and
approved metadata views. This includes joins, aggregates, CTEs and `EXPLAIN` without
execution where supported.

### 4.2 Session controls

Each request executes in a fresh read-only transaction with local controls, for example:

```text
transaction read only
statement timeout
transaction timeout
lock timeout
idle-in-transaction timeout
row limit
serialized byte limit
```

Initial defaults should be conservative and configurable, for example:

- 10 second statement timeout;
- 15 second transaction timeout;
- 2 second lock timeout;
- at most 1,000 returned rows;
- at most 2 MiB serialized result;
- one statement per call;
- no server-side cursor left open after the call.

A result reports truncation explicitly. The agent may request a higher bounded limit
only with an elevated read scope and still within server policy.

### 4.3 Read restrictions

Reject or hide:

- multiple statements;
- DML/DDL/COPY/CALL/DO/SET and transaction-control statements;
- system catalog fields exposing credentials or sensitive configuration;
- functions classified as unsafe/volatile with external side effects;
- unbounded large-object or binary export;
- queries outside allowlisted schemas;
- attempts to bypass row policy through unsafe functions.

The exact SQL AST and PostgreSQL role both enforce the request. A text denylist alone is
not sufficient.

## 5. `data_editor`: controlled broad DML

### 5.1 Supported statement class

The initial remote editor accepts parameterized:

- `INSERT`;
- `UPDATE`;
- `DELETE`;
- optionally `MERGE` only after dedicated tests.

It rejects DDL, role/ownership changes, extension operations, `TRUNCATE`, `COPY`,
procedural blocks, provider/network functions and multi-statement scripts.

### 5.2 Preview then apply

Every write is two-phase at the MCP application level:

#### Preview

The server:

1. authenticates principal and scope;
2. parses and normalizes the statement;
3. validates target schemas/tables/columns;
4. verifies a recent backup/restore evidence gate;
5. opens a transaction under `mdh_mcp_editor`;
6. computes a bounded target preview and execution plan;
7. executes with rollback where safe or uses an equivalent effect estimator;
8. returns a short-lived preview receipt.

The receipt binds:

```text
principal
session/correlation ID
normalized SQL hash
parameter hash
allowed targets
expected canonical revision
expected row minimum/maximum
backup evidence revision
expiry
```

#### Apply

The agent submits the preview receipt plus an idempotency key. The server rechecks all
bindings and executes one transaction. It records:

- exact target identity and before-revision;
- affected row count;
- returned stable row identities where bounded;
- statement/parameter fingerprints, not leaked secrets;
- canonical revision after commit;
- audit event and optional semantic outbox operations;
- commit receipt or explicit rollback/failure.

A changed database revision is not automatically fatal for every statement, but stated
preconditions and expected effects must still hold.

### 5.3 Impact tiers

Suggested default policy:

| Tier | Example | Gate |
|---|---|---|
| small | ≤100 affected rows | normal editor scope + recent backup |
| bulk | 101–5,000 rows | elevated scope + explicit reason + pre-change checkpoint |
| high impact | >5,000 rows, critical tables or cross-project identity repair | typed job/migration plan + owner approval; not generic apply |
| structural | DDL/roles/extensions | local break-glass only |

Thresholds are configuration, but no deployment may silently set them to unlimited.
Critical tables may always require a typed semantic/migration operation regardless of
row count.

### 5.4 Protected domains

Generic data-editor DML does not write:

- secret/credential material;
- migration checksums/history;
- provider operation receipts;
- append-only audit/event history;
- backup manifests;
- orchestrator-protected Kaggle ownership/control class;
- publication side-effect receipts;
- canonical identity merges that require ID remapping;
- Region Talk cutover state.

These use dedicated procedures/tools with domain preconditions.

## 6. Region Talk migration through an agent

The migration should be operable through `migration_operator` tools so an agent can do
most of the work while hard gates remain authoritative.

Proposed tool groups:

### Inventory/export

- register source revision and read-only credential reference;
- inspect YDB inventory plan;
- start bounded read-only export;
- validate manifest/counts/hashes;
- register immutable export batch.

### Landing/mapping

- dry-run and apply raw landing;
- list row kinds and undispositioned counts;
- register/enable a versioned transformer;
- run mapping for a bounded partition;
- inspect duplicate groups and quarantine.

### Resolution/reconciliation

- resolve one quarantine item with expected revision and reason;
- rerun affected mapping/reconciliation;
- get row-kind, identity, queue and semantic diffs;
- prove `fully_accounted` and `cutover_ready` independently.

### Shadow/cutover

- create shadow plan;
- start/observe exact-revision shadow run;
- compare legacy/new decisions;
- request final delta/freeze checkpoint;
- preview cutover;
- apply cutover only after all gates and a local owner approval token;
- start rollback within the retained window.

Raw editor SQL cannot set `cutover_ready`, falsify accounting, erase quarantine, rewrite
migration history or enable production publication.

## 7. Backup and recovery gates

A backup copy is not permission to perform arbitrary changes. It is one layer of
recovery.

### 7.1 Required cadence

Initial policy should provide:

- frequent local logical backups or equivalent snapshots appropriate to measured
  write volume;
- at least daily encrypted off-host copy;
- pre-change checkpoint for bulk/high-impact operator work;
- multiple retained generations;
- scheduled isolated restore drills.

The previous provisional one-day RPO is insufficient as the only protection once broad
remote writes are enabled. The actual target is set after measuring data volume and
backup/restore duration, then enforced by the operator gate.

### 7.2 Backup freshness gate

Before editor apply, the service checks:

- last backup completion and hash/readback verification;
- last restore-drill status and age;
- schema revision compatibility;
- whether a newer high-impact operation requires a fresh pre-change checkpoint;
- off-host generation availability.

A stale/failed gate blocks the write or restricts the surface to read-only.

### 7.3 Recovery journal

For critical tables, add an immutable operator-change journal containing stable row
identity, before/after hashes and bounded snapshots where legally and technically
appropriate. This does not replace PostgreSQL backup/WAL, but makes targeted repair and
audit practical.

## 8. Authentication and scopes

Illustrative scopes:

```text
hub:read
hub:write
operator:db:read
operator:db:write
operator:db:bulk
migration:read
migration:operate
kaggle:read
kaggle:write
kaggle:exchange
```

Scopes alone are not enough. Authorization also checks:

- principal allowlist and environment;
- OAuth resource/audience;
- operator profile enabled on that process;
- database role and target allowlist;
- resource ownership/control class;
- current backup gate;
- preview/lease/revision state;
- per-tool rate/concurrency budgets.

Remote credentials should be revocable without rotating the database owner password.
Provider and database credentials remain server-side.

## 9. Audit and observability

Every operator request records:

- principal/client/session/correlation identity;
- tool and scope decision;
- normalized query/statement fingerprint;
- targets, limits, timeout and revision;
- preview and apply receipts;
- affected/returned row counts and truncation;
- backup gate used;
- PostgreSQL error class without secret leakage;
- provider/migration operation links;
- final outcome and unknown-outcome state.

Alerts include:

- repeated denied target/scope attempts;
- unusually large or slow reads;
- preview/apply mismatch;
- lock/statement timeouts;
- bulk write or critical-table access;
- stale backup gate;
- attempts to mutate protected Kaggle or migration state;
- operator activity outside expected windows.

## 10. Release order

1. implement roles/grant tests;
2. expose catalog and read-only query in an isolated test database;
3. connect remote MCP with read-only scopes;
4. run adversarial SQL and data-exfiltration negative tests;
5. implement preview/apply against a disposable schema;
6. prove backup freshness and restore gates;
7. enable selected non-critical application schemas;
8. add migration-operator tools;
9. only then allow an agent to drive Region Talk migration;
10. keep break-glass administration local.

## 11. Mandatory tests

- reader role cannot write even if SQL parser is bypassed;
- editor role cannot DDL, change roles, read secrets or access server files;
- multi-statement and unsafe function calls are rejected;
- timeout, row and byte caps cannot be raised by query text;
- stale/forged/other-principal preview receipt is rejected;
- revision/row-bound mismatch rolls back atomically;
- repeated idempotency key returns the existing receipt;
- high-impact operation is blocked without checkpoint/elevated scope;
- protected tables reject generic DML at the database layer;
- failed apply leaves no partial canonical change;
- audit event cannot be modified by the same editor role;
- Region Talk cutover remains impossible while accounting/quarantine/shadow/backup gates
  fail;
- revoking OAuth access prevents new sessions without changing canonical DB ownership.
