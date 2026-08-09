# Security model

## Trust zones

1. PostgreSQL/internal services on the private devstand.
2. Public TLS/OAuth edge at `mcp-datahub.kenigevents.ru`.
3. MCP clients with profile/scoped identity.
4. Data connectors with separate service identity.
5. External workers/notebooks with untrusted outputs.
6. Kaggle provider account/resources with registry control classes.
7. Provider APIs and external content — untrusted.
8. Migration source credentials — temporary and read-only.

## Secrets

- environment/OS secret store/protected CI environment only;
- not in DB payload rows, artifacts, notebooks, logs or Git history;
- YDB credentials removed after accepted migration window;
- Joplin token remains on desktop adapter host;
- remote MCP signing/OAuth keys rotate independently from DB/provider passwords;
- Kaggle credentials stay server-side and are split between orchestrator and MCP
  sandbox identities where practical;
- plaintext production dumps never enter GitHub or exchange packages.

## Database access

- PostgreSQL is never internet-facing;
- split roles: owner/migrator, app, orchestrator, connector intake, MCP reader, MCP
  editor, migration operator, backup and monitor;
- remote roles have no superuser, ownership, `BYPASSRLS`, role/database creation,
  replication, extension installation, server file/program access;
- grants are explicit and negative-tested; new objects are not automatically writable;
- operator sessions use local statement/transaction/lock/idle timeouts and row/byte caps;
- generic editor uses preview/apply and cannot mutate protected accounting, audit,
  provider ownership or publication state;
- RLS may be added for mutually untrusted tenants; role grants remain the first
  enforcement boundary.

## Backup is recovery, not authorization

A recent verified backup and restore drill may be a prerequisite for broad writes, but
it does not make an otherwise prohibited operation acceptable. High-impact writes
require a pre-change checkpoint; multiple encrypted generations are retained and
readback-verified.

## Worker and connector input/output

- allowlisted artifact locators;
- checksum/signature before open;
- archive traversal/symlink/content-type/size limits;
- strict JSON Schema with unknown fields rejected;
- secret scanner before archival;
- no trust in prose returned by providers/models;
- connector exact replay is idempotent; conflicting replay is quarantined;
- accepted source evidence survives normalizer failure.

## MCP

- read-only semantic default;
- production OAuth resource/audience binding;
- scope + profile + dynamic target authorization;
- strict schemas and bounded responses;
- timeouts and concurrency/rate/egress budgets;
- no raw credentials/provider methods;
- development token loopback-only;
- operator SQL restricted by PostgreSQL role and AST/allowlist;
- destructive unknown outcome marked retry-unsafe and reconciled;
- correlation IDs and audit for every call;
- tools omitted from discovery when their profile/gate is disabled.

## Kaggle

- every resource has local registry control class;
- all platform-created datasets are private;
- protected notebooks/datasets are status-only through remote MCP;
- public dataset creation is absent from tool schema;
- exchange packages are TTL/recipient/hash-manifested and non-canonical;
- backup datasets cannot be downloaded/versioned/deleted through remote MCP;
- provider mutation uses lease, expected fingerprint and idempotency.

## Publishing

- exact candidate revision fingerprint;
- reviewer allowlist;
- conflicting/rewrite/stale reactions block;
- publication outbox idempotency key;
- provider history check after ambiguous timeout;
- media manifest/hash revalidated immediately before send;
- production flag and protected environment gate;
- no generic database or Kaggle tool can enable/execute publication.
