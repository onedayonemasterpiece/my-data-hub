# Security model

Security spans two planes.

## Devstand control plane

- no production PostgreSQL, PGDATA, canonical business data or master credentials at rest;
- operation/event ledgers store identities, epochs, leases and locators, never runtime
  credentials;
- provider actions are idempotent, fenced and auditable;
- stable MCP uses OAuth, Host/Origin checks, scopes, limits and revocation only after its
  later release gate.

## Kaggle master data plane

- write gate opens only after restore verification, migrations, latest epoch and live lease;
- short-lived credentials are role- and epoch-bound;
- connector landing, canonical committer, MCP reader/editor, backup/checkpoint and migrator
  roles stay separate;
- owner/superuser/DDL/BYPASSRLS/server-file/program execution are never remote-agent roles;
- checkpoints are private, hashed, read back and restore-smoked before HEAD promotion.

PR-A contains no production credentials, real provider calls or enabled write surface.
Publication, Region Talk and remote MCP writes remain disabled.

## Preserved detailed contract — bound by ADR-0016

The detailed material below is retained where topology-neutral. Any reference to a database, role, committer, backup or connector application is executed inside/against the latest ACTIVE Kaggle master; devstand execution claims are superseded.

## Trust zones

1. Lightweight devstand control services and operational metadata, with no master secret
   or canonical business rows.
2. The fenced Kaggle master PostgreSQL data plane and its restricted runtime identities.
3. Private Kaggle checkpoint Datasets and their separate promotion/readback authority.
4. Public TLS/OAuth edge at `mcp-datahub.kenigevents.ru`.
5. MCP clients with profile/scoped identity.
6. Data connectors with separate, short-lived epoch-bound identity.
7. External workers/notebooks with untrusted outputs.
8. Kaggle provider account/resources with registry control classes.
9. Provider APIs and external content — untrusted.
10. Migration source credentials — temporary and read-only.

## Secrets

- environment/OS secret store/protected CI environment only;
- not in DB payload rows, artifacts, notebooks, logs or Git history;
- YDB credentials removed after accepted migration window;
- Joplin token remains on desktop adapter host;
- remote MCP signing/OAuth keys rotate independently from DB/provider passwords;
- the fixed `datahub-owner` principal is unlocked only by the owner-controlled browser
  token ceremony already proven by eventsBot MCP; the raw high-entropy token stays in one
  mode-`0600` devstand file and never enters URLs, logs, MCP, Kaggle or Git;
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
  provider ownership, append-only scope/state/policy history or publication state;
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
- connector/producer cannot self-assign authoritative project/platform scope; routing and
  writer authority are server-attested;
- accepted source evidence survives normalizer or one consumer application failure.

## Data scope and policy

- shared actor/account/content/asset identity is not copied merely for another project;
- platform/project/pipeline/project-pipeline scopes are constrained registry objects, not
  client-provided tags;
- relation, namespaced state, usage and policy decisions have separate grants and writers;
- one pipeline cannot overwrite another scope's current state;
- applicable platform hard deny/blacklist overrides narrower allow;
- duplicate identity remap preserves every scope relation and applicable policy decision;
- an external side effect requires a fresh policy-evaluation receipt whose object,
  relationship and decision fingerprint still matches at dispatch;
- closing one project relation cannot delete another project's relation or the shared root.

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

- every resource has an explicit devstand control-registry class;
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
- policy-evaluation freshness/input-fingerprint check immediately before provider call;
- provider history check after ambiguous timeout;
- media manifest/hash revalidated immediately before send;
- production flag and protected environment gate;
- no generic database or Kaggle tool can enable/execute publication.
