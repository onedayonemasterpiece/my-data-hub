# Security model

## Trust zones

1. PostgreSQL/internal services on private host/network.
2. MCP clients with scoped identity.
3. External workers/notebooks with untrusted outputs.
4. Provider APIs and external content — untrusted.
5. Migration source credentials — temporary, read-only.

## Secrets

- только environment/OS secret store/protected CI environment;
- не в DB rows, artifacts, notebooks, logs, Git history;
- YDB credentials удаляются после accepted migration window;
- Joplin token остаётся на desktop adapter host;
- remote MCP signing/OAuth keys ротируются независимо от DB password.

## Database access

- PostgreSQL не публикуется в интернет;
- отдельные roles: migrator, app, read-only diagnostics, backup;
- app не имеет `CREATEDB`, superuser или extension install rights;
- RLS может быть добавлен при появлении нескольких mutually untrusted tenants;
  в single-owner phase enforced service scopes остаются primary boundary.

## Worker input/output

- allowlisted artifact locators;
- checksum/signature before open;
- archive traversal/symlink/content-type/size limits;
- JSON Schema with unknown fields rejected;
- secret scanner before archival;
- no trust in prose returned by providers/models.

## MCP

- read-only default;
- scope + tool + dynamic target authorization;
- strict schemas and bounded responses;
- timeouts and concurrency/rate/egress budgets;
- no raw credentials/provider methods;
- destructive unknown-outcome marked retry-unsafe;
- correlation IDs and audit for every call;
- remote transport disabled until OAuth boundary is complete.

## Publishing

- exact candidate revision fingerprint;
- reviewer allowlist;
- conflicting/rewrite/stale reactions block;
- publication outbox idempotency key;
- provider history check after ambiguous timeout;
- media manifest/hash revalidated immediately before send;
- production flag and protected environment gate.
