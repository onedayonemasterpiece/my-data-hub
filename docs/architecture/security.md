# Security architecture

## Assets

Canonical data, personal notes, source profiles, review decisions, provider credentials,
Telegram sessions, database backups and unpublished artifacts are private assets even
though the code repository is public.

## Main threats and controls

| Threat | Control |
|---|---|
| Public database exposure | private/loopback binding, firewall, TLS gateway only |
| Agent overreach | separate profiles/scopes, restricted DB roles, provider control classes |
| Unsafe broad DML | AST + grants + preview/apply + row limits + backup gate + audit |
| Stolen/replayed worker result | run-bound manifest, SHA-256, idempotency, expected input revision |
| Duplicate external effect | transactional outbox, provider receipt and dedupe key |
| Lost connector batch | producer durable spool, idempotent intake and receipt |
| Poisoned connector/migration row | immutable landing, schema validation and quarantine |
| Protected Kaggle mutation | registry control class plus server-side provider policy |
| Secret in notebook/log/exchange | scan, redaction, secret-free schemas, client encryption |
| DNS rebinding/host spoofing | OAuth resource, Host and Origin validation |
| Token leakage | short-lived/revocable OAuth; server-side provider/DB credentials |
| Destructive conflict resolution | expected revision, elevated scope, audit reason |
| Backup disclosure | client-side encryption, private storage, hash readback, restore tests |
| Backup treated as permission | authorization remains roles/scopes/gates; backup is recovery only |

## Database roles

Deployment creates distinct roles:

- schema/migration owner — local migrations only;
- application runtime;
- orchestrator/committer;
- connector intake/landing;
- MCP data reader;
- MCP data editor;
- migration operator;
- backup/restore;
- monitoring.

Remote roles have no superuser, ownership, `BYPASSRLS`, `CREATEDB`, `CREATEROLE`,
replication, extension installation, server-file or program-execution rights. Grants are
explicit and negative-tested. New objects do not become remotely writable by default.

## Remote MCP

- endpoint: `https://mcp-datahub.kenigevents.ru/mcp`;
- OAuth 2.1 resource/audience binding;
- read-only tools first;
- mutation profiles omitted until gates pass;
- application and database-layer target authorization;
- per-request timeout/row/byte/concurrency budgets;
- immutable correlation/audit receipts;
- development token never public.

## Connector security

Connector principals are service identities separate from human/agent MCP identity.
Each principal is bound to connector IDs/data products. Intake verifies schema, hash,
size and idempotency before acceptance. Artifact locations are allowlisted and scanned.

## Kaggle security

- all platform-created datasets are private;
- orchestrator production credentials and MCP sandbox credentials are separated where
  possible;
- protected resource authorization comes from PostgreSQL registry, not names;
- backup datasets are status-only through remote MCP;
- exchange packages have recipients, hashes, TTL and no secrets;
- ambiguous provider mutations reconcile before retry.

## Side-effect safety

An approved publication revision and outbound delivery are separate records. The
publication dispatcher reads a committed outbox row, verifies the approval fingerprint,
performs the provider call and stores the exact receipt. Retry with the same dedupe key
must not create a second publication.
