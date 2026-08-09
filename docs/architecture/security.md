# Security architecture

## Assets

Canonical data, personal notes, source profiles, review decisions, provider credentials,
Telegram sessions, database backups and unpublished artifacts are private assets even
though the code repository is public.

## Main threats and controls

| Threat | Control |
|---|---|
| Public database exposure | localhost/private binding, firewall, separate runtime user |
| Agent overreach | scoped semantic tools, no arbitrary writes, confirmation for side effects |
| Stolen/replayed worker result | run-bound manifest, SHA-256, idempotency and expected input revision |
| Duplicate external effect | transactional outbox plus provider receipt and dedupe key |
| Poisoned migration row | raw staging, schema validation, mapping quarantine, no silent coercion |
| Secret in notebook/log | pre-archive scan, structured redaction, secret-free schemas |
| DNS rebinding/host spoofing | origin and host validation at MCP/gateway |
| Token leakage | short-lived OAuth tokens remotely; environment/credential store locally |
| Destructive conflict resolution | expected conflict revision, elevated scope, audit reason |
| Backup disclosure | client-side encryption, private storage, tested restore |

## Database roles

Deployment creates distinct roles:

- `hub_owner` — migrations only, no network login in normal operation;
- `hub_app` — application reads/writes through repositories;
- `hub_mcp_read` — bounded read views/functions;
- `hub_migration` — temporary staging/import grants;
- `hub_backup` — backup privileges only.

The bootstrap SQL creates schemas, not production passwords or broad external grants.

## Side-effect safety

An approved publication revision and its outbound delivery are separate records. The
publication dispatcher reads an outbox row, verifies the approval fingerprint, performs the
provider call, then stores the provider receipt. A retry with the same dedupe key must not
create a second publication.
