# ADR-0010: Data connectors use versioned idempotent intake contracts

- Status: Accepted
- Date: 2026-08-09

## Context

`my-data-hub` must receive periodic statistics, discoveries and operational facts from
`events-bot-new` and future systems. Direct writes into shared canonical tables would
couple every producer to the internal schema and make retries, late corrections,
provenance and incident diagnosis unreliable.

## Decision

The default integration boundary is a versioned HTTPS intake API with a durable
connector registry, idempotent batch envelope, validation, staging, receipt and
quarantine lifecycle.

Supported connector modes are:

1. **push** — a producer submits a bounded batch over HTTPS;
2. **pull** — an orchestrator adapter reads an external API on a durable schedule;
3. **artifact handoff** — a manifest references a large immutable private artifact;
4. **trusted database landing** — an exceptional private-network path into a dedicated
   integration landing schema or stored procedure, never direct writes to canonical
   domain tables.

Every batch identifies connector, data product, schema version, period/watermark,
idempotency key, record count and SHA-256. A correction supersedes an earlier batch;
it does not mutate the historical intake receipt.

The intake transaction records the accepted batch before downstream normalization.
Canonical application is a separate, auditable step through the designated committer inside the ACTIVE Kaggle master.

## Consequences

- Producers can retry safely after timeout or service outage.
- Connector schema changes are explicit and independently testable.
- Producers do not receive general PostgreSQL access; after ensure/resolve, a bounded connector may use its epoch-bound master landing role.
- Daily bot statistics can be added before Region Talk migration without importing its
  legacy storage model.
- The connector API and MCP may share a TLS gateway, but use different routes,
  principals, scopes and rate limits.
