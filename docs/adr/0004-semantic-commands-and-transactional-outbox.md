# ADR-0004: Semantic commands and transactional outbox

- Status: Accepted
- Date: 2026-08-09

## Decision

Every remote/intermittent mutation carries domain intent, idempotency key,
expected revision and semantic preconditions. The canonical mutation, command
receipt and any required outbox event are committed atomically in PostgreSQL.
Arbitrary write SQL and WAL row diffs are not integration protocols.

## Consequences

Idempotent and proven commutative operations may merge. Conditional operations
use optimistic concurrency. Invariant-sensitive conflicts are quarantined.
External side effects execute only from committed outbox records and use their
own idempotency keys.
