# ADR-0003: Region Talk is the first workload and migration driver

- Status: Accepted
- Date: 2026-08-09

## Decision

Region Talk is the first complete pipeline moved to `my-data-hub`, including
all accumulated YDB rows and all recoverable operational history. It is not merely a
code donor.

The migration follows read-only export, raw landing, versioned normalization,
reconciliation, catch-up, shadow operation, final freeze, cutover and rollback.
Every exported source row must be retained in raw landing and receive one explicit
disposition: normalized, deduplicated, intentionally excluded, retained raw or
quarantined. No disposition deletes the original export evidence.

## Consequences

Generic abstractions are proven against Region Talk rather than designed in
isolation. Unknown row kinds are preserved. Known queue/cache/eligibility bugs
are repaired with explicit receipts, not reproduced as target behaviour. YDB
is not deleted automatically after cutover.
