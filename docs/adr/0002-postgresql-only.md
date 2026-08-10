# ADR-0002: PostgreSQL is the only canonical server-side database engine

- Status: Accepted; runtime clauses amended by ADR-0016
- Date: 2026-08-09

PostgreSQL remains the sole canonical server-side database engine. In production its only
ACTIVE writable primary runs in the Kaggle master Notebook. The devstand holds operational
control metadata only and no canonical business rows or PGDATA.

YDB is a read-only migration/rollback source. SQLite, Supabase, Joplin internals and
artifacts are not alternate canonical databases. Private Kaggle Datasets are verified
checkpoint storage, not a live database by themselves.

Canonical mutations and required outbox events share one master PostgreSQL transaction.
Availability is provided by ensure/resolve, leases/fencing and verified checkpoint recovery,
not a local devstand service.
