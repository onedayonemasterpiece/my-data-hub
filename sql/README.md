# PostgreSQL schema

`sql/migrations/*.sql` is the only canonical schema history. Migration numbers are unique,
checksums are recorded in `public.my_data_hub_schema_migration`, and an already-applied file
must never be edited.

The bootstrap targets PostgreSQL 18 with `pgvector`. A clean database must accept migrations
`0001` through `0009` in one pass before any Region Talk import is attempted.

The raw YDB landing layer is intentionally lossless: normalization is a separate, repeatable
step and every source row must end as `normalized`, `deduplicated`, `intentionally_excluded`, `retained_raw`, or `quarantined` with a
reason and reconciliation receipt.
