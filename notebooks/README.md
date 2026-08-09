# Notebook workers

Every directory is one isolated worker/lane. E5, BGE-M3, source-profile, writer and image
diagnostics are deliberately separate so model memory, dependency and failure domains do
not collapse into one kernel.

## Runtime contract

A worker receives `MY_DATA_HUB_NOTEBOOK_INPUT_MANIFEST` and writes
`MY_DATA_HUB_NOTEBOOK_RESULT_PATH`. It may read immutable artifacts and perform computation,
but it may not connect to canonical PostgreSQL, mutate YDB, publish to Telegram/VK or
advance a queue cursor. The local reconciler validates and commits results.

`00-platform-smoke` and `80-region-talk-migration-reconciliation` have implemented adapters.
Other Region Talk notebooks contain complete contract, accounting, error and atomic-output
plumbing; their `process_item()` adapters intentionally fail with
`PROCESSOR_ADAPTER_NOT_PORTED` until code is adapted from an exact donor revision and covered
by golden fixtures. A placeholder notebook therefore cannot be mistaken for a working
production stage.

## Activation gate

For each Region Talk worker, replace only `process_item()` and pin model/code revisions.
Record source and destination hashes in an adaptation manifest, then prove behavioural
equivalence on fixtures and shadow data before enabling that stage.
