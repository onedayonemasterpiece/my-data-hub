# Region Talk donor import gate

Region Talk is the first workload migrated to `my-data-hub`, not merely a code
example. Its accumulated YDB state and useful operational history must be
preserved before cutover.

## Donor repositories

The exact source commits must be pinned before implementation import:

- `onedayonemasterpiece/region-talk` — dedicated workload repository, migration
  plans, workers, schemas, fixtures and operating evidence;
- `onedayonemasterpiece/events-bot-new` — historical Region Talk implementation
  and product/editorial semantics that may not yet have been extracted into the
  dedicated repository.

## Curated import scope

Import with per-file provenance rather than copying repositories wholesale:

- architecture, orchestration, state/history and YDB migration documents;
- candidate, E5, BGE-M3, image, final-verifier, source-profile and writer workers;
- launch/status/result adapters and exact contracts;
- review, planning, publication, reaction and idempotency logic;
- fixtures, golden cases, incidents and product-metric audits;
- YDB row-kind readers needed for a complete lossless export.

Do not copy `.env`, session files, tokens, database dumps, cached models or
unrelated events-bot runtime. Every copied path receives source repository,
source commit and destination SHA-256 in the provenance manifest.

## Migration rule

The target backend is PostgreSQL. YDB is a temporary read-only migration source.
Legacy SQLite/Kaggle-state decisions may be retained as evidence, but they do not
override the accepted `my-data-hub` PostgreSQL architecture.

Unknown YDB row kinds are exported and retained. They are not dropped because a
mapper is not yet available. Production publication remains disabled until data
accounting, shadow comparison, exact-review canary and owner approval all pass.
The Region Talk pipeline remains `paused` throughout donor import and migration/cutover;
neither a pending provenance entry nor the verified target-vision import is evidence that
a Region Talk donor repository has been accessed or curated.
