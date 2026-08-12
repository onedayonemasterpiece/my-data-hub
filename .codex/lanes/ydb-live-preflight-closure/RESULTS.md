# Lane YDB-LIVE-PREFLIGHT-CLOSURE Results

## Status

committed

## Requirement IDs

- YDB-PREFLIGHT
- BLOGGER-SOURCE-EVIDENCE

## Branch

`integration/operational-mvp`

## Worktree

`/home/dev/.codex/worktrees/my-data-hub/operational-mvp`

## Base SHA

`fe46224`

## Files changed

- `src/my_data_hub/workloads/bloggers/ydb_reader.py`
- `src/my_data_hub/workloads/bloggers/schema.py`
- `scripts/provider/read_only_ydb_blogger_export.py`
- focused blogger/YDB tests and operations documentation
- `docs/operations/evidence/2026-08-12-operational-mvp/ydb-blogger-metadata-preflight-live.json`

## Commands run

- Temporarily changed the externally owned YDB serverless database from its observed
  `STOPPED`/RCU 0 state to `RUNNING`/RCU 10 with the existing editor authority.
- Minted and used only the dedicated database-scoped `ydb.viewer` token.
- Executed the zero-row write-denial probe and two independent full ordered
  QuerySnapshotReadOnly scans through ydb-python 3.31.2.
- Restored the database to exact `STOPPED`, throttling enabled, RCU 0 and destroyed the
  ephemeral viewer token file.

## Tests / verification

- The live zero-row UPDATE was denied with YDB server code 400040 and the exact structured
  AccessDenied issue codes 2028/2019.
- Two non-overlapping scans agreed on 266 rows, 266 record IDs, 14 batches/files, the
  record-ID-set hash and logical hash.
- The exact private receipt is mode 0600, model receipt SHA-256
  `f2dfef44a5596dc3e862cb388e9d202c7f9d3a737b8053c912f9601615787495`.
- No source row bytes were written to the devstand; the checked-in evidence is bounded
  metadata only.
- Full pytest passed with three expected skips; the only warnings are the existing
  jsonschema `RefResolver` deprecations.
- Focused blogger/YDB tests, repository validation (4,475 checks), compileall,
  Ruff and diff-check passed.

## Risks

The evidence proves only the provider-side metadata preflight. Blogger import still
requires an ACTIVE master, the viewer credential bound as an approved Kaggle User Secret,
transaction-bound fresh scans, durable checkpoint/restore/HEAD promotion, and MCP
accounting verification. None of those is claimed here.

## Merge notes

Keep the private receipt outside Git. The protected operator closure must consume that
exact receipt after the full master/tunnel deployment is ready.
