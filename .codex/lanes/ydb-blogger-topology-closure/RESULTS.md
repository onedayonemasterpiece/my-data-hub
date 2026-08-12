# Lane YDB-BLOGGER-TOPOLOGY-CLOSURE Results

## Status
implemented and committed; no live mutation performed

## Base / branch
- Base: `35866f9b9d1e46e0f47c340d5816d1b18a678f80`
- Branch: `agent/operational-mvp/ydb-blogger-topology-closure`
- Worktree: `/home/dev/.codex/worktrees/my-data-hub/ydb-blogger-topology-closure`

## Requirement closure
- **Y1 dynamic accounting — Done.** Production blogger request, duplicate, quarantine,
  import and closure models/schemas no longer require the observed 266-row inventory.
  The request count is required, bounded, and bound to its deterministic
  snapshot/batch identity. Runtime validators reconcile counts and hashes exactly.
- **No devstand raw-row export — Done.** Removed the protected JSONL artifact,
  manifest/receipt schemas, environment hook, loader, and post-delivery deletion
  hook. The provider command neither accepts an output-root nor writes source rows.
- **Provider-side read-only continuation — Done.** The provider command uses the
  dedicated viewer token only in process, proves the exact write denial, performs
  two non-overlapping ordered `QuerySnapshotReadOnly` scans, and writes one private
  metadata-only detached receipt. The receipt binds source database/table/query/schema,
  viewer principal/access-binding hash, source revision, dynamic count/set/logical
  hashes, timestamps, and deterministic batch identity; it carries no row or token.
- **ACTIVE-master continuation — Done.** Production requires the detached receipt,
  pinned database and matching reader identity. The master repeats the denial probe,
  performs a fresh hash-only scan, then streams a second direct scan into the importer
  transaction. The importer compares dynamic count, identity/logical hashes and
  source-file count before commit; drift raises inside the transaction.
- **Direct live/viewer-only behavior — Preserved.** The canonical rows still move
  directly from YDB to the ACTIVE master PostgreSQL transaction. No YDB write or cloud
  mutation was performed by this lane.
- **Tests/docs — Done.** Added a non-266 receipt example/test, request binding and scan
  drift tests, single-private-receipt/no-row-artifact assertions, updated repository
  schema generation, operational procedure, and migration notes.

## Compatibility boundary
`EXPECTED_BLOGGER_ROWS = 266` remains as an explicitly documented compatibility
export in `workloads/bloggers/master_stage.py` only because separately-owned embedding
production modules import it and still describe the already-observed 266-document
embedding closure. Blogger ingestion, request accounting, import/closure validation,
and their JSON schemas do not consume this constant. Acceptance/embedding fixtures
remain at their observed 266 values; they are not the blogger source-count contract.

## Validation
- `python3 -m compileall -q src tests` — PASS.
- `.venv/bin/ruff check ...` over all owned/affected Python — PASS.
- `.venv/bin/python scripts/validate_repository.py` — PASS, 4098 checks, zero errors.
- Focused provider/blogger/schema tests — PASS (32 tests).
- Full `.venv/bin/pytest -q` — PASS, 3 skips; only two pre-existing
  `jsonschema.RefResolver` deprecation warnings.
- `git diff --check` — PASS.
- `uv.lock` absent; no wheels, source-row artifacts, credentials or secrets committed.

## Observed blockers / evidence limits
- No provider preflight or live YDB import was run. Current source availability and a
  successfully observed receipt/import/checkpoint remain external evidence gates.
- A detached receipt is source-read evidence only and never claims import, checkpoint,
  or overall live success.
