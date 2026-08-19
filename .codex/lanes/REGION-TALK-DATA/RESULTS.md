# REGION-TALK-DATA results

## Scope

- Lane: `REGION-TALK-DATA`
- Requirements: `R01` full Region Talk direct transfer contract; `R02` typed articles/posts/candidates/frontier/schedule/review/cursor/LLM projections.
- Base SHA: `1068d103ec261a37dd31e1f6d11265e1e238c168`
- Reviewed implementation SHA: `5a787d595cfaae66d888598d22ea7dc998337e1d`
- Execution mode: serial writable lane in isolated worktree; no shared-scope files edited.

## Requirement result

### R01 — Done in code; operational proof remains with integration

- Added append-only migration `0023_region_talk_direct_snapshot.sql`.
- Closed source scope is exactly five YDB tables; all counts remain dynamic.
- Added in-memory Pass A counts/kind counts/ordered hashes and bounded Pass B pages (1..500) directly to PostgreSQL.
- Compact-state `kind` is read from its explicit column and never inferred from `pk`.
- Added ACTIVE epoch, exact pipeline-role and task checks, contiguous-page/key checks, payload hash verification, exact replay/conflict handling and final Pass A/Pass B/PostgreSQL reconciliation.
- Every landed row receives a terminal disposition. Valid unsupported kinds are retained raw; malformed rows are quarantined.
- No local source-row files, control-plane payloads, Dataset row payloads, generic SQL, or publication effects are used.
- Added dedicated `mdh_region_talk_pipeline` group, broker issuance allow-list and 5-minute bounded statement policy. The role receives only four fixed SECURITY DEFINER procedures, not table DML.
- Added v2 manifest/receipt JSON schemas and direct migration documentation.

Operational qualification (Kaggle adapter, live transfer, checkpoint) is intentionally outside this data lane and is not claimed here.

### R02 — Done for approved core mappings; unsupported semantics retained honestly

- Typed fixed projections/read views added for:
  - articles, posts, publication candidates and acquisition opportunities;
  - source frontier/candidates/status/edges, post intake, image/candidate memory;
  - publication schedule, review state/events, delivery history and operator feedback;
  - cursors/state snapshots and acquisition surfaces/runs;
  - Region Talk LLM request/budget idempotency.
- Blogger snapshot rows do not create actors/profiles. They reuse existing dedicated identity-map/profile evidence when present, preserving the reviewed 266-source-row to 263-canonical-actor outcome; otherwise they remain raw pending that reviewed path.
- `RegionTalkReader` exposes the fixed MCP seam requested by the MCP lane. Lists/search omit bodies and raw JSON; exact get returns only the typed body field.
- Replaced the `citext` equality in `region_talk.funnel_current` with `p.slug::text`, fixing reader execution without granting extension-owned public functions.
- Legacy kinds without an approved semantic transformation are deliberately `retained_raw`, not misrepresented as normalized.

## Evidence and commands

Passing after final edits:

```text
python -m compileall -q src tests
ruff check src/my_data_hub/workloads/region_talk src/my_data_hub/master_runtime/credentials.py tests/region_talk
pytest -q tests/region_talk tests/test_region_talk_migration.py tests/test_db_migrations.py
  33 passed
python scripts/validate_repository.py
  ok=true, checks=4620, errors=[]
```

The migration is parsed by `pglast.parse_sql` in the lane tests. Migration history is contiguous and the repository validator accepts schema revision 23.

A full `pytest -q` run reached 100%; all lane and non-upload tests passed. Fifteen unrelated provider-upload tests failed only at their production disk-reserve guard because shared devstand free space was about 304 MiB while several concurrent agents had active pytest trees under `/tmp/pytest-of-dev`. The same full suite passed earlier in the lane before shared disk pressure increased. No provider-upload code is in this lane.

## Risks / integration gates

1. A disposable PostgreSQL semantic execution proof was not run: the requested pgvector image tag was not locally tagged and its pull was stopped when disk space was critically low. SQL syntax, static role constraints and repository validation pass, but integration must execute all migrations against disposable PostgreSQL after space is recovered.
2. The separate Kaggle pipeline must provide `DirectYdbReader.scan_page(...)` using its read-only YDB driver and preserve keyset ordering/snapshot semantics; the runner/contract owns hashing and direct PG landing.
3. A supervised live run must dynamically reconcile current source counts/hashes, validate typed MCP reads, and create a verified checkpoint before cutover is claimed.
4. Valid unsupported operational kinds remain losslessly raw by design. Later semantic activation requires an append-only mapping migration; they are not silently dropped.
5. Publication effects remain disabled. Historical delivery records are read projections only and are never replayed.

## Changed files

- `sql/migrations/0023_region_talk_direct_snapshot.sql`
- `sql/admin/bootstrap_roles.sql`
- `sql/admin/role_contract.sql`
- `src/my_data_hub/master_runtime/credentials.py`
- `src/my_data_hub/workloads/region_talk/constants.py`
- `src/my_data_hub/workloads/region_talk/contracts.py`
- `src/my_data_hub/workloads/region_talk/direct_snapshot.py`
- `src/my_data_hub/workloads/region_talk/reader.py`
- `schemas/region-talk-direct-snapshot.v2.schema.json`
- `schemas/region-talk-direct-snapshot-receipt.v2.schema.json`
- `tests/region_talk/*`
- `docs/migrations/region-talk/README.md`
- `docs/migrations/region-talk/direct-snapshot-v2.md`
