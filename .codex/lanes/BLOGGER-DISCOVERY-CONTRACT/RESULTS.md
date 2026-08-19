# BLOGGER-DISCOVERY-CONTRACT lane results

Base: `c0d2a8d82278fab658fe8f9b79e81f4b7a14f06a`

## Implemented

- Closed deterministic inline-row and immutable provider-artifact submit contracts.
- Append-only PostgreSQL migration 0020 with landing, immutable preview plans,
  quarantine, fixed canonical apply/reconcile functions, semantic outbox and receipts.
- Sanitized `hub.bloggers_v1` and a fixed bounded reader facade.
- Minimum connector/reader/canonical-committer grants; no generic SQL or broad canonical
  DML grant.
- Metadata-only control migration 028 plus packaged twin, with exact replay/conflict,
  atomic preview/apply/reconcile and checkpoint lifecycle ledger projections, verified
  checkpoint authority, immutable durable checkpoint identity, and PREVIEWED-only
  dead-epoch rebind/re-preview continuation.
- Dedicated ACTIVE-epoch materializer role with exact accepted artifact claim/principal
  binding and full typed validation before artifact landing; generic connector denial.
- Explicit two-stage public validation: closed Draft 2020-12 structural schema plus a
  mandatory semantic validator for cross-value equality, cross-row identities and UTF-8
  byte bounds that JSON Schema cannot express.
- PREVIEWED reset checks authoritative PostgreSQL service epoch state and rejects reset of
  a still-live ACTIVE/DRAINING epoch; only exact STOPPED/FENCED or a newer fencing epoch
  permits rebind/re-preview.
- Unit/static tests for closed schemas, deterministic hashes, fixed SQL surfaces,
  metadata-only replay/conflict and append-only events.
- Disposable PostgreSQL proof for connector landing, semantic quarantine, direct-DML
  denial, preview, atomic apply, exact replay, sanitized read projection, one revision and
  one semantic outbox event.

## Deliberately not implemented in this lane

- Shared `mcp/catalog.py`, `mcp/server.py`, `mcp/service.py`, control-gateway/runtime or
  provider wiring. Those surfaces were reserved for another integration lane.
- Deployment or a live production data run. No live row counts, hashes or readiness are
  claimed here.
- Devstand business-row storage or a local PostgreSQL runtime.

## Validation

Passed on 2026-08-16:

- `uv run python -m compileall -q src tests`
- `uv run python scripts/validate_repository.py` — `4485` checks, no errors/notes
- `uv run python scripts/create_notebooks.py --check` — no drift
- `uv run ruff check .`
- `uv run pytest -q` — complete repository suite passed (four expected skips; two
  pre-existing `jsonschema.RefResolver` deprecation warnings)
- `MDH_RUN_DISPOSABLE_POSTGRES=1 uv run pytest -q
  tests/bloggers/test_discovery_postgres_live.py
  tests/bloggers/test_duplicate_resolution_postgres.py` — both disposable PostgreSQL
  proofs passed
- `git diff --check`

The branch-tip SHA is reported to the integrator outside this self-referential results
file.
