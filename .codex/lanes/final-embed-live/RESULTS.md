# FINAL-EMBED-LIVE results

Base: `97c40e4e0ba28b38c69a5f028748432d3475092f`
Branch: `agent/operational-mvp/final-embed-live`

## Requirement accounting

- **R01 — Partial:** append-only control migration 013 and durable request/claim/stage/checkpoint metadata CAS APIs are implemented. Control create/status/internal claim/stage receipt routes bind the exact ACTIVE operation and checkpoint prerequisite. The ACTIVE Notebook consumer is not implemented.
- **R02 — Done:** compact public blogger documents are deterministic, exact-column/count checked, use `blogger_compact_v1`, reject unapproved account fields, and never cross the devstand.
- **R03 — Partial:** exact deterministic input/worker provider refs and task identities are defined and the remote journal only authorizes them for the exact CLAIMED runtime. Worker packaging/launch/poll is not implemented.
- **R04 — Partial:** existing transactional importer replay/stale/conflict behavior is preserved and MCP coverage bypasses the known legacy view bug with an exact read-only join through model-bound jobs; no canonical migration outside this lane was changed. Production document/job materialization and artifact reconciliation are not implemented.
- **R05 — Missing:** no post-embedding checkpoint trigger/cold-restore consumer was added.
- **R06 — Partial:** The control capability remains deliberately unavailable unless an actual stage runner is injected; the MCP capability is intentionally not catalogued yet, so preflight fails closed; static importability never claims readiness. Coverage now resolves the ACTIVE PostgreSQL view with exact model IDs and canonical revision.
- **R07 — Partial:** `bloggers.search` has exact/FTS rankings plus separately dimensioned, normalized E5/BGE query-vector rankings and deterministic rank-only RRF. There is no production exact query encoder, so text-only calls honestly report both vector retrievers unavailable.
- **R08 — Done for changes delivered:** no local PostgreSQL, PGDATA, vectors, documents, credentials, or provider bytes are stored in the control ledger/devstand; modern-token/provider mutation paths were not invoked.
- **R09 — Partial:** focused and full tests passed before final small fail-closed capability additions; final focused tests pass. Repository validator result is recorded below.

## Important fail-closed risk

This is **not a complete Gate K** and must not be represented as one. Production `create_app()` does not inject an embedding stage runner, so `/control/v1/embedding-production/capabilities` returns 503. The MCP control adapter also refuses the capability. Consequently the existing closure command exits before request/provider mutation. This is intentional: credentials are not yet the only blocker because the ACTIVE-master consumer, same-adapter worker execution, artifact import, checkpoint/restore, and exact query encoder remain absent.

## Validation

- `uv run python -m compileall -q src tests`
- focused: control runtime/ledger, production orchestration, compact documents, SQL correction, MCP dynamic contracts
- full: `uv run pytest -q` — PASS (2 expected opt-in skips)
- `uv run python scripts/validate_repository.py` — PASS, 3092 checks / 0 errors
