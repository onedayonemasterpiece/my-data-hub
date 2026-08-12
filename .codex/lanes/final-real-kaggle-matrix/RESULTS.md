# FINAL-MATRIX lane results

## Scope

- Base: `46aafc3813b400f70a1bbeb8e040125ff96da5ce`
- Branch: `agent/operational-mvp/final-real-kaggle-matrix`
- Owned gate: opt-in real Kaggle >=15-run acceptance matrix.

## Delivered

- One 16-scenario driver uses only the existing `KaggleProviderAdapter` and generated platform-smoke Notebook/runtime contracts.
- A modern-token preflight occurs before ledger/adapter/plan/wheel construction or provider mutation. Dataset, Notebook, and matrix entry points return bounded blockers without a token.
- Stable UUID5 planning provides distinct task/run/work/checkpoint identities. Exact completed receipts support zero-mutation restart; incomplete runs reconcile the exact source/run before push. A fsynced mode-`0600` launch fence forbids a second physical push when a previously launched run is absent.
- Each real scenario binds private input Dataset version/package, exact source version/hash, numeric provider run/kernel identity, exact selective result output, typed item accounting, optional checkpoint manifest identity, retry/fault observation, and claim-bound cleanup.
- Coverage includes exact identity, retry observations, reconciliation/idempotency, stale identity denial, checkpoint manifest bindings, cleanup replay, and three sequential soak variants.
- Operational receipts are marked live only on the uninjected CLI path. Fake-adapter tests mark their temporary receipts non-live and make no provider mutation/evidence claim.
- Added strict plan, per-scenario, and summary schemas/examples; synthetic examples are documented as non-evidence.
- The opt-in provider-real workflow runs the matrix after token preflight and retains the plan plus separate scenario receipts.

## Validation

- Focused provider matrix tests: 13 passed.
- Full `pytest -q`: passed (2 intentional opt-in skips).
- `python -m compileall -q src tests scripts`: passed.
- Ruff on changed Python: passed.
- Repository validator: 3,008 checks, zero errors, `ok: true`.
- Generated Notebook check: no drift.
- `git diff --check`: passed.

## Evidence boundary

No live Kaggle call or provider mutation was performed in this lane. The workflow/CLI must be run with a modern token to create real evidence. Checkpoint scenarios prove exact typed manifest/result binding only; they do not claim a physical PostgreSQL checkpoint or restore was exercised.
