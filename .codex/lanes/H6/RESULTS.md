# Lane H6 Results

## Status

committed

## Requirement IDs

- H6-CALLBACK-REPLAY — Blocked: no safe callback suppression, stale-output injector, or bounded event-history tool exists.
- H6-DRAIN-ROTATION — Partial: FM13 exact claim-gated rotation request/poll client is implemented; clean drain and credential revocation/rotation remain blocked.
- H6-CHECKPOINT-FAULTS — Partial: FM06 exact claim-gated normal restore request/poll client is implemented; corruption and forced restore-smoke failure injection remain blocked.
- H6-CONTROLLED-ROW — Blocked: no owner-approved exact disposable SQL/parameters/revision/cleanup fixture exists.
- H6-ACCELERATED-SOAK — Blocked: no event stream/controller can prove required heartbeat/read/checkpoint/recovery counts.

No provider PASS is claimed. The implemented FM06/FM13 path can return a locator only after an owner claim is re-read through the real provider gateway, the exact task/run/source identity matches, the durable action reaches `DURABLE_COMPLETE`, and the matrix runner independently reconciles and downloads the exact Kaggle output.

## Branch

`agent/operational-mvp/h6-operational-interfaces`

## Worktree

`/home/dev/.codex/worktrees/my-data-hub/h6-operational-interfaces`

## Base SHA

`4916d166e7df80ab676c619a8e2eae7d0ada7b8b`

## Head SHA

Implementation commit: `86ebe916b6cd22bc7e27f0a65a4f027575ba4b5f`

The final branch tip additionally contains this results-only bookkeeping commit.

## Files changed

- `.github/workflows/provider-real.yml`
- `docs/operations/operational-kaggle-matrix.md`
- `examples/provider/operational-kaggle-evidence-claims.v1.example.json`
- `schemas/provider/operational-kaggle-evidence-claims.v1.schema.json`
- `scripts/provider/operational_kaggle_driver.py`
- `scripts/provider/operational_kaggle_matrix.py`
- `tests/provider/test_operational_kaggle_driver.py`
- `tests/provider/test_operational_kaggle_matrix.py`
- `.codex/lanes/H6/RESULTS.md`

## Commands run

- `.venv/bin/pytest -q tests/provider/test_operational_kaggle_driver.py tests/provider/test_operational_kaggle_matrix.py`
- `.venv/bin/ruff check scripts/provider/operational_kaggle_driver.py scripts/provider/operational_kaggle_matrix.py tests/provider/test_operational_kaggle_driver.py tests/provider/test_operational_kaggle_matrix.py`
- `.venv/bin/python scripts/validate_repository.py`
- `.venv/bin/python -m compileall -q src tests`
- `.venv/bin/pytest -q`
- `.venv/bin/pytest --collect-only`
- `git diff --check`

## Tests / verification

- Focused H6 provider tests: **56 passed**.
- Full test suite: **738 collected; 737 passed, 1 opt-in skip**. Only the existing `jsonschema.RefResolver` deprecation warning was emitted.
- `python -m compileall src tests`: passed.
- Repository validator: **3,182 checks, 0 errors**.
- Ruff on changed Python files: passed.
- `git diff --check`: passed.
- Checklist review found three initial safety issues (provider read routing, resume identity, duplicate claims) and one malformed-acceptance issue; all H6-side findings were corrected and re-reviewed. H1 confirmed its provider read route bypasses master resolution/ensure and the PostgreSQL broker.

## Risks

- Integration must merge H1/H2 before H6. H6 relies on H1/H2's exact `provider.resources.read` control-gateway route and exact restore/rotation MCP bindings. Without that dependency, FM06/FM13 remain honestly BLOCKED before action.
- No live Kaggle Notebook, restore, or rotation was executed in this lane; modern provider/OAuth credentials and an owner-issued real evidence claim are still required.
- A resume requires the claim document to carry the previously accepted exact `operation_id`. Missing or mismatched resume identity is typed FAIL and never creates a replacement action.
- The driver deliberately retains explicit blockers for callback/replay injection, clean drain/credential rotation, corruption/forced restore failure, controlled row fixture, and soak orchestration.

## Merge notes

1. Merge/cherry-pick H1/H2 operator/provider gateway changes first.
2. Cherry-pick H6 implementation commit `86ebe916b6cd22bc7e27f0a65a4f027575ba4b5f`.
3. Cherry-pick the following H6 results-only commit.
4. Re-run focused H1/H2 + H6 tests and the full suite on the integration branch.
