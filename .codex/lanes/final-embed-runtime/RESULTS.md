# FINAL-EMBED runtime completion

Base: `702cc53`
Branch: `agent/operational-mvp/final-embed-runtime`

## Implemented

- ACTIVE master claims the durable embedding request after blogger work, verifies the exact modern Kaggle token, verified blogger revision, 266-row public projection, wheel, and remaining shared deadline before the first provider effect.
- Compact documents and deterministic E5/BGE jobs are packaged provider-side into two exact private protected input Datasets. The exact generated E5/BGE notebooks are packaged in the wheel, receive only their exact numeric Dataset source, and launch through the existing checkpoint coordinator's single `KaggleProviderAdapter` before shared bounded polling.
- Only `embedding-result.json` is selectively downloaded. The exact run/model/job/artifact is validated; no broad output, checkpoint, PGDATA, document, or vector bytes cross the control plane.
- `PostgresEmbeddingImporter` materializes exact current documents/jobs and imports each model/vector space in the same epoch-fenced PostgreSQL transaction. Exact replay, stale/conflict handling, canonical revision advancement, and checkpoint-required outbox remain atomic.
- The same exact workers encode the bounded Gate-K query using the model query contract. Query vectors are stored only in append-only, epoch-fenced canonical master tables (`0014`); control receives hashes/dimensions only. MCP hashes query text, resolves the exact cached E5/BGE spaces, and applies deterministic rank-only RRF with exact/FTS fallback.
- The sanitized stage receipt must be ACKed before checkpointing. Execution or ACK failure fences/stops without promoting partial Gate-K bytes. The existing verified checkpoint, callback-loss reconciliation, rotation, cold restore, coverage, and hybrid validation path completes the closure.
- Control and MCP production capability surfaces now reflect the installed concrete implementation. External and in-master modern-token preflights both fail before provider mutation.

## Validation

- Focused Gate-K/control/master/MCP/provider-journal/migration suite: PASS.
- `python -m compileall src tests`: PASS.
- Repository validator: 3176 checks, 0 errors after removal of test-generated edge `__pycache__`.
- Focused Ruff: PASS.
- Full `pytest -q`: all Gate-K and unrelated tests pass except one pre-existing base failure: `tests/test_architecture_invariants.py::test_repository_wide_deployment_surface_is_closed` still hard-codes the old deploy allowlist and rejects the `deploy/yandex-edge/**` files already present at base `702cc53`. This lane did not edit edge/deploy files or that test.

## Live blocker / residual risk

- No modern Kaggle token or production corpus was used, and no provider mutation was performed. Therefore there is no live run ID, artifact, coverage, checkpoint, restore, or MCP production receipt to claim.
- Real Kaggle scheduling/model-download duration and the pinned worker dependency environment remain live-only risks. The code enforces a shared 9000-second provider deadline inside the ACTIVE allocation and fails without checkpoint promotion if it cannot finish.
- A failed claimed runtime without a complete stage receipt is terminalized fail-closed; operators must issue a new idempotency key after correcting the live cause. Exact retries within an attempt and exact artifact/import replays are idempotent.
