# H3-ADMISSION results

## Scope

- Lane: `H3-ADMISSION`
- Requirement: remove the circular embedding-production admission dependency on an already `CHECKPOINT_VERIFIED` request.
- Base SHA: `0b86000cf2a0adaf15a99feae44fb823474d5bb7`
- Implementation head SHA: `4f41530762a252b2efdf19be4633c3cc4e81f4ae`
- Final lane head: the documentation-only commit containing this file is reported in the parent handoff.

## Outcome

Implemented an append-only `my-data-hub-embedding-production-capabilities.v2` admission contract with two honest roles:

- `control_executor` attests the exact repository stage runner, one pinned `KaggleProviderAdapter`, the current ACTIVE master, current canonical revision/checkpoint, and immutable worker assets.
- `mcp_observer` attests only its read-only observation of that same ACTIVE-master/prerequisite binding; model and JSON Schema validation forbid it from claiming runner or provider-adapter availability.

The external closure validates that both bindings match each other and the exact FINAL-BLOGGER canonical revision/checkpoint before `create_request`. The request POST repeats executable-runtime and prerequisite checks. A blocker returns without a durable request. An exact replay returns the existing request (`created: false`) without requiring a live runtime or creating another effect; identity/hash conflict handling remains fail closed. Terminal request status and the closure receipt remain the only completion/coverage/checkpoint evidence.

The request ledger now omits immutable `worker_assets` from its stored JSON because the ledger correctly rejects token-shaped keys such as model `max_tokens`; strict `EmbeddingProductionRequest` validation restores the exact frozen defaults and reproduces the original request hash in the ACTIVE master.

No SQL migration, role, grant, database checksum, deployment/operator, blogger-deduplication, or operational-matrix file changed.

## Changed files

- `src/my_data_hub/embeddings/production.py`
- `src/my_data_hub/control_plane/app.py`
- `src/my_data_hub/control_plane/adapters.py`
- `schemas/embeddings/embedding-production-capabilities.v2.schema.json`
- `examples/embeddings/embedding-production-capabilities.v2.example.json`
- `tests/embeddings/test_production_orchestration.py`
- `tests/control/test_control_runtime_wiring.py`
- `docs/operations/final-embedding-closure.md`
- `.codex/lanes/H3-ADMISSION/RESULTS.md`

## Evidence and commands

All commands ran from `/home/dev/.codex/worktrees/my-data-hub/h3-embedding-admission` using the repository dev environment at `/home/dev/.codex/worktrees/my-data-hub/operational-mvp/.venv`.

- `python -m pytest tests/control/test_control_runtime_wiring.py tests/embeddings/test_production_orchestration.py -q` — PASS, 27 tests.
- `python -m pytest tests/provider/test_scheduled_control_interfaces.py tests/mcp tests/test_mcp_sdk_v2_contract.py -q` — PASS, 76 tests.
- `python -m pytest -q` — PASS (two pre-existing `jsonschema.RefResolver` deprecation warnings; two tests skipped by their existing conditions).
- `python scripts/validate_repository.py` — PASS, 3232 checks, zero errors/notes.
- `python scripts/create_notebooks.py --check` — PASS, no drift.
- `python scripts/scan_tracked_secrets.py` — PASS.
- `python -m compileall -q src tests scripts` — PASS.
- `ruff check .` — PASS.
- `git diff --check` — PASS.
- `mypy src tests` — NOT A LANE REGRESSION GATE in the reused environment: 629 repository-wide errors, beginning with missing PyYAML stubs and pervasive pre-existing Pydantic/FastAPI typing errors across 109 files. No claim of a mypy pass is made.

## Focused assertions

- The first request is accepted while its state is only `REQUESTED`; no prior embedding request or completion receipt is needed.
- Missing safe adapter blocks capability and POST; the ledger remains unchanged.
- Exact replay returns `created: false` and the same `REQUESTED` state.
- A stale blogger checkpoint binding blocks before request creation.
- v2 admission contains neither `verified_checkpoint_restore` nor `mcp_hybrid_search` completion claims.
- MCP observer output contains no runner or adapter implementation claim and must match the control prerequisite binding.
- v1 schema/example remain unchanged as append-only historical artifacts.

## Risks / follow-up

- These are repository and synthetic runtime tests, not evidence of a real Kaggle embedding run or production readiness.
- The MCP observer intentionally proves only its ledger/interface view; the loopback control executor is the sole runtime/adapter attestation. The closure requires both roles and an exact shared binding.
- Deployment may legitimately remain blocked until the production runtime has the exact runner, pinned adapter, ACTIVE master, and current FINAL-BLOGGER checkpoint.
