# Lane gates-a-b results

## Status

Committed and green.

## Requirement IDs

- Gate A — enforce one direct Kaggle transport implementation repository-wide.
- Gate B — isolate poison operations, persist bounded failure evidence, and require exact reconciliation before mutation retry.
- Gate B property/state-machine evidence — at least 10,000 deterministic FakeKaggle histories combining crash, retry, duplicate, and reorder.

## Git identity

- Base SHA: `6b1cebdd1e81541669b66f63e6369905c58dcc11`
- Implementation head SHA before this results receipt: `f98563f2e3623dc9afff5f8fc5b063c0f3b4a2c8`
- Branch: `agent/gates-a-b`

## Changed files

- `scripts/validate_repository.py`
- `src/my_data_hub/orchestrator/master/coordinator.py`
- `tests/control/test_gate_a_b_closure.py`
- `.codex/lanes/gates-a-b/RESULTS.md`

## Implementation evidence

- The repository validator scans production Python ASTs plus executable shell, workflow, YAML, Dockerfile, Makefile, and Notebook text for direct Kaggle SDK, HTTP, and CLI transports.
- Direct imports/calls of `my_data_hub.providers.kaggle.KaggleProviderAdapter` remain valid reviewed call sites; tests and documentation are excluded from production-implementation classification.
- The only implementation file is pinned to `src/my_data_hub/providers/kaggle/adapter.py`, including its official `kaggle.api.kaggle_api_extended` SDK import.
- The existing exact unauthenticated private-Dataset denial probe is narrowly pinned by path, call line, and endpoint shape. It cannot authenticate or mutate and is not classified as a provider transport implementation; drift fails validation.
- `MasterCoordinator.reconcile_all` now catches an operation-scoped exception, appends a bounded non-secret self-transition receipt to `operation_log`, returns that operation's current handle, and continues with later independent operations.
- A claimed effect remains `IN_PROGRESS`; a later pass must call exact provider reconciliation and executes again only after an exact `ABSENT` result. An exception message is not persisted.
- The Hypothesis property test uses `max_examples=10_000` with `derandomize=True`. Every generated FakeKaggle history contains a scripted before- or after-mutation crash, exact reconciliation/retry, reordered delivery, duplicate replay, and exactly-once physical-effect assertions for all three provider effects.

## Commands and results

- Initial red test: `uv run --extra dev pytest tests/control/test_gate_a_b_closure.py -q`
  - Reproduced six failures: poison reconciliation aborted the batch and the Kaggle validator function was absent.
- Focused final test: `uv run --extra dev pytest tests/control/test_gate_a_b_closure.py -q`
  - `9 passed` (includes 10,000 deterministic Hypothesis/FakeKaggle histories).
- Repository/schema validation: `uv run --extra dev python scripts/validate_repository.py`
  - `3765` checks, zero errors, zero notes.
- Compile validation: `uv run --extra dev python -m compileall -q src tests`
  - Passed.
- Notebook drift validation: `uv run --extra dev python scripts/create_notebooks.py --check`
  - `drift: []`, `written: []`.
- Full suite: `uv run --extra dev pytest`
  - `1078 passed, 3 skipped, 2 warnings in 96.52s`.
  - Skips are pre-existing opt-in/environment tests; warnings are pre-existing `jsonschema.RefResolver` deprecations.
- Lint: `uv run --extra dev ruff check scripts/validate_repository.py src/my_data_hub/orchestrator/master/coordinator.py tests/control/test_gate_a_b_closure.py`
  - Passed.
- `git diff --check`
  - Passed.

## Risks and integration notes

- Static enforcement intentionally detects declared SDK imports, provider HTTP endpoints, and Kaggle CLI invocations; deliberately obfuscated or runtime-generated command/host strings are outside static proof and still require review.
- The exact unauthenticated denial-probe exception is fail-closed: harmless line or endpoint drift requires an explicit validator review/update.
- Reconciliation failures remain nonterminal because a provider mutation may already exist. This preserves exact recovery rather than fabricating terminal failure or blindly retrying.
- This lane does not alter the runtime SDK, notebooks, connectors, acceptance matrix, receipts, or checkpoint topology.
