# H6-EVIDENCE-DRIVER Results

## Status

Committed implementation. No production credential, MCP mutation, Kaggle run, Dataset, Notebook, restore, protected-resource probe, runtime event history, or live PASS was attempted or claimed.

## Requirement IDs

- `FM01` — private disposable Dataset create/version/readback/delete plus a distinct exact evidence Notebook.
- `FM02` — exact source/run/output Notebook lifecycle and claim-bound cleanup.
- `FM03` — exact callback-ingested runtime history with heartbeat and terminal event binding.
- `FM06` — claim-gated durable restore terminal, exact restored ACTIVE master provider identity/revision, and post-action evidence Notebook.
- `FM22` — MCP-managed private Dataset and Notebook lifecycles with deterministic distinct subtask IDs.
- `FM23` — exact registered protected-resource denial with no mutation attempt and a distinct evidence Notebook.
- `H6-TWO-PHASE` — append-only READY/reconciliation/CLEANUP protocol; PASS requires durable cleanup COMPLETE.

## Branch / worktree

- Branch: `agent/operational-mvp/h6-evidence-driver`
- Worktree: `/home/dev/.codex/worktrees/my-data-hub/h6-evidence-driver`
- Base SHA: `764ffc31f58bad015f28d3a5fefc784e91f67ca6`
- Tested implementation head SHA: `edcda07a3687d3bf5527b369c5b9df073f76108f`
- The RESULTS commit is a documentation-only successor; use the branch tip from the final handoff as the final head SHA.
- Effort/risk: extra-high because the lane changes a production mutation/reconciliation protocol and crosses provider claims, exact output receipts, cleanup authorization, runtime-history identity, restore identity, and response-loss recovery.

## Prerequisite commits carried on this branch

These were authored in separate lanes and are already present in the current integration line; they are recorded here to explain the tested local ancestry, not as H6-EVIDENCE-DRIVER-owned changes:

- `71bfebc` (source `6b562e7`) — admit FM01/FM03 evidence Notebooks and cleanup.
- `c5bffb8` (source `4141142`) — expose exact ACTIVE master `provider_run_ref` / `provider_kernel_id`.
- `c969cfc` (source `c4a9992`) — acceptance Notebook `dataset_inputs` contract.
- `30f4973` (source `df386e0`) — provider-run exact input-claim authority.

## Outcome

### Exact evidence scenarios

- The trusted driver now calls the exact provider evidence-plane tools rather than generic provider resource envelopes:
  - `provider.acceptance.dataset.lifecycle`
  - `provider.acceptance.notebook.lifecycle`
  - `provider.acceptance.claim.get`
  - `provider.acceptance.claim.cleanup`
  - `runtime.events.history`
  - existing `checkpoint.restore.request`, `operation.get`, `master.status`, and `provider.protected_resource.probe`.
- Every provider lifecycle uses deterministic matrix/scenario/subtask identities. The acceptance controller durably commits its task claim before its first provider effect.
- FM01/FM22 derive distinct deterministic Dataset and Notebook task UUIDs, so one terminal subtask cannot mask another. Dataset cleanup is inline and exact before the evidence Notebook is launched.
- FM22 deliberately sends `dataset_inputs: []`. Current provider authority requires an `mcp_managed` input claim to share the Notebook task ID, which conflicts with the required distinct Dataset/Notebook subtask IDs. The implementation proves both lifecycles but does not claim that the Notebook consumed the Dataset.
- FM03 accepts only an externally configured exact `(run_id, attempt_id, epoch)`. It validates the bounded metadata-only event projection, unique event/sequence identities, at least one `runtime.heartbeat`, and exactly one final `runtime.terminal`.
- FM23 selects the exact registered `orchestrator_protected` reference, requires `PROTECTED_RESOURCE_DENIED`, and requires `mutation_attempted=false` before launching its distinct evidence run.
- FM06 retains the owner-issued pre-action evidence claim. After exact restore `DURABLE_COMPLETE`, it requires an ACTIVE `master.status` with numeric provider run/kernel identity and canonical revision equal to the selected checkpoint. The restored master provider run is never substituted with the verifier Notebook run.

### Two-phase outer reconciliation and cleanup

- Append-only v2 driver request/result schemas add `EXECUTE` and `CLEANUP` phases.
- Exact evidence executions return `READY`, not PASS, with provider run identity plus acceptance task, provider claim, output file/tree hashes, and output-read receipt.
- The outer matrix independently reconciles the exact numeric run through its one real `KaggleProviderAdapter`, requires terminal `complete`, downloads only `operational-result.json`, validates the exact scenario assertions/lifecycle gates, and compares independently observed file/tree hashes with the durable `OUTPUT_READ` receipt.
- Before destructive cleanup, the outer runner writes a mode-0600 reconciliation fence containing the validated receipt draft, exact cleanup request, execute result, and a canonical SHA-256 over the fence payload.
- The cleanup driver independently re-reads the acceptance claim, verifies the exact task/claim/run/output receipt, calls `provider.acceptance.claim.cleanup`, and returns PASS only after `claim.get` reports `cleanup_state=COMPLETE` and one exact cleanup receipt.
- A cleanup response lost after successful deletion is reconciled through `claim.get`. A rerun consumes the hashed reconciliation fence and does not try to download the deleted Notebook again.
- Missing credentials/configuration or a missing pre-action resume claim remains `BLOCKED` with `mutations_started: 0`. Any possible or ambiguous post-mutation lifecycle/cleanup response is reconciled or returns `FAIL`, never BLOCKED.
- The injected/fake matrix path remains hard-coded BLOCKED and cannot create a live PASS.

### Compatibility

- Driver v1 schemas/examples remain unchanged as historical artifacts. The executable matrix/driver now use v2.
- FM20 signed deployment-evidence v2 behavior and owner-managed exact Notebook locator remain unchanged.
- Unclosed FM scenarios retain their specific non-mutating blockers; no new generic PASS or default-reader scope was added.

## Commands and evidence

All commands ran in the isolated worktree against implementation head `edcda07a3687d3bf5527b369c5b9df073f76108f` using the synchronized integration virtual environment.

```text
pytest -q tests/provider/test_operational_kaggle_driver.py \
  tests/provider/test_operational_kaggle_matrix.py
PASS: 73 focused tests

pytest -q tests/control/test_acceptance_evidence.py \
  tests/control/test_master_request_bridge.py \
  tests/provider/test_operational_kaggle_driver.py \
  tests/provider/test_operational_kaggle_matrix.py
PASS: 81 focused/dependency tests

python scripts/validate_repository.py
PASS: 3296 checks, 0 errors, 0 notes

python scripts/create_notebooks.py --check
PASS: no drift

python -m compileall -q src tests
PASS

mypy
PASS: no issues in the repository's 5 configured strict targets

ruff check scripts/provider/operational_kaggle_driver.py \
  scripts/provider/operational_kaggle_matrix.py \
  tests/provider/test_operational_kaggle_driver.py \
  tests/provider/test_operational_kaggle_matrix.py
PASS

pytest -q
PASS: 818 collected; 816 passed, 2 expected environment-gated skips
(two pre-existing jsonschema RefResolver deprecation warnings)

python scripts/scan_tracked_secrets.py
PASS

git diff --check
PASS
```

An explicit `mypy --strict` run against the two executable scripts is not a repository gate and reports their pre-existing dynamic Pydantic/import typing limitations. The configured strict mypy target passes and no suppression was added.

## Risks / live prerequisites

- No live provider/operator/reader token, owner, FM03 runtime identity, FM06 owner claim, exact provider run, or production result was available. Unit/fake responses and checked-in examples are contract tests only, never live evidence.
- FM03 requires the selected runtime history to fit the 200-event bounded projection and contain the terminal event; a truncated or mismatched history fails before any evidence Notebook mutation.
- FM06 response-loss resume still requires the already documented owner claim to carry the accepted restore operation ID. Without it, resume is FAIL and never creates a replacement restore.
- The FM02 Notebook cannot observe its own terminal provider receipt from inside itself. Its generated result binds the exact planned contract; READY is emitted only after the independent acceptance controller has observed terminal completion and exact output, and final PASS still requires outer download plus cleanup COMPLETE.
- FM22 proves separate Dataset and Notebook lifecycles, not Dataset consumption, until input-claim authority gains a safe cross-subtask delegation contract. Authority was not weakened.
- Local reconciliation fences are mode 0600 and content-hashed. They are trusted owner-side recovery state, not provider evidence and not a substitute for the durable control-ledger claim.
- Unclosed callback fault injection, drain, checkpoint fault, controlled fixture, and soak scenarios remain their specific BLOCKED states.

## Changed files owned by this lane

- `docs/operations/operational-kaggle-matrix.md`
- `examples/provider/operational-kaggle-driver-request.v2.example.json`
- `examples/provider/operational-kaggle-driver-result.v2.example.json`
- `examples/provider/operational-kaggle-evidence-driver.v1.example.json`
- `schemas/provider/operational-kaggle-driver-request.v2.schema.json`
- `schemas/provider/operational-kaggle-driver-result.v2.schema.json`
- `schemas/provider/operational-kaggle-evidence-driver.v1.schema.json`
- `scripts/provider/operational_kaggle_driver.py`
- `scripts/provider/operational_kaggle_matrix.py`
- `tests/provider/test_operational_kaggle_driver.py`
- `tests/provider/test_operational_kaggle_matrix.py`
- `.codex/lanes/H6-EVIDENCE-DRIVER/RESULTS.md`

## Integration notes

The current integration already contains the four prerequisite source commits listed above. Integrate the H6 implementation commit and then the RESULTS commit. Do not cite examples, fake gateway tests, or schema validation as FM01/FM02/FM03/FM06/FM22/FM23 live PASS; only a real outer matrix run that independently reconciles exact Kaggle output and observes durable cleanup COMPLETE can do so.
