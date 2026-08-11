# H6-EVIDENCE lane results

## Identity

- Lane: `H6-EVIDENCE`
- Requirements: FM01, FM02, FM03 common evidence plane; evidence support for FM06, FM22, FM23
- Base SHA: `ed95ee2f9503650c08bb6bb56d1444fe46414cb8`
- Tested implementation SHA: `c34bd8312e5b4a453d83babc3552cfb6456f28fe`
- Branch: `agent/operational-mvp/h6-evidence`
- Worktree: `/home/dev/.codex/worktrees/my-data-hub/h6-evidence`

## Requirement result

- **FM01 — Done:** `provider.acceptance.dataset.lifecycle` durably claims the task before mutation, creates and versions one exact private disposable Dataset, performs numeric-version readback, persists hashes/fingerprint/claim receipts, and cleans it inline.
- **FM02 — Done:** `provider.acceptance.notebook.lifecycle` durably claims the task before mutation, pushes one exact private disposable Notebook, records numeric source/kernel/run identity, polls terminal status, selectively downloads one bounded output file, verifies its expected SHA-256, and stores metadata-only evidence. It deliberately returns `cleanup_state=PENDING` so the outer runner can download `operational-result.json` first.
- **FM03 — Done:** `runtime.events.history` reads at most 200 metadata-only event envelopes keyed by exact `(run_id, attempt_id, epoch)` and never returns `sanitized_json`.
- **FM06/FM23 support — Done:** Notebook evidence accepts those scenario IDs and `provider.acceptance.claim.get` returns the exact durable provider claim/output locator for the downstream control action/probe.
- **FM22 support — Done:** both Dataset and Notebook acceptance lifecycles accept FM22 and use the same injected `KaggleMCPProviderGateway` / `KaggleProviderAdapter`.
- **Cleanup — Done:** `provider.acceptance.claim.cleanup` requires the exact scenario/task/resource claim, numeric run reference, output-read receipt, and idempotency key. It reconciles a committed delete receipt and appends one cleanup receipt.
- **Failure semantics — Done:** provider mutations use deterministic effect identities. Crash/retry reconciles committed provider claims/receipts; unreconciled/ambiguous outcomes terminalize `FAILED`. No post-mutation path emits `BLOCKED`.
- **Security/data boundary — Done:** new tools are operator/provider-operator only; reader scopes are unchanged. Only metadata, numeric provider identities, fingerprints, hashes, and cleanup receipts are stored. Synthetic request source/file bytes and selected output bytes are transient and are absent from the ledger.

## Integration hooks

- `LedgerControlReader(..., provider_gateway=gateway)` automatically constructs one `AcceptanceEvidenceController` over that same gateway.
- Provider-operator tools (`provider:write`):
  - `provider.acceptance.dataset.lifecycle`
    - exact arguments: `scenario_id`, `task_id`, `idempotency_key`, `resource_ref`, `title`, `file_name`, `file_sha256`, `file_utf8`, `version_file_sha256`, `version_file_utf8`
  - `provider.acceptance.notebook.lifecycle`
    - exact arguments: `scenario_id`, `task_id`, `task_run_id`, `idempotency_key`, `resource_ref`, `title`, `code_file`, `source_utf8`, `dataset_sources`, `output_file_name`, `expected_output_sha256`, `max_output_bytes`
  - `provider.acceptance.claim.get`
    - exact arguments: `scenario_id`, `task_id`
  - `provider.acceptance.claim.cleanup`
    - exact arguments: `scenario_id`, `task_id`, `claim_sha256`, `provider_run_ref`, `output_receipt_sha256`, `idempotency_key`
- Acceptance operator tool (`acceptance:probe`):
  - `runtime.events.history`
    - exact arguments: `run_id`, `attempt_id`, `epoch`, optional `limit` (1..200)
- Notebook cleanup ordering: call lifecycle, reconcile/download the outer operational result, then call cleanup with values from the durable `PROVIDER_NOTEBOOK` and `OUTPUT_READ` evidence entries.

## Validation evidence

All required gates passed from the lane worktree:

```text
.venv/bin/ruff check .
# All checks passed!

.venv/bin/python -m compileall -q src tests scripts
# exit 0

.venv/bin/python scripts/validate_repository.py
# {"checks":3275,"errors":[],"notes":[],"ok":true}

.venv/bin/python scripts/create_notebooks.py --check
# {"drift":[],"mode":"check","written":[]}

.venv/bin/pytest -q
# 100%, exit 0; two pre-existing jsonschema.RefResolver deprecation warnings

cmp control_migrations/015_acceptance_evidence.sql \
  src/my_data_hub/control_plane/ledger/sql/015_acceptance_evidence.sql
# exit 0

git diff --check
# exit 0
```

Focused tests prove persist-before-effect, crash/response-loss reconciliation without duplicate create, no raw request bytes in SQLite, exact Notebook output/read/cleanup receipts, cleanup idempotency, FAIL-not-BLOCKED semantics, exact epoch-keyed metadata-only runtime history, and no reader-profile scope expansion.

## Risks / non-gates

- Live Kaggle calls were not made in this lane. Production provider behavior remains gated by the repository's existing Kaggle 2.2.4 adapter and its live operational matrix.
- `mypy src` is not a repository definition-of-done gate and remains red at the base repository level (304 pre-existing errors, predominantly missing/untyped Pydantic symbols). The new module only adds the same baseline Pydantic diagnostic class; Ruff, compile, repository validation, Notebook drift, and the full test suite pass.

## Changed files

- `control_migrations/015_acceptance_evidence.sql`
- `src/my_data_hub/control_plane/ledger/sql/015_acceptance_evidence.sql`
- `src/my_data_hub/control_plane/ledger/store.py`
- `src/my_data_hub/control_plane/acceptance_evidence.py`
- `src/my_data_hub/control_plane/adapters.py`
- `src/my_data_hub/mcp/catalog.py`
- `src/my_data_hub/mcp/server.py`
- `src/my_data_hub/mcp/service.py`
- `tests/control/test_acceptance_evidence.py`
- `tests/control/test_mcp_operator_provider.py`
- `tests/control/test_ledger_master.py`
- `docs/operations/acceptance-evidence-control-plane.md`
- `.codex/lanes/H6-EVIDENCE/RESULTS.md`
