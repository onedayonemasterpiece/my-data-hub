# L01-provider results

## Lane contract

- Lane: `L01-provider`
- Requirements: `R02`, `R05`, `R13` support
- Base SHA: `74f3bf457040f078e42b489252642dcf352760d4`
  (includes architecture-reset merge `de657d63e4662e69dfb7169bc67aa65e8a9bda71`)
- Tested implementation SHA: `e97775f0dea29af0073edab7df4907ad71f0ea9f`
- Writable scope used: `src/my_data_hub/providers/kaggle/**`, `tests/provider/**`, this results file
- No real provider mutation was performed, as explicitly required for this lane.

## Delivered

1. Added the repository's one concrete `KaggleProviderAdapter`. It uses only the
   official `kaggle==2.2.4` `KaggleApi` surface and has no requests, curl, CLI,
   `DirectKaggleClient`, KaggleHub, or other transport fallback.
2. Added protocol/fake-compatible contracts for exact provider identity,
   persist-before-effect intent, task-created resource claims, effect receipts,
   dataset version identity, kernel numeric ID/source version/task-run identity,
   terminal status, and exact output identity. The adapter owns no ledger.
3. Added bounded retry classification for 429, Retry-After seconds/HTTP-date,
   retryable 408/5xx, timeouts and connection failures. Attempts, delay,
   Retry-After, elapsed time and jitter are capped; clock, sleeper and random
   source are injectable for deterministic tests. The factory replaces Kaggle
   2.2.4's internal create/version `with_retry` hook with this bounded policy.
4. Added owned dataset/kernel inventory with bounded pagination. Provider names
   and prefixes carry no authority; existing `ProviderRegistry` classifies every
   unknown observation as `external_read_only`.
5. Added private-only dataset create/version/readback and notebook push/run
   paths. Public creation is hard-coded absent. Every mutation requires a
   caller-owned journal to durably persist the exact intent before the API call.
6. Added exact cleanup controls: the caller ledger must attest the exact claim;
   task ID, effect ID, provider ref, kind, control class, disposable bit,
   fingerprint and version are hash-bound. Permanent resources and unregistered
   or foreign resources fail closed. There is no slug/prefix cleanup.
7. Added private dataset proof hooks requiring both authenticated exact version
   readback and an unauthenticated 401/403/404 denial observation.
8. Added exact notebook bindings across provider kernel numeric ID, source
   version, source SHA-256, embedded task-run UUID, terminal status and bounded
   output receipt. Since Kaggle 2.2.4 status/output calls are latest-by-slug, the
   adapter rechecks the current numeric kernel ID, source version and source hash
   before status and both before/after output retrieval; a newer/stale run fails
   closed.
9. Pinned six exact donor blobs from
   `onedayonemasterpiece/events-bot-new@416d17e689acf0a4f69f2b4d1db5dad5b46c4bca`
   for provider lifecycle, callback/heartbeat, output recovery, E5 pattern and
   BGE-M3 pattern. Each pin records the required inventory fields and an explicit
   adaptation reason removing donor fallback, delete/recreate and prefix-GC
   behavior.

## Official package evidence

- Downloaded wheel: `kaggle-2.2.4-py3-none-any.whl`
- Wheel SHA-256:
  `47d3b258363f4693ade67d875b1c108bfaa87a469f4b6fe85903e7c9757aa09c`
- Inspected `kaggle/api/kaggle_api_extended.py` and verified the exact 2.2.4
  signatures used here: `dataset_list_with_response`, `dataset_status`,
  `dataset_create_new`, `dataset_create_version`, `dataset_download_files`,
  `dataset_delete`, `kernels_list_with_response`, `kernels_push`,
  `kernels_pull`, `kernels_status`, `kernels_output`, and `kernels_delete`.
- Installed `kaggle==2.2.4` with `kagglesdk==0.1.37` in an isolated temporary
  validation environment. The focused compatibility test inspected these
  official runtime signatures and passed without authenticating or mutating.

## Commands and test evidence

All commands ran from the L01 isolated worktree.

```text
uv venv /tmp/mdh-l01-venv.glOdoG
uv pip install --python /tmp/mdh-l01-venv.glOdoG/bin/python -e '.[dev]'
uv pip install --python /tmp/mdh-l01-venv.glOdoG/bin/python 'kaggle==2.2.4'

/tmp/mdh-l01-venv.glOdoG/bin/ruff check src/my_data_hub/providers/kaggle tests/provider
All checks passed!

/tmp/mdh-l01-venv.glOdoG/bin/pytest -q tests/provider tests/test_kaggle_control.py
23 passed

/tmp/mdh-l01-venv.glOdoG/bin/python -m compileall -q src tests
/tmp/mdh-l01-venv.glOdoG/bin/ruff check src tests
All checks passed!

/tmp/mdh-l01-venv.glOdoG/bin/pytest -q
258 passed
```

`git diff --check` also passed before commit.

## Changed files

- `src/my_data_hub/providers/kaggle/__init__.py`
- `src/my_data_hub/providers/kaggle/adapter.py`
- `src/my_data_hub/providers/kaggle/contracts.py`
- `src/my_data_hub/providers/kaggle/provenance.py`
- `src/my_data_hub/providers/kaggle/retry.py`
- `tests/provider/test_kaggle_adapter.py`
- `tests/provider/test_kaggle_contracts.py`
- `.codex/lanes/L01-provider/RESULTS.md`

## Required integration edits (shared files not modified)

1. Replace the current optional dependency
   `kaggle = ["kagglehub>=0.3,<1"]` in `pyproject.toml` with the exact official
   provider dependency `kaggle = ["kaggle==2.2.4"]`.
2. Install the `kaggle` extra in real-provider/canary jobs. The adapter factory
   deliberately rejects a missing distribution or any version other than 2.2.4.
3. Implement `ProviderEffectJournal` in the durable control ledger lane. Its
   `persist_intent` must commit before returning; resource-claim attestation must
   consult persisted state rather than reconstructing a claim from a name.
4. Runtime notebooks must emit `my-data-hub-run-receipt.json` with exact
   `task_run_id`, `provider_ref`, `source_version`, `source_sha256`, and
   `terminal_state=complete` fields. The receipt is capped at 64 KiB.
5. Add the shared repository validator rule that rejects any second concrete
   Kaggle transport implementation outside
   `src/my_data_hub/providers/kaggle/adapter.py`.

## Risks and remaining real-provider gates

- `R13` real-run receipts are not claimed. The explicit lane instruction forbade
  real provider mutations. Root/integration must execute the >=15 real run IDs,
  canaries, fault/soak matrix and cleanup only after journal/orchestrator/runtime
  integration and credentialed safety gates are in place.
- Real private dataset create/readback/delete, notebook source/run/output/delete,
  and unauthenticated-denial canaries remain unexecuted for the same reason.
- Kaggle 2.2.4 exposes kernel status/output by current slug rather than an
  independent historical session ID. The adapter therefore binds the provider
  numeric kernel ID + exact source version + embedded task-run UUID + exact
  source/output receipts and refuses reads after the slug advances. Real canary
  receipts must preserve this composite identity; do not report a standalone
  provider session ID unless a real response proves one.
- The official download helper has its own bounded per-file resume loop. The
  adapter additionally bounds each top-level call and hashes streamed readback,
  but real-provider soak evidence is still required for large checkpoint files.
