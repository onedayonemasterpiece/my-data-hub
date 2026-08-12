# checkpoint-adapter-primitives lane results

## Scope

- Lane ID: `checkpoint-adapter-primitives`
- Base SHA: `0f370d080056c1608d62b4390fa4bb8ae67adaf8`
- Implementation head SHA: `e22a415634f9c717de4a59bb6c1fe2a700acf9ec`
- Branch: `agent/operational-mvp/checkpoint-adapter-primitives`

Implemented only the central Kaggle adapter's brokered Dataset blob primitives:

- one-shot Dataset blob start through `kaggle==2.2.4` generated SDK types;
- redacted opaque blob-token and signed-URL grant contract;
- one-shot private Dataset create/version finalization from caller-supplied blob tokens;
- canonical per-file description binding for operation, master run, epoch, manifest hash, file hash, and exact size;
- current-version plus exact numeric-version metadata reconciliation using names, sizes, and descriptions only;
- lost-response handling that reconciles the one expected numeric version and never blindly repeats a mutation.

No control ledger, application, checkpoint runtime, deployment, migration, CLI, `kagglehub`, second `KaggleApi`, or checkpoint-byte download path was changed.

## Changed files

- `src/my_data_hub/providers/kaggle/adapter.py`
- `src/my_data_hub/providers/kaggle/contracts.py`
- `tests/provider/test_kaggle_brokered_adapter.py`
- `.codex/lanes/checkpoint-adapter-primitives/RESULTS.md`

## SDK evidence

Targeted source inspection used the official PyPI wheels for `kaggle==2.2.4` and its declared `kagglesdk>=0.1.35,<1.0` dependency. The implementation uses the same generated request types and client surfaces as the pinned adapter:

- `ApiStartBlobUploadRequest` and `ApiBlobType.DATASET`;
- `ApiDatasetNewFile`;
- `ApiCreateDatasetRequest`;
- `ApiCreateDatasetVersionRequestBody` and `ApiCreateDatasetVersionRequest`;
- `KaggleApi.build_kaggle_client()` and `dataset_list_files()`.

An installed-extra smoke constructed all six real generated types and printed:

`ApiStartBlobUploadRequest ApiCreateDatasetRequest ApiCreateDatasetVersionRequest True`

## Commands and results

- `uv run --extra dev --extra kaggle python <generated SDK type smoke>` — PASS.
- `uv run --extra dev --extra kaggle pytest -q tests/provider/test_kaggle_brokered_adapter.py tests/provider/test_kaggle_contracts.py` — PASS (`15 passed`).
- `.venv/bin/python scripts/validate_repository.py` — PASS (`3718` checks, zero errors).
- `.venv/bin/python scripts/scan_tracked_secrets.py` — PASS.
- `.venv/bin/python -m compileall -q src tests scripts` — PASS.
- `.venv/bin/pytest` — PASS (`1077 passed, 2 skipped`; two existing jsonschema deprecation warnings).
- `.venv/bin/ruff check .` — PASS.
- `.venv/bin/mypy` — PASS (`Success: no issues found in 5 source files`).
- `.venv/bin/python scripts/create_notebooks.py --check` — PASS (zero drift).
- `git diff --check` — PASS.

## Security and ambiguity evidence

Focused tests prove:

- secret dataclass fields are absent from `repr`;
- provider exceptions containing sample blob tokens and signed URLs are not chained or copied into public adapter errors;
- blob start executes exactly once on ambiguous failure;
- create/version finalization executes exactly once;
- a lost version response resolves from exact metadata without a second mutation;
- re-entry after a lost response reconciles before mutation;
- reconciliation requests `owner/slug/<numeric-version>` and never invokes Dataset download methods;
- mismatched current version, description, size, digest, or authority binding fails closed.

## Risks / remaining evidence

- No live Kaggle Dataset was mutated in this lane. Real-provider evidence remains an integration responsibility after the broker service imports these canonical contracts.
- Kaggle Dataset file-name preservation for broker-started nested relative names is validated against the generated request contract and fakes, not a live upload.
- The SDK dependency smoke resolved the currently permitted `kagglesdk` release under `kaggle==2.2.4`'s declared range; the repository continues to pin the public Kaggle package itself at `2.2.4`.
