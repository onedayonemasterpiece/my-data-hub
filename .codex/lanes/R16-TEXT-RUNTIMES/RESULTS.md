# R16-TEXT-RUNTIMES results

- Base SHA: `771d68cd7841b02524aeeaa25eb6db737b8f3124`
- Implementation head SHA: `ff1f04da06293efdc969f104116907b20cf21985`
- Worktree: `/home/dev/.codex/worktrees/my-data-hub/r16-text-runtimes`
- Branch: `lane/r16-text-runtimes`
- Live mutation/deployment: none

## Requirement closure

| ID | Status | Evidence |
|---|---|---|
| R16-01 exact E5 runtime | Partial / asset-blocked | Real offline `AutoModel`/`AutoTokenizer` implementation uses the repo pin `d128750597153bb5987e10b1c3493a34e5a4502a`, exact prefixes, mean pooling, 768 dimensions and L2. Production model and semantic-bank bytes are absent, so the registry prevents attachment. |
| R16-02 exact BGE-M3 runtime | Partial / asset-blocked | Real local-path `BGEM3FlagModel` implementation uses repo pin `5617a9f61b028005a4858fdac845db406aefb181`, dense-only output, 1024 dimensions and L2. Production model and semantic-bank bytes are absent, so the registry prevents attachment. |
| R16-03 offline/private assets | Done for contract; acquisition blocked | Complete canonical manifest builder and fail-closed verifier cover full file inventory, file hashes/sizes, model identity, semantic bank, source and exact installed dependency versions. No worker download path exists. Registry entries remain `external_assets_required`. |
| R16-04 typed 0028 input/output | Done | Runtime consumes `StageExecutionPayload` text input plus the private `runtime_pin`, and returns exact bounded `StageResultMetadata` metrics. Direct worker binds master/epoch from `StageWorkPayloadReceipt`; supervisor receives no payload. |
| R16-05 deterministic receipts | Done | Both stage fixtures execute twice with identical canonical result metadata and hashes; evidence scores/fingerprint use fixed six-decimal canonical JSON. |
| R16-06 source/model/pin attestation | Done | Tests deny raw pin tamper, validly re-hashed wrong asset identity, wrong epoch, model-file tamper, source hash mismatch, dependency mismatch and unmanifested symlink before encoding. Runtime uses the master receipt producer ID and emits the exact 0030 identity fields. |
| R16-07 truthful readiness | Done | Operations matrix explicitly reports both production stages unavailable. Candidate public Kaggle model sources are recorded only as unverified acquisition leads requiring central private smoke and full file/tokenizer/model proof. |

## Commands and evidence

All commands used the existing operational virtual environment with `PYTHONPATH=src:.` in
the isolated worktree.

- `python -m ruff check src/my_data_hub/workloads/region_talk/text_runtimes.py src/my_data_hub/workloads/region_talk/notebook_stages.py tests/region_talk/test_text_runtimes.py`
  - PASS: `All checks passed!`
- `python -m pytest -q tests/region_talk/test_text_runtimes.py tests/region_talk/test_stage_dispatch.py tests/region_talk/test_stage_execution.py`
  - PASS: `30 passed`
- `python -m compileall -q -f` on the two owned runtime modules and focused test
  - PASS
- `python scripts/validate_repository.py`
  - PASS: `4584` checks, zero errors and zero notes
- `uv build --wheel --out-dir /tmp/mdh-r16-dist-<timestamp>` followed by `unzip -l`
  - PASS: the built wheel contains
    `my_data_hub/workloads/region_talk/assets/text-runtime-assets.v1.json`
- `git diff --check`
  - PASS

Per owner direction, this lane did not run the full repository pytest suite; root owns the
final full gate after all concurrent lanes integrate.

## Changed files

- `src/my_data_hub/workloads/region_talk/text_runtimes.py`
- `src/my_data_hub/workloads/region_talk/notebook_stages.py`
- `src/my_data_hub/workloads/region_talk/assets/__init__.py`
- `src/my_data_hub/workloads/region_talk/assets/text-runtime-assets.v1.json`
- `tests/region_talk/test_text_runtimes.py`
- `docs/operations/region-talk-text-runtimes.md`
- `.codex/lanes/R16-TEXT-RUNTIMES/RESULTS.md`

## Remaining blockers and risks

1. Neither exact pinned model snapshot is present or independently proven against the named
   upstream commit. Candidate Kaggle sources are not accepted assets.
2. The exact canonical semantic-bank file is absent. Only its required SHA-256 is known.
3. No live/private Kaggle model run or tokenizer-output equivalence receipt exists for these
   assets. Fixture encoders prove the runtime boundary and deterministic math, not model bytes.
4. A later reviewed asset-acquisition commit must build/verify both manifests, publish the
   bytes privately, and replace each registry null manifest hash. Until then discovery returns
   `None` and the worker truthfully remains `FAILED_RETRYABLE`.
5. 0030 registration is master/control-owned and was not called or hardcoded in this lane.
   R16 only exposes the frozen runtime-owned metadata and validates the exact private receipt.
