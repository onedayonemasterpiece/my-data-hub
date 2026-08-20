# R20-HEAVY-CONTRACTS results

- Base SHA: `771d68cd7841b02524aeeaa25eb6db737b8f3124`
- Implementation HEAD/evidence SHA: `9b9f88780132ad5cd0a1a1715b90ed2f97cb4194`
- Effort/risk: high; security-sensitive evidence, model, and result admission contracts
- Scope: new Region Talk heavy-stage modules, schemas, offline asset manifest,
  donor shadow fixture/tests, and operations documentation only

## Requirement closure

| Requirement | Status | Evidence |
|---|---|---|
| Closed typed input/result contracts for image scoring, verifier, writer | Done | `heavy_contracts.py`; generated JSON schemas; all models frozen/extra-forbid |
| No arbitrary SQL/network/provider surface | Done | only narrow injected protocols in `heavy_runtimes.py`; no SQL string or provider construction |
| Exact candidate/fact/source/media/upstream provenance | Done | recomputed nested hashes, current-revision checks, typed upstream results/receipts, `validate_heavy_result_against_input` |
| Authoritative media acquisition/object authority | Done | exact frozen 0031 `region-talk-media-artifact-acquisition-receipt.v1`; receipt recomputation and per-item identity checks; reader authorizes receipt hash; mutable metadata flags alone are insufficient |
| SSRF/unhashed media rejection | Done | source URL policy, safe relative `object_ref`, exact source/artifact byte hashes and byte limits |
| Provider/model response binding | Done | scorer, VLM, verifier, writer, and critic response/request/model/producer fingerprints; model bundle equality |
| Publication/notification disabled | Done | literal false in inputs, acquisition receipts, results, asset manifest, and derived guard metrics |
| Deterministic donor logic | Done | reviewed legacy image thresholds with low-score abstention, verifier grounding, writer audit/max-one-rewrite; exact donor commit/path/file hashes documented |
| Migration 0030/0031 compatibility | Done at contract boundary | `heavy_dag_bridge.py` parses exact minimal work shapes including embedded 0031 receipt+hash and derives the exact SQL guard metric keys; evidence enrichment remains intentionally outside this lane |
| Offline dependency/model manifest | Done, honestly not ready | `heavy-runtime-assets.v1.json` disables downloads and records unresolved wheels, CLIP/LAION/NIMA files, and unversioned Gemini roles; `production_ready=false` |
| Donor shadow regression | Done | canonical sanitized fixture bound by SHA-256 in asset manifest; four donor review cases remain `needs_visual_review`, never fabricated terminal rejects |

## Commands and evidence

Passing final gates:

```text
python -m compileall -q src tests
ruff check src/my_data_hub/workloads/region_talk/heavy_*.py tests/region_talk/test_heavy_contracts.py
# All checks passed!

pytest -q tests/region_talk/test_heavy_contracts.py tests/test_cli_and_repository_contracts.py
# 22 passed

python scripts/validate_repository.py
# {"checks":4855,"errors":[],"notes":[],"ok":true}

git diff --check
# clean
```

Full-suite evidence:

```text
pytest
# 1657 passed, 4 skipped, 15 failed
```

All 15 failures are pre-existing provider-upload tests stopped by the actual host disk
reserve guard, outside this lane: `/dev/vda2` had only `258M` free while
`MIN_UPLOAD_DISK_RESERVE_BYTES=536870912` (512 MiB). Every failure is
`ProviderUploadError: provider upload staging disk reserve would be violated`. This lane
does not own provider upload limits/tests and did not weaken that invariant. The focused
suite and repository validator pass after the final 0031 alignment.

## Changed files

- `docs/operations/region-talk-heavy-runtimes.md`
- `schemas/region-talk-heavy-runtime-assets.v1.schema.json`
- `schemas/region-talk-heavy-stage-input.v1.schema.json`
- `schemas/region-talk-heavy-stage-result.v1.schema.json`
- `src/my_data_hub/workloads/region_talk/assets/__init__.py`
- `src/my_data_hub/workloads/region_talk/assets/heavy-runtime-assets.v1.json`
- `src/my_data_hub/workloads/region_talk/heavy_assets.py`
- `src/my_data_hub/workloads/region_talk/heavy_contracts.py`
- `src/my_data_hub/workloads/region_talk/heavy_dag_bridge.py`
- `src/my_data_hub/workloads/region_talk/heavy_runtimes.py`
- `tests/region_talk/fixtures/heavy_image_donor_shadow.v1.json`
- `tests/region_talk/test_heavy_contracts.py`
- `.codex/lanes/R20-HEAVY-CONTRACTS/RESULTS.md`

## Residual production blockers and integration notes

1. The offline asset manifest is intentionally `production_ready=false`: exact wheel
   closure, model/config/tokenizer hashes, LAION/NIMA weights, immutable Gemini revisions,
   and a live smoke receipt are absent. Required execution therefore fails retryably.
2. Root integration must order the R19 0031 acquisition-authority migration before wiring
   the R20 bridge. R20 matches the final frozen 0031 receipt and image `input_data` keys
   confirmed by the R19 owner.
3. Migration 0030/0031 produces a deliberately sparse work envelope. A root-owned private
   worker/reader must enrich it from authoritative master views/receipts into the richer
   closed R20 input and prove the same work fingerprint/revision. R20 does not fabricate
   this evidence or edit `notebook_stages.py`.
4. No live Google/model/media call was made. No production data, credential, or personal
   session was read or written.
