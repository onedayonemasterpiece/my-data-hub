# R21-TEXT-ASSET-ACQUISITION results

- Base SHA: `76e2b20f03d9f43090cbc27273d1bea4fd5731dd`
- Implementation/evidence head SHA: `e9f29f4934302e11d54c5435dae0692ef5d54b3d`
- Branch: `lane/r21-text-asset-acquisition`
- Isolated worktree: `/home/dev/.codex/worktrees/my-data-hub/r21-text-asset-acquisition`
- Live mutation: two disposable private Kaggle smokes only; both exact claimed Notebooks deleted
- Deployment/production data mutation: none

## Requirement closure

| ID | Status | Evidence |
|---|---|---|
| R21-01 exact official revision metadata | Done | Canonical official recursive/expanded Hugging Face tree metadata binds E5 `d128750...` (23 files) and BGE `5617a9...` (30 files), including every LFS SHA-256/Git blob OID. Asset receipt `a9bf9a773342bb1593801f34bdd8d230b44c4a934842deea0b444ad5371aae70`. |
| R21-02 no-secret exact-version Kaggle smoke | Done | One central KPA attached the exact five-part `model_sources`, offline/private/disposable/protected, hashed every mounted file, ran fixed tokenizer/dense output, emitted only bounded canonical metadata, and cleaned up. No Notebook Kaggle credential or devstand model-byte download. |
| R21-03 E5 candidate acquisition | Blocked, fail closed | Candidate has 9 versus 23 files and material weight/config/tokenizer differences. Registry remains `external_assets_required`; exact private-output fallback is not falsely represented as numeric because official `kernel_sources` accepts only an unversioned slug. |
| R21-04 BGE candidate acquisition | Done under explicit logical contract | Exact model source has all 29 logical runtime files byte-identical to upstream and executes normalized 1024d dense output. Only `imgs/.DS_Store` is excluded as non-runtime OS metadata. Manifest SHA `5230124c29168d9ebc5d14d997fa7ac194b0a9ccf043d55039cba94bc9a2c9e2`; exclusion tamper is rejected. |
| R21-05 semantic bank provenance | Done | AST-literal reconstruction from donor commit `d727cc4...`, blob `50a68c...`; 11 labels, 29 examples, 6,702 canonical logical bytes, exact SHA `4ec81e6ede79f3dae1bb366a06366e7197d960e1c04e124f77b3db12f2f1981f`. |
| R21-06 runtime admission and truth | Done for owned asset seam | BGE registry exposes only exact `yethukmutt/bge-m3/Transformers/m3/1`; discovery requires one mount, hashes the full 29-file inventory, packaged bank, manifest/source/dependencies before encoder import. E5 stays unavailable. Publication/notification false. |
| R21.2 exact HF private kernel output | Provider/cross-lane blocker | Official Kaggle metadata supports only `username/kernel-slug` for `kernel_sources`, not a numeric producer version. A unique frozen v1 producer plus central prelaunch readback and worker complete-tree hashing is defensible, but needs coordinated stage-launch KPA attachment/admission outside this asset-only lane. No builder was created or falsely registered. |

## Live receipts

### Attempt 1 — dependency assumption rejected

- Run: `zigomaro/mdh-region-talk-text-asset-smoke/1`
- Push effect: `a3f58cf2-4b14-5f32-8e90-3100ae818294`
- Failure: `fixed_output:bge_m3_embedding`, `ModuleNotFoundError`, bounded message SHA
  `b25e6d60a7ffd02ac9f55d518be9a484cc2fc3f6610c324157354ba5a2bd78ab`
- Cleanup effect: `adc58015-f44e-5599-b156-2341bf9e8af2`; cleanup receipt SHA
  `8662b5a7d03f6f68c1e0f6c556a5966ddb121a3390716bd768e2e776827c974a`
- Root cause was corrected without a runtime install/download: the smoke now uses the
  image-pinned Transformers implementation of BGE's documented normalized `[CLS]` dense formula.

### Attempt 2 — exact candidate decision

- Run: `zigomaro/mdh-region-talk-text-asset-smoke/1`
- Source commit: `56be900974da43391f0f779718608406d64f6628`
- Source SHA: `ca31a6c0513ab708091edb3b26a70040aadce1b4986b5c5d488b4677ad6e9b91`
- Observation SHA: `387e3bab80ef55349b77090108fb3f29840c11ef2eb1b3a21a370d8346c97390`
- Strict whole-tree receipt outcome: `MISMATCH`; receipt SHA
  `2d5a395a9d330b113a4b5e322f9e1ad01d48686b380a9a3de013a9d39c0c0a37`
- E5 fixed dense SHA: `9172ac2bc8bbd8d461022ebd92300cf3f7b64c9705818854d7cd22932ad00af6`
- BGE fixed dense SHA: `9d14431f4c203aa6243e2370ec8612e14e02a28740444be4042ccda38c3e0021`
- Cleanup effect: `fd954e24-bca7-5324-8a7e-7a851fbae76f`; cleanup receipt SHA
  `b848023d0934f58b0fb6ed8fb08399a6218c895686c86a89a1f8367e910f64d0`

## Gates

Using `/home/dev/.codex/worktrees/my-data-hub/operational-mvp/.venv/bin/python` with
`PYTHONPATH=src:.` where needed:

- Focused pytest: provider adapter, text acquisition/runtime, stage dispatch/execution — PASS,
  `70 passed`.
- Ruff on owned Python/tests — PASS.
- Focused forced `compileall` — PASS.
- `python scripts/validate_repository.py` — PASS, `4625` checks, zero errors/notes.
- `git diff --check` — PASS.
- Full repository pytest intentionally not run; root owns the final full gate across concurrent lanes.

## Changed files

- `src/my_data_hub/providers/kaggle/adapter.py`
- `scripts/provider/run_region_talk_text_asset_smoke.py`
- `scripts/provider/assets/region_talk_text_asset_smoke.py`
- `src/my_data_hub/workloads/region_talk/text_asset_acquisition.py`
- `src/my_data_hub/workloads/region_talk/text_runtimes.py`
- `src/my_data_hub/workloads/region_talk/assets/text-model-official-trees.v1.json`
- `src/my_data_hub/workloads/region_talk/assets/semantic-bank.v1.json`
- `src/my_data_hub/workloads/region_talk/assets/region-talk-bge-m3-assets.v1.json`
- `src/my_data_hub/workloads/region_talk/assets/text-runtime-assets.v1.json`
- `tests/provider/test_kaggle_adapter.py`
- `tests/region_talk/test_text_asset_acquisition.py`
- `tests/region_talk/test_text_runtimes.py`
- `docs/operations/region-talk-text-asset-acquisition.md`
- `docs/operations/region-talk-text-runtimes.md`
- `docs/operations/evidence/2026-08-20-r21-text-assets/*.json`
- `.codex/lanes/R21-TEXT-ASSET-ACQUISITION/RESULTS.md`

## Remaining risks/blockers

1. E5 exact bytes are not acquired; it must remain retryable/unavailable.
2. Stage-launch assembly must attach the registry's exact BGE model source and the existing
   reviewed offline dependency closure. Until attached, discovery fails closed before encoder import.
3. The live asset smoke exercised BGE's documented dense formula through Transformers; the exact
   FlagEmbedding 1.4.0 wheel/import is separately attested by the existing dependency smoke, but
   the combined FlagEmbedding+BGE candidate was not run in this lane.
4. An E5 kernel-output producer cannot be presented as numerically pinned in consumer metadata.
   Any later unique-slug fallback must verify central current-version/source immediately before
   launch and hash the complete worker mount before model import/runtime-pin acceptance.
