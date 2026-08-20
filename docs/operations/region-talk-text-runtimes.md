# Region Talk offline text runtimes

## Current executable matrix

| Stage | Exact model | Runtime implementation | Reviewed model/bank bundle | Current result |
|---|---|---|---|---|
| `e5_embedding` | `intfloat/multilingual-e5-base@d128750597153bb5987e10b1c3493a34e5a4502a` | attached, local-files-only attention-mask mean pooling, L2 normalization | **model absent; bank verified** | `FAILED_RETRYABLE` (`HEAVY_RUNTIME_NOT_ATTACHED`) |
| `bge_m3_embedding` | `BAAI/bge-m3@5617a9f61b028005a4858fdac845db406aefb181` | attached, local-path FlagEmbedding dense-only, L2 normalization | **exact full model tree unproven; bank verified** | `FAILED_RETRYABLE` (`HEAVY_RUNTIME_NOT_ATTACHED`) |

The implementations are executable against deterministic fixture encoders, but neither
production stage is ready. The canonical `semantic_bank_v1` is now reconstructed and verified
at SHA-256 `4ec81e6ede79f3dae1bb366a06366e7197d960e1c04e124f77b3db12f2f1981f`,
but neither complete pinned model snapshot has passed the strict acquisition proof.
The committed registry therefore marks both entries `external_assets_required`, has no
manifest hash, and deliberately returns no attached runtime. This is an external asset
acquisition blocker, not evidence of a successful model execution.

A central private Kaggle smoke has now rejected the exact-version public candidates. E5
`tanviranjumapurbo/multilingual-e5-base/Transformers/default/1` has only 9 of the upstream
revision's 23 files and differs in material runtime files. BGE-M3
`yethukmutt/bge-m3/Transformers/m3/1` matches 29 of 30 upstream files but omits
`imgs/.DS_Store`; strict whole-tree equivalence therefore remains unproven. Both candidates
executed deterministic normalized dense output offline, but the receipt outcome is `MISMATCH`,
not readiness. See `region-talk-text-asset-acquisition.md`.

## Offline asset contract

`text_runtimes.build_text_runtime_asset_manifest` inventories an already acquired private
bundle. It never downloads a model. A valid bundle has:

- every regular model file listed once, sorted by relative path, with size and SHA-256;
- `config.json`, tokenizer data and the stage's exact safetensors or PyTorch weight file;
- the canonical, complete and self-receipted semantic bank;
- the exact stage/model revision, dimensions, prefixes, pooling and encoder contract;
- the exact installed distribution versions needed by that reviewed bundle; and
- the SHA-256 of the runtime source that will verify and execute it.

The verifier rejects non-canonical manifests, unexpected/missing/tampered files, symlinks,
path escape, semantic-bank mismatch, source mismatch and dependency-version mismatch before
the encoder is imported. E5 uses `local_files_only=True` with both Hugging Face offline flags.
BGE-M3 receives only the verified local model directory. There is no `snapshot_download`
or runtime network acquisition path.

To close the blocker, acquire each exact upstream snapshot in a reviewed central acquisition
flow, assemble the complete offline dependency closure,
build and independently verify each manifest, then commit the reviewed manifest SHA into
`text-runtime-assets.v1.json`. Publish the bytes only as a private Kaggle input. Do not change
an entry to `verified` until those exact bytes and hashes are reviewable.

## Master runtime pin and private 0028 binding

`TextRuntimeRegistrationMetadata` exposes only the runtime-owned registration fields:
stage, contract version, model ID/revision, encoder contract, semantic-bank version/hash,
runtime-source hash and asset-manifest hash. The master/control caller remains responsible
for the verified provider image identity, its source commit, ACTIVE epoch and effective
canonical revision.

The worker receives the resulting `region-talk-stage-runtime-pin-receipt.v1` only in the
private 0028 `input_data.runtime_pin`. The runtime verifies its receipt hash, pin hash,
server producer formula, generation chain shape, master/epoch capability, model, contract,
semantic bank, source, manifest and image identity before the first encoder call. Successful
metadata uses the receipt's `producer_exact_id` and repeats the image/source/pin hashes that
the master-owned result validator cross-checks. The supervisor sees no payload, token or
database URL. Publication and notification remain false.

## Determinism

For fixed typed text, semantic-bank bytes, model bytes, dependencies, runtime source and pin,
the scores are rounded to six decimal places and the evidence fingerprint is canonical JSON
SHA-256. Focused tests execute both stages twice with deterministic normalized fixture vectors
and prove identical typed result metadata hashes. These tests validate orchestration and
math contracts; they are not a substitute for a receipt from either absent production model.
