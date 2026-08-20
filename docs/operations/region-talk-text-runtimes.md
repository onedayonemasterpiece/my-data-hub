# Region Talk offline text runtimes

## Current executable matrix

| Stage | Exact model | Runtime implementation | Reviewed model/bank bundle | Current result |
|---|---|---|---|---|
| `e5_embedding` | `intfloat/multilingual-e5-base@d128750597153bb5987e10b1c3493a34e5a4502a` | attached, local-files-only attention-mask mean pooling, L2 normalization | **verified frozen producer output + bank** | executable when the fenced kernel source and runtime pin are attached |
| `bge_m3_embedding` | `BAAI/bge-m3@5617a9f61b028005a4858fdac845db406aefb181` | attached, local-path FlagEmbedding dense-only, L2 normalization | **verified exact logical model source + bank** | executable when the exact model source and runtime pin are attached |

Both implementations are executable against deterministic fixture encoders and provider-mounted
model bytes, but this does not by itself declare the entire Region Talk production lifecycle ready.
The canonical `semantic_bank_v1` is reconstructed and verified
at SHA-256 `4ec81e6ede79f3dae1bb366a06366e7197d960e1c04e124f77b3db12f2f1981f`,
and both complete logical runtime snapshots have passed their acquisition proofs. The committed
registry marks both entries `verified` and binds each reviewed manifest hash and provider carrier.

A central private Kaggle smoke has now rejected the exact-version public candidates. E5
`tanviranjumapurbo/multilingual-e5-base/Transformers/default/1` still has only 9 of the upstream
revision's 23 files and remains rejected. A separate frozen, protected E5 producer downloaded the
exact public Hugging Face revision without Kaggle credentials, verified all 23 files provider-side,
and exposed its content-addressed output through `kernel_sources`. BGE-M3
`yethukmutt/bge-m3/Transformers/m3/1` matches 29 of 30 upstream files but omits
`imgs/.DS_Store`. The committed BGE manifest explicitly classifies only that OS metadata as
non-runtime and binds every remaining file. See `region-talk-text-asset-acquisition.md`.

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

The one producer-only `snapshot_download` path is isolated in the frozen private builder and pins
the exact 40-hex revision plus all official file paths. Before every E5 consumer launch, central
control re-reads the producer's current source hash, numeric source version, kernel ID, run ref and
terminal status. The worker then hashes the complete mount and semantic bank before importing the
model. BGE similarly receives an exact five-part numeric `model_sources` identity; central control
proves that provider version exists before launch, and response-loss reconciliation reads back the
exact attached model source. Drift in either carrier fails retryably rather than launching against
unreviewed bytes.

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
and prove identical typed result metadata hashes. A live disposable E5 consumer additionally
verified all 23 mounted files (5,322,810,412 bytes), loaded the pinned model offline and emitted a
768-dimensional fixed-output receipt. These proofs validate the runtime seam; final production
readiness still depends on the separately integrated master/runtime-pin lifecycle gates.
