# Region Talk exact text-asset acquisition

## Decision

The 2026-08-20 central Kaggle smoke rejected E5 and initially reported strict whole-tree
`MISMATCH` for BGE-M3. A subsequent reviewed logical-model contract excludes exactly the
upstream OS metadata file `imgs/.DS_Store`; all 29 model, tokenizer, configuration,
documentation and image files delivered by the exact BGE model source match the pinned
revision. BGE-M3 is therefore registered as `verified`; E5 remains
`external_assets_required`.

| Stage | Required upstream revision | Exact Kaggle model source observed | Central result |
|---|---|---|---|
| `e5_embedding` | `intfloat/multilingual-e5-base@d128750597153bb5987e10b1c3493a34e5a4502a` | `tanviranjumapurbo/multilingual-e5-base/Transformers/default/1` | **Mismatch**: 9 observed files versus 23 official-tree files, plus differing config, tokenizer and weights. |
| `bge_m3_embedding` | `BAAI/bge-m3@5617a9f61b028005a4858fdac845db406aefb181` | `yethukmutt/bge-m3/Transformers/m3/1` | **Verified logical model**: 29/29 required files match; only non-runtime OS metadata `imgs/.DS_Store` is explicitly excluded. |

The BGE candidate executed its 1024-dimensional normalized dense output and all 29 attached
files match their exact upstream byte identities. The committed manifest binds this finite
logical-file contract, the exclusion, fixed provider source, official-tree receipt, semantic
bank, runtime source and dependency versions. Discovery hashes the complete attached model
before constructing `BGEM3FlagModel`. This approves the asset, not the full stage lifecycle:
the launcher must attach the exact model source and register the corresponding runtime pin.

## Evidence and method

The repository pins the complete recursive/expanded file metadata returned by the official
Hugging Face API for each exact commit in
`text-model-official-trees.v1.json`. LFS objects are compared by byte size and SHA-256;
ordinary Git blobs are compared by byte size and Git blob OID. Primary endpoints:

- `https://huggingface.co/api/models/intfloat/multilingual-e5-base/tree/d128750597153bb5987e10b1c3493a34e5a4502a?recursive=true&expand=true`
- `https://huggingface.co/api/models/BAAI/bge-m3/tree/5617a9f61b028005a4858fdac845db406aefb181?recursive=true&expand=true`

One central `KaggleProviderAdapter` submitted an offline, private, disposable,
orchestrator-protected Notebook with both exact five-part, version-pinned `model_sources`.
The worker rejects Kaggle credential environment/files, hashes every attached regular file,
uses `local_files_only=True`, and emits only a bounded canonical observation. It exercises E5
attention-mask mean pooling and BGE-M3's documented normalized `[CLS]` dense representation.
No model bytes were downloaded to the devstand and no Notebook network access or user secret
was enabled.

The canonical central artifacts are:

- `evidence/2026-08-20-r21-text-assets/kaggle-text-asset-observation.json`, SHA-256
  `387e3bab80ef55349b77090108fb3f29840c11ef2eb1b3a21a370d8346c97390`;
- `evidence/2026-08-20-r21-text-assets/kaggle-text-asset-acquisition-receipt.json`, receipt
  SHA-256 `2d5a395a9d330b113a4b5e322f9e1ad01d48686b380a9a3de013a9d39c0c0a37`;
- provider run `zigomaro/mdh-region-talk-text-asset-smoke/1`, exact source commit
  `56be900974da43391f0f779718608406d64f6628`, deleted after receipt;
- cleanup effect `fd954e24-bca7-5324-8a7e-7a851fbae76f`, receipt SHA-256
  `b848023d0934f58b0fb6ed8fb08399a6218c895686c86a89a1f8367e910f64d0`.

An earlier attempt failed at BGE fixed-output import because model-source attachment does not
install `FlagEmbedding`. Its bounded failure and cleanup receipt are recorded separately. The
corrected smoke uses the already image-pinned Transformers implementation of the documented
BGE dense formula; it performs no install or download.

## Semantic bank

`semantic-bank.v1.json` is reconstructed by AST-literal extraction from donor commit
`d727cc4e256f8018e86c571a531f7ff20b2056fc`, Git blob
`50a68cf33a3e587a3bdcd1668f0d56ccd8b556b6`. The canonical logical bank has 11 labels,
29 examples, 6,702 UTF-8 JSON bytes and SHA-256
`4ec81e6ede79f3dae1bb366a06366e7197d960e1c04e124f77b3db12f2f1981f`.
This closes semantic-bank provenance but does not override either failed model comparison.

## E5 private-output fallback assessment

The official Kaggle kernel metadata contract accepts `kernel_sources` only as
`username/kernel-slug`; it has no numeric version field. Central output download can bind
`username/kernel-slug/version`, but a consumer launch cannot name that immutable version.
Consequently this lane did not create an E5 builder and did not extend KPA with a falsely exact
numeric source contract.

A safe later design is possible only as a coordinated launcher/runtime change: create a unique,
orchestrator-protected builder frozen after version 1; centrally re-read its exact source and
current version immediately before launch; attach the unversioned unique slug; and have the
worker hash the complete mounted tree against the committed official manifest before importing
the tokenizer/model or accepting a runtime pin. A producer update races only to fail closed;
it must never be accepted as the named generation. That consumer attachment and admission seam
is outside this asset-registry lane, so E5 remains blocked rather than pretending the provider
offers numeric `kernel_sources`. Runtime downloads, unpinned references and devstand model-byte
copies remain forbidden.
