# Region Talk exact text-asset acquisition

## Decision

The first 2026-08-20 central Kaggle smoke rejected the public E5 candidate and initially reported
strict whole-tree `MISMATCH` for BGE-M3. A subsequent reviewed logical-model contract excludes exactly the
upstream OS metadata file `imgs/.DS_Store`; all 29 model, tokenizer, configuration,
documentation and image files delivered by the exact BGE model source match the pinned
revision. BGE-M3 is registered as `verified`. E5 is now also `verified` through a separate
frozen protected producer whose provider-side output matches the complete official 23-file tree.

| Stage | Required upstream revision | Exact Kaggle model source observed | Central result |
|---|---|---|---|
| `e5_embedding` | `intfloat/multilingual-e5-base@d128750597153bb5987e10b1c3493a34e5a4502a` | frozen producer `zigomaro/mdh-region-talk-e5-assets-v1/1` | **Verified**: exact 23/23 official files, exact canonical bank and live offline 768-dimensional consumer output. The earlier public model candidate remains rejected. |
| `bge_m3_embedding` | `BAAI/bge-m3@5617a9f61b028005a4858fdac845db406aefb181` | `yethukmutt/bge-m3/Transformers/m3/1` | **Verified logical model**: 29/29 required files match; only non-runtime OS metadata `imgs/.DS_Store` is explicitly excluded. |

The BGE candidate executed its 1024-dimensional normalized dense output and all 29 attached
files match their exact upstream byte identities. The committed manifest binds this finite
logical-file contract, the exclusion, fixed provider source, official-tree receipt, semantic
bank, runtime source and dependency versions. Discovery hashes the complete attached model
before constructing `BGEM3FlagModel`. This approves the asset, not the full stage lifecycle:
the launcher must attach the exact model source and register the corresponding runtime pin.
The concrete single-KPA launcher now does that and preflights the exact numeric Model version.

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

## E5 frozen private-output producer

The official Kaggle kernel metadata contract still accepts `kernel_sources` only as
`username/kernel-slug`; it has no numeric consumer version field. The safe fallback is therefore
implemented as one permanent private `ORCHESTRATOR_PROTECTED` producer frozen at version 1.
Before each consumer launch, central control reads the current source and verifies exact source
SHA-256 `345cbeba4f1deb143a3af571594e92d19c536458d157078a071b62b7804861fa`,
version `1`, kernel ID `131338450`, run ref and `COMPLETE` status against the committed metadata-only
authority SHA-256 `c874531f044d31ef6953387f103dd34c6f4674ed569cafbaaf7f97856881d931`.
Only then does the single KPA attach `zigomaro/mdh-region-talk-e5-assets-v1`.

The producer itself uses no provider or Hugging Face credential, pins the exact upstream commit,
and verifies all official paths, sizes and Git/LFS identities before emitting output. Every worker
locates one exact weight root, hashes the complete mounted 23-file tree and the semantic bank, and
only then imports Transformers with offline/local-only flags. Producer source/version/content drift
therefore yields `FAILED_RETRYABLE`; it cannot be accepted as the named generation.

Live provider evidence:

- frozen producer run `zigomaro/mdh-region-talk-e5-assets-v1/1`, source commit `258e166723a09da90d1d37cfc946d5f2d81476e3`, remains private and protected;
- producer receipt SHA-256 `47427d3086012093eac5718355a69c53e2583291e7a3e46e2b465d08ea2a2573` and exact 23-file inventory SHA-256 `b27d94353f1b60ac9817b4d4aa10fd9a38129f4d2404be835a000202f64026f4`;
- disposable consumer run `zigomaro/mdh-region-talk-e5-consumer-smoke/1`, source commit `a23cd63cdfc7ec82f787508e8ad40b074df3b35f`, receipt SHA-256 `d8248782d7c007e472a552e5a226d5c48b69eba46800494ae286619895a01d4f`;
- consumer verified 23 files / 5,322,810,412 bytes, produced 768 dimensions with fixed-output SHA-256 `054317752ee2e2343dda3051120aa82290cc6144bf5c21e8c64fb332d8c720bb`, and was deleted with cleanup receipt SHA-256 `adc3256079857308b83980cf3dbd292fb209478ba547d9e53132229a0bff1395`;
- post-cleanup readback found the consumer absent while the protected producer remained exact version 1 / kernel ID 131338450 / `COMPLETE`.

No model bytes were downloaded to the devstand. Runtime downloads, unpinned references and
provider credentials inside builder or worker remain forbidden.
