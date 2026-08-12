# L06-embeddings results

## Lane identity

- Lane: `L06-embeddings`
- Requirement support: `R09`, plus typed worker/benchmark receipt support for `R13`
- Base SHA: `74f3bf457040f078e42b489252642dcf352760d4`
- Implementation head before this evidence record: `251cb329d68a44297631a81970da3627b7789c04`
- Branch: `agent/operational-mvp/l06-embeddings`

## Outcome

Local implementation is complete for this lane's owned scope.

- E5 is pinned to `intfloat/multilingual-e5-base@d128750597153bb5987e10b1c3493a34e5a4502a`: 768 dimensions, `query: `/`passage: ` prefixes, attention-mask mean pooling, L2 normalization and maximum 512 tokens.
- BGE-M3 is pinned to `BAAI/bge-m3@5617a9f61b028005a4858fdac845db406aefb181`: 1024 dimensions, native dense-only output and L2 normalization. It uses a separate vector-space identity.
- Compact blogger documents normalize Unicode, whitespace and set-like fields before deterministic document, input and job hashing.
- Immutable Pydantic contracts validate artifact accounting, model/revision/space, dimensions, finite/nonzero vectors and L2 norm tolerance (`1e-3`). Worker receipts include timeout, retry and terminal-failure policy.
- The worker uses an injected `DenseEncoder`; fake encoders cover behavior without model downloads. Workers emit artifacts and have no YDB/PostgreSQL/provider mutation path.
- Pure replay planning covers insert, exact no-op, same-key conflict, replacement/current staleness, same-revision conflict and late stale-result rejection.
- Dense routing rejects E5-query/BGE-index and BGE-query/E5-index cross-space requests.
- RRF accepts ranks only, rejects raw-score fields, accumulates exact fractions, applies stable document-ID ties and requires every requested retriever to be completed or explicitly unavailable.
- HNSW is disabled by default and requires a matching observed benchmark receipt. The example is explicitly `EXAMPLE-NOT-OBSERVED` with `capacity_proven=false`; it is not evidence.
- Separate deterministic primary sources exist for E5 and BGE-M3 worker notebooks. Timestamps and encoder implementations are explicit inputs.

## Donor adaptation evidence

Read-only donor: `/home/dev/projects/events-bot-new-region-talk-runtime-20260801` at `416d17e689acf0a4f69f2b4d1db5dad5b46c4bca`.

| Source | Git blob | Retained | Removed |
|---|---|---|---|
| `kaggle/RegionTalkCandidateReport/region_talk_candidate_report.py` | `179031c51c3fd5d75092e88acd10a5eb9c74d1f1` | E5 prefixes, masked-mean pool, max 512, L2, compact input | business logic, dynamic install, YDB I/O, lifecycle coupling |
| `kaggle/RegionTalkBgeM3Enrichment/region_talk_bge_m3_enrichment.py` | `0598777af0c91d9d716817666cfc45c81cda642c` | isolated runtime, dense-only flags, L2 | YDB persistence, secret discovery, dynamic install, provider transport |
| `tests/test_region_talk_bge_m3_enrichment.py` | `45be3a9d8b4e59ff2484981c6a33ea7428df4a58` | validation expectations as discovery evidence | donor persistence assertions |

The owner-assigned contract supplies the exact Hugging Face revisions; the donor source did not prove them.

## Validation evidence

Commands run:

```text
uv run --extra dev python -m compileall -q src tests notebooks/templates/embedding_workers
uv run --extra dev pytest -q tests/embeddings
uv run --extra dev pytest -q
uv run --extra dev ruff check .
uv run --extra dev python scripts/validate_repository.py
```

Observed results:

- Embedding tests: `20 passed`.
- Full pytest: all tests passed, no failures.
- Ruff: `All checks passed!`.
- Repository validator: `2456` checks, `0` errors, `0` notes, `ok: true`.
- Compileall and `git diff --check`: passed.

No model download, real Kaggle mutation, production-data read, canonical write or benchmark was performed.

## Exact integration requests

### L03 PostgreSQL migrations

Add append-only migrations for:

1. A registry keyed by model key, exact revision and encoder contract, including dimensions, max tokens, pooling/prefix/normalization, vector space and lifecycle status.
2. Separate 768/1024 pgvector or halfvec storage whose constraints prevent cross-space imports/search.
3. Search-document and job identity matching `document_id + representation_kind + exact model/revision + input_hash`.
4. Immutable landing/import that validates the manifest before a single transaction: exact replay returns the receipt, same key/different payload quarantines, a newer revision stales the previous current vector and a late older result stays stale.
5. Coverage/terminal-exception and active-index metadata. HNSW remains off unless an accepted `embedding-search-benchmark-receipt.v1` matches the bounded space.

### Root notebook generator

Update the generator (not edited here) to consume:

- `notebooks/templates/embedding_workers/e5_worker.py` -> private `e5-blogger-embedding-worker`
- `notebooks/templates/embedding_workers/bge_m3_worker.py` -> private `bge-m3-blogger-embedding-worker`

Metadata must set canonical writes and external side effects false and record exact source SHA/model revision, input/output schemas, protected/private class, retry/timeout and retention. Add both to generator idempotence/`--check`.

### Notebook dependencies/runtime

Implement `DenseEncoder` adapters only in isolated notebook environments:

- E5: `torch`, `transformers` and tokenizer assets; enforce exact snapshot, prefixes, masked mean, max 512 and L2.
- BGE: `torch` plus `FlagEmbedding` (or shadow-equivalent dense adapter); enforce exact snapshot, dense true, sparse/ColBERT false and L2.

Pin package versions and image digest only after a real canary. The donor dynamically installed unpinned packages, so version claims here would be false evidence. Record the proven lock/image digest in model and benchmark receipts.

### Provider/R13

Run both generated workers through the single Kaggle adapter, record real run/source/input/output identities, import via the master transaction, checkpoint/restore and add real IDs to the R13 ledger. This lane does not claim corpus coverage, Recall@K, latency, capacity, checkpoint size or a real Kaggle completion.

## Risks

- No production encoder adapter or notebook dependency/image lock exists yet.
- No real coverage, exception accounting, quality, capacity or cold-restore evidence exists.
- Shared generator and PostgreSQL migrations must integrate the requests above.

## Changed files

```text
examples/embeddings/*
notebooks/templates/embedding_workers/*
schemas/embeddings/*
src/my_data_hub/embeddings/*
tests/embeddings/*
.codex/lanes/L06-embeddings/RESULTS.md
```
