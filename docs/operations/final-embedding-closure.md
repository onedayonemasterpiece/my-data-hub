# FINAL-EMBED production closure

FINAL-EMBED is an opt-in postcondition gate that starts only after a validated `my-data-hub-blogger-closure.v1` receipt proves the 266-row blogger import, verified checkpoint, and cold restore. It does not replace FINAL-BLOGGER and it never moves canonical rows or vectors through the devstand.

## Command and safety order

```bash
python scripts/embeddings/run_final_embedding_closure.py run \
  --idempotency-key final-embed-YYYYMMDD \
  --source-revision "$EXACT_COMMIT_SHA" \
  --blogger-receipt artifacts/blogger-closure.json \
  --probe-query 'калининград культура' \
  --receipt artifacts/embedding-production-closure.json
```

The first decision is the modern Kaggle token preflight. Only `KAGGLE_API_TOKEN` or a regular, non-symlinked `access_token` under `KAGGLE_CONFIG_DIR` qualifies; legacy `kaggle.json` does not. A missing token exits 78 before reading the prerequisite or contacting control/MCP. A missing prerequisite, missing MCP credential, or absent/mismatched live production capability also exits 78 before `create_request`, which is the first operation allowed to start provider/master mutation.

Both loopback control and canonical MCP must return the exact `my-data-hub-embedding-production-capabilities.v1` contract. It requires:

- execution inside the ACTIVE Kaggle master, not on the devstand;
- the single repository `KaggleProviderAdapter` pinned to `kaggle==2.2.4`;
- the generated private `orchestrator_protected` E5 and BGE-M3 workers with their exact model revisions and primary-source hashes;
- transactional import through `PostgresEmbeddingImporter` in the ACTIVE primary;
- verified checkpoint/restore and a vector-enabled MCP hybrid-search implementation.

Capability reads are non-mutating. The command does not fall back to a second provider transport, a local database, generic SQL, or direct vector import.

## Required live stage contract

After both capabilities match, the command stores one deterministic request. The future/live master implementation must derive all 266 compact search documents and jobs inside the ACTIVE master, create exact private runtime inputs there, and launch the existing generated workers through the single adapter. The returned metadata must bind, for each model:

- a distinct task run and exact provider ref/kernel/version/source hash;
- private, complete, `orchestrator_protected` status;
- exact input Dataset version/package/jobs hashes;
- exact `embedding-result.json` artifact ID, run ID, artifact hash, and selective output tree hash;
- the matching transactional import receipt with 266 inserts, zero stale/failed rows, canonical revision, and checkpoint-required outbox identity.

The stage must report exactly 266/266 coverage in both pinned vector spaces, publish a verified checkpoint at the final canonical revision, and stop. The external command then requests an exact cold restore, proves the restored revision and 100% MCP coverage, and runs one bounded `bloggers.search` probe. Success requires `exact`, `fts`, `e5`, and `bge_m3` all completed, no unavailable retriever, a nonempty bounded result, and `complete: true`. The durable receipt contains only identities, counts, hashes, and state; it never contains the query text, documents, vectors, credentials, or search rows.

## Current integration blocker

At integration base `d79ce65`, the generated workers, pinned model contracts, and transactional importer exist, but the required live interfaces do not:

- control has no `/control/v1/embedding-production/capabilities` or request/status API;
- MCP has no `embedding.production.capabilities` tool;
- the current control `embedding.coverage` adapter returns zero/ABSENT scaffold;
- the PostgreSQL MCP `bloggers.search` implementation hardcodes E5/BGE-M3 as unavailable.

Therefore the production command intentionally exits 78 before mutation on the current deployment. This lane does not claim a real run, 100% observed coverage, checkpoint/restore, or hybrid search. Integration must implement the capability and stage contracts inside the master/control and vector-aware bounded MCP broker; it must not weaken the command to accept the scaffold. The JSON files under `examples/embeddings/` are synthetic schema illustrations, not live evidence.
