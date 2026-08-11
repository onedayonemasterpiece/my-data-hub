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

Admission uses the append-only `my-data-hub-embedding-production-capabilities.v2` contract. The old v1 file remains historical, but is not accepted because its `verified_checkpoint_restore: true` and `mcp_hybrid_search: true` fields confused executable admission with terminal evidence. The two v2 observations have deliberately different roles:

- loopback control returns `control_executor` only when the exact repository stage runner is installed, the production runtime owns the single repository `KaggleProviderAdapter` pinned to `kaggle==2.2.4`, and an ACTIVE master has a current checkpoint;
- canonical MCP returns `mcp_observer` from its read-only ledger view and is forbidden to claim runner or provider-adapter availability;
- both observations bind the same master instance, run, attempt, epoch, canonical revision, current blogger checkpoint, and generated worker assets;
- that binding must equal the canonical revision and checkpoint in the validated FINAL-BLOGGER receipt.

Capability reads are non-mutating and do **not** assert that workers ran, vectors were imported, coverage is complete, a new checkpoint was verified, or cold restore/search succeeded. Only terminal request status followed by the closure receipt can prove those facts. A missing runner, unsafe/unavailable adapter, inactive/stale master, missing checkpoint, or differing control/MCP binding blocks before request creation. The POST endpoint repeats the admission checks so a race between preflight and acceptance cannot create a request. The command does not fall back to a second provider transport, a local database, generic SQL, or direct vector import.

## Required live stage contract

After both admission observations match, the command stores one deterministic request. Replaying the same request ID and exact request hash returns the existing durable state with `created: false`; reusing the ID with different metadata is a conflict. Replay remains available even if the original runtime has since stopped, because it does not create a second request or provider effect. A first request is accepted only against the still-current exact admission binding.

The live master derives all 266 compact search documents and jobs inside the ACTIVE master, creates exact private runtime inputs there, and launches the generated workers through the single adapter. The returned metadata must bind, for each model:

- a distinct task run and exact provider ref/kernel/version/source hash;
- private, complete, `orchestrator_protected` status;
- exact input Dataset version/package/jobs hashes;
- exact `embedding-result.json` artifact ID, run ID, artifact hash, and selective output tree hash;
- the matching transactional import receipt with 266 inserts, zero stale/failed rows, canonical revision, and checkpoint-required outbox identity.

The stage must report exactly 266/266 coverage in both pinned vector spaces, publish a verified checkpoint at the final canonical revision, and stop. The external command then requests an exact cold restore, proves the restored revision and 100% MCP coverage, and runs one bounded `bloggers.search` probe. Success requires `exact`, `fts`, `e5`, and `bge_m3` all completed, no unavailable retriever, a nonempty bounded result, and `complete: true`. The durable receipt contains only identities, counts, hashes, and state; it never contains the query text, documents, vectors, credentials, or search rows.

## Evidence boundary

Repository wiring and synthetic tests prove only the admission, request/replay, and fail-closed contract. They are not evidence of a real Kaggle worker run, vector coverage, checkpoint/restore, or hybrid-search result. The JSON files under `examples/embeddings/` are synthetic schema illustrations, not live evidence. A deployment without the executable production runtime or without matching current blogger prerequisite evidence continues to return a blocker and must not be described as ready or complete.
