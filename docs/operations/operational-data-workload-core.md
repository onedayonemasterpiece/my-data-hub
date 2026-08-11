# Operational data-workload core (FM16–FM19, FM21)

`my_data_hub.acceptance.data_workloads` is a metadata-only, resumable orchestration
contract for a later exact-source Kaggle acceptance notebook. It cannot produce a
live `PASS`: its only successful terminal result is `EVIDENCE_READY` with
`live_evidence=false`. The eventual evidence runner must independently validate and
sign live claims.

## Fixed flow

1. **FM16:** persist the deterministic H5 v1 request identity, submit it, and require
   lossless accounting plus an explicit duplicate quarantine. Persist the review
   hashes and stop at `AWAITING_OWNER_AUTHORIZATION`. Only an authorization envelope
   bound to that exact batch, request, operation, identity set, record set, and every
   duplicate group may start v2. V2 must end with 266 durable dispositions, at least
   one `deduplicated` disposition, no pending/quarantined rows, and a verified
   checkpoint. Actor/account counts are observed metadata, never hard-coded equality.
2. **FM17:** capture the complete migration accounting hash, persist a deterministic
   restore operation identity, cold-restore the exact checkpoint, and require a new
   master instance/run and higher epoch. The post-restore accounting object and
   canonical revision must equal the pre-restore values exactly.
3. **FM18/FM19:** persist and submit one H3 request covering the exact pinned E5 and
   BGE-M3 worker assets. One terminal checkpoint is split into model-specific hashed
   evidence; task identities must be distinct and each model must account for all 266
   documents with no failed/stale items.
4. **FM21:** use only the fixed `fm21_hub_project_fixture.v1` adapter contract. Require
   empty insert preview, persist its operation ID before apply, await the post-insert
   verified checkpoint, require a one-row delete preview, persist before delete,
   await its checkpoint, and finish with a zero-row delete preview proving cleanup.

Every accepted/replayed mutation is idempotent. An ambiguous mutation becomes typed
`FAIL` with its already-persisted identity; resume polls status rather than submitting
again. Cleanup failures remain resumable. Rejections, prerequisite drift, changed
restore accounting, and missing checkpoints never become evidence-ready.

## Boundary and persistence

Gateway implementations translate existing H1/H3/H5 control-plane receipts into the
protocol models. They must not put raw blogger rows, vectors, SQL, credentials, DSNs,
or signed bearer receipts in state. State contains UUIDs, counters, canonical
revisions, fixed model IDs, and SHA-256 digests only. Persist `DataWorkloadState` on
the Kaggle master side (or an approved metadata ledger), never in a devstand business
data store. The JSON schemas and examples are:

- `schemas/provider/operational-data-workload-state.v1.schema.json`
- `schemas/provider/operational-data-evidence.v1.schema.json`
- matching files under `examples/provider/`

Fake gateways are useful only for transition testing. Since the result type has no
`PASS` value and the evidence bundle fixes `live_evidence` to false, fake dependencies
cannot manufacture production acceptance.
