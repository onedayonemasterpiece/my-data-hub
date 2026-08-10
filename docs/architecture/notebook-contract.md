# Notebook worker contract

This contract covers ordinary compute notebooks. The Kaggle master Notebook is governed
by ADR-0016 and the lease/fencing/DB-gate/checkpoint contract instead.

## Principle

A notebook is an ephemeral compute worker, not a database owner. It receives an immutable
input manifest and emits one immutable result envelope plus optional evidence files.

## Required input identity

- `run_id` and `workload`;
- `stage` and stage-contract version;
- canonical revision at dispatch time;
- ordered work item IDs;
- input artifact locations and SHA-256 hashes;
- model/prompt/policy identifiers;
- resource and output limits;
- callback/publish destination that cannot mutate canonical state directly.

## Required output

`schemas/notebook-result.v1.schema.json` defines:

- run/stage/result identity;
- input-manifest hash;
- producer code and model identity;
- status (`succeeded`, `partial`, `failed`);
- per-item results and explicit failures;
- metrics/provider usage;
- artifact manifest and hashes;
- no secret-bearing fields.

## Fail-closed rules

A worker must fail without processing when required input, schema or hash validation fails.
It must never silently substitute remembered state, “latest” model input or an empty dedupe
registry. A partial run names every item not completed.

## Separation of heavy stages

E5, BGE-M3/Qwen embedding and image/VLM workers stay separate unless measured evidence
proves one runtime is safe and cheaper. This preserves the existing Region Talk resource
boundary and makes failures attributable.

## Acceptance

The orchestrator accepts output only after:

1. schema validation;
2. exact run/work-item and input hash match;
3. artifact hash verification;
4. model/policy compatibility check;
5. idempotency/conflict check;
6. domain invariant validation inside the canonical transaction.
