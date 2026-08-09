# Observability

## Correlation identity

Every operation should be traceable by relevant identities:

```text
request/trace/principal/client
command/work/batch/provider-operation/preview receipt
run and stage run
canonical before/after revision
input/payload/result/provider fingerprints
artifact and manifest hashes
code/model/policy/schema version
```

## Platform runtime

- PostgreSQL connections, transaction errors, lock waits, slow queries and disk;
- migration/canonical revision;
- API/orchestrator heartbeat and dangerous gate state;
- work/outbox lag and expired leases;
- public MCP TLS/OAuth availability and auth denials;
- internal port exposure drift;
- local/off-host backup and restore-drill age/status.

## Connectors

- accepted/duplicate/conflict/rejected/quarantined batches;
- expected cadence, last accepted/committed, watermark lag;
- producer spool depth/oldest pending where available;
- accepted-to-committed latency and schema-version drift;
- receipt delivery failures.

## Kaggle

- inventory reconciliation age/completeness;
- resources by privacy/control class/status;
- protected-resource authorization denials;
- MCP-managed provider operations, leases and unknown outcomes;
- exchange expiry/acknowledgement;
- backup/checkpoint status without exposing contents.

## MCP data operator

- calls/denials by principal/profile/scope/target;
- row/byte truncation and statement/lock timeouts;
- preview created/expired/applied/mismatch;
- impact tier and affected rows;
- backup gate used;
- protected-target attempts;
- complete audit/commit receipt.

## Region Talk funnel

```text
sources discovered -> sources accessed -> posts fetched -> KO main subject
-> E5 present -> BGE present -> text eligible -> media available
-> all images evaluated -> strong image -> final verifier confirmed
-> sent to review -> approved -> published
```

Every zero result has a classified reason: no new supply, waiting dependency, policy
reject, provider limit, transport failure, terminal media, operator backlog or defect.

## Queue health

- inflow, completion and net backlog by stage;
- oldest actionable and p50/p95 age;
- leased/running/retry/quarantine/terminal counts;
- expired leases and duplicate result conflicts;
- exact URL lane latency;
- downstream blocked reasons.

Logs are structured/redacted. Full payloads, bearer tokens, SQL sensitive parameters and
provider credentials are not logged. Large evidence stays in private artifacts; public
repository contains synthetic examples and aggregate receipts.
