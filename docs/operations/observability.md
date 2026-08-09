# Observability

## Correlation identity

Every operation should be traceable by:

```text
request_id / trace_id
actor_id and client_id
command_id or work_item_id
run_id and stage_run_id
canonical revision
input fingerprint
artifact/result ID and hash
code/model/policy version
```

## Region Talk product funnel

At minimum report by run/cohort and discovery method:

```text
sources discovered -> sources accessed -> posts fetched -> KO main subject
-> E5 present -> BGE present -> text eligible -> media available
-> all images evaluated -> strong image -> final verifier confirmed
-> sent to review -> approved -> published
```

Every zero result has a classified reason: no new supply, waiting dependency, policy reject,
provider limit, transport failure, terminal media, operator backlog or system defect.

## Queue health

- inflow, completion and net backlog by stage;
- oldest actionable and p50/p95 age;
- leased/running/retry/quarantine/terminal counts;
- expired leases and duplicate result conflicts;
- exact URL lane latency;
- downstream blocked reasons.

## Database/runtime

- connections, transaction errors, lock waits and slow queries;
- table/index size, vacuum/analyze status and vector index health;
- outbox lag and failed dispatch attempts;
- backup age and last restore drill;
- MCP calls, auth denials, rate/concurrency/response-limit denials;
- notebook duration, peak resource, provider usage and artifact bytes.

Logs are structured and redacted. Large evidence bundles remain private artifacts; the public
repository receives only synthetic examples and aggregate receipts.
