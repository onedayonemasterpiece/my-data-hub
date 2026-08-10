# Observability and evidence

## Service health

- DB connectivity, migration/canonical revision and connection budget;
- orchestrator heartbeat, scheduler/publication gates;
- due/leased/expired/quarantined work counts;
- outbox age;
- artifact store read/write probe;
- local/off-host backup age, hash/readback and restore-drill status;
- public MCP TLS/OAuth/liveness and profile state;
- internal ports/firewall exposure evidence.

## Data connector health

Per connector/data product:

- last accepted batch and acceptance age;
- expected cadence/lateness;
- producer spool depth/oldest batch where reported;
- accepted, duplicate, conflict, rejected and quarantined counts;
- bytes/records and schema-version distribution;
- acceptance receipt delivery failures;
- routing fan-out and unmatched/disabled consumer counts;
- last committed/reconciled, accepted-to-stage latency, watermark and quarantine per
  consumer/scope;
- oldest uncommitted consumer application;
- current pause/incident state per connector and consumer.

## Kaggle provider health

- inventory reconciliation age and pagination completeness;
- resource counts by control class/privacy/status;
- protected-resource policy violations;
- MCP-managed operation queue, lease and unknown outcome;
- notebook queued/running/completed/failed duration;
- dataset version/privacy/readback state;
- exchange package expiry/acknowledgement;
- backup/checkpoint freshness only, without exposing protected contents.

## MCP operator health

- calls/denials by principal/profile/tool/scope;
- query/write latency, timeout and truncation;
- preview created/expired/applied/rejected;
- affected row and impact tier distribution;
- protected-target denials;
- backup gate state at apply;
- revoked/expired/wrong-audience/Host/Origin failures;
- audit receipt completeness.

## Pipeline health

Per run/stage:

- queued/launched/succeeded/failed/blocked;
- attempt counts and latency;
- provider/resource usage;
- zero-result reason;
- exact input/output hashes;
- code/model/contract versions;
- stable logical pipeline and exact project-pipeline scope;
- relation/state/usage/policy anomalies kept separate from work execution status.

## Data scope and policy health

- active catalog objects and relations by object type/scope/relation kind;
- required data with missing or ambiguous scope lineage;
- namespaced-state counts plus normalized classes, writer conflicts and cross-scope overwrite
  attempts;
- usage events with unresolved run/stage/project-pipeline identity;
- active policy decisions by scope/outcome/reason;
- policy evaluations missing decisions, relationship evidence or a current input fingerprint;
- pending outbox intents whose allow receipt became stale/expired;
- Region Talk raw-without-batch-scope and normalized/deduplicated-target-without-relation
  counters.

## Product funnel: Region Talk

- discovered sources;
- accessed sources;
- exact KO hits;
- fetched posts;
- KO main-subject;
- T1/T2/T3 eligible;
- media available/all images scored/strong image;
- LLM confirmed;
- sent to review;
- human approved/revoked;
- planned/published.

North Star and quality metrics remain separate from infrastructure green status.

## Queue diagnostics

- ordered/unordered sources;
- duplicate queue sequence;
- actionable cached / needs resolve / cooldown / retry / terminal;
- p50/p95 queue age;
- head blocked key/reason/age;
- exact pending and p95 latency;
- lane quota consumption.

## Reproducibility keys

Worker result:

```text
{git_sha, pipeline_version, run_id, task_id, input_hash, result_hash}
```

Connector batch:

```text
{connector_id, data_product, batch_id, idempotency_key, schema_version, payload_sha256}
{consumer_id, application_id, target_scope_id, routing_registry_revision, application_status}
```

Object/policy decision:

```text
{object_id, object_revision, scope_id, relation/state revision, usage_event_id}
{policy_key, policy_version, evaluation_id, policy_input_fingerprint, effective_outcome}
```

Provider operation:

```text
{resource_ref, control_class, provider_fingerprint, operation_id, request_hash, receipt_hash}
```

Operator write:

```text
{principal, preview_receipt, sql_hash, parameter_hash, canonical_before, canonical_after}
```

## Structured events

A log line is JSON with timestamp, level, event name, correlation ID and relevant
run/task/batch/resource/project/pipeline/scope/object identities. Full payloads and SQL
parameters containing sensitive values, bearer tokens and secrets are not logged.

## Scheduled evidence

- pull-request CI: contracts/unit/ephemeral PostgreSQL;
- post-deploy: commit/revision/service/auth/synthetic connector;
- nightly: backup, connector, queue, MCP negative and Kaggle inventory;
- weekly: isolated restore and disposable Kaggle lifecycle;
- migration/cutover: protected manual workflow with machine-readable receipt.

## Retention

- compact DB events/audit: durable initially;
- verbose run/provider/test bundles: private artifact retention policy;
- failed connector/migration/shadow evidence: retain until resolution and rollback window;
- multiple backup generations plus scheduled readback;
- exchange package TTL and dependency-aware deletion;
- restore drill before declaring backup operational.
