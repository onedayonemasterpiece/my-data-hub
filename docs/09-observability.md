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

- last accepted and last canonical-committed batch;
- expected cadence/lateness and watermark lag;
- producer spool depth/oldest batch where reported;
- accepted, duplicate, conflict, rejected and quarantined counts;
- bytes/records and schema-version distribution;
- accepted-to-committed latency;
- receipt delivery failures;
- current pause/incident state.

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
- code/model/contract versions.

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
run/task/batch/resource/project identities. Full payloads, SQL parameters containing
sensitive values, bearer tokens and secrets are not logged.

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
