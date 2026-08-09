# Observability and evidence

## Three levels

### Service health

- DB connectivity/migration revision;
- orchestrator heartbeat;
- due/leased/expired task counts;
- outbox age;
- artifact store read/write probe;
- backup age/readback status.

### Pipeline health

Per run/stage:

- queued/launched/succeeded/failed/blocked;
- attempt counts and latency;
- provider/resource usage;
- zero-result reason;
- exact input/output hashes;
- code/model/contract versions.

### Product funnel: Region Talk

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

North Star and quality metrics remain separate from infrastructure green
status.

## Queue diagnostics

- ordered/unordered sources;
- duplicate queue sequence;
- actionable cached / needs resolve / cooldown / retry / terminal;
- p50/p95 queue age;
- head blocked key/reason/age;
- exact pending and p95 latency;
- lane quota consumption.

## Reproducibility key

Любой result должен быть восстанавливаем по:

```text
{git_sha, pipeline_version, run_id, task_id, input_hash, result_hash}
```

## Structured events

Log line — JSON object с timestamp, level, event_name, correlation_id,
run/task/workload/project IDs. Полные payloads и secrets не логируются.

## Retention

- compact DB events: durable initially;
- verbose run bundles: artifact retention policy;
- failed migration/shadow evidence: не удалять до accepted cutover + rollback;
- monthly random archive readback;
- restore drill before declaring backup operational.
