# Data connector architecture

Status: `R1 PUSH INTAKE/SPOOL IMPLEMENTED / SAME-HOST CANARY PENDING`
Date: 2026-08-10
Related decisions: ADR-0010, ADR-0015
Contract: [`../schemas/data-connector-envelope.v1.schema.json`](../schemas/data-connector-envelope.v1.schema.json)

Implemented in R1: exact versioned push intake, explicit connector principal binding,
PostgreSQL landing/receipt/watermark/quarantine tables, one-transaction acceptance,
exact replay/conflict classification, a canonical daily-statistics committer with
same-transaction semantic outbox, durable restart-safe producer spool, bounded status
MCP read, and a live disposable PostgreSQL flow. Pull/artifact/trusted-landing adapters
and a deployed events-bot canary are not claimed by this status.
ADR-0015 multi-consumer application and scoped participation extensions remain an
accepted design pending append-only migrations and runtime implementation.

## 1. Purpose

Data connectors are the controlled boundary through which another system supplies
observations, statistics, discovered objects, files or change facts to `my-data-hub`.
They are not a second MCP and not a direct shortcut into shared canonical tables. One
accepted batch may feed several projects/pipelines through independently tracked
server-side consumers without copying or reaccepting the source payload.

The first expected real connector is a daily aggregate from `events-bot-new`, but the
architecture must also support:

- event/site analytics;
- source discovery results;
- publication/provider receipts;
- files and evidence bundles;
- scheduled external API imports;
- future Joplin bridge deltas;
- one-time migration/export packages.

## 2. Connector modes

### 2.1 Push connector — default for owned services

A producer creates a versioned batch, writes it to a durable local spool/outbox and
submits it to the HTTPS intake API. The service acknowledges acceptance before any
expensive downstream processing.

Use for bots, sites, backend services and desktop bridges that can initiate HTTPS.

### 2.2 Pull connector — for external APIs

The orchestrator stores schedule, cursor/watermark and credentials reference, then
claims a bounded pull task. The adapter fetches the external source and submits the
same internal envelope to intake/landing.

Use only where the source cannot push or where polling semantics are required.

### 2.3 Artifact handoff — for large batches and files

The envelope references an immutable private artifact with media type, byte length and
SHA-256. The intake service first records the manifest, then downloads/scans/validates
the artifact through a separate bounded worker.

Use for exports, document packages, images, code bundles and notebook results. The
artifact location is not canonical state and cannot replace a batch receipt.

### 2.4 Trusted database landing — exception

A co-located or private-network service may write into a dedicated `integration`
landing table or invoke a narrow stored procedure under its own PostgreSQL role.

This mode is allowed only when all of the following are true:

- the producer is operated in the same trust domain;
- network access is private;
- grants are limited to its own connector landing objects;
- the same envelope, idempotency and receipt semantics are preserved;
- it cannot write shared `hub`, `analysis`, `orchestration`, `region_talk` or publication
  tables directly.

Direct writes to canonical domain tables are prohibited because they bypass schema
versioning, provenance, quarantine and reconciliation.

## 3. Availability behavior

A producer does not need a separate preflight request asking whether the master database
is available. It submits the idempotent batch. The result is authoritative:

| Result | Producer behavior |
|---|---|
| `202 Accepted` | persist receipt; remove batch from spool only after receipt is durable |
| `200/201` replay receipt | same idempotency key already accepted; compare hash and finish |
| `409 Conflict` | same identity/key with different content; retain locally and alert |
| `422 Unprocessable Entity` | contract/schema failure; quarantine locally and alert |
| `429 Too Many Requests` | retry after server guidance |
| `502/503/504` or timeout | retain exact bytes and retry with backoff/jitter |
| authentication failure | stop automatic retry storm; alert and require credential repair |

The spool must survive producer restart. Retries reuse the same batch bytes,
idempotency key and hash. A producer must not generate a fresh identity after an
ambiguous timeout.

If the devstand itself is stopped, neither PostgreSQL nor the orchestrator can wake
itself. The connector therefore waits in its spool. A future independent Yandex Cloud
availability controller may start the host, but it is not part of connector correctness.

### 3.1 Optional external wake controller

If the devstand is intentionally allowed to sleep, add a small control-plane component
outside that host, for example an authenticated Yandex API Gateway/Function or another
always-available service. It may:

1. observe an unavailable intake/health endpoint;
2. authenticate the producer or operator request;
3. start only the approved VM through a narrowly scoped service account;
4. return `202 starting` plus retry guidance;
5. never accept canonical payloads or hold database credentials;
6. rate-limit starts and record an audit event.

The producer still retains and retries its exact batch. The wake controller does not
redirect writes to Kaggle and does not become a second orchestrator.

## 4. Versioned envelope

Every batch uses
[`data-connector-envelope.v1.schema.json`](../schemas/data-connector-envelope.v1.schema.json)
and includes:

- `contract_version`;
- stable `connector_id` and `data_product`;
- UUID `batch_id`;
- producer-selected `idempotency_key`;
- payload `schema_version`;
- `produced_at` and observation period;
- source cursor/watermark where applicable;
- delivery mode and record count;
- exact payload/artifact SHA-256;
- either bounded inline records or one artifact reference;
- optional superseded batch and correction reason;
- non-secret trace metadata;
- optional producer-declared routing hints for diagnostics only.

The transport request also carries authenticated principal and correlation ID. Those
values are server-attested in the receipt rather than trusted from payload fields.
Project/pipeline scope is authoritative only when resolved from the server-side consumer
registry; a producer hint cannot grant membership, change policy or choose a platform scope.

Hash rule:

- inline mode: `payload_sha256` is SHA-256 of the RFC 8785 canonical UTF-8 JSON bytes of
  `inline_records`;
- artifact mode: `payload_sha256` equals SHA-256 of the exact artifact bytes and must
  match `artifact.sha256`;
- `record_count` must match inline item count or the validated artifact manifest.

The server retains/verifies exact accepted bytes or an exact immutable artifact reference,
not a re-serialized approximation.

## 5. Registry and storage model

The implementation should add an append-only migration for a dedicated connector
schema, provisionally named `integration`:

| Object | Purpose |
|---|---|
| `integration.connector` | stable registration, owner, principal, mode, policy and state |
| `integration.data_product` | schema versions, sensitivity and normalizer contract |
| `integration.batch` | immutable accepted envelope identity, hash and lifecycle |
| `integration.batch_payload` | bounded inline payload or artifact locator/hash |
| `integration.batch_event` | append-only acceptance transitions and diagnostic evidence |
| `integration.data_product_consumer` | data product → target scope, routing predicate and normalizer contract |
| `integration.batch_application` | independent lifecycle/receipt for one batch and one consumer |
| `integration.watermark` | last committed source cursor/period per product/consumer/partition |
| `integration.quarantine` | invalid, conflicting or semantically unresolved batches |
| `integration.receipt` | accepted/committed/rejected receipt returned to producer |

The exact schema names may change through an ADR before implementation. The ownership
rules may not: intake owns immutable source evidence; a normalizer/committer owns
canonical application. Connector acceptance and each consumer application have separate
idempotency identities and receipts.

## 6. Lifecycle

Batch acceptance:

```text
received → authenticated → contract_validated → accepted
```

Each matched consumer then has an independent application lifecycle:

```text
routed → staged → normalized → canonical_committed → reconciled
```

Batch terminal alternatives:

```text
rejected_auth | rejected_contract | conflicting_replay
```

Consumer application alternatives:

```text
paused | skipped_not_matched | quarantined_semantic | failed_retryable
failed_terminal | superseded | expired_uncommitted
```

`accepted` means the platform has durably taken responsibility for the source batch. It
does not mean the data has already changed a canonical projection. The producer can
query the receipt to distinguish transport success from canonical application. A failed or
paused optional consumer does not rewrite the accepted batch or another consumer status. A
consumer marked required may block a product-level reconciliation gate without changing
source acceptance.

## 7. Consumer routing and data scope

`connector_id` and `data_product` identify producer/contract, not project membership.
`integration.data_product_consumer` is a many-to-many server-side registry containing:

- exact `data_product` and supported schema versions;
- target `platform`, `project`, `pipeline` or `project_pipeline` scope;
- normalizer contract/version and routing predicate;
- required/optional behavior and lifecycle status;
- allowed sensitivity/retention class.

After acceptance, routing is evaluated against the immutable envelope and the exact consumer
registry revision. One `batch_application` is created per matched consumer. Its unique
identity is `(batch_id, consumer_id, consumer_contract_version)`. The application records
exact input hash, scope, normalizer, routing-registry revision, application reason, canonical
revision, target references and receipt.

A canonical object created or resolved by an application receives only the relations/states
required by that consumer. If two consumers deduplicate to one object, both scope relations and
usage histories are preserved. Mere batch acceptance does not create project membership. Adding a consumer later does not
retroactively route historical batches: an operator must start a bounded, idempotent backfill
that records its reason and exact consumer/registry revision.

See [`22-data-scope-and-pipeline-participation.md`](22-data-scope-and-pipeline-participation.md).

## 8. Idempotency and corrections

Recommended identities:

```text
acceptance uniqueness:
  (connector_id, idempotency_key)

content integrity:
  payload_sha256

consumer application uniqueness:
  (batch_id, consumer_id, consumer_contract_version)

periodic product uniqueness after normalization:
  (data_product, consumer_id, producer_partition, period_start, period_end, schema_version)
```

An exact replay returns the existing receipt. A replay with the same identity but a
different hash is quarantined as a conflict.

Corrections are append-only:

```text
new_batch.supersedes_batch_id = previous_batch_id
```

The canonical projection may point to the corrected observation, while both source
batches, every consumer application and the supersession chain remain queryable. A correction
is routed again under an attested registry revision; it does not silently mutate prior
application receipts.

## 9. First real connector: events-bot daily statistics

Suggested product identity:

```text
events-bot.daily-statistics.v1
```

Initial payload should be deliberately small and non-sensitive. Example fields:

- local reporting date and timezone;
- total events added;
- counts by city;
- counts by event type/category;
- ingestion/provider run count;
- rejected/deferred/error counts;
- source system revision;
- optional quality counters already computed by the bot.

Do not send individual user identities, access tokens, message bodies or a database
dump for the first connector.

The bot flow:

1. at the end of the reporting period, compute one deterministic aggregate;
2. serialize canonical JSON and calculate SHA-256;
3. write envelope bytes to a local durable outbox;
4. POST the same envelope until a valid receipt is stored;
5. retain a bounded local history for reconciliation;
6. expose its own pending/oldest-spooled/last-receipt health metrics.

A sample envelope is available at
[`../examples/contracts/data-connector-envelope.v1.example.json`](../examples/contracts/data-connector-envelope.v1.example.json).

## 10. Intake API contract

Provisional routes:

```text
POST /intake/v1/batches
GET  /intake/v1/batches/{batch_id}/receipt
GET  /intake/v1/batches/{batch_id}/applications
GET  /intake/v1/batches/{batch_id}/applications/{consumer_id}
GET  /intake/v1/connectors/{connector_id}/health
```

Large artifact upload may later add a server-issued upload session, but an artifact
must still be finalized through `POST /intake/v1/batches`.

The API must enforce:

- service-to-service identity separate from MCP OAuth user identity;
- exact connector/principal binding;
- content type and compressed/uncompressed size limits;
- JSON Schema and unknown-field rejection;
- hash verification before acceptance;
- per-connector rate/concurrency budgets;
- no redirects to arbitrary artifact hosts;
- no credentials or secrets in envelope/logs;
- no synchronous provider/model work on the intake request.

## 11. MCP relationship

MCP provides operator visibility and control, not the producer transport itself.
Expected MCP tools after implementation:

- `connector.list`;
- `connector.get`;
- `connector.batch.list`;
- `connector.batch.get_receipt`;
- `connector.batch.list_applications`;
- `connector.batch.get_application`;
- `connector.consumer.list`;
- `connector.quarantine.list`;
- `connector.quarantine.resolve` with expected revision;
- `connector.replay_normalization` for an already accepted batch;
- `connector.pause` / `connector.resume` under an operator scope.

MCP must not fabricate a producer batch or rewrite accepted source bytes. A manual
operator correction is a new attested batch or a semantic resolution record.

## 12. Observability

Per connector/data product:

- last accepted timestamp and per-consumer last committed/reconciled timestamps;
- expected cadence and lateness;
- current spool depth reported by producer where available;
- accepted/duplicate/conflict/rejected/quarantined counts;
- bytes and records;
- routing fan-out and unmatched/disabled consumer counts;
- normalization latency per consumer/scope;
- watermark lag per consumer;
- oldest uncommitted application;
- schema-version distribution;
- receipt delivery failures.

An incident is suspected when a daily connector is missing beyond its grace window,
spool age grows, hashes conflict, a new schema appears without a registered normalizer,
or accepted batches stop reaching required consumer reconciliation. A batch may be healthy
for one consumer and incident-suspect for another; alerts must name the consumer and scope.

## 13. Mandatory tests

1. exact replay is a no-op and returns the same receipt;
2. same idempotency key with a different hash is rejected/quarantined;
3. producer outage spool survives restart;
4. platform outage causes retry, not loss or new identity;
5. payload limit and unknown-field rejection work;
6. unauthorized connector cannot submit for another connector ID;
7. late correction supersedes but does not erase history;
8. normalizer failure leaves accepted source evidence intact;
9. direct connector DB role cannot write canonical schemas;
10. one batch can be traced producer → acceptance receipt → each consumer application →
    scoped canonical objects → reconciliation;
11. one batch routes to two consumers with independent statuses and receipts;
12. failure/paused state of one optional consumer does not mutate another application;
13. exact replay does not duplicate consumer applications;
14. producer routing hint cannot assign a project/pipeline or override registry routing;
15. two consumers deduplicating to one object preserve both scope relations;
16. accepted batch alone does not imply project membership or policy approval;
17. adding a consumer does not silently create applications for historical batches; an
    explicit replay/backfill creates idempotent, attributed applications only.
