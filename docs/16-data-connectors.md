# Data connector architecture

Status: `R1 PUSH INTAKE/SPOOL IMPLEMENTED / DEVSTAND CANARY BLOCKED`
Date: 2026-08-09
Related decision: ADR-0010
Contract: [`../schemas/data-connector-envelope.v1.schema.json`](../schemas/data-connector-envelope.v1.schema.json)

Implemented in R1: exact versioned push intake, explicit connector principal binding,
PostgreSQL landing/receipt/watermark/quarantine tables, one-transaction acceptance,
exact replay/conflict classification, a canonical daily-statistics committer with
same-transaction semantic outbox, durable restart-safe producer spool, bounded status
MCP read, and a live disposable PostgreSQL flow. Pull/artifact/trusted-landing adapters
and a deployed events-bot canary are not claimed by this status.

## 1. Purpose

Data connectors are the controlled boundary through which another system supplies
observations, statistics, discovered objects, files or change facts to `my-data-hub`.
They are not a second MCP and not a direct shortcut into shared canonical tables.

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
- non-secret trace metadata.

The transport request also carries authenticated principal and correlation ID. Those
values are server-attested in the receipt rather than trusted from payload fields.

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
| `integration.batch_event` | append-only transitions and diagnostic evidence |
| `integration.watermark` | last committed source cursor/period per product/partition |
| `integration.quarantine` | invalid, conflicting or semantically unresolved batches |
| `integration.receipt` | accepted/committed/rejected receipt returned to producer |

The exact schema names may change through an ADR before implementation. The ownership
rules may not: intake owns immutable source evidence; a normalizer/committer owns
canonical application.

## 6. Lifecycle

```text
received
→ authenticated
→ contract_validated
→ accepted
→ staged
→ normalized
→ canonical_committed
→ reconciled
```

Terminal alternatives:

```text
rejected_auth
rejected_contract
conflicting_replay
quarantined_semantic
expired_uncommitted
```

`accepted` means the platform has durably taken responsibility for the source batch. It
does not mean the data has already changed a canonical projection. The producer can
query the receipt to distinguish transport success from canonical application.

## 7. Idempotency and corrections

Recommended identities:

```text
acceptance uniqueness:
  (connector_id, idempotency_key)

content integrity:
  payload_sha256

periodic product uniqueness after normalization:
  (data_product, producer_partition, period_start, period_end, schema_version)
```

An exact replay returns the existing receipt. A replay with the same identity but a
different hash is quarantined as a conflict.

Corrections are append-only:

```text
new_batch.supersedes_batch_id = previous_batch_id
```

The canonical projection may point to the corrected observation, while both source
batches and the supersession chain remain queryable.

## 8. First real connector: events-bot daily statistics

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

## 9. Intake API contract

Provisional routes:

```text
POST /intake/v1/batches
GET  /intake/v1/batches/{batch_id}/receipt
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

## 10. MCP relationship

MCP provides operator visibility and control, not the producer transport itself.
Expected MCP tools after implementation:

- `connector.list`;
- `connector.get`;
- `connector.batch.list`;
- `connector.batch.get_receipt`;
- `connector.quarantine.list`;
- `connector.quarantine.resolve` with expected revision;
- `connector.replay_normalization` for an already accepted batch;
- `connector.pause` / `connector.resume` under an operator scope.

MCP must not fabricate a producer batch or rewrite accepted source bytes. A manual
operator correction is a new attested batch or a semantic resolution record.

## 11. Observability

Per connector/data product:

- last accepted and last committed timestamps;
- expected cadence and lateness;
- current spool depth reported by producer where available;
- accepted/duplicate/conflict/rejected/quarantined counts;
- bytes and records;
- normalization latency;
- watermark lag;
- oldest uncommitted batch;
- schema-version distribution;
- receipt delivery failures.

An incident is suspected when a daily connector is missing beyond its grace window,
spool age grows, hashes conflict, a new schema appears without a registered normalizer,
or accepted batches stop reaching canonical commit.

## 12. Mandatory tests

1. exact replay is a no-op and returns the same receipt;
2. same idempotency key with a different hash is rejected/quarantined;
3. producer outage spool survives restart;
4. platform outage causes retry, not loss or new identity;
5. payload limit and unknown-field rejection work;
6. unauthorized connector cannot submit for another connector ID;
7. late correction supersedes but does not erase history;
8. normalizer failure leaves accepted source evidence intact;
9. direct connector DB role cannot write canonical schemas;
10. one batch can be traced producer → receipt → canonical objects → reconciliation.
