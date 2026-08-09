# Модель данных

## Shared catalog: материал один, проектов много

Канонические сущности общего назначения не копируются внутрь каждого
конвейера. Проекты, авторы/издания, внешние аккаунты, материалы и медиа
хранятся в схеме `hub`; Region Talk и будущие workloads добавляют к ним
собственные проекции и результаты.

```text
hub.project

hub.actor
  └─ hub.external_account

hub.content_item
  ├─ hub.content_identity
  ├─ hub.content_author → hub.actor
  ├─ hub.content_asset
  ├─ hub.project_content → hub.project
  └─ hub.provenance_event
```

Это исключает модель «по копии публикации на каждый проект». Один
`hub.content_item` может быть связан с несколькими проектами через
`hub.project_content`, а project-specific статус и metadata остаются на связи.

## Текущее состояние и история

`hub.content_item`, `hub.actor`, `hub.external_account` и другие core-таблицы
содержат текущую каноническую проекцию и монотонное поле `revision`. В первой
схеме проекта нет вымышленной универсальной таблицы revisions: история
решений и наблюдений сохраняется в специализированных append-only слоях:

- `hub.provenance_event` — происхождение, обнаружение и source evidence;
- `analysis.result` и `analysis.embedding` — immutable результаты моделей;
- `orchestration.work_item_event` — переходы работы очереди;
- `sync.command`, `sync.command_receipt`, `sync.audit_event` — команды,
  результаты и аудит;
- `region_talk.candidate_revision` — exact редакционная revision, к которой
  привязываются review и публикационные решения.

Изменение current projection не должно стирать evidence, на котором оно
основано.

## External identity и dedupe

Для материалов natural identity хранится отдельно:

```text
(namespace, normalized_value) → hub.content_item.content_id
```

в `hub.content_identity`. Для автора/издания внешняя учётная запись хранится в
`hub.external_account` с уникальными platform/external ID или normalized URL.
Дополнительные aliases находятся в `hub.entity_alias`; происхождение legacy
ключей при миграции фиксируется в `migration.legacy_identity_map`.

Примеры identity:

- Telegram channel ID или username;
- VK owner ID;
- canonical URL публикации;
- DOI или другой внешний идентификатор.

Дедупликация может свести несколько source identities к одному UUID, но исходные
ключи и evidence не удаляются.

## Content, FTS и media

`hub.content_item` хранит компактный текущий материал: тип, title, summary,
body excerpt, язык, URL, hash, время публикации и статус. Русский FTS формируется
как stored `tsvector` и индексируется GIN; trigram index поддерживает поиск по
названию.

`hub.content_asset` хранит упорядоченные изображения, видео, документы и другие
assets с URL, hash, размером, геометрией и статусом. Бинарные файлы не становятся
каноническими PostgreSQL rows: они остаются в artifact storage, а БД хранит
locators, checksums и evidence.

## Analysis и embeddings

`analysis.model` задаёт точную identity модели и encoder contract.
`analysis.result` дедуплицируется по content/project/model/result-kind,
policy/input/output fingerprints и является append-only.

`analysis.embedding` поддерживает 384, 768 и 1024 dimensions отдельными
`pgvector` columns с dimension-specific HNSW cosine indexes. Embedding — derived
projection: source content и model contract остаются достаточными для
пересчёта; vector не является единственным источником содержания.

## Orchestrator state

Канонические имена сущностей orchestration:

- `orchestration.pipeline` и `orchestration.pipeline_stage` — versioned registry;
- `orchestration.run` — один planning/execution cycle;
- `orchestration.stage_run` — состояние конкретной стадии в run;
- `orchestration.work_item` — durable lease/retry/idempotency queue;
- `orchestration.work_item_dependency` — зависимости между work items;
- `orchestration.work_item_event` — append-only trace переходов;
- `orchestration.worker_artifact` — immutable artifact metadata;
- `orchestration.worker_result_inbox` — проверяемый ingress notebook results;
- `orchestration.batch` — bounded operational batch accounting.

`queue_seq` в `orchestration.work_item` неизменяем; priority хранится отдельно.
Workers не меняют canonical state напрямую: они возвращают typed result envelope,
который валидируется и принимается reconciler-ом.

## Semantic commands, offline changesets и side effects

`sync.command` — ограниченная typed-команда с idempotency key, causal revision и
semantic payload. `sync.changeset_header` + `sync.changeset_operation` реализуют
transactional-outbox protocol для disconnected sessions; применение фиксируется
в `sync.applied_changeset`, конфликты — в `sync.conflict`, remap provisional IDs —
в `sync.id_remap`.

Любой внешний side effect создаётся через `sync.external_outbox`. Delivery
сохраняет exact idempotency key и receipt; прямой вызов провайдера до canonical
commit запрещён. `sync.checkpoint` хранит verified locators и hashes физических
или portable PostgreSQL checkpoints.

## Region Talk projection

`region_talk.source` — canonical source record. Неизменяемый admission order
хранится в `region_talk.source_work_projection.queue_seq`; priority и readiness
отделены от него. Explicit states различают cached, entity resolve, scan due,
cooldown, retry и terminal состояния.

Публикационный контур разделён на:

- `region_talk.post_intake`, text/image/source evidence и evaluations;
- `region_talk.publication_candidate` — current candidate projection;
- `region_talk.candidate_revision` — immutable copy + ordered media fingerprint;
- `region_talk.review_decision` — решение только по exact revision;
- `region_talk.publication_plan` и `publication_attempt` — план и idempotent
  delivery history.

Production publication остаётся выключенной отдельным configuration gate и не
включается самим фактом миграции.

## Migration landing

YDB rows сначала попадают без смысловой потери в `migration.raw_record` вместе с
batch/table/source PK, row kind, source timestamp, canonical JSON и SHA-256.
`migration.row_disposition` присваивает каждой строке один terminal класс:

```text
normalized | deduplicated | intentionally_excluded | retained_raw | quarantined
```

`migration.legacy_identity_map` связывает source key с новым target, а
`migration.reconciliation_run`/`finding` и `cutover_receipt` хранят проверку и
решение о переключении. Cutover возможен только когда raw count совпадает с
manifest и undispositioned rows равны нулю; необъяснённые active quarantines
блокируют переключение.

## Joplin boundary

Схема `joplin` хранит links, note revisions, cursors и conflicts для будущей
синхронизации. PostgreSQL не пишет напрямую во внутреннюю SQLite-базу Joplin:
integration идёт через Joplin Data API/плагин и semantic note deltas.

## Retention

По умолчанию:

- canonical metadata, evidence, commands, receipts и audit — durable;
- run/work events — durable с будущей partition/archive policy;
- raw migration landing — durable до принятой reconciliation и проверенного
  backup, затем policy-based archive;
- full HTML/media/model artifacts — отдельное artifact storage с retention class;
- credentials, Telegram sessions и private keys — никогда не попадают в БД,
  notebook output или repository artifacts.

## Planned integration plane

The infrastructure supplement reserves a future `integration` schema, to be introduced
only through append-only migrations and repository tests.

### Data connectors

```text
integration.connector
integration.data_product
integration.batch
integration.batch_payload
integration.batch_event
integration.watermark
integration.quarantine
integration.receipt
```

Acceptance evidence and canonical application are separate. Exact replay reuses the
receipt; a conflicting hash is quarantined. Corrections append a superseding batch.

### Provider resources and operations

```text
integration.provider_resource
integration.provider_operation
integration.provider_event
```

A resource records provider reference, privacy, origin and control class. Provider
mutation requires expected fingerprint, lease/fencing token, idempotency and receipt.
Names/prefixes do not authorize access.

### Operator evidence

The final schema/name will be chosen with the implementation ADR/migration, but it must
persist append-only:

- query/write request identity and principal;
- preview receipt and expiry;
- SQL/parameter fingerprints and approved targets;
- backup/revision/effect gates;
- apply/rollback outcome and affected identities/counts;
- immutable audit/commit receipt.

The MCP editor role cannot modify its own audit or protected gate tables.
