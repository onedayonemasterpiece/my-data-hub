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
`hub.project_content`, а project-specific статус и metadata остаются на связи. Однако
`hub.project_content` является content-specific bootstrap relation, а не универсальной
моделью принадлежности всех shared objects.

## Scope, participation, state и policy

Текущий bootstrap решает multi-project reuse только для материалов через
`hub.project_content`. Это недостаточно для actors, accounts, assets и будущих типов, а
также не описывает независимые pipeline states. ADR-0015 принимает универсальный target:

```text
hub.catalog_object
hub.scope                         # platform/project/pipeline/project_pipeline
hub.relation_definition
hub.object_scope_relation
hub.object_scope_relation_event
hub.state_definition
hub.object_scope_state
hub.object_scope_state_event
hub.policy_definition
hub.policy_decision
hub.policy_evaluation
hub.policy_evaluation_decision

orchestration.pipeline_identity   # stable logical identity
orchestration.project_pipeline
orchestration.object_usage_event
orchestration.pipeline_object_summary
```

Физические имена вводятся новой append-only migration. До реализации они не считаются
runtime-возможностью.

Модель разделяет пять слоёв:

1. lifecycle самой сущности;
2. persistent relation с project/pipeline scope;
3. exact namespaced state в конкретном scope;
4. append-only факт usage в run/stage/work item;
5. versioned policy decision (`allow`, `deny`, `review_required`, `no_opinion`).

Exact state отображается на малый normalized class для общих отчётов, но normalized class
не является разрешением. `orchestration.work_item.status` остаётся только execution state.
Platform-wide hard deny/blacklist применяется ко всем applicable projects/pipelines и не
может быть снят local allow.

Один catalog object может иметь несколько relations и разные states одновременно. Merge
или dedupe объединяет aliases, provenance и все scope relations; он не выбирает «последний
проект» и не создаёт копию объекта.

Полный contract, scope-resolution rules и тесты:
[`22-data-scope-and-pipeline-participation.md`](22-data-scope-and-pipeline-participation.md).

## Текущее состояние и история

`hub.content_item`, `hub.actor`, `hub.external_account` и другие core-таблицы
содержат текущую каноническую проекцию и монотонное поле `revision`. В первой
схеме проекта нет вымышленной универсальной таблицы revisions: история
решений и наблюдений сохраняется в специализированных append-only слоях:

- `hub.provenance_event` — происхождение, обнаружение и source evidence;
- `analysis.result` и `analysis.embedding` — immutable результаты моделей;
- `orchestration.work_item_event` — переходы работы очереди;
- target `hub.object_scope_relation_event` — история открытия/закрытия/remap relations;
- target `hub.object_scope_state_event` — история namespaced scoped states;
- target `orchestration.object_usage_event` — факты использования объектов pipelines;
- target `hub.policy_decision` и `hub.policy_evaluation` — append-only policy history и
  воспроизводимые effective-decision receipts;
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
ключи и evidence не удаляются. Все project/pipeline relations объединяются как set union;
отсутствие relation после dedupe считается migration/data-integrity defect.

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
policy/input/output fingerprints и является append-only. Scope-neutral result может
переиспользоваться несколькими consumers; scope-sensitive result включает exact evaluation
scope в identity и не перезаписывает result другого pipeline.

`analysis.embedding` поддерживает 384, 768 и 1024 dimensions отдельными
`pgvector` columns с dimension-specific HNSW cosine indexes. Embedding — derived
projection: source content и model contract остаются достаточными для
пересчёта; vector не является единственным источником содержания.

## Orchestrator state

Канонические имена сущностей orchestration:

- `orchestration.pipeline` и `orchestration.pipeline_stage` — versioned registry;
- target `orchestration.pipeline_identity` — stable logical pipeline identity;
- target `orchestration.project_pipeline` — many-to-many project assignment;
- `orchestration.run` — один planning/execution cycle;
- `orchestration.stage_run` — состояние конкретной стадии в run;
- `orchestration.work_item` — durable lease/retry/idempotency queue;
- `orchestration.work_item_dependency` — зависимости между work items;
- `orchestration.work_item_event` — append-only trace переходов;
- `orchestration.worker_artifact` — immutable artifact metadata;
- `orchestration.worker_result_inbox` — проверяемый ingress notebook results;
- `orchestration.batch` — bounded operational batch accounting;
- target `orchestration.object_usage_event` и `pipeline_object_summary` — usage evidence
  и rebuildable current summary.

`queue_seq` в `orchestration.work_item` неизменяем; priority хранится отдельно.
Workers не меняют canonical state напрямую: они возвращают typed result envelope,
который валидируется и принимается reconciler-ом. Run/work с `project_id` записывает exact
`project_pipeline` scope; pipeline version upgrade не обнуляет long-lived state/history.

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
включается самим фактом миграции. Shared actor/account/content/asset roots получают explicit
Region Talk relation; workload projection или имя schema не заменяют эту связь. Exact
Region Talk status публикуется в namespaced scoped-state projection с одним writer-ом.

## Migration landing

YDB rows сначала попадают без смысловой потери в `migration.raw_record` вместе с
batch/table/source PK, row kind, source timestamp, canonical JSON и SHA-256. Region Talk
export batch обязательно содержит Region Talk origin project scope, который наследуется
всеми raw rows, включая excluded/retained/quarantined.
`migration.row_disposition` присваивает каждой строке один terminal класс:

```text
normalized | deduplicated | intentionally_excluded | retained_raw | quarantined
```

`migration.legacy_identity_map` связывает source key с новым target, а
`migration.reconciliation_run`/`finding` и `cutover_receipt` хранят проверку и
решение о переключении. Cutover возможен только когда raw count совпадает с
manifest и undispositioned rows равны нулю; необъяснённые active quarantines
блокируют переключение. Для `normalized`/`deduplicated` dispositions target write,
legacy map, provenance, Region Talk relation и disposition фиксируются одной transaction.
Отсутствующая Region Talk relation является отдельным blocking reconciliation finding.

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
integration.data_product_consumer
integration.batch_application
integration.watermark
integration.quarantine
integration.receipt
```

Acceptance evidence and canonical application are separate. One immutable batch may route
to several `data_product_consumer` records, each with an independent application status and
receipt. Exact replay reuses both acceptance and consumer application identities; a
conflicting hash is quarantined. Corrections append a superseding batch.

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
