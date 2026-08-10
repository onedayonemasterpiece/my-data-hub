# Области данных, участие в конвейерах и scoped policy

Status: `ACCEPTED DESIGN / IMPLEMENTATION PENDING`

Date: 2026-08-10

Related decision:
[`ADR-0015`](adr/0015-data-scope-and-pipeline-participation.md)

## 1. Зачем это нужно

`my-data-hub` должен хранить общую идентичность автора, аккаунта, материала или
asset один раз, но позволять нескольким проектам и конвейерам независимо работать с
этим объектом.

Например, один блогер может одновременно:

- входить в Region Talk как проверенный источник;
- быть кандидатом в другом исследовательском конвейере;
- быть временно остановлен в конкретном pipeline из-за rate limit;
- иметь platform-wide решение `publication.source_eligibility = deny` с причиной
  `global_blacklist`, которое блокирует публикацию во всех проектах;
- иметь публикации, уже использованные разными конвейерами с разными результатами.

Один столбец `status` не способен выразить эти факты без потери смысла. Статус
очереди, принадлежность проекту, факт использования и разрешение на внешний side
effect — разные сущности.

## 2. Главный принцип

Для каждого durable datum должна существовать однозначная **scope lineage**, но это не
означает, что `project_id` и `pipeline_id` нужно физически дублировать в каждой таблице.
Scope определяется одним из четырёх способов:

1. **direct object scope** — общий canonical object связан с project/pipeline scope;
2. **batch scope** — raw connector/migration row наследует scope immutable batch;
3. **parent scope** — child/projection разрешает scope через canonical parent и при
   необходимости хранит собственный evaluation scope;
4. **execution scope** — work/result/usage разрешает project/pipeline через run, stage и
   stable logical pipeline identity.

Если datum обязан иметь scope, но ни один путь не разрешается однозначно, canonical
application завершается fail-closed.

## 3. Термины

### Catalog object

Shareable canonical root с устойчивой UUID identity. В первую очередь:

- `actor`;
- `external_account`;
- `content_item`;
- `content_asset`;
- другие будущие корневые сущности, которые реально могут использоваться несколькими
  workloads.

Не каждая физическая строка становится catalog object. Raw rows, attempts, events и
project-specific projections сохраняют собственные ключи и получают scope через batch,
parent или execution lineage.

### Scope

Устойчивый контекст, в котором объект связан, получает состояние или policy:

| Scope kind | Пример | Назначение |
|---|---|---|
| `platform` | `platform` | решение действует во всём data hub |
| `project` | `project:region-talk` | решение/принадлежность действует для проекта |
| `pipeline` | `pipeline:region-talk.main` | правило относится к logical pipeline во всех проектах |
| `project_pipeline` | `project-pipeline:region-talk:region-talk.main` | точный runtime-контекст проекта и pipeline |

`project_pipeline` необходим, потому что одна версия pipeline definition может быть
переиспользована несколькими проектами, а состояние объекта при этом должно различаться.

### Relation

Устойчивая семантическая связь объекта со scope. Минимальный словарь relation kinds:

- `member` — объект входит в предметную область проекта;
- `managed` — scope отвечает за управление объектом;
- `referenced` — scope использует объект, но не владеет его lifecycle;
- `originated_in` — объект действительно был впервые создан из этого scope.

`originated_in` нельзя автоматически ставить дедуплицированному объекту, который уже
существовал в общем каталоге. Для него фиксируются `member`/`referenced` и точная
provenance связь с imported raw row.

### Usage

Append-only факт, что pipeline наблюдал, выбрал, потребил, пропустил, обработал или
создал объект в конкретном run/stage/work item. Usage не равен membership и не является
разрешением.

### Scoped state

Текущее workflow/domain состояние объекта в конкретном scope. Точный state всегда
namespaced, например:

```text
region_talk.source/active
editorial_review/approved
source_discovery/blocked_rate_limit
```

Для общих экранов и метрик каждый state отображается на небольшую normalized class:

```text
unknown | observed | candidate | in_review | approved | active
paused | blocked | excluded | completed | failed | archived
```

Normalized class нужна для сводок. Нельзя разрешать публикацию только по ней.

### Policy decision

Versioned, reasoned решение вида:

```text
policy_key + policy_version + object + scope
→ allow | deny | review_required | no_opinion
```

Policy отвечает на вопрос «разрешено ли действие», а не «на какой стадии находится
workflow».

## 4. Пять независимых слоёв состояния

| Слой | Пример | Каноническая роль |
|---|---|---|
| Entity lifecycle | аккаунт удалён на платформе | факт о самой сущности |
| Scope relation | actor является member Region Talk | устойчивая принадлежность/связь |
| Scoped workflow state | actor approved в pipeline A, candidate в pipeline B | текущее состояние в контексте |
| Pipeline usage | pipeline B обработал actor в run 42 | append-only операционное evidence |
| Policy decision | global publication deny | разрешение/запрет с precedence |

Эти слои нельзя обновлять одним общим `status` и нельзя выводить один из другого без
явного versioned правила.

## 5. Целевая логическая модель

Названия ниже являются принятым target contract. Физическая реализация добавляется
только новой append-only migration.

### 5.1 Stable pipeline identity

Существующая `orchestration.pipeline` хранит versioned definition. Перед scoped state
нужен устойчивый logical identity:

```text
orchestration.pipeline_identity
  pipeline_identity_id UUID PK
  pipeline_key         TEXT UNIQUE       # region-talk.main
  owner
  lifecycle_status

orchestration.pipeline
  ... existing versioned definition ...
  pipeline_identity_id FK
  version

orchestration.project_pipeline
  project_pipeline_id UUID PK
  project_id          FK hub.project
  pipeline_identity_id FK orchestration.pipeline_identity
  relation_status
  configuration_ref
  UNIQUE (project_id, pipeline_identity_id)
```

State и долгоживущие relations ссылаются на `pipeline_identity`, а run — на exact
versioned `pipeline` definition. Обновление версии pipeline не обнуляет историю объекта.

### 5.2 Catalog object registry

```text
hub.catalog_object
  object_id        UUID PK
  object_type      TEXT
  created_at
  retired_at       NULLABLE
```

Shareable root tables используют тот же UUID и FK на registry. Допустимые object types
управляются allowlisted registry/constraint, а не произвольной строкой из клиента.

### 5.3 Scope registry

```text
hub.scope
  scope_id                 UUID PK
  scope_kind               platform | project | pipeline | project_pipeline
  scope_key                TEXT UNIQUE
  project_id               NULLABLE FK hub.project
  pipeline_identity_id     NULLABLE FK orchestration.pipeline_identity
  project_pipeline_id      NULLABLE FK orchestration.project_pipeline
  lifecycle_status
```

CHECK constraints требуют:

- `platform`: все subject FK равны `NULL`, разрешена одна platform row;
- `project`: задан только `project_id`;
- `pipeline`: задан только `pipeline_identity_id`;
- `project_pipeline`: задан только `project_pipeline_id`.

`project_pipeline` scope не хранит вторую независимую пару project/pipeline: он ссылается
на одну association row. Это исключает drift между routing configuration и scope registry.
`scope_key` является стабильным human-readable key, но FK/unique constraints остаются
авторитетными.

### 5.4 Persistent relation

```text
hub.relation_definition
  relation_kind
  definition_version
  allowed_object_types
  allowed_scope_kinds
  lifecycle_status

hub.object_scope_relation
  relation_id
  object_id
  scope_id
  relation_kind
  definition_version
  valid_from
  valid_to              NULLABLE
  provenance_event_id
  created_by
  revision

hub.object_scope_relation_event
  relation_event_id
  relation_id
  event_kind            # opened/closed/corrected/merge_remapped
  previous_revision
  next_revision
  reason/evidence
  occurred_at
```

Активная relation уникальна по `(object_id, scope_id, relation_kind)`. Definition registry
ограничивает допустимые object/scope combinations; произвольная строка из producer-а не
создаёт новый relation kind. Завершение связи не удаляет историю: current interval
закрывается, а append-only event фиксирует причину и revision.

Начальный словарь применяет `member` прежде всего к project scope, `referenced` — к
project/pipeline/project-pipeline, `managed` — только к явно назначенному owner scope, а
`originated_in` — только при доказанном создании объекта из этого scope.

### 5.5 Scoped state and history

```text
hub.state_definition
  state_namespace
  state_code
  definition_version
  normalized_class
  terminal
  allowed_object_types
  allowed_scope_kinds
  writer_identity

hub.object_scope_state
  object_id
  scope_id
  state_namespace
  state_code
  definition_version
  state_revision
  reason_code
  evidence_ref
  changed_by
  changed_at

hub.object_scope_state_event
  event_id
  object_id
  scope_id
  state_namespace
  previous_state
  next_state
  reason/evidence
  run_id/stage_id/work_item_id NULLABLE
  occurred_at
```

Одна current row существует на `(object_id, scope_id, state_namespace)`. Exact namespace
имеет одного объявленного writer-а. `normalized_class` разрешается из exact
`state_definition`; если она денормализуется для скорости, constraint/trigger не позволяет
ей расходиться с registry. Workload-specific таблица может оставаться богатой проекцией,
но не должна стать вторым независимым writer-ом того же namespace.

### 5.6 Policy definitions and decisions

```text
hub.policy_definition
  policy_key
  policy_version
  subject/object types
  applicable relations
  combiner
  default_outcome
  fail_closed

hub.policy_decision
  decision_id
  object_id
  scope_id
  policy_key
  policy_version
  outcome
  reason_code
  evidence/provenance
  valid_from/valid_to
  supersedes_decision_id
  decided_by
  decided_at

hub.policy_evaluation
  evaluation_id
  action_key
  object_id
  evaluation_scope_id
  object_revision
  policy_key
  policy_version
  effective_outcome
  relationship_evidence_ref
  policy_input_fingerprint
  valid_until             NULLABLE
  evaluated_at

hub.policy_evaluation_decision
  evaluation_id
  decision_id
  application_order
  effect
```

Decision rows append-only. Current/effective views выбирают действующие решения, но не
стирают superseded history. Evaluation — immutable receipt конкретной проверки; join rows показывают exact decisions,
сформировавшие outcome. `policy_input_fingerprint` связывает object/related-object
revisions, traversed relation revisions, evaluation scope и applicable decision set. Policy
definition задаёт freshness/TTL. Любое изменение этих входов делает старый receipt
непригодным для нового side effect, даже если его `effective_outcome` был `allow`.

Для публикационного контура базовый combiner — `deny_overrides`:

1. собрать platform, project, pipeline и exact project-pipeline decisions;
2. применить явно объявленное relationship traversal, например
   `content → author → actor/external_account`;
3. любой applicable hard `deny` блокирует действие;
4. более узкий scope может добавить deny/review, но не отменить platform deny;
5. отсутствие обязательного решения даёт fail-closed outcome согласно policy definition;
6. evaluation receipt хранит exact decision IDs, relationship traversal evidence,
   object revisions, evaluation scope и policy version.

Пример global blacklist:

```text
object: actor/<uuid>
scope: platform
policy: publication.source_eligibility/v1
outcome: deny
reason: global_blacklist
```

Это решение применяется ко всем публикационным pipelines, пока не superseded новым
обоснованным decision. Локальное `allow` не снимает его.

### 5.7 Pipeline usage

```text
orchestration.object_usage_event
  usage_event_id
  object_id
  scope_id                 # usually project_pipeline
  run_id
  stage_id
  work_item_id
  usage_kind               # observed/selected/consumed/produced/skipped/completed/failed
  input_or_output
  object_revision
  policy_evaluation_ref
  occurred_at

orchestration.pipeline_object_summary
  object_id
  scope_id
  first_seen_at
  last_seen_at
  usage_count
  last_usage_kind
  last_run_id
```

Summary — rebuildable projection. Append-only usage event — evidence. Удаление или
архивация work item не удаляет доказательство участия объекта.

## 6. Правила записи

Canonical transaction, которая меняет предметные данные, фиксирует согласованно:

```text
domain/current projection
+ required object/scope relation or scoped state
+ provenance/usage event
+ policy evaluation receipt when authorization is involved
+ required transactional outbox
```

Обязательные инварианты:

- нельзя создать отдельную копию объекта только из-за нового project/pipeline;
- membership не выводится из `work_item`;
- usage не создаёт `member` автоматически;
- state transition не создаёт policy allow автоматически;
- policy deny не переписывает workflow state; effective view объединяет их при чтении;
- scope/state/policy нельзя прятать только в невалидируемом JSON metadata;
- merge/dedupe переносит aliases, provenance и union всех scope relations;
- conflicting current states одного namespace/scope при merge не разрешаются last-write-wins:
  применяется явный namespace merge rule либо создаётся blocking conflict;
- policy decisions проигравшего duplicate UUID не теряются: identity remap учитывается до
  effective evaluation, поэтому blacklist нельзя обойти merge-ом;
- закрытие relation одного project/pipeline не удаляет shared object и не закрывает связи
  других scopes; entity retirement/erasure выполняется отдельной lifecycle/retention policy
  с tombstone и проверкой всех активных отношений;
- external side effect ссылается на exact object revision, project-pipeline scope и policy
  evaluation receipt; перед provider dispatch проверяется текущий policy input fingerprint,
  а stale/expired receipt приводит к повторной evaluation либо fail-closed.

### 6.1 Writer authority

- generic connector/producer не пишет relations, states или policy decisions напрямую;
- canonical committer может писать relation/state/usage только для consumer/namespace,
  закреплённого server-side registry;
- platform-wide policy и blacklist изменяются отдельной restricted role/typed command с
  reason, evidence, expected revision и immutable audit;
- pipeline может предложить review evidence, но не может сам снять applicable platform deny;
- generic MCP data editor не меняет relation/state/policy history или evaluation receipts;
- внешняя публикация и другие side effects fail-closed при отсутствии свежего evaluation.

## 7. Чтение и объяснимость

Для любого объекта оператор/MCP должны уметь получить четыре независимых ответа:

1. в каких проектах объект состоит или используется;
2. какими pipelines и в каких runs он фактически обрабатывался;
3. какой exact и normalized state действует в каждом scope;
4. почему действие разрешено/запрещено и какие decisions сформировали effective policy.

Запрос «покажи Region Talk» использует explicit project relation, а не эвристику по
`region_talk.*`, URL, provenance text или последнему pipeline run. Удобство чтения дают
views, но не новые источники истины, например:

```text
hub.object_scope_current_v
hub.object_effective_policy_v
orchestration.object_participation_v
migration.raw_record_effective_scope_v
```

## 8. Connectors: один batch, несколько consumers

`connector_id` и `data_product` описывают источник/contract, но не являются scope.
Routing хранится на принимающей стороне:

```text
integration.data_product_consumer
  consumer_id
  data_product_id
  target_scope_id
  normalizer_contract/version
  routing_predicate
  required
  lifecycle_status

integration.batch_application
  batch_id
  consumer_id
  application_status
  attempt/lease
  canonical_revision
  result/receipt/quarantine
  routing_registry_revision
  application_reason     # initial_route/explicit_backfill/reprocess
```

Один `integration.batch` принимается и хэшируется один раз. Для него создаётся отдельная
application identity на каждого matched consumer-а. Поэтому pipeline A может завершить
normalization, pipeline B — ожидать review, а pipeline C — быть paused без копирования
payload.

Producer может передать диагностический scope hint, но authoritative routing определяется
server-attested registry. Нельзя доверять producer-у назначение platform/project policy.
Добавление нового consumer-а не переписывает историческую маршрутизацию автоматически:
старые batches получают application только через явный bounded backfill/reprocess с
зафиксированными registry revision, reason и idempotency identity.

## 9. Region Talk migration contract

### 9.1 Stable identities

До baseline export должны существовать и быть зафиксированы в receipt:

```text
hub.project.slug = region-talk
orchestration.pipeline_identity.pipeline_key = region-talk.main
hub.scope = project:region-talk
hub.scope = pipeline:region-talk.main
hub.scope = project-pipeline:region-talk:region-talk.main
```

### 9.2 Raw origin scope

Каждый `migration.export_batch` Region Talk получает обязательный origin project scope и
mapping target project-pipeline scope. Все `migration.raw_record` наследуют их через FK на
batch. Это persisted, queryable маркировка, а не free-form tag; для удобства прямых запросов
используется `migration.raw_record_effective_scope_v`, а не копирование mutable scope columns
в каждую raw row.

Это правило распространяется на:

- known и unknown row kinds;
- `normalized` и `deduplicated`;
- `intentionally_excluded`;
- `retained_raw`;
- `quarantined`.

Так выполняется требование «всё перенесённое из YDB относится к Region Talk» на уровне
неизменяемой migration lineage, даже когда строка не стала активным business object.

### 9.3 Normalized targets

В одной bounded PostgreSQL transaction normalizer записывает:

```text
target object/projection
+ migration disposition
+ legacy identity/target references
+ provenance link to raw row
+ required Region Talk object-scope relation
+ scoped state/usage where applicable
```

Для `normalized` target обычно создаётся `member` или `referenced` relation. Для
`deduplicated` target relation добавляется к уже существующему object; новый object не
создаётся. `originated_in` ставится только когда Region Talk действительно породил новый
canonical object.

`intentionally_excluded` сохраняет Region Talk batch lineage и owner-approved reason, но
не обязан становиться `member`. `retained_raw`/`quarantined` не получают фиктивный target.

### 9.4 Region Talk-specific projections

Строка в `region_talk.*` не заменяет scope relation. Каждая projection, у которой есть
shared actor/account/content/asset root, обязана ссылаться на него; shared root должен иметь
Region Talk relation. Work items и usage events используют exact Region Talk
project-pipeline scope.

### 9.5 Reconciliation additions

Кроме прежнего row accounting должны быть равны нулю:

```text
raw_without_region_talk_batch_scope
normalized_target_without_region_talk_relation
deduplicated_target_without_region_talk_relation
region_talk_projection_without_scoped_shared_root
scope_relation_without_raw_or_provenance_evidence_during_migration
work_or_usage_with_ambiguous_project_pipeline_scope
```

Для каждого duplicate group отчёт показывает:

- все legacy source rows;
- один canonical object;
- сохранённые aliases/provenance;
- union project/pipeline relations;
- exact scoped states и policies без взаимного перезаписывания.

Scope-completeness — отдельный cutover gate. Баланс dispositions без него недостаточен.

## 10. Совместимость с текущим bootstrap

Сейчас:

- `hub.project_content` покрывает только content membership;
- `region_talk.source.status` и другие workload tables содержат локальные states;
- `orchestration.work_item.status` описывает execution;
- `orchestration.pipeline` одновременно играет роль versioned definition;
- connector consumer/application tables ещё не реализованы.

Переход выполняется без удаления released migrations:

1. новая append-only migration создаёт stable pipeline identity, scope/catalog registries,
   relations, state/policy/usage tables and views;
2. core rows backfill-ятся в catalog registry;
3. Region Talk project/pipeline scopes создаются идемпотентно;
4. `hub.project_content` backfill-ится без ложного membership: исходная строка всегда
   сохраняет `referenced`/history evidence, а `member` создаётся только для явно допустимых
   status semantics; exact legacy status переносится в отдельный namespaced state;
5. `hub.project_content` временно остаётся content-specific compatibility/domain extension,
   но не получает второго независимого writer-а;
6. каждому state namespace назначается единственный writer и normalized mapping;
7. repository/UoW, MCP и orchestrator начинают писать relation/state/usage/policy evidence
   атомарно;
8. connector routing получает per-consumer application records;
9. migration fixture получает scope/dedupe/policy negative tests;
10. только после этого начинается реальная YDB normalization.

Ни одна старая released migration не редактируется.

## 11. Минимальный набор тестов

### Schema and backfill

- clean и upgrade migration;
- повторный apply не создаёт duplicate scopes/relations;
- каждый supported core object зарегистрирован;
- invalid scope FK combination и project-pipeline drift отклоняются;
- недопустимая relation kind/object/scope комбинация отклоняется;
- один active relation на key, а open/close/correction имеет append-only event;
- state namespace/code/version не из registry отклоняется;
- normalized class не может расходиться с exact definition.

### Multi-project/pipeline

- один object связан с двумя projects без копирования;
- один object имеет разные exact states в двух project-pipeline scopes;
- pipeline version upgrade сохраняет logical identity и history;
- usage в pipeline не создаёт membership;
- membership без usage остаётся допустимой;
- закрытие relation одного проекта не закрывает relation другого и не удаляет shared object.

### Policy

- platform deny блокирует project/pipeline allow;
- project/pipeline deny может ужесточить global allow;
- конфликт или неизвестная обязательная policy версия fail-closed;
- evaluation receipt воспроизводит effective outcome и exact decision set;
- новый applicable deny или relationship/object revision инвалидирует pending allow receipt;
- generic editor не может изменить decision/evaluation history;
- duplicate identity remap не теряет deny;
- supersede сохраняет полную историю.

### Connector

- один batch создаёт две независимые applications;
- failure одного consumer-а не меняет acceptance receipt и состояние другого;
- exact replay не создаёт повторные applications;
- producer scope hint не меняет server routing;
- новый consumer не получает historical batches без explicit idempotent backfill.

### Region Talk migration

- каждая raw row разрешает Region Talk batch scope;
- normalized и deduplicated targets имеют Region Talk relation;
- dedupe с pre-existing object не создаёт копию и не теряет relation;
- intentionally excluded сохраняет origin lineage без ложного membership;
- missing scope relation создаёт blocking reconciliation finding;
- cutover невозможен при scope-completeness failure.

## 12. Не-цели

- универсальная BPM/workflow-платформа;
- один enum, описывающий все бизнес-состояния;
- автоматическое наследование разрешений без versioned policy;
- добавление project/pipeline FK в каждую физическую таблицу;
- перенос production policy в имена схем, tags или JSON metadata;
- включение Region Talk publication самим фактом миграции/маркировки.
