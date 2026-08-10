# Orchestrator and master lifecycle

Status: `TARGET CONTRACT / FAKEKAGGLE IMPLEMENTATION DEFERRED AFTER PR-A`

The devstand orchestrator is a lightweight control plane. It persists operation identity,
provider intent, callbacks, run evidence, leases, fencing epochs, service registry and
checkpoint locators. It does not store canonical content and is not a proxy for bulk data.

## Required state machine

`ABSENT -> REQUESTED -> STARTING -> RESTORING -> REGISTERING -> ACTIVE -> DRAINING -> CHECKPOINTING -> STOPPED`
with `FAILED`, `FENCED`, `CHECKPOINT_FAILED`, and `ORPHANED` terminal/error states.

Rules:

- persist a transition before each provider side effect;
- `ensure_master` is idempotent and concurrent calls create at most one run;
- each new master uses a monotonically increasing epoch;
- only latest-epoch ACTIVE service resolves;
- lease expiry closes the DB gate and rejects old callbacks/heartbeats;
- reconcile platform status with exact callback/output evidence;
- never place credentials in event records.

After resolve, workers/connectors use short-lived epoch-bound credentials and connect
directly to the Kaggle master data plane. External clients use stable devstand MCP.

PR-A exposes only a truthful `master=ABSENT` status. Deterministic clock, FakeKaggle,
property tests, durable ledger and real adapter are later ordered work. Region Talk
scheduling and publication remain disabled.

## Preserved detailed contract — bound by ADR-0016

The detailed material below is retained where topology-neutral. Any reference to a database, role, committer, backup or connector application is executed inside/against the latest ACTIVE Kaggle master; devstand execution claims are superseded.

## 1. Назначение

Оркестратор продвигает durable work к продуктовому результату. Он не является
непрозрачным in-memory workflow engine и не считает зелёный notebook достаточным
доказательством canonical success.

Текущий bootstrap реализует безопасный **plan-only loop**. Provider launch,
result application и external side effects остаются отдельными адаптерами и по
умолчанию выключены.

## 2. Короткий tick

Целевая последовательность одного recoverable tick:

```text
wake up
→ obtain one project/tick fence
→ recover expired leases
→ inspect immutable worker-result inbox
→ validate and reconcile acceptable outputs
→ resolve exact project-pipeline scope and effective policy
→ materialize newly eligible work
→ claim a bounded batch
→ dispatch a worker or execute a local stage
→ persist run/stage/work events
→ commit and exit
```

Supervisor может вызывать tick регулярно, но correctness не зависит от памяти
между вызовами.

## 3. Durable state, созданный migrations

- `orchestration.pipeline` — versioned workload definition и operational status;
- `orchestration.pipeline_stage` — versioned stage/contract/lane;
- `orchestration.run` — correlated planned/scheduled/manual flow;
- `orchestration.stage_run` — выполнение конкретной стадии;
- `orchestration.work_item` — work, retry counters и lease owner/token/expiry;
- `orchestration.work_item_dependency` — causal dependency;
- `orchestration.work_item_event` — append-only transition evidence;
- `orchestration.worker_artifact` — input/output locator + SHA-256;
- `orchestration.worker_result_inbox` — immutable typed result envelope;
- `sync.external_outbox` — committed intent для внешнего side effect.

Claiming использует `FOR UPDATE SKIP LOCKED` в
`orchestration.claim_work_items(...)`. `queue_seq` immutable; выбор идёт по
priority, available time и admission order.

ADR-0015 дополнительно требует новой append-only migration для
`orchestration.pipeline_identity`, `project_pipeline`, `object_usage_event` и scope/state/
policy tables. Это accepted target, а не часть уже доказанного bootstrap.

Повторная регистрация того же pipeline version обновляет definition/stage metadata,
но **не изменяет operational status**. Поэтому `db migrate` не может случайно
поставить активный pipeline на паузу или включить вручную остановленный pipeline.

## 4. Region Talk stage graph

```text
exact URL / research intake ─┐
                             ├→ source/post identity → E5 → BGE-M3 → fusion
source/post discovery ───────┘                                  ↓
                                      text gate → image/profile → verifier
                                                                  ↓
                                                 writer → review → plan → publish
```

E5, BGE-M3, image diagnostics, source profile и writer остаются разными compute
lanes. Worker не получает canonical write credentials.

## 5. Pressure-aware planning

Порядок по умолчанию:

1. принять/сверить уже готовые worker results;
2. дренировать exact URL и downstream backlog;
3. завершить fusion/gates/image/profile/verifier/writer/review;
4. планировать публикацию только для exact approved revision;
5. запускать новый discovery, когда downstream pressure допускает это.

`publication_dispatch` отключён в pipeline definition, а production publishing
дополнительно защищён независимым configuration gate.

## 6. Work state

Фактический state machine `orchestration.work_item`:

```text
pending → leased → running → succeeded
   ↑          │        ├→ failed_retryable
   │          │        ├→ failed_terminal
   └──────────┘        ├→ quarantined
                       └→ cancelled
```

Timeout/expired lease не удаляет work. Recovery создаёт evidence и возвращает
работу в допустимое retry state с fencing token. Этот state machine описывает выполнение
работы: `succeeded` не означает project membership, editorial approval или publication
eligibility объекта.

## 7. Idempotency и causality

- work identity: `(pipeline_id, stage_id, dedupe_key)`;
- dispatch manifest: exact `run_id`, ordered `work_item_id`, stable logical pipeline,
  exact project-pipeline scope, canonical/object revisions, input fingerprint,
  contract/model/policy versions and policy-evaluation receipt;
- worker result: `result_id`, exact input-manifest SHA-256 и result SHA-256;
- inbox uniqueness предотвращает double apply;
- dependency DAG не позволяет применить dependent result раньше prerequisites;
- external idempotency key выводится из target + exact approved fingerprint;
- wall-clock не используется как canonical conflict order.

## 8. Acceptance worker result

Canonical committer проверяет:

1. JSON Schema, byte/item limits и отсутствие запрещённых полей;
2. run/stage/work identities and unambiguous project-pipeline scope;
3. exact input-manifest hash и expected canonical/input revision;
4. model, prompt, policy и code identity;
5. artifact locator/hash;
6. explicit succeeded/partial/failed item accounting;
7. stage-specific invariants;
8. idempotency/conflict policy;
9. required object-scope relations and effective policy revision;
10. secret scan до durable acceptance.

Только затем domain mutation, required scoped state/relation/usage event, work
transition, receipt и outbox фиксируются одной PostgreSQL transaction.

## 9. External side effects

Review delivery и publication разделены на canonical intent и provider execution:

1. transaction принимает exact object revision, project-pipeline scope, policy-evaluation
   receipt и его input fingerprint и записывает `sync.external_outbox`;
2. непосредственно перед network call dedicated dispatcher проверяет receipt TTL и что
   текущий policy input fingerprint не изменился; stale/unknown приводит к повторной
   evaluation либо fail-closed без provider call;
3. dedicated dispatcher выполняет network call;
4. provider receipt/ambiguous outcome сохраняется;
5. перед retry выполняется reconciliation с target и повторная policy freshness check;
6. one idempotency identity не создаёт второй пост.

Kaggle notebook не отправляет review card и не публикует напрямую.

## 10. Observability

Каждый run должен объяснить:

- почему выбран конкретный work;
- code/model/prompt/policy/input identities;
- каждую попытку, lease и duration;
- terminal outcome или blocking gate;
- canonical mutations и revision/receipt;
- provider usage без secrets;
- exact revision и project/pipeline scope, увиденные оператором;
- object usage events, scoped state transition and effective policy decision IDs;
- итог external target или ambiguous reconciliation state.

## 11. Data connector responsibilities

Оркестратор выполняет pull-коннекторы и downstream-нормализацию уже принятых
push-batches, но HTTPS intake не ждёт завершения pipeline. Intake сначала фиксирует
immutable batch и receipt; затем server-side routing создаёт независимую
`batch_application` для каждого matched consumer-а. Durable work продвигает каждую
application через validation, staging, normalization, canonical commit и reconciliation.
Failure/paused state одного consumer-а не изменяет acceptance receipt и не блокирует другого,
если consumer не объявлен required для общего gate.

Missed pull schedule восстанавливается из persisted due/watermark state. Push-producer
при недоступности devstand хранит batch в собственном durable spool и повторяет exact
idempotency identity. Оркестратор не является availability preflight service для
producer.

## 12. Object participation and policy

Planner не выводит membership или policy из существования work item. До materialization он:

1. разрешает stable logical pipeline и exact platform/project/pipeline/project-pipeline
   scopes;
2. читает exact namespaced object state каждого applicable scope;
3. вычисляет effective policy по versioned combiner;
4. fail-closed при unknown/conflicting required policy;
5. сохраняет policy evaluation receipt и object revision в work/dispatch manifest;
6. после canonical apply пишет append-only object usage event.

Один object может иметь разные states в разных scopes. Platform-wide hard deny/blacklist
блокирует все applicable pipelines; local allow не может его ослабить. Usage event не
создаёт project membership автоматически, а membership без фактического usage допустима.

Подробности: [`22-data-scope-and-pipeline-participation.md`](22-data-scope-and-pipeline-participation.md).

## 13. Kaggle resource ownership

Каждый запуск/датасет имеет control-plane registry control class:

- orchestrator создаёт и управляет `orchestrator_protected` resources;
- remote MCP видит для них только bounded status;
- MCP-created resources относятся к `mcp_managed` или `mcp_exchange` и не могут быть
  подхвачены оркестратором без explicit adoption;
- provider rename/rediscovery не меняет authorization.

Provider dispatch использует lease, expected provider fingerprint, idempotency и
reconciliation after ambiguous outcome. Неподдержанная provider operation не
эмулируется скрытым web automation без отдельного решения.

## 14. Control/master availability

Оркестратор работает на lightweight devstand независимо от состояния master. При
`master=ABSENT` он остаётся доступен, идемпотентно создаёт или возвращает `ensure_master`
operation и через Kaggle adapter запускает fenced master lifecycle. PostgreSQL supervisor,
restore, DB gate и checkpoint agent работают внутри master Notebook. Недоступный devstand
не переносит control authority в worker: producer сохраняет exact payload в durable spool,
а optional external wake controller может только поднять approved control host с минимальным
IAM scope.
