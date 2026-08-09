# Orchestrator design

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
работу в допустимое retry state с fencing token.

## 7. Idempotency и causality

- work identity: `(pipeline_id, stage_id, dedupe_key)`;
- dispatch manifest: exact `run_id`, ordered `work_item_id`, canonical revision,
  input fingerprint, contract/model/policy versions;
- worker result: `result_id`, exact input-manifest SHA-256 и result SHA-256;
- inbox uniqueness предотвращает double apply;
- dependency DAG не позволяет применить dependent result раньше prerequisites;
- external idempotency key выводится из target + exact approved fingerprint;
- wall-clock не используется как canonical conflict order.

## 8. Acceptance worker result

Canonical committer проверяет:

1. JSON Schema, byte/item limits и отсутствие запрещённых полей;
2. run/stage/work identities;
3. exact input-manifest hash и expected canonical/input revision;
4. model, prompt, policy и code identity;
5. artifact locator/hash;
6. explicit succeeded/partial/failed item accounting;
7. stage-specific invariants;
8. idempotency/conflict policy;
9. secret scan до durable acceptance.

Только затем domain mutation, work transition, receipt и outbox фиксируются одной
PostgreSQL transaction.

## 9. External side effects

Review delivery и publication разделены на canonical intent и provider execution:

1. transaction принимает exact revision и записывает `sync.external_outbox`;
2. dedicated dispatcher выполняет network call;
3. provider receipt/ambiguous outcome сохраняется;
4. перед retry выполняется reconciliation с target;
5. one idempotency identity не создаёт второй пост.

Kaggle notebook не отправляет review card и не публикует напрямую.

## 10. Observability

Каждый run должен объяснить:

- почему выбран конкретный work;
- code/model/prompt/policy/input identities;
- каждую попытку, lease и duration;
- terminal outcome или blocking gate;
- canonical mutations и revision/receipt;
- provider usage без secrets;
- exact revision, увиденную оператором;
- итог external target или ambiguous reconciliation state.
