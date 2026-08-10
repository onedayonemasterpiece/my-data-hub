# Project Charter

## Миссия

Собрать в одном управляемом контуре авторов, издания, аккаунты, материалы,
краткие карточки, связи с проектами, результаты анализаторов, provenance и
историю прохождения через конвейеры — без дублирования одного объекта на
каждый проект и без множества несогласованных state stores. Принадлежность проекту,
состояние в конкретном project/pipeline scope, факт usage и глобальная policy должны быть
явными, независимыми и объяснимыми.

## Первые пользовательские роли

- **Владелец/оператор**: видит состояние данных и конвейеров, проверяет
  кандидатов, выполняет широкие, но контролируемые операции с данными и
  управляет запуском.
- **Агент через semantic MCP**: ищет и объясняет данные, создаёт предметные
  команды и запускает разрешённые конвейеры.
- **Агент через operator MCP**: после отдельного допуска получает широкое
  bounded-чтение и контролируемый DML под отдельной PostgreSQL-role,
  preview/apply, backup и audit gates; не получает superuser/owner.
- **Data connector**: передаёт версионированные идемпотентные batch-наблюдения
  через intake/landing и получает durable receipt.
- **Notebook worker**: выполняет bounded stage и возвращает подписанный
  результат.
- **Кодовый агент**: разворачивает и изменяет систему через repository/CI,
  сохраняя provenance и acceptance evidence.

## Первая продуктовая задача

Перенести `Region Talk` с накопленной историей из YDB в `my-data-hub`,
устранить известные дефекты очереди и после shadow/canary запустить прежний
продуктовый конвейер на PostgreSQL-backed оркестраторе.

Region Talk остаётся первым migration workload, но до тяжёлого переноса должны
быть доказаны инфраструктура, backup/restore, remote MCP, ADR-0015 scope/policy
foundation, synthetic multi-consumer connector, Kaggle control classes и operator access
в disposable schema.

## Scope первой версии

Входит:

- PostgreSQL schema и миграции;
- object/project/revision/provenance model;
- stable logical pipeline identity and platform/project/pipeline/project-pipeline scopes;
- object-scope relations, namespaced state, append-only usage and scoped policy decisions;
- pipeline/run/task/attempt/outbox;
- artifact/result contracts;
- local API и MCP stdio;
- remote MCP на `mcp-datahub.kenigevents.ru` с OAuth boundary;
- semantic, data-reader, data-editor, migration-operator и Kaggle profiles;
- data connector registry/intake/receipt/quarantine contracts;
- Kaggle inventory, protected/MCP-managed/exchange resource policies;
- Region Talk domain projection;
- YDB migration landing, mapping, accounting, reconciliation;
- notebook skeletons;
- Joplin adapter boundary;
- backup/restore, audit, health и CI;
- test-first devstand/nightly/provider workflows.

Не входит до отдельного решения:

- публичный UI каталога;
- production auto-publishing;
- прямой доступ notebooks к canonical DB;
- попытка реализовать Joplin Sync Server или читать Joplin desktop SQLite;
- remote PostgreSQL owner/superuser, DDL или role administration через MCP;
- публичные Kaggle datasets, созданные через MCP;
- использование Kaggle как master database/failover canonical head;
- удаление YDB сразу после cutover;
- хранение больших media/full HTML как default.

## Продуктовые критерии успеха Region Talk migration

1. 100% YDB rows accounted; zero unexplained loss.
2. Все actionable sources имеют canonical key и immutable positive
   `queue_seq`.
3. Нет duplicate `queue_seq`; priority не меняет порядок admission.
4. Uncached sources не исчезают из selection pool.
5. Exact-post fast lane существует как отдельная стадия.
6. Один versioned eligibility contract используется всеми consumers.
7. Доступны run-level funnel, zero-result reasons и воспроизводимый trace.
8. Три последовательных shadow runs не дают unexplained semantic drift.
9. Review и publish разрешения привязаны к exact revision fingerprint.
10. Каждая YDB row имеет Region Talk migration scope, а каждый normalized или
    deduplicated shared target — явную Region Talk relation.
11. Deduplication сохраняет union project/pipeline relations и не создаёт копию объекта.
12. Production publishing остаётся выключенным до отдельного owner gate.

## Engineering quality gates

- migrations применяются на чистой и upgrade-path БД и повторно не изменяют
  released SQL;
- strict JSON Schema для worker/external/connector/exchange artifacts;
- retries и connector delivery идемпотентны; один accepted batch может иметь несколько
  независимых consumer applications;
- fail-closed при неизвестном schema/tool/data scope/revision/control class или policy
  version;
- секреты отсутствуют в artifacts/logs/test fixtures;
- backup restore проверяется, а не только создаётся;
- каждая mutation оставляет audit event и correlation ID;
- data-editor ограничен database grants, preview/apply и impact gates;
- orchestrator-protected Kaggle resources не мутируются через remote MCP;
- Region Talk migration не начинается до infrastructure-first acceptance и реализации
  scope-completeness gates из ADR-0015.
