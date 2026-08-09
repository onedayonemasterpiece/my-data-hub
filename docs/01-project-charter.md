# Project Charter

## Миссия

Собрать в одном управляемом контуре авторов, издания, аккаунты, материалы,
краткие карточки, связи с проектами, результаты анализаторов, provenance и
историю прохождения через конвейеры — без дублирования одного объекта на
каждый проект и без множества несогласованных state stores.

## Первые пользовательские роли

- **Владелец/оператор**: видит состояние конвейеров, проверяет кандидатов,
  принимает exact-revision решения и управляет запуском.
- **Агент через MCP**: ищет и объясняет данные, создаёт предметные команды,
  запускает разрешённые конвейеры, но не получает прямой write-SQL.
- **Notebook worker**: выполняет bounded stage и возвращает подписанный
  результат.
- **Кодовый агент**: разворачивает и изменяет систему через repository/CI,
  сохраняя provenance и acceptance evidence.

## Первая продуктовая задача

Перенести `Region Talk` с накопленной историей из YDB в `my-data-hub`,
устранить известные дефекты очереди и после shadow/canary запустить прежний
продуктовый конвейер на PostgreSQL-backed оркестраторе.

## Scope первой версии

Входит:

- PostgreSQL schema и миграции;
- object/project/revision/provenance model;
- pipeline/run/task/attempt/outbox;
- artifact/result contracts;
- local API и MCP stdio;
- remote MCP design и security boundary;
- Region Talk domain projection;
- YDB migration landing, mapping, accounting, reconciliation;
- notebook skeletons;
- Joplin adapter boundary;
- backup/restore, audit, health и CI.

Не входит до отдельного решения:

- публичный UI каталога;
- production auto-publishing;
- прямой доступ notebooks к canonical DB;
- попытка реализовать Joplin Sync Server или читать Joplin desktop SQLite;
- arbitrary agent SQL;
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
10. Production publishing остаётся выключенным до отдельного owner gate.

## Engineering quality gates

- migrations применяются на чистой БД и повторно не изменяют released SQL;
- strict JSON Schema для всех worker/external artifacts;
- retries идемпотентны;
- fail-closed при неизвестном schema/tool scope/revision;
- секреты отсутствуют в artifacts/logs/test fixtures;
- backup restore проверяется, а не только создаётся;
- каждая mutation оставляет audit event и correlation ID.
