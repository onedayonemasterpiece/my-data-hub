# Первый продуктовый срез MDH после аудита

Статус: проект реализации, 04.11.2026; текущий runtime не изменён. Обязательная последовательность — [исправленный план](implementation-plan.md). Предыдущая версия этого файла доступна в Git history на `6b7a68044d9a6cca002c916158b8cbfa8ee1ea6b` и не является действующим исполнительским списком.

## Данные и обработка

Один notebook выполняет capture и последовательный processing loop рядом с единственной PostgreSQL. Полный исходник сохраняется прежде модели. Модель оценивает один новый post revision одним запросом; готовый structured result хранится целиком. PG unique constraints обеспечивают дедупликацию, обычный SELECT выбирает необработанное. Stage orchestration, второй result store и generic connector refactor для этого не требуются.

Предлагается одна следующая contiguous migration `sql/migrations/0023_regional_monitoring.sql` (номер перепроверить перед реализацией). Предметные таблицы находятся в той же canonical PG; они не дублируются на Dev.

| Объект | Минимальные данные и назначение |
|---|---|
| `monitoring.post_revision` | `id`, `source_key`, `external_post_id`, `source_revision_key`, URL, published/observed time, полный текст, optional связь с existing content_item, полный `analysis_result`, actual model/prompt version, последняя ошибка и время допустимого повтора. UNIQUE(source_key, external_post_id, source_revision_key) |
| `monitoring.source_cursor` | Одна строка на источник: cursor и observed-through. Обновляется в транзакции с принятой порцией post revisions |
| `monitoring.post_chunk` | Revision ID, ordinal, offsets в полном тексте; детерминированные границы без потери хвоста. Это описание материала, не очередь LLM-стадий |

Post revision — canonical representation этого workload. Existing content_item при связывании остаётся общей проекцией/идентичностью, а не вторым независимо редактируемым полным текстом. Один предметный master handler пишет revision/result; capture не получает прямых прав на чужие таблицы. Существующие роли, scopes и single-committer принцип сохраняются.

Состояние обработки выводится из сохранённых данных: результат есть — готово; результата нет и повтор допустим — к обработке; ошибка хранится с причиной. Один последовательный worker не требует lease таблицы или специальных `processing` recovery phases. Детерминированная ошибка одной публикации не блокирует остальные; pending не называется отрицательным аналитическим выводом.

## Один аналитический запрос

Вход: полный текст текущей revision, region/time scope, источниковые поля и доступный prior context из SQL/FTS. Инструкция требует вернуть:

- `verdict`: значимое новое / повтор / уточнение / недостаточно оснований;
- `change`: конкретное изменение, а не пересказ всего текста;
- `why_significant`, время события и предмет/место;
- `source_evidence`: ссылка и фрагменты именно прочитанной публикации;
- `prior_evidence`: связанные прежние revisions и отличие от них;
- uncertainty и границу наблюдённого корпуса.

Соблюдается существующая изоляция инструкций: содержимое публикации — данные, а не команды агенту. Из операторского scope не выводятся новые факты. Повтор пресс-релиза не считается дополнительным независимым подтверждением. Старое событие, впервые попавшее в корпус, не объявляется впервые случившимся.

Полный текст не обрезается ради контекста. Для длинного материала подобрать допустимый маршрут с достаточным окном; пока его нет, сохранить материал как необработанный и продолжить другие. Разбиение на chunks не означает дополнительные extract/judge/writer вызовы. BGE подключается после первого результата; если подключён, полный encoder input остаётся ≤512 tokens, включая overhead, при сохранении существующего model contract и blogger representations.

## Модули и границы изменения

| Owned scope | Действие |
|---|---|
| Existing `master_runtime/notebook_entrypoint.py`, `bootstrap.py`, `postgres.py` | Один normal lifecycle: физическая база и WAL восстанавливаются до PG start; запись после завершения recovery |
| Existing `checkpoints/kaggle_runtime.py`, `publisher.py`, `control_plane/runtime.py` | Один publisher основной базы и временного WAL, current/previous references; сохранить streaming upload, снять обязательный verifier/acceptance |
| Proposed `workloads/regional_monitoring/capture.py` | Один разрешённый источник, typed post input и атомарный cursor |
| Proposed `workloads/regional_monitoring/process.py` | SELECT pending → одна модель → сохранить полный JSON result; без новой очереди |
| Proposed `workloads/regional_monitoring/reader.py` | Предметные запросы списка/карточки/coverage |
| Existing `mcp/catalog.py` и master/data-plane adapter | Добавить предметные MCP методы с нынешней авторизацией; сохранить действующие APIs |

Не расширять жёстко собранный DailyStatisticsCommitter до общего framework ради одного источника. Новый маленький предметный handler регистрируется в существующей master boundary, оставляя статистику и её потребителей работоспособными. Не удалять весь старый checkpoints package по оценке LOC: сначала отсоединить второй runtime owner, проверить callers и удалить ставший мёртвым код вместе с тестами. Единственный runtime path должен стать короче.

## Выдача пользователю

`novelty.list(region, period)` возвращает значимое новое, объяснение, время и источник. `novelty.get(id)` показывает полную карточку, source/prior evidence и уточнения. Статус корпуса отдельно показывает, какие источники/период собраны и какая часть ещё не проанализирована. Существующий MCP wake/resolve обеспечивает доступ к notebook; frontend и новый gateway не требуются.

Карточка сохраняется в PostgreSQL транзакционно. Аварийная долговечность определяется опубликованными base/WAL версиями; статус показывает свежесть внешнего сохранения. Выбор владельца закрыт [физической базой + WAL](STORAGE-BASE-WAL.md), без Dev journal и отдельной пилотной схемы. Неопубликованные транзакции ещё могут потеряться; локальный COMMIT не называется внешним сохранением.

## Приёмка

Один реальный источник → полный post → один model result → карточка через MCP → её WAL опубликован → notebook убит без STOP → свежий notebook восстанавливает base + WAL → та же revision и тот же ответ через MCP без повторного inference. Затем штатный STOP создаёт одну базовую версию, после ready удаляет старый временный Dataset, а следующий старт читает ту же карточку. Повтор capture не создаёт дубль.

Три различающих реальных PostgreSQL-сценария определены в STORAGE-BASE-WAL. Предметные проверки ограничены изменёнными границами: correction создаёт revision; репост не становится независимым подтверждением; ошибка модели не равна «нового нет». Одна карточка не доказывает ёмкость 100 ГБ — это отдельная измеряемая часть принятой рабочей схемы П3, а не будущая замена хранения.
