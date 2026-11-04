# My Data Hub: аудит принятой схемы и реализации

Решение владельца от 04.11.2026: **физическая базовая копия + штатный WAL во временном Dataset**, один publisher и длинные сессии Kaggle. Исходная ветка docs/storage-wal-scheme-20261104 включена в main на bcd7acfb. Вердикт CHANGES_REQUIRED относился к прежнему проекту; новая реализация не объявляется принятой по факту обновления документов.

**Продуктовая приёмка:** реальная карточка → её WAL опубликован → внезапное исчезновение notebook → новый notebook → та же карточка через MCP без нового inference. Затем STOP → одна готовая основная версия → временный Dataset удалён.

## Читать в этом порядке

| Документ | Что он устанавливает |
|---|---|
| [Решения владельца](owner-decisions.md) | Выбор хранения закрыт; ~100 ГБ учитываются сразу, snapshot-only и Dev journal отменены |
| [Физическая база + WAL](STORAGE-BASE-WAL.md) | Формат, restore/stop, timeline, рост хвоста, один publisher, три реальных теста |
| [Живые проверки П1–П4](STORAGE-PREFLIGHT-RESULT.md) | Что уже измерено, точные ресурсы/receipt, ограничения передачи и ёмкости |
| [План реализации](implementation-plan.md) | Один notebook, существующий lifecycle, конкретные изменения, аварийная и штатная приёмка |
| [Предметная реализация](novelty-implementation.md) | Полные revisions, SQL pending, один вызов модели, MCP карточка |
| [Исторический ответ](AUDIT-RESPONSE.md), [полученное резюме](OPUS-AUDIT-SUMMARY.md) | Происхождение CHANGES_REQUIRED и последующих решений; не текущая storage-спецификация |

OpenCode устанавливается актуальным официальным installer на каждом старте; отдельный release service не нужен. Общая библиотека не откладывает карточку: переиспользуется работающий provider/transport MDH, а совпавший код выделяется при реальном втором потребителе. Исходники runtime этой интеграцией документов не изменены; master при проверке был ABSENT.

## Исходники, достаточные для глубокого review

Opus достаточно доступа к MDH и РКБ. В MDH смотреть master_runtime/postgres.py (генерация archive/recovery и ready), bootstrap.py (physical restore до PG), notebook_entrypoint.py (cold base после fast-stop), checkpoints/archive.py и manifest.py (убрать второй logical dump согласованно со всеми readers), provider_storage.py/brokered_upload.py (сохранить streaming transport), publisher.py/kaggle_runtime.py и control_plane/runtime.py (единственный owner). Пути относятся к src/my_data_hub; детали и exact исходный SHA приведены в протоколе проверок.

- [Хранение РКБ](rkb-storage-evidence.md) — фиксированный срез; не требование перенести его Dev journal.
- [Другие проекты](cross-project-evidence.md) — сохранённые сведения о EventsBot/DAEP и возможностях reuse.
- [Начальный общий код](shared-runtime-source.md), [границы проверки](runtime-code-evidence.md) — исторический source snapshot 52644f2, без доказанного adoption.
- [Отозванный ADR-0020](../../adr/0020-shared-runtime-regional-monitoring.md).
- [Первоначальное исследование](../../architecture/regional-monitoring-shared-kaggle-runtime-20261104.md).
- [Аудит упрощения РКБ A2–A4](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/2cfb2bbf6c95d1ef4efaf5f7871b5e3d559921f5/docs/audits/rkb-complexity-audit-2-20261023.md).

Authority: [AGENTS](../../../AGENTS.md), [source of truth](../../00-source-of-truth.md), [invariants](../../../architecture/invariants.yaml), [ADR-0016](../../adr/0016-kaggle-postgresql-master-architecture-reset.md). Exact imported source и исполняемые invariants не менялись. Полный OPUS-AUDIT-RESULT из чужого worktree не получен; сохранено честно обозначенное пересланное резюме.

## Вопросы следующего review

1. Выполняются ли измерения до storage-кода без второго credentialed publisher или обхода provider authority?
2. Сохраняется ли один lifecycle и stream upload при удалении обязательных verifier/acceptance ветвей?
3. Восстанавливаются ли полный PGDATA, подходящий base_id, WAL и timeline history до первой записи; не теряются ли прежние данные?
4. Сохраняется ли WAL за время online публикации; остаётся ли current reference на прежней паре до готовности новой базы с её WAL; удаляется ли старый хвост только после переключения?
5. Учтены ли фактические пределы /kaggle/working, scratch, Dataset и upload для целевых ~100 ГБ?
6. Возвращает ли одна модель полезную карточку с источниками, а ошибки остаются отличимы от «нового нет»?

Документация, микропробы Kaggle и прежние unit tests не доказывают восстановление карточки. Результаты реальной PostgreSQL/карточной приёмки должны быть приложены после реализации; согласие владельца со схемой уже получено.
