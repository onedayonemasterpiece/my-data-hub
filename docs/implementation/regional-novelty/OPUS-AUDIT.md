# My Data Hub: текущий проект после CHANGES_REQUIRED

Редакция 04.11.2026 после пересланного аудита. Прежний план на commit `6b7a68044d9a6cca002c916158b8cbfa8ee1ea6b` получил **CHANGES_REQUIRED** и заменён. Вердикт по новой редакции ещё не получен.

**Цель:** первая карточка значимого нового из реальной публикации, прочитанная через MCP и сохранившаяся после Dataset save, остановки и запуска нового notebook. Общий framework не является предварительным этапом.

## Читать в этом порядке

| Документ | Назначение |
|---|---|
| [Ответ на аудит](AUDIT-RESPONSE.md) | Принятые находки, фактические уточнения, отозванные решения и два вопроса review |
| [Решения владельца](owner-decisions.md) | Обязательный продукт и единственная новая граница: потеря хвоста после последнего снимка ещё не согласована |
| [Исправленный план](implementation-plan.md) | Один notebook, один lifecycle/checkpoint owner, первый реальный цикл и последующая карточка |
| [Первый продуктовый срез](novelty-implementation.md) | Полные revisions, SQL pending, один model call и предметный MCP |
| [Полученное резюме аудита](OPUS-AUDIT-SUMMARY.md) | Текст, пересланный владельцем; полный worktree report не выдан за прочитанный |

Предложение текущей редакции: один notebook с PostgreSQL/OpenCode; SQL выбирает необработанные revisions; официальный OpenCode installer выполняется при старте; logical dump в Dataset сохраняется перед остановкой; следующий notebook восстанавливает snapshot и продолжает работу. Без Dev journal, отдельной stage orchestration, собственного release service и обязательного adoption двух consumers перед первой карточкой.

**Решение о snapshot-only пока proposed.** Оно сохраняет последний подтверждённый Dataset; при внезапной потере текущего notebook непокрытый хвост может потеряться и потребовать повторного сбора/анализа. Публичность источника не гарантирует восстановление прежнего текста/ответа. Исправленный план не заявляет согласия владельца с этой ценой.

## Источники для углубления

Opus по-прежнему достаточно доступа к MDH и РКБ. Сведения из других репозиториев сохранены здесь; обращаться к ним для основного чтения необязательно.

- [Существующий сложный storage РКБ](rkb-storage-evidence.md) — фиксированный срез кода; не требование переносить его в пилот.
- [Контракты других проектов](cross-project-evidence.md) — provenance и возможности последующего reuse; не prerequisites первой карточки.
- [Начальный общий код и тесты](shared-runtime-source.md), [пределы проверки](runtime-code-evidence.md) — исторический snapshot source commit `52644f2748b602f54304e56424eee720d12a691b`, без consumers/live adoption.
- [Отозванное предложение ADR-0020](../../adr/0020-shared-runtime-regional-monitoring.md) — почему Dev journal больше не является выбранным переносом.
- [Первоначальное исследование](../../architecture/regional-monitoring-shared-kaggle-runtime-20261104.md) — широкий продуктовый контекст.
- [Аудит упрощения РКБ, A2–A4](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/2cfb2bbf6c95d1ef4efaf5f7871b5e3d559921f5/docs/audits/rkb-complexity-audit-2-20261023.md).

Authority: [AGENTS](../../../AGENTS.md), [source of truth](../../00-source-of-truth.md), [invariants](../../../architecture/invariants.yaml), [ADR-0016](../../adr/0016-kaggle-postgresql-master-architecture-reset.md). Exact imported source не менялся. Runtime source этой редакцией не изменён; platform.status при подготовке показывал ABSENT, а не доказанную готовность.

## Проверить в следующем review

1. Есть ли ровно один normal lifecycle/checkpoint owner, и удалены ли из его вызовов прежние обязательные broker/verifier/acceptance зависимости?
2. Правильно ли logical restore выполняется после PG start и не теряется ли существующий physical checkpoint при миграции?
3. Достаточна ли SQL identity публикации и cursor для повторного capture без дублей?
4. Возвращает ли одна модель полезную карточку с настоящим source/prior evidence, а ошибки остаются отличимы от «нового нет»?
5. Появляется ли первая карточка до разработки универсального runtime, BGE fleet и 100-ГБ профиля?
6. Ясна ли владельцу новая durability-граница, и не приписано ли согласие на неё?

Приёмка реализации: реальный пост → карточка → save → stop → новый notebook → та же карточка через штатный MCP. Документация и прежние 16 unit tests этого результата не подтверждают.
