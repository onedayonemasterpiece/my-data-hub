# План реализации MDH: первое значимое новое в регионе

База: `my-data-hub/main@77a62c53c881120164f525a226ec296dcb3c77b3`, 2026-11-04. `AGENTS.md` прочитан на этом SHA. Код соответствует предыдущему `91f7d9b...`: новый commit добавляет аналитическую документацию. Ниже **проект будущей реализации**; описанные новые модули и возможности ещё предстоит реализовать. Приложение [cross-project-evidence.md](cross-project-evidence.md) содержит необходимые недоступные исполнителю контракты других проектов.

## 1. Продуктовый результат и уже принятые решения

Первый полезный результат: владелец получает по заданному региону список **значимых новых фактов или существенных изменений** с объяснением, первоисточниками, временем публикации/обнаружения и тем, с чем сравнивалась новизна. Пример запроса: «Что существенно нового появилось в регионе за последние шесть часов?» Ответ обязан отличать новую информацию от нового репоста, от старого факта, впервые попавшего в неполный корпус, и от необработанной очереди.

Новая информация включает, например, начало/закрытие общественно значимого объекта, новое решение по инфраструктуре, новый региональный проект, существенное изменение доступности услуги, анонс с новым проверяемым содержанием. Список — стартовая инженерная policy, не исчерпывающая таксономия и не автоматическая шкала важности по числу репостов.

Решения владельца для этой поставки:

- общий runtime — отдельная versioned dependency; RunCoveer умеет работать независимо от DevCoveer и открытого ChatGPT;
- при free-quota exhaustion запускается свежий Kaggle **executor**, задача продолжается с сохранённой стадии; PG master живёт независимо;
- по умолчанию OpenCode provider keys отсутствуют; если выбранный маршрут требует keys, используются несколько **явно настроенных** bindings;
- durability/private Dataset берутся из работающей схемы РКБ; внешние backup-сервисы не входят в этот проект;
- SQL, роли и migrations проектируют и применяют инженеры; этот вопрос не возвращается владельцу;
- первым внедряется novelty, затем расширенные trends, гости региона и полная entity analytics.

AGENTS сохраняет один writable PG primary в Kaggle, typed connector landing, один canonical committer, epoch fencing и одну транзакцию canonical write + semantic outbox. Default MCP не получает generic DDL. Region Talk остаётся отдельным paused workload: новый monitoring pipeline не требует его запуска. Exact imported owner-source не переписывается. [M-Agents]

## 2. Реальная исходная точка

| Уже есть | Существенный gap первого среза | Решение |
|---|---|---|
| `hub.content_item`, identity, author, asset refs, FTS | `body_excerpt`/summary не заменяют immutable полный текст каждой source revision | Additive `hub.content_revision` и source linkage |
| `ConnectorEnvelope`, landing, receipts, durability | Нормализатор и коммиттер работают для daily statistics; это не generic content importer | Bounded dispatcher по зарегистрированному `data_product` + новый source committer |
| `analysis.result` с полным JSON и provenance | Нет привязки к полной immutable revision и принятой novelty projection | Additive FK revision и узкая novelty projection |
| `analysis.embedding`, BGE_M3 exact contract, embedding workers | `embeddings.documents.SearchDocument` — `blogger_compact_v1`; нельзя выдавать его за content chunks | Отдельный content-chunk representation и importer; existing blogger API сохраняется |
| `orchestration.work_item`, dependencies, worker_result_inbox | Нужны этапы monitoring и adapter shared runtime; нельзя добавлять ещё одну очередь | Зарегистрировать pipeline/stages в существующей orchestration |
| MCP catalog/broker/scopes и epoch credentials | Нет продуктового `monitoring.novelty.*` API | Узкие read tools через existing authenticated broker |
| Connector lifecycle до `DURABLE_COMPLETE` | Нельзя приравнять executor local file или обычный receipt к verified Dataset | Применить существующие состояния и RKB profile, без новой backup-платформы |

Точные исходники: [M-Core], [M-Analysis], [M-Connector], [M-ConnectorRuntime], [M-Documents], [M-Models], [M-Orchestration], [M-Catalog].

**Критичный implementation detail:** `DirectoryConnectorDurabilitySessionBroker` сейчас создаёт `PostgresDailyStatisticsCommitter`. Добавление JSON schema нового envelope без изменения этой сборки оставит принятые source batches без canonical apply. Менять dispatch и его регистрацию нужно в одной поставке. [M-ConnectorRuntime]

## 3. Сквозной путь и границы

1. Source adapter читает ограниченную порцию из явно настроенного website/TG/VK источника.
2. Он отправляет сырой source revision в existing connector landing.
3. Source committer в PG сохраняет полный текст, source identity/provenance и следующий capture cursor; ставит work items в той же canonical transaction.
4. Детерминированный chunker создаёт поисковые чанки; BGE worker дополняет их embeddings. Raw и FTS доступны независимо от LLM quota.
5. Novelty worker получает revision и найденные предыдущие материалы региона, сравнивает факты и возвращает один законченный структурированный результат: новизна, значимость, рубрики, evidence, known/uncertain.
6. Master сохраняет полный `analysis.result`, обновляет read projection и очередь. Result retry читает сохранённую запись, не запускает модель снова.
7. MCP возвращает найденные карточки и coverage за окно. Private Dataset checkpoint подтверждает долговечность в рамках готового RKB/MDH lifecycle.

**Новизна оценивается относительно наблюдённого корпуса с явным watermark.** «Не найдено раньше» не становится утверждением «этого никогда не было». Не делаем обязательными graph/entity resolution, два LLM judge, внешнюю публикацию, полный crawler всего интернета или отдельный UI до первой карточки.

Сохранять один полный текст в raw и производные chunk/search projections допустимо. Existing ограниченный recovery journal на Dev/control plane сохраняет exact payload и outcome bytes, необходимые для replay, с предусмотренными сроками и границами доступа. Это допустимая восстановительная копия, а не вторая canonical БД или query corpus. Дополнительные копии raw для поиска на controller, в отчётах и heartbeat не создаются. Media — ссылки и метаданные, без постоянного image archive.

## 4. Конкретные модули и ответственность

Все новые пути ниже — **проектные**, на изученном SHA их ещё нет.

| Изменение | Назначение и граница |
|---|---|
| `src/my_data_hub/workloads/regional_monitoring/contracts.py` | Pydantic `SourceMaterialV1`, `ContentChunkV1`, `NoveltyAssessmentV1`, query/coverage models; extra=forbid, время с timezone |
| `.../sources.py` | Чтение source registry/config; canonical source identity, cursors, due selection; без собственного scheduler engine |
| `.../adapters/web.py`, `telegram.py`, `vk.py` | Получить текст/дату/URL/source revisions, bounded pagination, edit/deletion observations; transport errors отдельно от пустой выдачи |
| `.../capture.py` | Создать existing `ConnectorEnvelope`, submit/readback, удержать повторяемую порцию до существующего durability receipt; без SQL в source worker |
| `.../postgres.py` | `SourceMaterialCommitter`, accepted analysis apply, novelty read projection. Все canonical writes на master и existing revision/outbox boundary |
| `src/my_data_hub/connectors/committers.py` | Новый маленький `data_product → committer` dispatcher; зарегистрированные типизированные реализации, не dynamic import из payload |
| `connectors/runtime.py`, `durability.py` | Dependency injection dispatcher вместо жёсткого daily-statistics committer; existing statistics routes остаются |
| `.../chunking.py` | Детерминированные chunks/offsets с версией policy; без inference |
| `embeddings/content_documents.py`, `content_importer.py` | Content representation и применение chunk vectors с existing model identity/worker delivery |
| `.../retrieval.py` | PostgreSQL FTS + BGE candidates; предыдущий corpus snapshot, exact duplicate/repost signals, независимость источников |
| `.../novelty.py` | Один смысловой ответ по source + prior evidence; typed verdict и evidence validation, без скрытого второго judge |
| `.../runtime.py` | Thin adapter shared Kaggle/OpenCode runtime: dispatch, save, resume, quota recovery. Без локальной копии provider/WSS/updater |
| `.../service.py`, `reader.py` | Bounded product operations/read queries, region scope, pagination, coverage |
| `mcp/catalog.py`, `contracts.py`, `policy.py`, `scopes.py`, `postgres_broker.py` | Новые read tool names/schemas и delegation в reader; existing broker выдаёт роли и epoch session |
| `config/pipelines/regional-monitoring.v1.json` | Capture → chunks/embedding → novelty stages, версии, timeouts, retry categories; existing orchestrator |
| `schemas/regional-monitoring/source-material.v1.schema.json`, `novelty-assessment.v1.schema.json` | Версионированные boundary schemas, включённые в wheel |
| `docs/operations/regional-monitoring.md` | Запуск, actual statuses, source config, quota recovery, Dataset continuity и запрос результата |

Точные пути registration/migration tests выбирает исполнитель в текущей структуре, не создавая параллельный framework. Роли и SQL являются инженерной задачей. Не отдавать обычному OpenCode worker owner/superuser, DDL, BYPASSRLS или server-file privileges.

## 5. Append-only миграции

На изученном HEAD последние canonical SQL migrations — `0022_region_talk_stable_project_identity.sql`. Предлагаемые номера 0023–0025 резервировать при реализации **после fresh HEAD check**; при чужой новой migration использовать следующие свободные последовательные номера. Исторические 0001–0022 не редактировать. Control-plane migrations не создают canonical/query raw corpus; существующий bounded exact-payload recovery journal сохраняется.

### 0023_regional_monitoring_capture.sql

- `monitoring.source`: стабильные `source_id/source_key`, platform, nullable existing `hub.external_account.account_id`, canonical source URL, region, enabled/config, optional source-family reference. Это подписка monitoring, не новая копия identity человека.
- `monitoring.source_cursor`: source_id + partition, cursor/watermark, last successful capture, next_due_at и observed outcome. Queue/lease остаются в `orchestration.work_item`.
- `hub.content_revision`: revision_id, existing content_id, source_id, source_external_id, source_revision_key, полный `body_text`, title/language/source URL, published/observed timestamps, необходимые raw metadata и media refs. Natural uniqueness `(source_id, source_external_id, source_revision_key)`.
- Register новый data product `regional-monitoring.source-material.v1` и его bounded schema в существующей integration registry; существующие counters не переименовывать.
- Одна функция/committer transaction принимает сохранённый batch, разрешает exact content identity, сохраняет revision и provenance, обновляет current content projection, ставит work items и следующий cursor, фиксирует canonical revision/semantic outbox.

Один provider post имеет один logical `content_id` и несколько revisions при правке. Повтор с тем же revision и содержимым — exact replay; тот же external revision с другим payload — conflict/quarantine, не silent overwrite. Другой источник с тем же новостным текстом остаётся своим source evidence; cross-source repost не сливается в одну identity.

`hub.project` — consumer/application project. Не записывать туда строительство дороги, постановку или региональную инициативу как если бы существующая таблица была semantic project graph.

### 0024_content_chunks_and_retrieval.sql

- `hub.content_chunk`: chunk_id, content_revision_id, chunk_policy_version, ordinal, character offsets, chunk text, derived text hash. Unique `(revision_id, chunk_policy_version, ordinal)`; offsets привязаны к сохранённому тексту.
- GIN по русскому chunk tsvector, bounded lookup по revision; existing content FTS остаётся для совместимости.
- `analysis.content_chunk_embedding`: chunk_id, existing model_id, vector(1024), encoder contract/input fingerprint, created_at, unique exact chunk/model/encoder input. Индекс pgvector создаётся по существующему стилю, tuning следует фактическому объёму.
- Additive nullable `analysis.result.content_revision_id` FK для новых content analyses; старые blogger и legacy result rows валидны.
- Если одновременно меняются wire schemas existing embedding worker, ввести новую content representation/contract version, а не подставить article в `blogger_compact_v1`.

BGE model contract уже определён: `BAAI/bge-m3@5617a9f61b028005a4858fdac845db406aefb181`, dense 1024, L2, native pooling, max 8192 tokens. Этот существующий факт и базовый model contract сохраняются неизменными. **Новый content representation v1 получает отдельный encoder-input contract с полным фактическим input ≤512 tokens, включая special tokens и любой prefix.** Chunker считает токены tokenizer выбранной модели после добавления overhead; query input этого представления подчиняется тому же пределу. Representation/input-contract version входит в identity проекции и cache, не меняя молча старые blogger embeddings. E5 и BGE не смешиваются в одном distance comparison. [M-Models]

### 0025_regional_novelty.sql

- Полный immutable assessment хранится в существующей `analysis.result`, `result_kind='regional_novelty_v1'`; не создавать вторую универсальную result DB.
- `monitoring.novelty_item` — read projection: item_id, source revision, accepted result_id, region, verdict, significance category, first detected time, source published time, corpus comparison revision/watermark, nullable related prior item. Нужны индексы region/time и result identity.
- `monitoring.novelty_evidence` — ссылки item → конкретные content revisions, relation (`primary`, `prior`, `independent_support`, `repost`, `contradiction`), span/attribution. Нельзя считать три репоста тремя независимыми источниками.
- `monitoring.accept_novelty_result(...)` либо эквивалентный master committer проверяет references/input revision/work lease и фиксирует full result + projection + work completion/outbox транзакционно. Worker не пишет напрямую в canonical tables.
- Узкие SQL views/functions для bounded read/query; grants выдаются в existing role contract, не всем authenticated clients.

Избежать глобального semantic topic-ID, основанного только на строке заголовка. Для пилота достаточно exact prior evidence links и объяснённого relation. Полное объединение событий/людей/проектов можно добавить позднее без переписывания raw revisions.

## 6. Контракты входа и результата

### SourceMaterialV1 внутри существующего envelope

```text
source_id, platform, source_external_id, source_revision_key
canonical_url, published_at?, observed_at, source_cursor_after?
title?, body_text, language?, author_account_ref?
media_refs[], repost_of?, raw_metadata
```

Envelope уже несёт connector_id, data_product, batch_id, idempotency_key, schema_version, observed_period, source_cursor и payload digest. **Не дублировать цепочку receipt/hash на каждом внутреннем слое.** Existing transport integrity используется на границе; внутри PG достаточно FK, uniqueness, transaction и typed semantics.

При `published_at` неизвестном API возвращает null + observed time. Deleted/unavailable observation не стирает ранее сохранённый текст. Cursor подтверждается только вместе с accepted canonical change; окончательная долговечность следует existing `DURABLE_COMPLETE`, а не факту HTTP success.

### NoveltyAssessmentV1

```text
input_revision_id, region_key, policy_version
comparison_corpus_revision/watermark
verdict: new_significant | known | not_significant | uncertain
what_changed, why_significant, significance_category
primary_evidence[{revision_id, span_start, span_end, attribution}]
prior_evidence[{revision_id, relation, explanation}]
topics[], unresolved_mentions[]
model/provider actual identity, prompt_version
```

Это proposed schema. Статус transport/pipeline `failed_retryable` не кодируется как semantic `known`. `uncertain` означает, что ответ модели получен, но evidence не позволяет вынести положительный смысловой вывод. Необработанная стадия остаётся pending. Не требуется обязательная повторная модель для отрицательного novelty verdict: двухмодельная политика владельца касается EventsBot no-event.

Проверка references — локальная: ссылки относятся к доступным revisions, span существует в сыром тексте, prior evidence действительно входило в comparison set, region scope не притворяется фактом. Это не второй LLM judge. Модельные данные/текст источника не могут сами выдать worker дополнительные права или поменять instruction scope.

Existing `worker_result_inbox` содержит полный envelope и ограничение `UNIQUE(stage_run_id, input_manifest_sha256)`, а `stage_run` — `UNIQUE(run_id, stage_id)`. Поэтому **не переиспользовать один stage_run для всех отдельных source units и всех retries**. Конкретная простая раскладка пилота: capture run относится к source portion, после raw commit каждая content revision получает отдельный content-processing run с chunk/embed/novelty stages. Это позволяет сохранить законченный результат каждой единицы через существующий inbox, не дожидаясь всего source batch. Новый input после изменения revision/policy получает новый run; сетевой повтор — прежнюю result identity. Не drop constraint ради записи нескольких неразличимых результатов и не создавать новый workflow engine.

## 7. Retrieval и правило значимости

Для каждого нового revision:

1. Exact identity/revision replay завершается до модели. Content hash и repost links исключают повтор уже принятого текста, сохраняя новый источник.
2. Retrieve prior corpus по региону/времени через FTS и BGE; текущее source revision не попадает в «предыдущие» материалы. Previous revision этого же post проверяется отдельно.
3. Передать модели source facts, релевантные prior excerpts с revision IDs и явный scope. Длинный текст доступен через bounded чтение chunks; нельзя молча обрезать важный хвост.
4. Один ответ определяет, что именно стало новым/изменилось и чем это значимо; рубрики извлекаются тем же вызовом.
5. Для `new_significant` сохранить explanation и первичный источник; для известных/неважных/uncertain сохранить full assessment, чтобы recovery не повторял ту же работу.
6. Повторное появление существующего факта не создаёт новую карточку. Существенная правка создаёт новый assessment с relation к прежнему; cancellation/correction не должна оставлять старую карточку без видимого уточнения.

Счётчик независимых свидетельств строится по источнику происхождения и explicit repost chain; при неизвестной зависимости возвращать uncertainty. Более высокий similarity не равен identity. Пресс-релиз на пяти сайтах — одно происхождение, пока нет свидетельств независимого подтверждения.

## 8. API и полезные запросы

Добавить к existing MCP catalog, OAuth/scopes и master broker; бизнес-данные не проходят через новую внешнюю БД.

| Новый tool | Вход | Выход/acceptance |
|---|---|---|
| `monitoring.novelty.list` | region_key, since/until, significance filters, limit, opaque cursor | items + next_cursor + coverage; сортировка detected_at/item_id стабильна, область caller проверена |
| `monitoring.novelty.get` | item_id | что новое, почему значимо, полный assessment lineage, primary/prior/source links, актуальные уточнения |
| `monitoring.status` | region_key, time window | capture watermark и analysis watermark отдельно, pending count/oldest age, quota retry state, embedding coverage, existing durability status |
| `content.get` | content_id или revision_id | сохранённая версия текста, provenance, timestamps, media refs; длинное тело с cursor/offset |
| `content.search` | query, region/time, mode=lexical/semantic/hybrid, limit/cursor | релевантные excerpts и exact revision/source links; возвращается actual mode и неполное vector coverage |

Предлагаемые scopes — `monitoring:read`, `content:read`; authenticated principal не получает all-region доступ автоматически. Internal workers используют existing role-bound master credentials и connector scopes. Конфигурация sources/roles и применение migrations остаются engineering/admin plane; не добавлять неограниченный SQL tool в обычный MCP.

**Query acceptance:**

- «Что нового за последние шесть часов?» возвращает только `new_significant` выбранного региона, объяснение и источники. Если за окно часть корпуса не обработана, ответ явно partial.
- «Почему этот материал новый?» показывает конкретный delta к связанным prior revisions либо отсутствие наблюдённого аналога с corpus boundary, не пустое «уникально».
- «Найди сообщение, где впервые объявили изменение» возвращает earliest **observed source evidence**, не утверждает глобальное первенство.
- «Что изменилось после исправления сообщения?» показывает старую и новую immutable revisions и актуальный assessment.
- В режиме lexical corpus доступен при отсутствии BGE/LLM; semantic не выдаёт FTS за vector search.

Публичная публикация в Telegram/VK и draft в EventsBot не входят в первый срез. При последующей интеграции отдавать typed accepted fact/source references, а не передавать MDH полномочия публикации.

## 9. Планирование и recovery без второго движка

Зарегистрировать стадии `capture`, `chunk`, `embed`, `novelty` в existing orchestration. Natural dedupe: source/partition/input cursor для capture; revision + policy для derived work. Использовать existing `available_at`, `lease_owner`, `lease_token`, `lease_expires_at`, `result_ref`, dependencies; runtime generation связывается с этими полями, не заменяет master epoch.

Предлагаемые стартовые параметры пилота, **не наблюдённые метрики и не новые решения владельца**:

| Параметр | Начальное инженерное значение | Зачем |
|---|---|---|
| Source capture interval | 1 час, per-source override | Оставляет запас до ориентира шести часов, без обещания обработки при отсутствии compute |
| Capture portion | До 50 records; стартовый raw budget ≤512 KiB, затем обязательная проверка полного encoded `ExactCommand` ≤1 MiB | Hard per-command limit профиля РКБ строже default 2 MiB connector envelope; порция уменьшается по фактическому размеру wire-команды с metadata и encoding overhead |
| Content chunk/input policy v1 | Tokenizer-aware полный input ≤512 tokens, включая special tokens/prefix; корректные offsets | Границы и overlap выбираются по контексту в пределах общего лимита; обязательного фиксированного overlap нет, длинный документ покрывается последовательностью chunks |
| OpenCode concurrent executors MDH pilot | 1, configurable shared admission | Сначала доказать путь, затем масштабировать на несколько endpoints без fork |
| Fresh recovery executor | Новый attempt/execution после quota exhaustion | Не переиспользовать исчерпанную сессию; не запускать бесконечные notebooks при повторном общем refusal |

Большой single record не обрезается: он доставляется bounded landing parts с replay и последующей полной сборкой revision либо через применимый durable artifact path, сохранность которого переживает потерю executor. Каждая фактически закодированная `ExactCommand`, включая поля envelope и overhead, должна занимать не более 1 MiB; стартовый raw budget 512 KiB сам по себе этого не доказывает. Ссылка на файл исчезнувшего Kaggle kernel не является durable handoff. Landing parts/replay material сохраняются предусмотренным recovery lifecycle; cursor подтверждается после полной canonical сборки, а не после получения одной части. Telegram/VK pagination ограничивается source cursor и разрешениями. Sources берутся из существующего явно разрешённого списка; новый monitoring не запускает бесконтрольный discovery.

Recovery:

- **ACK lost:** lookup existing acceptance/result по прежним IDs; не генерировать новый batch/inference.
- **Free quota exhausted:** немедленно сохранить законченные outputs на master, записать continuation/тип отказа, завершить или reconcile старый executor, начать свежий Kaggle executor и resume. При account/global cooldown новый физический executor не получает обходной бюджет; повторный refusal возвращает retry-later.
- **Kernel submit/status UNKNOWN:** exact reconciliation; stale worker не двигает cursor, shared credentials не используются конкурентно.
- **PG master недоступен:** existing master resolver/ensure/recovery; не создавать PostgreSQL на controller или обычном executor.
- **Полная потеря master:** восстановление из current/previous verified private Dataset по профилю РКБ. Не вводить внешнее хранилище в этот проект. Не объявлять ещё не подтверждённый checkpoint нулевой потерей.
- **Новый текст/изменённая policy:** новая input identity и только зависимые derived stages. Уже пригодные raw/embeddings/facts не повторяются без изменения входа.

Для незавершённой работы ограничивать накопление непереданных результатов. Короткие завершённые порции предпочтительнее долгого turn, итог которого существует только в summary executor. Независимый producer/controller не удаляет единственную replay-копию до предусмотренного существующим lifecycle подтверждения durability.

## 10. Зависимости, объём и данные

Existing Python base: >=3.12; FastAPI/aiohttp/httpx, Pydantic, psycopg, pgvector PostgreSQL уже предусмотрены репозиторием. Shared runtime подключить exact version/artifact dependency; provider Kaggle SDK остаётся внутри выбранного provider adapter. Существующий optional `kaggle==2.2.4` не считать вечным pin: его совместимость проверяется с общей библиотекой.

Для TG/VK/Web нужны transport-specific extras только у capture worker. Не устанавливать Telethon, OpenCode, torch/transformers и модельные веса в лёгкий control plane по умолчанию. BGE использует существующий embedding worker; не загружать BGE в PG master ради каждого query. Query embedding cache может использовать уже имеющийся контракт, но model-space/input identity обязаны совпадать.

OpenCode устанавливается/обновляется общим executor bootstrap. No-key default означает отсутствие **provider keys**, а не анонимные WSS/PG endpoints. Key-required profiles принимают только явно заданные secret refs; нескольких aliases одного account недостаточно для независимых quota scopes.

Полный необходимый текст и source provenance — canonical data. Chunks, vectors, FTS, novelty projection — восстановимые derived data; полезные полные model assessments сохраняются для reuse. Изображения — ссылки. Verbose logs/heartbeat имеют retention, не занимают business tables бесконечно.

Формула планирования: `N_chunks × 1024 × 4 bytes` только на dense coordinates BGE; 1 млн векторов ≈4.096 GB без индексов/строк. Это расчёт, не production measurement. Цель **менее 100 ГБ относится к активной БД**: raw, revisions, chunks, embeddings, индексы и служебные таблицы; внешние backup-копии в этот размер не входят. Рабочий disk budget отдельно включает WAL/temp/backup staging и reserve. Capacity private Dataset для current/previous verified checkpoints — отдельный инженерный бюджет готового RKB profile. Внешние копии/backup-сервисы остаются вне текущего проекта. Не обещать пригодность online БД близко к 100 ГБ без фактического disk/checkpoint budget.

## 11. Что удаляется и что сохраняется

| Объект | Действие |
|---|---|
| Новая копия Kaggle client/WSS/updater внутри MDH workload | Не создавать; thin adapter импортирует общий package |
| Жёсткая сборка только `PostgresDailyStatisticsCommitter` | Заменить bounded registry/dispatcher; сохранить сам working statistics committer |
| Использование `blogger_compact_v1` для статей | Не расширять семантику молча; добавить content representation |
| Монолитный full-session import после notebook COMPLETE | Новый путь делает incremental result commit; старый compatibility path удалять только после миграции его реальных потребителей |
| Дополнительная quota БД в MDH/runtime | Не создавать; существующая ARC authority |
| Generic duplicate results/workflow/receipt framework | Не создавать; existing analysis.result/orchestration/connectors |
| Existing Region Talk/bloggers schemas и paused state | Сохранить; новый первый срез не требует их reimplementation |
| Exact imported source и historical migrations | Сохранить неизменными |
| External backups/platform | Вне текущей поставки; использовать готовую RKB Dataset схему |

Фактические obsolete files удаляются после импорта всех callers на shared dependency; нельзя удалить recovery-path только из-за наличия нового skeleton.

## 12. Последовательность поставки и доказательство результата

### A. Raw + один настоящий источник

Добавить 0023, contracts, committer dispatcher и один source adapter; после этого MCP `content.get/search` должен прочитать полный сохранённый материал. Затем подключить ещё два adapter тем же envelope, не тремя разными apply. Evidence: exact source revision → batch → PG revision → existing Dataset receipt → чтение после master restore. Использовать готовый RKB durability profile без нового external backup проекта.

### B. Retrieval + первая novelty карточка

Добавить 0024/0025, chunks/BGE, retrieval, single semantic assessment и MCP list/get/status. Принять один end-to-end настоящий региональный material, получить объяснённую novelty карточку со ссылками. Наличие Markdown, model string или unit tests не заменяет это наблюдение.

### C. Независимое исполнение и quota recovery

Подключить общий runtime dependency, cold start на RunCoveer без DevCoveer, сохранить завершённую стадию, заменить executor при quota отказе и продолжить с тем же job. Проверить несколько endpoints/worker identities, хотя стартовая concurrency пилота может быть 1. OpenCode actual version и artifact проверяются на выбранном executable; автоматическое обновление не меняет активный attempt посередине.

### Конкретные проверочные сценарии

| Сценарий | Принимаемый результат |
|---|---|
| Одинаковая порция доставлена дважды | Один canonical raw revision, прежний receipt, один work item |
| Потеря ACK после analysis commit | Readback возвращает full saved result; inference не повторён |
| Изменился текст существующего post | Две immutable revisions, current projection новая, ранее принятое evidence доступно |
| Пять копий одного пресс-релиза | Одна содержательная novelty, origin/reposts видны; нет фиктивных пяти независимых подтверждений |
| Старый факт впервые прочитан | Вердикт и объяснение учитывают observed corpus boundary; не «впервые в мире» |
| Известный проект получил существенное обновление | Новая карточка с delta и prior refs, а не blanket duplicate |
| Существенный факт в хвосте длинного текста | Найден через chunk/полный текст, tail не потерян |
| LLM/embedding недоступны | Raw/FTS доступны, backlog и freshness partial видны |
| Free-quota отказ | Свежий executor, сохранённые stages reused, PG master не перезапущен; account cooldown не сброшен |
| Старый worker после takeover | Immutable evidence/receipt можно принять/читать, cursor/domain effects запрещены |
| Fresh master из verified Dataset | Те же принятые raw/results/query output в пределах checkpoint, без внешнего backup |
| Consumer или region scope чужой | Доступ запрещён; отсутствие DDL/owner прав у обычного worker сохранено |

Fixtures должны быть frozen обезличенными/разрешёнными source packets с ожидаемыми **различающими** outcomes, не тестами, повторяющими код. Начальный набор включает website/TG/VK, correction, repost, long text, unknown timestamp, quota/ACK loss. Новые реальные sources/model calls выполняются исполнителем только в явно разрешённом runtime этапе; эта постановка их не выполняла.

Перед source-ready выполнить обязательные AGENTS gates (`compileall`, `pytest`, repository/schema/notebook validation, contiguous migrations и архитектурные запреты local PG). Интеграционную приёмку ограничить указанными изменёнными границами. Не делать реализацию гостей/всего entity graph/всех trends дополнительным gate первого novelty.

Полезные наблюдаемые поля первого результата: capture lag, analysis lag, oldest pending, vector coverage, age of verified Dataset checkpoint, actual OpenCode version. Значений production-метрик здесь нет: они не измерялись. Не добавлять новую observability-платформу ради этих полей.

## 13. Источники текущего MDH

[M-Agents]: https://github.com/onedayonemasterpiece/my-data-hub/blob/77a62c53c881120164f525a226ec296dcb3c77b3/AGENTS.md
[M-Core]: https://github.com/onedayonemasterpiece/my-data-hub/blob/77a62c53c881120164f525a226ec296dcb3c77b3/sql/migrations/0002_core_content.sql
[M-Analysis]: https://github.com/onedayonemasterpiece/my-data-hub/blob/77a62c53c881120164f525a226ec296dcb3c77b3/sql/migrations/0003_analysis.sql
[M-Orchestration]: https://github.com/onedayonemasterpiece/my-data-hub/blob/77a62c53c881120164f525a226ec296dcb3c77b3/sql/migrations/0004_orchestration.sql
[M-Connector]: https://github.com/onedayonemasterpiece/my-data-hub/blob/77a62c53c881120164f525a226ec296dcb3c77b3/src/my_data_hub/connectors/postgres.py
[M-ConnectorRuntime]: https://github.com/onedayonemasterpiece/my-data-hub/blob/77a62c53c881120164f525a226ec296dcb3c77b3/src/my_data_hub/connectors/runtime.py
[M-Documents]: https://github.com/onedayonemasterpiece/my-data-hub/blob/77a62c53c881120164f525a226ec296dcb3c77b3/src/my_data_hub/embeddings/documents.py
[M-Models]: https://github.com/onedayonemasterpiece/my-data-hub/blob/77a62c53c881120164f525a226ec296dcb3c77b3/src/my_data_hub/embeddings/models.py
[M-Catalog]: https://github.com/onedayonemasterpiece/my-data-hub/blob/77a62c53c881120164f525a226ec296dcb3c77b3/src/my_data_hub/mcp/catalog.py

