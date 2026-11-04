---
schema_version: 1
idea_id: research-20261104-my-data-hub-shared-kaggle-runtime
title: "My Data Hub: региональный мониторинг и общий Kaggle/WSS/OpenCode runtime"
context_id: portfolio.inbox
idea_kind: research
status: proposed
created_at: 2026-11-04T13:13:40Z
analysis_status: complete
intake_session_ids:
  - research-20261104-my-data-hub-shared-runtime
source_event_ids:
  - slack-C0CEC3KQ80P-1793792133.508289
  - slack-C0C9LS6L3J8-1792275502.136979
  - voice-20261104-131144-7d36ec0a
  - voice-20261104-132920-2bb6d7ff
  - voice-20261104-133619-918e7805
  - voice-20261103-135543-e9d9a1aa
  - voice-20261029-191818-995cc4a1
---

# My Data Hub: региональный мониторинг и общий Kaggle/WSS/OpenCode runtime

Дата анализа: **4 ноября 2026 года, Europe/Kaliningrad**. Поручение: изучить исходные требования и разные проекты, подготовить общий дизайн, записать выводы в IdeaHub и my-data-hub. Реализация продукта, обновление установленного OpenCode, запуск Kaggle, новые модельные вызовы и deployment в этот объём не входят. Запись аналитических документов поручена явно.

## 1. Главный вывод

**My Data Hub стоит развивать как накопительную региональную базу на существующей PostgreSQL-модели. Общий код управления Kaggle, WSS и OpenCode следует оформить отдельным версионированным runtime-пакетом с тонкими адаптерами продуктов.** РКБ — основной донор конкретных механизмов транспорта и жизненного цикла. EventsBot — донор захвата источников, идемпотентного импорта и reconciliation Kaggle. Сам My Data Hub уже содержит полезные контракты данных, PostgreSQL, миграций и checkpoint. My-daep определяет отдельный сценарий распределённой разработки и восстановления кода.

Это рекомендация анализа, а не сообщение о созданной библиотеке. Факт переиспользования должен означать один поддерживаемый исходник и зависимость потребителя от его версии; копирование похожих файлов по четырём репозиториям проблему не решит.

Общий код допускает независимые экземпляры controller на RunCoveer и DevCoveer. Продукты сохраняют свои расписания, источники, БД, права, предметные решения и результаты. Общая квотная authority уже существует в `ai-resource-control`: её клиент подключается к runtime, новый независимый limiter не нужен. Разные controller не должны владеть одним kernel или одной эксклюзивной Telegram-сессией одновременно.

## 2. Источники и границы доказательств

Прочитаны оба указанных Slack-треда целиком, три полные расшифровки 4 ноября, дополнение Ultra от 3 ноября, голосовое о совместном runtime от 29 октября, исследование 30 октября и прежние owning-документы My Data Hub/Миры. Выжимки голосовых сверены с расшифровками: модельные разделы «Решения» сами по себе не трактовались как новые решения владельца.

| Источник | Проверенная исходная версия / наблюдение | Что это доказывает |
|---|---|---|
| `regional-knowledge-base/main` | `0b7590628dcb29a270727f3920607084dde30720` | Механизмы в конкретном исходном коде; не автоматическое доказательство deployment каждого файла |
| `events-bot-new/main` | `79f8b43374dd9b4b531533d7145f4c04974e61b5` | Текущие адаптеры, scheduler, parser, Kaggle и Live reuse |
| Ultra PR747 | OPEN, `merged=false`, head `406826d9cae0b51e229dc1f28d9fdd4683a4f31f` | Проект внедрения; новая реализация не считается находящейся в main |
| `my-data-hub/main` | `91f7d9bfb077df4d81c5604ae695fa32aa0f428c` | Существующая схема и код control/data plane |
| My Data Hub `platform.status` | Повторное чтение 04.11 в 15:20:47 Europe/Kaliningrad: `control_plane_ready=true`, `master_state=ABSENT`, `capabilities=[]`, заявленный `deployed_commit=f7eb3c5f674b772751c815aa055a52f1f52e1828` | Доступен контрольный сервис; текущий вызов не подтверждает работающий PostgreSQL master или готовую базу мониторинга. Commit — поле ответа endpoint, а не независимая проверка импортированного пакета |
| `my-daep/master` | `d3559d731bb74fbb1262114b9aad15687bb54701` | README, два проектных документа, sample CSV и gitignore; собственного исполняемого runtime в этом дереве нет |
| `live-interaction/main` | `e710e8e8143a3724d16250a6e439ccd178c279ca` | Образец общей библиотеки и границы адаптера; не готовый протокол задач Kaggle |
| `ai-resource-control/main` | `f359f6b6326f4ee11f851b3d451adf210e5259dd` | Существующий контракт `WorkloadAdmission` для OpenCode/GigaChat/native Codex; rollout каждого consumer проверяется отдельно |
| `idea-hub/main` при чтении | `5a829bd8b635dfa2cab6ac634cb201eab318b230` | Исходные записи и предыдущие решения; итоговый commit аналитики будет более новым |

Локальный checkout My Data Hub на DevCoveer находился на другой feature-ветке. Он не объявлен актуальным main и не изменялся. В этом анализе не выполнялись benchmark, fault injection, восстановление базы, новая приёмка production или проверка provider quota путём inference. Ниже «есть в коде», «наблюдалось через endpoint», «требует реализации» и «рекомендовано» различаются.

Основные первичные требования: [тред My Data Hub](https://mira-projects.slack.com/archives/C0CEC3KQ80P/p1793792133508289), [тред Ultra](https://mira-projects.slack.com/archives/C0C9LS6L3J8/p1792275502136979), [мониторинги и тренды](https://github.com/onedayonemasterpiece/idea-hub/blob/5a829bd8b635dfa2cab6ac634cb201eab318b230/inbox/voice/2026/11/voice-20261104-131144-7d36ec0a.md), [хранение и ресурсы](https://github.com/onedayonemasterpiece/idea-hub/blob/5a829bd8b635dfa2cab6ac634cb201eab318b230/inbox/voice/2026/11/voice-20261104-132920-2bb6d7ff.md), [сущности, SQL, POI и объём](https://github.com/onedayonemasterpiece/idea-hub/blob/5a829bd8b635dfa2cab6ac634cb201eab318b230/inbox/voice/2026/11/voice-20261104-133619-918e7805.md).

## 3. Нормализованные требования владельца

### 3.1. My Data Hub

| ID | Требование | Следствие для дизайна |
|---|---|---|
| MDH-01 | Единая накопительная база разных мониторингов сайтов, Telegram и VK | Источник сохраняется независимо от будущего редакционного использования |
| MDH-02 | Прямые коннекторы, включая Telethon и VK | Общий транспорт не превращается в ещё один обязательный сервис чтения соцсетей |
| MDH-03 | PostgreSQL на Kaggle, долговременное сохранение в private Dataset; опора на РКБ | Один writable primary, восстановление из конкретной версии checkpoint |
| MDH-04 | Переиспользовать запуск, выключение, продление и правило 37 минут РКБ | Механизм полезной активности подтверждён кодом; число 37 сохраняется как требование владельца, текущая числовая конфигурация в аудите не установлена |
| MDH-05 | Наполнение через MCP и через OpenCode/ноутбуки | Оба входа используют одни предметные процедуры и natural keys |
| MDH-06 | Хранить сырую смысловую информацию и первичные рубрики | Полный необходимый текст и ссылка сохраняются раньше необязательных аналитических стадий |
| MDH-07 | Обязательная векторизация BGE, лексический и смысловой поиск, чанки длинных текстов | Векторизация может догонять сохранённый корпус; её очередь и неполный охват должны быть видны |
| MDH-08 | Суточные/недельные региональные тренды по независимым источникам | Репосты и общая новостная рассылка не считаются множеством независимых свидетельств |
| MDH-09 | Общественно/регионально значимое новое выявлять примерно в пределах четверти суток | Цель свежести до 6 часов; требуется разрешить конфликт с полной недоступностью бесплатного inference |
| MDH-10 | Мнения гостей региона, с подтверждением, что автор приезжий | Хранить основание и время применимости guest-classification; неизвестность не превращать в положительный факт |
| MDH-11 | Личности, организации, реальные проекты и места; связи с публикациями и между сущностями | Не смешивать автора, упомянутого человека, инициатора проекта и владельца аккаунта |
| MDH-12 | Несколько аккаунтов одной личности на разных площадках | Общая identity и независимые внешние идентификаторы аккаунтов |
| MDH-13 | Организации не обязательно юридические лица | Поддерживать коллективы и неопределённый subtype без обязательного поиска юридических реквизитов |
| MDH-14 | Перед созданием искать существующую сущность, учитывать похожие названия | Exact IDs/aliases → лексические и векторные кандидаты → обоснованное связывание; similarity не равна тождеству |
| MDH-15 | Сводки по человеку или проекту за период, включая слова, действия, анонсы и обещания | Источник, авторство/атрибуция и временная привязка хранятся отдельно от суммаризации |
| MDH-16 | Связь с POI Street Story и передача новых фактов | Street Story сохраняет authority POI; MDH хранит ссылки, evidence и нужную поисковую проекцию |
| MDH-17 | Ссылки на изображения, без постоянного хранения самих изображений | Media refs не требуют media archive; недоступная позже ссылка не означает, что изображение сохранено |
| MDH-18 | Расширяемая схема, миграции, хранимые процедуры, возможность SQL на Kaggle без полной выкладки | Требует отдельного решения о границе operator/DDL; нынешний remote MCP DDL запрещает |
| MDH-19 | Точность и качество важнее предельной скорости | Семантические решения проверяются по источнику; SLO РКБ не копируется автоматически |
| MDH-20 | Доверять PostgreSQL; не плодить проверки текста и служебные квитанции | Сохранить транзакцию, уникальность, authority и внешнюю durability; убрать повторные проверки одной записи на каждом слое |
| MDH-21 | Долгосрочно база менее 100 ГБ | Считать текст, векторы, индексы, версии и служебные данные; отдельно учитывать резервные копии и диск runtime |
| MDH-22 | Обработка по расписанию и доступности бесплатных моделей | Завершать полезные порции, сохранять continuation и повторять позже; окончание одного attempt не обнуляет job |
| MDH-23 | OpenCode должен оставаться актуальным вообще | Общий постоянный механизм обновления всех используемых установок, а не разовый upgrade одного notebook |
| MDH-24 | Общее решение для взаимодействия Kaggle/WSS разных продуктов | Одна библиотека, версии, совместимость и адаптеры; исправление общей ошибки распространяется через зависимость |

### 3.2. Отдельные требования Smart Update Ultra

[Дополнение 3 ноября](https://github.com/onedayonemasterpiece/idea-hub/blob/5a829bd8b635dfa2cab6ac634cb201eab318b230/inbox/voice/2026/11/voice-20261103-135543-e9d9a1aa.md) требует: отрицательный вердикт слабой модели о наличии события подтверждает вторая модель; сложные составные расписания обрабатывает сильная роль/Google; writer выбирается по качеству, GLM назван прошлым кандидатом для проверки. Если вторая модель недоступна, исход остаётся pending, а не превращается в подтверждённое «события нет».

Эта политика принадлежит event-потребителю. Она не требует двух анализов каждого материала My Data Hub и не разрешает отбрасывать из его корпуса тексты, не являющиеся событиями. Для мониторинга людей, мнений и проектов такой текст может быть основным полезным источником.

Предположение владельца, что новый Kaggle IP восстановит бесплатные лимиты, нужно сохранить как гипотезу, а не как гарантию архитектуры. [Прежний DAEP recovery-контракт](https://github.com/onedayonemasterpiece/idea-hub/blob/5a829bd8b635dfa2cab6ac634cb201eab318b230/docs/my-daep-kaggle-zen-quota-recovery.md) уже прямо требует проверить доступность и запрещает бесконечное создание notebook при повторном quota-state.

## 4. Что реально можно переиспользовать

### 4.1. РКБ

**РКБ даёт наиболее конкретный набор доноров для общего runtime.** В `scripts/production/bge_wss_transport.py` имеются ATTACH/WELCOME, protocol version, run incarnation/server generation, ограниченные очереди, heartbeat, reconnect grace и корреляция результатов. В `bge_channel.py` находятся strict fields, Bearer, attach deadline, ограничения frames и повторная проверка claim перед отправкой; `kaggle_protocol.py` отвергает duplicate JSON keys, NaN/Infinity и невалидные/избыточные frames. Это реализация с BGE/evidence handler, из которой нужно выделить нейтральную часть. [Worker transport](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/0b7590628dcb29a270727f3920607084dde30720/scripts/production/bge_wss_transport.py#L415-L483), [server channel](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/0b7590628dcb29a270727f3920607084dde30720/src/regional_knowledge/bge_channel.py#L83-L190), [protocol parsing](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/0b7590628dcb29a270727f3920607084dde30720/src/regional_knowledge/kaggle_protocol.py#L441-L499).

Полезный bootstrap-приём: authenticated control поднимается до импорта Torch и загрузки модели. Notebook уже наблюдаем и управляем, но socket connection не объявляется готовностью BGE/PG. Reconnect grace позволяет пережить перезапуск controller без уничтожения тёплого runtime; недействительная аренда блокирует запись. Отмена клиентского ожидания не освобождает lock реального синхронного encode до его окончания. Для read-корреляции используются request ID и поколения connection/turn; один request ID недостаточен. Эти принципы переносятся на jobs с учётом различия между устаревшим ответом текущему клиенту и сохраняемым immutable результатом законченной попытки. [Bootstrap/defaults](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/0b7590628dcb29a270727f3920607084dde30720/scripts/production/bge_wss_transport.py#L47-L121), [resource ownership](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/0b7590628dcb29a270727f3920607084dde30720/scripts/production/bge_wss_transport.py#L170-L205).

**37 минут: требование владельца подтверждено источником, числовая текущая настройка — UNKNOWN.** В коде проверен `record_request_activity()`: полезное намерение пользователя продлевает активность; запись агрегируется до минуты, чтобы не брать SQLite-lock на каждое чтение. Idle-drain учитывает `useful_idle_seconds`, provider version, канал и ожидающего writer. В draining прежний канал нужен для завершения работы, checkpoint и STOP; новые serving/claims не выдаются. Истёкшая локальная lease сама по себе не доказывает прекращение notebook — reservation сохраняется до remote terminal proof. До переноса нужно установить фактическое значение конфигурации и точное событие начала 37-минутного окна. Подставлять сюда heartbeat, максимальный срок notebook или интервал checkpoint нельзя. [Lifecycle](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/0b7590628dcb29a270727f3920607084dde30720/src/regional_knowledge/bge_queue.py#L171-L265).

В `BgeQueue` уже есть durable startup demand и current/successor, чтобы новое пользовательское действие не требовалось после перезапуска controller. Однако singleton current/successor — политика serving РКБ, а не универсальная топология пула OpenCode. Общими должны быть lease, identity, fencing и reconcile, а число workers и размещение остаются настройкой профиля. [Current/successor и demand](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/0b7590628dcb29a270727f3920607084dde30720/src/regional_knowledge/bge_queue.py#L231-L283).

**Checkpoint-донор:** `kaggle_dataset_checkpoint.py` связывает capture с текущим writer binding, версиями PG/pgvector/PostGIS/model/tokenizer/protocol/schema; один refresh lock исключает параллельный capture. `saved_seq` означает подтверждённый native readback Dataset, а не PostgreSQL APPLIED. Перед STOP ожидается текущий checkpoint; retired writer не публикует новый. Default 30 минут между checkpoints и 15 минут quiet time не объясняют пользовательское число 37. [Checkpoint implementation](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/0b7590628dcb29a270727f3920607084dde30720/src/regional_knowledge/kaggle_dataset_checkpoint.py#L158-L260). Отчёт о восстановлении различает Dataset identity и environment pin и прямо ограничивает свои доказательства synthetic regressions: это полезный контракт, не проведённая данным аудитом live acceptance. [Restore report](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/0b7590628dcb29a270727f3920607084dde30720/docs/testing/verified-checkpoint-restore-20261027.md#L3-L22).

**BGE-контракт РКБ:** 1024 FP32 и полный input ≤512 tokens, закреплённый tokenizer, исполнение на Kaggle. Tokenizer считает special tokens без truncation/padding; это не лимит в символах. Фактическое соблюдение chunking во всех ingestion-путях данным аудитом не установлено. В общий контракт переносятся необходимые параметры совместимости, а не весь набор RKB-specific хеш-проверок каждого текста. [Repository contract](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/0b7590628dcb29a270727f3920607084dde30720/AGENTS.md), [tokenizer](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/0b7590628dcb29a270727f3920607084dde30720/src/regional_knowledge/kaggle_import_tokenizer.py).

В ядро не должны попасть `search_worker` как обязательная роль, история/источники/редакционная модель РКБ, Telegram bindings, evidence contracts, конкретный native-route registry, BGE/PG/PostGIS как обязательные зависимости каждого worker. Полная production-готовность общего извлечения и точный worst-case RPO в этом read-only анализе не измерялись.

### 4.2. EventsBot

**Сильные доноры:** `video_announce/kaggle_client.py`, `kaggle_registry.py`, `kaggle_status.py`, immutable VK source packet и общий Google AI admission. В текущем коде есть pre-push durable intent, exact reconciliation неизвестного запуска, run identity, dedup callback, sequence, heartbeat и leases. Но `KaggleClient` также содержит правила video events; переносить весь класс как универсальный SDK было бы неверно. [KaggleClient](https://github.com/onedayonemasterpiece/events-bot-new/blob/79f8b43374dd9b4b531533d7145f4c04974e61b5/video_announce/kaggle_client.py), [registry](https://github.com/onedayonemasterpiece/events-bot-new/blob/79f8b43374dd9b4b531533d7145f4c04974e61b5/kaggle_registry.py), [status](https://github.com/onedayonemasterpiece/events-bot-new/blob/79f8b43374dd9b4b531533d7145f4c04974e61b5/kaggle_status.py).

У Telegram и guide-пути повторяется последовательность подготовки Dataset, ожидания видимости версии, фиксации baseline/intent, push и получения output. Это конкретный кандидат на объединение в provider/bootstrap adapter. Источники, cursor и расписания останутся у каждого продукта.

В текущем registry job связан с `job_type:kernel_ref`, а reconciliation знает два конкретных Dataset. В общем multiworker runtime логические job/attempt/generation должны связываться с физическим provider execution: Kaggle account, kernel reference и наблюдаемой версией/идентификатором запуска. Повторное использование kernel reference не позволяет принять старый output за новый результат.

**Ultra ещё предстоит реализовать.** PR747 открыт; оба документа из Slack находятся в его head и отсутствуют в прочитанном main. Предложенные `ultra_runtime/`, `smart_update_prepared.py`, `smart_update_stage_store.py` не обнаружены в дереве main. Текущий `smart_event_update` не принимает полный готовый набор facts/writer/stage results; его facade может повторить подготовку. У обычного Telegram-пути сохраняется ожидание notebook output перед импортом. [PR747](https://github.com/onedayonemasterpiece/events-bot-new/pull/747), [проект Ultra](https://github.com/onedayonemasterpiece/events-bot-new/blob/406826d9cae0b51e229dc1f28d9fdd4683a4f31f/docs/architecture/smart-update-ultra-targeted-implementation.md), [существующий update](https://github.com/onedayonemasterpiece/events-bot-new/blob/79f8b43374dd9b4b531533d7145f4c04974e61b5/smart_event_update.py), [Telegram service](https://github.com/onedayonemasterpiece/events-bot-new/blob/79f8b43374dd9b4b531533d7145f4c04974e61b5/source_parsing/telegram/service.py).

Минимальное изменение продукта: сохранять полный завершённый stage payload сразу; legacy и Ultra preparation передают результат одному domain apply. Повтор доставки получает прежний результат commit и не вызывает модель снова. ACK записи результата и разрешение использовать факт — разные обязанности.

**Нельзя вынести все старые retry-ветки без разбора.** В `remote_telegram_session.py` при transient status error достаточно старая запись перестаёт блокировать новый run; это расходится с принципом, что неизвестный исход остаётся активным до terminal evidence/fencing. В отчёте это механизм в коде, а не доказанный production-инцидент. Аналогично текущий `conditionally_verify_source_decision` вызывает verifier только при contradiction facts, поэтому новое требование обязательной второй модели для слабого отрицательного ответа ещё требует изменения. [UNKNOWN path](https://github.com/onedayonemasterpiece/events-bot-new/blob/79f8b43374dd9b4b531533d7145f4c04974e61b5/remote_telegram_session.py#L202-L301), [verifier condition](https://github.com/onedayonemasterpiece/events-bot-new/blob/79f8b43374dd9b4b531533d7145f4c04974e61b5/source_parse_contract.py#L935-L945).

### 4.3. My Data Hub

Платформа уже содержит `hub.actor`, `external_account`, `content_item`, `content_identity`, `content_author`, `content_asset`, `entity_alias`; отдельные analysis/search projections, миграции, operator policy, connector durability и master coordinator. Полезную модель не требуется создавать заново. [Core schema](https://github.com/onedayonemasterpiece/my-data-hub/blob/91f7d9bfb077df4d81c5604ae695fa32aa0f428c/sql/migrations/0002_core_content.sql), [search schema](https://github.com/onedayonemasterpiece/my-data-hub/blob/91f7d9bfb077df4d81c5604ae695fa32aa0f428c/sql/migrations/0012_bloggers_search.sql).

При этом проверенные 22 SQL-миграции не задают универсального first-class набора `content_revision`/`content_chunk`/упоминаний/тем/трендов для нового сценария. `content_item` предлагает `body_excerpt`, а `analysis.embedding` привязан к content item. Компактный `SearchDocument` уже существует, но ориентирован на actor/blogger. JSONB позволяет положить дополнительное поле, однако не заменяет проверенную модель полного источника и привязанных к нему чанков. Это предметный пробел, а не отсутствие PostgreSQL как технологии.

Есть важная терминологическая ловушка: нынешний `hub.project` — проект-потребитель/область данных (например, Region Talk). «Проект реконструкции вокзала» из нового требования — сущность предметного мира. Их нужно развести в схеме, иначе доступ, pipeline scope и упоминания реального проекта смешаются.

**Транспорт уже отличается от РКБ.** `master-tunnel-broker.md` описывает reverse SSH forward, ограниченные сертификаты и epoch-bound доступ PostgreSQL; это не WSS. Общий WSS для control/jobs сам по себе не заменяет PostgreSQL data plane или поток backup. Существующий broker можно сохранить отдельным адаптером с тем же владельцем lifecycle; альтернативный путь оценивается по нужному трафику и границам доступа. Удалять SSH-механизм допустимо только после доказанной замены его сетевой изоляции и fencing. Второй независимый controller одного master при этом не нужен. [Tunnel contract](https://github.com/onedayonemasterpiece/my-data-hub/blob/91f7d9bfb077df4d81c5604ae695fa32aa0f428c/docs/operations/master-tunnel-broker.md), [master coordinator](https://github.com/onedayonemasterpiece/my-data-hub/blob/91f7d9bfb077df4d81c5604ae695fa32aa0f428c/src/my_data_hub/orchestrator/master/coordinator.py).

Наблюдаемый `master_state=ABSENT` согласуется с документированным provider-only профилем, где доступны управление ресурсами и статус, но отсутствует data plane. Сам по себе этот ответ не является новым дефектом; он означает, что нынешний подключённый сервис не доказывает готовность нового продукта. README содержит исторические PR-A формулировки; состояние определялось не только по нему. [Provider-only deployment](https://github.com/onedayonemasterpiece/my-data-hub/blob/91f7d9bfb077df4d81c5604ae695fa32aa0f428c/docs/operations/provider-only-mcp-deploy.md).

### 4.4. My-daep и установленный OpenCode

**My-daep — пока дизайн и задание на реализацию.** В полном дереве `master` на указанном SHA пять файлов: README, два проектных документа, sample CSV и gitignore. README отдельно помечает executable DAEP, GitHub App installation и живой Kaggle pilot как незавершённые. Read-only проверка checkout 04.11 в 13:10:58 UTC показала тот же commit, clean и ahead/behind 0/0; host inventory в 13:21:32 UTC не показал зарегистрированного runtime проекта. Это вывод о проверенных default branch/checkout, не обо всём возможном экспериментальном коде. [README](https://github.com/onedayonemasterpiece/my-daep/blob/d3559d731bb74fbb1262114b9aad15687bb54701/README.md), [implementation task](https://github.com/onedayonemasterpiece/my-daep/blob/d3559d731bb74fbb1262114b9aad15687bb54701/docs/implementation-task.md).

Дизайн полезен как контракт: детерминированный supervisor отдельно от смысловых решений OpenCode, глобальная точка входа, atomic reservation workers, attempt/sequence и ACK после durable record, state/cursor вне окна чата, checkpoint каждого затронутого задания, подтверждённая остановка и восстановление без повторной интеграции. Лимит 1–4 workers относится к этому профилю, не становится без измерения глобальным лимитом всех продуктов. Пилот с двумя Kaggle workers и восстановлением одного ещё требуется. [Distributed mode](https://github.com/onedayonemasterpiece/my-daep/blob/d3559d731bb74fbb1262114b9aad15687bb54701/docs/distributed-mode.md).

**Транспорт DAEP также не WSS:** проектный документ описывает исходящий HTTPS для events/heartbeat/results и long polling команд. Поэтому из него переиспользуются lifecycle/recovery требования; существующая реализация WSS из этого репозитория не извлекается. Его продуктовая ответственность — coding jobs, зависимости задач, Git checkpoints, code integration и итоговый PR.

Задание называет кандидатов `dataset-loop-mcp` на feature SHA `0e1533057d5d8b63318c1a6841ce45063b074d33`: `orchestration/github_access.py`, `api/worker.py`, `providers/kaggle_live.py`. Там же предупреждается о неполном `busy_slots` и `cancel_kernel`, использовавшем delete вместо безопасного stop. Это список доноров из документа, а не новая аттестация их production-готовности; текущий main dataset-loop имеет другой SHA. Нельзя переносить эти операции под новыми именами без проверки семантики. [Donor manifest](https://github.com/onedayonemasterpiece/my-daep/blob/d3559d731bb74fbb1262114b9aad15687bb54701/docs/implementation-task.md#L43).

**Локальный OpenCode server есть, его свежесть не установлена.** Read-only host probe `codex-devcoveer-mcp` 04.11 в 13:26:53 UTC показал `opencode-devcoveer.service` active/running, процесс с 02.11 20:32:04 UTC, запуск `/home/dev/.opencode/bin/opencode serve` на loopback. Но semantic version бинарника не получена; probe runtime file вернул `loaded_process_version=not_observable`, `disk_matches_loaded_process=null`. Следовательно, ни последняя upstream-версия, ни версия файла на диске не выдаются за фактически загруженный runtime. Наличие локального server не подтверждает OpenCode на Kaggle.

В DAEP уже требуется pinned OpenCode для воспроизводимости worker, но кода постоянного discovery/update/attestation в проверенном дереве нет. Новое требование владельца должно дополнить pin политикой обновления, ограничением отставания и проверкой установленной/исполняемой версии. Если update source недоступен, можно временно использовать последнюю проверенную версию с явным статусом; это не равнозначно «актуальность подтверждена».

### 4.5. Что означает «как у Миры» на реальном коде

В EventsBot `static_site_live_socket.py` — тонкий адаптер над `live_interaction.socket_transport.serve_socket`. Авторизация, origin/resource binding остаются в продукте, общий пакет владеет транспортом. Это доказанный в исходниках образец границы reuse. Использовать нужно эту организацию кода; audio/PCM-протокол не требуется переносить в задачи Kaggle. [Live adapter](https://github.com/onedayonemasterpiece/events-bot-new/blob/79f8b43374dd9b4b531533d7145f4c04974e61b5/static_site_live_socket.py#L1-L51), [межпроектная модель Миры](https://github.com/onedayonemasterpiece/idea-hub/blob/5a829bd8b635dfa2cab6ac634cb201eab318b230/docs/cross-project-live-operating-model.md).

`ai-resource-control` уже имеет `WorkloadAdmission` с provider/account scopes, резервированием, send fencing и сохранением UNKNOWN. Клиентский пакет можно устанавливать на каждой машине; независимые счётчики одного provider budget — нельзя. Существующая authority не становится БД материалов MDH. У текущего EventsBot версия consumer package отстаёт от рассматриваемого нового workload SDK, поэтому доступность интерфейса в репозитории не объявляется его production rollout. [Workload contract](https://github.com/onedayonemasterpiece/ai-resource-control/blob/f359f6b6326f4ee11f851b3d451adf210e5259dd/docs/research-workloads.md), [consumer adapter](https://github.com/onedayonemasterpiece/events-bot-new/blob/79f8b43374dd9b4b531533d7145f4c04974e61b5/google_ai/live_resources.py).

Сам SDK не перехватывает OpenCode routing. Адаптер должен проводить фактические provider sends, включая внутренние retries и subagents, через предусмотренную границу admission/accounting. Вызов `before_send` и завершение учёта нельзя заменить наличием зависимости в requirements. Оценочный workload budget не является доказательством настоящей квоты провайдера или точным биллингом токенов.

## 5. Предлагаемая общая архитектура

### 5.1. Один пакет, независимые размещения

Рабочее имя отдельного пакета/репозитория — `kaggle-opencode-runtime`. Это предложение размещения, создание репозитория в данном анализе не выполнялось. Базовый Kaggle runtime не должен требовать OpenCode, PostgreSQL или BGE: они подключаются профилями. Иначе РКБ получает ненужного code agent, а executor разработки — ненужную базу знаний.

| Потребитель общего пакета | Независимый workload | Где возникает предметный результат |
|---|---|---|
| Адаптер РКБ | PostgreSQL, evidence/search и BGE | База и проверенный checkpoint РКБ |
| Адаптер My Data Hub | Master PG, capture и аналитические workers | Материал/связи MDH и его checkpoint |
| Адаптер EventsBot | OpenCode-роли Smart Update на Kaggle | Event apply в owning БД EventsBot на RunCoveer |
| Адаптер DAEP | OpenCode, рабочая копия и восстановление разработки | Код и checkpoints репозитория задачи |

Зависимость от одного пакета не требует маршрутизации всех данных через один сервер. Каждый controller обслуживает собственный WSS endpoint и физический pool. Если один controller передаёт pool другому, передача требует reconciliation и fencing прежнего владельца; независимость размещений не даёт права одновременно управлять одним kernel.

### 5.2. Состав общего кода и границы

| Общая обязанность | Состав | Что остаётся продукту |
|---|---|---|
| Kaggle provider adapter | create/push/status/output/stop, identity запуска, reconciliation неоднозначного ответа | Выбор момента запуска, расписание источников, политика использования пула |
| Worker bootstrap | Детерминированная установка проверяемого runtime и зависимостей, attach, health | Набор workload dependencies; Playwright только профилю, которому он нужен |
| WSS transport | Auth, versioned envelope, request routing, heartbeat, reconnect, ACK, ограничение очередей | Значение payload, предметные инструменты и право изменять ресурс |
| Operation lifecycle | job/attempt/generation, affinity, unknown outcome, cancellation/drain | Критерий готовности события, факта, документа или кода |
| Result delivery | Повтор доставки одного результата, устойчивый delivery key, readback прежнего commit | Транзакция и сохранение полного output в owning store |
| OpenCode executor | Нативный CLI/server, запуск/продолжение, фактическая версия, structured result | Prompt, model role, инструменты, corpus, writer policy |
| Resource admission adapter | Подключение существующей authority, соблюдение cooldown/deadline | Приоритеты допустимых задач и заранее разрешённых маршрутов |
| PG lifecycle profile | Single writer, epoch, restore/checkpoint, write gate | SQL-схема, stored procedures и миграции конкретной БД |
| Наблюдаемость | Кодовая/исполняемая версия, возраст очереди, состояние канала, причина завершения | Продуктовые показатели качества и свежести |

Предлагаемый минимальный transport envelope содержит `protocol_version`, `deployment/project`, `job_id`, `attempt_id`, `worker_id`, `generation`, `request_id`, тип сообщения и payload. Добавлять request-level source revision нужно там, где она защищает предметную запись. Новый «паспорт» на каждое предложение текста не нужен.

Транспорт обеспечивает повторяемую доставку. **Единственность эффекта обеспечивается owning transaction/unique key**, а не рекламным обещанием exactly-once WebSocket. Закрытие сокета не отменяет уже отправленную работу; timeout клиента не разрешает запустить конкурирующий attempt. Предыдущая generation не создаёт новый предметный эффект и не продвигает cursor. Её законченный результат можно идемпотентно сохранить с исходным attempt identity; переиспользование принимает текущий владелец. Readback уже сохранённого результата остаётся доступным.

Соединение, жизнь notebook и работа модели имеют отдельные сроки. Heartbeat доказывает присутствие worker, progress — продвижение работы, пользовательская активность — основание продления нужного сервиса. Смешение этих сигналов либо убивает полезную работу, либо держит пустой notebook бесконечно.

### 5.3. Общий код не означает общее владение данными

| Актив | Канонический владелец |
|---|---|
| Книги, исторические evidence, accepted stories | РКБ |
| Накопленные социальные публикации, новые мониторинги, темы и аналитические связи | My Data Hub |
| Event, фестиваль, расписание, очередь и решение о публикации | EventsBot |
| POI identity, название/геометрия и предметное состояние Street Story | Street Story |
| Изменённый программный код и checkpoint разработки | Репозиторий задачи в GitHub; DAEP хранит lifecycle metadata |
| Бюджеты одной provider/account/model области | Существующая общая resource authority |

В MDH допустима производная POI-проекция для поиска: `street_story_id`, revision, название, aliases, координаты по необходимости. Её можно восстановить из владельца. Новый факт из публикации направляется в Street Story как evidence-backed предложение с идентификатором источника, а не как безусловная перезапись подтверждённой карточки. Повторное чтение того же факта должно возвращать прежний outcome.

## 6. Модель данных и аналитика MDH

### 6.1. Накопление прежде дорогой интерпретации

Предлагаемый путь: источник → точная identity и версия текста → сохранение → первичные темы/чанки → доступная enrichment-очередь → сущности/связи → поисковые проекции и аналитика. Полученный материал не исчезает из-за того, что закончилась квота классификатора, не построен вектор или пока не решена неоднозначность имени.

Сбор выполняет обычный connector/script. Для повторного чтения известного канала не нужно каждый раз просить модель сочинять crawler. OpenCode нужен для смысловых ролей и действительно агентных задач. Источник «нет события» остаётся материалом MDH; event consumer получает свою проекцию и свой cursor.

Предлагаемые предметные объекты:

| Объект | Минимальное назначение |
|---|---|
| Источник и аккаунт | Platform/external ID/URL, связь с actor, правила мониторинга |
| Материал и его версия | Полный необходимый текст, время публикации/наблюдения/правки, язык, ссылка и версия capture |
| Media reference | Ссылка/внешний ID, порядок и подпись, если она есть; без постоянных бинарных файлов |
| Чанк | Source revision, порядок и текстовые offsets, полный текст фрагмента, версия подготовки |
| Тема | Несколько рубрик на материал с основанием/версией классификации |
| Сущность | Person / organisation / real-world project / place; aliases и внешние IDs |
| Упоминание | Фрагмент материала → кандидат/принятая сущность; роль и неоднозначность |
| Утверждение/отношение | Кто что сказал/сделал/анонсировал, к какому объекту и времени относится, evidence |
| Производный результат | Сводка/тренд/novelty с покрытием источников, интервалом и версией обработки |

Это рекомендуемая эволюция существующей схемы, не список уже реализованных таблиц. Не требуется отдельная graph DB: связи можно хранить в PostgreSQL с нормальными ключами и индексами.

### 6.2. Разрешение сущностей

Сначала exact platform ID и уже известные aliases; затем нормализованное имя/лексические кандидаты и при пользе embedding-кандидаты. Обоснованное решение связывания учитывает тип, место, связанные аккаунты и контекст. Два человека с одинаковой фамилией не объединяются только из-за близкого вектора. Неоднозначность сохраняется как кандидат, а материал при этом доступен для поиска.

«Автор публикации», «человек, слова которого цитируются», «человек, о котором написано», «организатор» и «обещавший действие» — разные роли. Пример: пост о реконструкции вокзала может связывать автора аккаунта, выступившего чиновника, проект и POI. Сводка «что говорил человек» должна выбирать атрибутированные высказывания, а не любые посты, где встретилось имя.

Для организаций достаточно идентичности, названия, aliases и имеющихся сведений о типе. Не требуется каждый раз добывать юридическую карточку. Для реальных проектов нужны связи с участниками, местами, стадиями и источниками; они не используют `hub.project` как scope приложения.

### 6.3. Поиск и BGE

В My Data Hub `BGE_M3` сейчас задан с 1024 dimensions и `max_tokens=8192`, E5 — с 512. Репозиторный контракт РКБ задаёт BGE input ≤512 tokens с учётом special tokens. Это различие policies, а не доказанная ошибка MDH. Общая интеграция обязана согласовать точные модель/revision, tokenizer, prefix, pooling, normalization, dtype, полный предел input и версию chunking. Совпадение строки «BGE» не доказывает совместимости результатов. [MDH model contracts](https://github.com/onedayonemasterpiece/my-data-hub/blob/91f7d9bfb077df4d81c5604ae695fa32aa0f428c/src/my_data_hub/embeddings/models.py), [RKB contract](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/0b7590628dcb29a270727f3920607084dde30720/AGENTS.md).

Чанк делится по смысловым границам и измеряется токенизатором выбранного контракта. Ориентир «4000 символов» из голосовой — описание длинного текста, не надёжная техническая граница. Исходник сохраняется полностью; нельзя молча обрезать хвост перед encoder. На запросе полезны exact/FTS и semantic retrieval с единым ранжированием и явным указанием фактического охвата. Недоступность vector worker не должна автоматически запрещать точный/лексический поиск.

Векторы сущностей полезны как компактная проекция имени, aliases, описания и связи с местом. Их стоит добавлять там, где они улучшают поиск кандидатов; не копировать в entity-vector всю историю публикаций. Точный ID, alias и период остаются структурными фильтрами.

### 6.4. Тренды, новое и гости региона

**Тренд** — рост/распределение обсуждения относительно обычного фона, с поправкой на копирование, происхождение и охват наблюдаемых источников. Хранить число материалов, независимых source families и авторов, динамику по времени и примеры evidence. Сеть из нескольких сайтов, перепечатавших один пресс-релиз, не равна независимому интересу жителей. Не называть тренд «мнением всего региона», если покрыта лишь наблюдаемая выборка.

**Новизна** — отдельная задача. Новый для коллектора URL может рассказывать о старом объекте; нужна связь с прежним состоянием сущности и временем события. Полезные типы: новый объект/маршрут/выставка/постановка, начало/окончание работ, изменение доступности. Личная бытовая новость без общей ценности не обязана попадать в региональную подборку.

**Гость региона** — атрибуция с источником, датой и неопределённостью. Отсутствие адреса в профиле не доказывает, что человек приезжий. В MDH переиспользуются `source_profile_evidence` и разделение источника/поста из Region Talk, но его старый publication pipeline не становится обязательным этапом обычного мониторинга. [Region Talk schema](https://github.com/onedayonemasterpiece/my-data-hub/blob/91f7d9bfb077df4d81c5604ae695fa32aa0f428c/sql/migrations/0006_region_talk.sql), [workload boundary](https://github.com/onedayonemasterpiece/my-data-hub/blob/91f7d9bfb077df4d81c5604ae695fa32aa0f428c/docs/architecture/region-talk-first-workload.md).

## 7. Надёжность, ресурсы и размер

### 7.1. PostgreSQL commit и внешняя долговечность

У EventsBot каноническая БД находится вне Kaggle-worker. В MDH PostgreSQL находится внутри Kaggle. Поэтому одинаковая фраза «записали результат в БД» даёт разную защиту от потери notebook.

Для MDH нужно явно определить: запись принята в текущем PG; запись включена в подтверждённую внешнюю версию Dataset; producer может безопасно забыть свою pending-доставку. Нельзя обещать нулевую потерю уже подтверждённых данных, опираясь только на `COMMIT` внутри исчезающего runtime. Это вопрос RPO — допустимого окна потери — и механизма replay.

Предлагается сохранить существующий checkpoint coordinator и объединять закрытие порций по canonical revision. Не создавать отдельный Dataset upload каждого поста и не требовать пять одинаковых readback одного текста. Нормальные транзакции, unique keys и outbox гарантируют целостность внутри PG; проверка внешнего checkpoint нужна на границе сохранения/восстановления. [Connector durability](https://github.com/onedayonemasterpiece/my-data-hub/blob/91f7d9bfb077df4d81c5604ae695fa32aa0f428c/src/my_data_hub/connectors/durability.py), [schema durability](https://github.com/onedayonemasterpiece/my-data-hub/blob/91f7d9bfb077df4d81c5604ae695fa32aa0f428c/sql/migrations/0018_connector_durable_delivery.sql).

Полный результат дорогого inference должен переживать kernel-loss, если обещано не повторять эту работу. Возможность заново скачать raw source позволяет пересчитать, но сама по себе не сохраняет выполненный inference. Неизвестный исход записи сначала читается по прежнему operation ID; создание новой job не является восстановлением.

### 7.2. Бесплатные модели и независимый жизненный цикл БД

При quota exhaustion сохраняется выполненное, работа переносится на разрешённую доступную модель либо остаётся в очереди до `retry_after`. Смена модели в живом OpenCode обычно дешевле нового окружения; новый notebook оправдан состоянием runtime или ограниченной политикой recovery. Старый контракт DAEP с обязательным новым notebook нужно явно пересмотреть при принятии этой оптимизации, а не молча считать уже изменённым.

Недоступность LLM не должна сама по себе перезапускать PostgreSQL. DB/master и OpenCode executor имеют разные обязанности. Предпочтительно отделять их жизненные циклы; фактическое размещение на одном/разных notebook выбирать по измеренной памяти и бюджету. Raw-сбор и доступный поиск могут продолжаться, пока смысловая очередь ждёт ресурсов.

Квотная область определяется provider/account/model, а не количеством IP или названий consumer. Отдельно контролируются физический kernel/pool, ресурсы одной базы и эксклюзивная Telethon auth-session. Общий модельный limiter не решает эти другие виды конкуренции.

### 7.3. Менее 100 ГБ — цель хранения, не доказанная ёмкость runtime

Официальная документация Kaggle указывает до 20 GB сохраняемого output `/kaggle/working`, дополнительные непостоянные scratch-файлы и ограниченные сессии; документация Datasets отдельно указывает 200 GB на Dataset и стандартный общий private storage. Это разные ограничения. Конкретный writable disk/квота аккаунта в этом аудите не измерялись. [Notebook specifications](https://www.kaggle.com/docs/notebooks), [Dataset specifications](https://www.kaggle.com/docs/datasets).

Следовательно, «база менее 100 ГБ» ещё не доказывает, что активный PGDATA этого размера можно надёжно сохранить и быстро восстановить выбранным путём. На диске одновременно нужны БД, WAL, временные индексы, backup staging и reserve; минимум два checkpoint дополнительно расходуют persistent quota.

Расчёт, не измерение production: миллион векторов по 1024 float32-координаты занимает около **4,096 GB только координат**, до индексов, строк и версий. Поэтому наиболее существенны число чанков, дубли, политика сохранения старых embeddings и рабочий индекс. Ссылки вместо изображений полезны, но не решают весь рост.

Практичная политика: полный необходимый текст и evidence сохраняются; повторные HTML/JSON-копии, verbose logs, heartbeat и временные ответы имеют понятный срок; производные vectors пересоздаются из источника; старые версии удаляются только по согласованной retention/recovery policy. Автоматическое удаление данных этим анализом не поручено. Ранний документ MDH задавал более консервативные рабочие размеры; новая цель владельца должна быть явно сопоставлена с физическим backup/restore budget, а не заменена произвольным потолком.

## 8. OpenCode должен оставаться свежим

Требование относится ко всем исполняемым установкам: Kaggle bootstrap, долгоживущие worker, локальный OpenCode DevCoveer и любые executor-профили. Контроль одной CLI-версии на одной машине недостаточен.

Предлагаемый общий контракт актуальности:

1. **Обнаружение свежей stable-версии автоматически.** Проверять официальный канал регулярно и перед подготовкой нового окружения; кешировать результат. Предлагаемый стартовый интервал — раз в сутки, как настройка, не новое обязательное число из голоса.
2. **Точное закрепление внутри attempt.** Новый проверенный runtime получает конкретную версию/артефакт; активная задача продолжает работать на своей версии. `latest` не меняет исполняемый код посередине работы.
3. **Короткая совместимость перед promotion.** Проверить запуск, native server/SDK handshake, требуемые MCP/tool contracts, отмену и чтение результата; полноценный турнир моделей не нужен для каждого обновления CLI.
4. **Контролируемое продвижение.** Сначала один отдельный worker, затем новые задачи. Предлагаемая цель — не более суток задержки для совместимого stable-релиза; исключение фиксирует причину и срок пересмотра.
5. **Фактическая версия в наблюдаемом состоянии.** `installed_version`, `expected_version`, binary/artifact identity, `latest_checked_at`, статус обновления и причина временной задержки. Запись в конфиге не заменяет `opencode --version` выбранного executable.
6. **Откат.** Хранить последнюю рабочую версию для новых попыток при регрессии; задача и принятые результаты сохраняются. Несовместимость не разрешает незаметно перейти к другой модели или другому платному маршруту.
7. **Каталог моделей обновляется отдельно.** Название `*-free` и присутствие в списке не доказывают доступность, цену, качество или tool calling. Выбор роли учитывает разрешённую стоимость, фактический отказ и общий quota scope.
8. **Один код updater.** Удалить продуктовые install-скрипты, вечные pins без проверки и альтернативные версии бинарника после миграции; все потребители используют один механизм. Сам факт установки OpenCode не означает запущенный service или работающий worker.

**Проверенная особенность официальных версий на 04.11.2026.** Доступен stable-релиз [v2.2.5 от 30 октября](https://github.com/anomalyco/opencode/releases/tag/v2.2.5), при этом GitHub API `releases/latest` вернул [v1.19.5 от 28 октября](https://github.com/anomalyco/opencode/releases/tag/v1.19.5). Поэтому один GitHub-флаг `latest` не определяет свежую поддерживаемую основную ветку. Updater должен проверять выбранный официальный release channel, доступность новых major и необходимость миграции; бессрочно удерживать v1 под видом актуальности нельзя.

В [документации v1](https://dev.opencode.ai/docs/config/) `autoupdate: true`/`notify` означает уведомление, а не автоматическую установку; для обновления предусмотрены `/update` и `opencode upgrade`. В [документации v2](https://opencode.ai/v2/docs/config) используется глобальный `update: "disable" | "notify" | "auto"`, по умолчанию `notify`; значение из project config игнорируется. Даже автоматическая установка v2 **не перезапускает уже работающий server**. Следовательно, runtime обязан отдельно активировать новую версию через управляемый drain/restart или новое окружение. CLI [описывает upgrade на конкретную версию](https://opencode.ai/v2/docs/cli/commands/); миграция [v1 → v2 имеет отдельный major-путь](https://dev.opencode.ai/docs/cli/). Это проверка официальных механизмов, не сообщение об обновлении пользовательских установок.

## 9. SQL, миграции и простота

Текущие `AGENTS.md`/MCP policy запрещают remote DDL и owner/superuser. Пожелание свободнее изменять схему на Kaggle требует нового явного контракта, поскольку его нельзя честно назвать уже поддержанным. [Operating contract](https://github.com/onedayonemasterpiece/my-data-hub/blob/91f7d9bfb077df4d81c5604ae695fa32aa0f428c/AGENTS.md), [MCP architecture](https://github.com/onedayonemasterpiece/my-data-hub/blob/91f7d9bfb077df4d81c5604ae695fa32aa0f428c/docs/architecture/mcp.md).

Рекомендация: повторяемые импорты, entity-link и сохранение результата выполняются хранимыми процедурами и транзакциями. Исследовательский SQL исполняется на master под отдельной ограниченной ролью. Аддитивные миграции доставляются как версионированный SQL-пакет и могут применяться без сборки всего приложения; владелец схемы и право миграции остаются явными. Произвольный агент не получает неограниченный DDL только потому, что его соединение аутентифицировано.

Нужно сохранить проверки, защищающие разный результат: чужой scope, stale writer, повтор эффекта, настоящее противоречие, потеря checkpoint. Убрать предлагается дублирующие транскрипцию хеш-проверки по всем слоям, повтор всего inference после transport timeout, второй controller одной операции и отдельные реализации одинакового bootstrap. Новые статусы вводить только если они меняют действие клиента/восстановления; обычные ошибки PostgreSQL и transaction rollback не требуют второй системы подтверждения истинности текста.

## 10. Порядок реализации и критерии результата

Это предлагаемый порядок будущих работ, не начатое внедрение и не обещанный календарь.

### Поставка 1 — общий runtime с двумя настоящими потребителями

Зафиксировать extraction manifest: модуль-источник, точный commit, сохраняемый контракт, новый package path, два consumer adapter и удаляемая локальная ветка. Сначала общий WSS/provider/bootstrap и политика актуальности OpenCode, затем подключение РКБ и минимального MDH worker. РКБ не требуется переделывать целиком, чтобы доказать reuse. Полезный результат: исправление общего transport-кода используется обоими проектами через одну версию зависимости.

Различающие проверки: независимый холодный запуск RunCoveer без DevCoveer; два worker/endpoint без перепутанных ответов; reconnect без повторного domain effect; потеря ответа после send не создаёт второй run; старое поколение не может изменить authoritative state, но уже законченный payload не теряется. Проверять нужно только затронутые границы, а не делать всю будущую программу аналитики gate этой поставки.

### Поставка 2 — один законченный путь данных MDH

Настоящий источник → сохранённая версия полного текста → рубрики/чанки → FTS/BGE → ответ MCP со ссылкой → подтверждённый Dataset checkpoint → чтение после нового master. MCP-ввод и scheduler-ввод используют одну процедуру и не создают дубли. Неполученный embedding не блокирует уже сохранённый raw и точный поиск. Именно этот результат доказывает базу продукта; README и зелёная unit suite его не заменяют.

### Поставка 3 — люди, организации, проекты и POI

Показать реальные запросы «что говорил/обещал человек за месяц», «что происходило с проектом», «какие новые факты относятся к этому POI». Проверить одинаковые имена, несколько аккаунтов, неоднозначное связывание и обновление уже известного материала. Street Story остаётся владельцем POI; принятый факт не обнуляется из-за нового кандидата.

### Поставка 4 — тренды, новое и адаптивная работа

На ограниченном понятном наборе источников измерить задержку от публикации до capture и до аналитического обнаружения, независимость источников, долю пересказов, ложные новинки и coverage. Сымитированная/реальная недоступность моделей сохраняет queue/cursors и не выдаёт отсутствие обработки за отсутствие событий. Общая новостная копия и независимые свидетельства различаются. Ultra затем использует тот же runtime со своим prepared apply и своим двухмодельным отрицательным решением.

### Метрики, которые помогают владельцу

Возраст самого старого необработанного материала; capture lag и analysis lag отдельно; доля корпуса с доступными векторами; число независимых источников в тренде; неоднозначные entity links; возраст внешнего checkpoint/максимальная неподтверждённая ревизия; cold restore time; полезные завершённые результаты относительно повторного inference; фактическая версия OpenCode и её отставание от проверенного stable.

Эти показатели не требуют новой observability-платформы. Их достаточно вывести из существующей очереди, lifecycle и owning БД. Численных production-результатов в этом отчёте нет: они не измерялись.

## 11. Решения, для которых полезен ответ владельца

| Вопрос | Предлагаемое исходное решение | Почему ответ меняет реализацию |
|---|---|---|
| Если все бесплатные модели недоступны, допустима ли задержка аналитики более 6 часов или нужен разрешённый резерв? | Сбор источников продолжать; задержку enrichment показывать; без неразрешённого платного fallback | Нельзя одновременно гарантировать срок inference и не иметь гарантированно доступного вычисления |
| Какое окно потери допустимо при внезапном исчезновении Kaggle? Что означает подтверждение «сохранено»? | Полезный подтверждённый результат должен быть внешне durable; pending PG-запись показывать отдельно | Определяет checkpoint/replay и задержку окончательного подтверждения |
| Свободный SQL нужен владельцу для исследований, для миграций или также обычным автоматическим агентам? | Чтение/предметные процедуры доступны по ролям; миграции — отдельный версионированный путь | Нынешний remote DDL запрещён; расширение полномочий нельзя считать технической мелочью |
| Менее 100 ГБ относится к активной БД с индексами или ко всему хранилищу с backup? Какой срок хранения полного текста? | Считать active data/indexes и backup раздельно, перед пилотом определить retention | От этого зависят размер online-корпуса, checkpoint и восстановление |
| Какие источники и один первый продуктовый результат приоритетны: новое за 6 часов, тренды, гости или люди/проекты? | Использовать известные источники, проверить один сквозной путь и один понятный результат | Задаёт объём пилота и критерий качества без строительства сразу всех аналитических режимов |

Название общего пакета, структура каталогов и технические детали extraction можно выбрать инженерно. Эти вопросы не мешают завершить анализ и фиксацию требований; они нужны для корректного будущего исполнения.

## 12. Что зафиксировано и что остаётся предложением

Проверены текущий код, документы и ограниченный read-only статус MDH. Зафиксированы пользовательские требования, конкретные доноры, несовпадения реализаций и рекомендуемая граница общей библиотеки. Основное противоречие — ожидание готового универсального runtime при наличии нескольких отдельных реализаций и проектных документов — описано по источникам.

Новый runtime не создан, PostgreSQL не запускался, OpenCode не обновлялся, данные не мигрировали, сообщения в Slack не отправлялись. Обновление аналитических документов не утверждает предложенную архитектуру и не меняет исполняемые policies. Следующий инженерный результат — одна реальная общая зависимость и работающий путь MDH через неё, затем предметная аналитика.
