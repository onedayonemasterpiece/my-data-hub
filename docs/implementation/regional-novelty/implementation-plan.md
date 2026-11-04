# Проект реализации: значимое новое в регионе и общий Kaggle runtime

Дата: 04.11.2026. Статус: готовый проект реализации для аудита; начальный общий source slice реализован отдельно. Действующие решения владельца: [owner-decisions.md](owner-decisions.md). Точка входа аудитора: [OPUS-AUDIT.md](OPUS-AUDIT.md).

## 1. Результат и границы

Пользователь получает через My Data Hub подборку **новых общественно значимых изменений в Калининградской области**, каждое с понятным объяснением нового, временем события/обнаружения, местом или объектом, первичными ссылками и основанием сравнения с прежним состоянием. Повторный URL/репост известного факта не создаёт новую новость. Сбор и состояние обработки сохраняются при смене Kaggle executor.

Первый интерфейс — существующий MCP MDH: предметные получение списка/подробности/поиск и статус актуальности корпуса. В этой поставке не нужен отдельный сайт, мобильное приложение, публикация в TG/VK, новый чат-бот или новый публичный gateway. Не требуется сначала реализовать все тренды, гостей и истории людей. Их будущие потребности учитываются полным raw, identities, revisions, mentions и ссылками на POI.

Шесть часов — продуктовая цель от публикации/доступности источника до обнаружения. Capture lag и analysis lag измеряются отдельно. При quota-block действует принятая замена executor; неизвестная реальная доступность после замены не заменяется ложным обещанием срока.

## 2. Что уже существует и что меняется

| Обязанность | Сейчас | Целевое изменение |
|---|---|---|
| PostgreSQL/master | MDH coordinator, single writer/epoch, Dataset checkpoint | Сохранить owner; добавить RKB-style exact recovery journal и совместимый Dataset adapter |
| Ingress | ConnectorEnvelope, landing, idempotency, durability; daily-statistics committer | Расширить существующий dispatch по data_product и добавить regional-monitoring handler |
| Контент | actor/account/content_item/body_excerpt, identities, assets, provenance | Аддитивные full revisions/chunks/mentions/novelty; старые потребители не переосмысливаются |
| Поиск | FTS, E5/BGE contracts, blogger_compact_v1 | Отдельный content/chunk search contract с BGE512; прежние пространства сохраняются |
| Runtime | Отдельные реализации РКБ/EventsBot/MDH и DAEP design | Один пакет kaggle-runtime + адаптеры, удаление заменённых локальных нейтральных функций |
| OpenCode | Отдельный локальный server, DAEP pinned requirement | Общий bootstrap/update/process adapter с фактической версией и чистым auth context |
| Модельные ресурсы | ai-resource-control workload API, consumer versions различаются | Один существующий admission/accounting authority, охватывающий реальные sends |
| Product API | Ограниченный существующий MCP | Добавить предметные методы novelty через существующий каталог и ACL |

Новый пакет не является обязательным центральным сервером. RunCoveer и DevCoveer могут иметь собственные controllers и WSS endpoints, использовать одну версию библиотеки и владеть разными физическими пулами. Существующая общая resource authority остаётся общей зависимостью, когда речь идёт об одном provider/account бюджете.

## 3. Размещение и владельцы

| Компонент | Где работает | Владелец состояния |
|---|---|---|
| MDH API/MCP, scheduler, callbacks, operation ledger | Существующий лёгкий control plane | MDH |
| Ограниченный exact recovery spool | Существующая control SQLite MDH | Тот же MDH lifecycle owner; не новый сервис |
| Единственная canonical PG + FTS/pgvector | Kaggle master | Текущий master generation |
| Dataset current/previous verified checkpoints | Private Kaggle Dataset | Checkpoint owner MDH |
| Capture/semantic executors | Независимые Kaggle executions | Controller соответствующего продукта |
| BGE | Kaggle embedding profile | Encoder adapter, input contract |
| OpenCode process | Kaggle executor; локальные установки используют общий updater | Executor adapter |
| Shared code | Приватный kaggle-runtime | Один upstream-пакет, версии потребителей |

Исчерпание LLM квоты меняет executor, не PG master. Master и BGE могут быть размещены по принятому RKB профилю после проверки памяти; это не причина связывать их жизнь с бесплатной моделью OpenCode. Pool admission учитывает реальную ёмкость аккаунта без обещания универсального числа notebook slots. DAEP-specific 1–4 workers не становится глобальным числом.

WSS доставляет команды, состояние и ограниченные результаты. Существующий MDH SSH PostgreSQL data plane не удаляется лишь потому, что появился общий WSS. Его замена допустима только при сохранении epoch/role isolation и при наличии работающего пути того же трафика. Большой PG archive передаётся из Kaggle напрямую по scoped transfer, не через JSON frames или Dev filesystem.

## 4. Первый shared slice и следующий перенос

В kaggle-runtime начат пакет Python 3.11+ без runtime dependencies: строгий JSON codec из РКБ, stable release/freshness policy, неизменяемый attempt pin, stop-before-replace policy и explicit credential subset. Его исходники и тесты полностью доступны в [снимке для аудита](shared-runtime-source.md). Это рабочие функции, но ещё не controller loop/driver/WSS service.

Для каждого следующего переноса фиксируются donor module@SHA, сохраняемый wire/SQL контракт, новый owner module, два consumer adapter и удаляемые локальные функции. Не начинать с механического копирования многотысячного файла с domain handlers.

| Шаг | Общий код | Адаптеры/удаляемое дублирование | Критерий |
|---|---|---|---|
| R1 | generic codec, release policy, rotation policy | Начальный пакет уже создан; adapters ещё не подключены | 16 различающих unit tests и import/compile |
| R2 | WSS auth attach, generation routing, bounded queues, reconnect | RKB bge_channel/bge_wss_transport оставляют handlers; MDH runtime_sdk подключает общий transport | Один transport используется двумя adapters; delayed frame не создаёт чужой эффект |
| R3 | Kaggle intent/push/status/reconcile/terminal observation | EventsBot/MDH provider operations; dataset-loop delete-as-stop не переносится | Потеря ответа на launch/stop не создаёт второй execution |
| R4 | OpenCode bootstrap, release acquisition, native session/events/cancel, rotation execution | Consumer даёт model roles/tool allowlist; install/retry logic уходит из локальных скриптов | Fresh executor с пустым auth context продолжает ту же job после stop |
| R5 | RKB exact journal/coverage/capture/restore interfaces | MDH PG/SQL/route registry, существующий ledger; RKB-specific Case forcing остаётся у РКБ | Kernel loss → Dataset base → exact replay → полезный read без нового inference |

R2 и R3 разрабатываются ограниченными изменениями; не делать готовность всего универсального framework условием первого MDH источника. Но shared adoption считается завершённым только после реальной зависимости двух потребителей и удаления заменённых локальных реализаций. Копия снимка в документации не считается вторым consumer.

## 5. Поток данных первого продукта

1. Scheduler выбирает существующий разрешённый источник и cursor. Исполнитель переносит начальный набор из реально используемого source registry EventsBot, затем фиксирует его в MDH; не выдумывает живые channel IDs и не просит владельца выбирать SQL/миграции.
2. Обычный connector получает материал с platform/external_id/source_revision, опубликованным/наблюдённым временем и media URLs. Telethon session имеет одного владельца; local E2E credentials не подставляются вместо production source identity.
3. Existing landing/ingress принимает полный смысловой текст и exact revision. Повтор той же identity/revision возвращает прежний outcome. Cursor продвигается вместе с принятием покрытой порции, не после окончания всех LLM-стадий.
4. На accepted revision создаётся задача нормализации/чанков, lexical search и enrichment. Полный текст не режется до BGE512: ограничиваются только отдельные encoder inputs с offsets в полном тексте.
5. Смысловая роль извлекает candidate change: предмет, тип изменения, факт, effective time, место, evidence spans. Поля, взятые из операторской инструкции, не становятся source evidence.
6. Роль сопоставления получает bounded candidates из существующего корпуса: exact identity/alias/FTS/BGE и прежнее состояние предмета. Итог — новое значимое изменение, повтор, уточнение прежнего либо недостаточно основания. Неоднозначность хранится в данных и не блокирует независимые материалы.
7. Одна предметная транзакция сохраняет итог и обязательный outbox. Повтор доставки не вызывает LLM заново и не дублирует карточку. Отдельная failure одного материала не блокирует весь run.
8. Предметный MCP возвращает полезные результаты с источниками, периодом, capture/analysis lag и известным coverage; отсутствие обработанных материалов не превращается в «ничего нового».

Изображения — ссылки и подписи. Не вводится скачивание архива изображений. Для удаления/редактирования source появляются новые ревизии/статусы, прежняя evidence-связь не переписывается незаметно.

Конкретные предлагаемые миграции, модули, ingress и API: [novelty-implementation.md](novelty-implementation.md). Они названы как планируемые файлы и не выдаются за уже существующий код.

## 6. Сохранение и остановка по РКБ

Применяется проверенная цепочка RKB: exact command/outcome в ограниченном Dev recovery store → canonical PG apply → verified Dataset coverage. Архив PG не хранится на Dev; Dev journal содержит точные recovery bytes, не канонический query corpus. Отдельный ADR описывает это отличие от старой формулировки «metadata only».

Групповая policy до оптимизации: quiet 900s, интервал 1800s, planned stop и необходимые schema boundaries. Нет Dataset upload каждой статьи. Таймер checkpoint не является useful-idle. MDH useful window=2220s по ранее явному требованию, при donor default 1800s. Подробности clock/restart/capture/current-owner в [RKB evidence](rkb-storage-evidence.md).

Во время capture фиксируется пригодный prefix, затем длительная передача не держит весь intake lock. Позднейшие операции не получают Saved от прежней версии. Lost create/version ACK сверяется по сохранённому publication intent. Retired writer не продвигает pointer. Kernel loss восстанавливает последний verified base и exact journal tail; модель не запускается повторно для реконструкции уже полученного вывода.

Внешний backup на диск/другое облако не проектируется. Current/previous Dataset — основной механизм. Полная совместная потеря Dev journal и kernel покрывается лишь последней verified версией; обычный kernel loss покрывается Dataset+сохранным Dev journal.

### Ёмкость

Цель active DB <100 GB реализуется как учёт data+indexes+retained revisions и понятная политика роста. Сначала рабочий объём ограничен измеренной допустимой ёмкостью, без удаления непокрытого результата. Current donor hard caps 8 GiB archive/16GiB unpacked/900s restore проходят через несколько validators; изменение одного значения недопустимо.

Перед повышением рабочего размера согласовать один capacity profile для config, capture, tar, transfer ticket, readback, restore, free-space и STOP deadline. Измерить действительный active PGDATA плюс временный pg_basebackup/tar/readback и время восстановления. Если полный capture не укладывается, оптимизируется существующий Dataset path; лёгкий incremental режим не объявляется реализованным по старой фразе docs 15min/4h. Эта техническая работа не возвращается владельцу вопросом о бэкапах.

## 7. OpenCode bootstrap, freshness и credentials

### Запуск

Bootstrap — обычный скрипт, без LLM для установки. Он устанавливает exact выбранный OpenCode artifact, нужные workload dependencies, подключает общий runtime и запускает native OpenCode server/CLI. Control attach появляется до тяжёлой подготовки, readiness — после реальной готовности заданного профиля. GitHub доступа для capture не нужно; code-writing профиль DAEP получает свои отдельные разрешения.

Default provider credential map пуста. Fresh environment не получает предыдущие auth.json, config secrets, session cache и ambient provider keys. Явный scoped key alias разрешён только для требующего ключ провайдера; provider key и Kaggle launch credential — разные секреты. Kaggle control credentials остаются у controller, executor получает только необходимые scoped операции/transfer tickets. Наличие пустого env dict не считается достаточной изоляцией, если процесс всё ещё читает чужой auth directory.

### Обновление

Общий updater получает официальный перечисляемый stable release feed, а не только endpoint latest. Начальная major для нового внедрения — v2 после проверки нужных native server/session/MCP контрактов; v1 поддерживается лишь на переходе существующих consumers. Нельзя перезаписать активный attempt pin.

Инженерный default: discovery раз в 24 ч и при подготовке окружения с кешем; promotion совместимого релиза для новых attempts в пределах 24 ч после успешной проверки. Если релиз несовместим, причина и следующая проверка видны; это не бессрочное freeze. Новая major — отдельная migration task. Last-known-good применяется временно при отказе update source/регрессии, с явным возрастом наблюдения.

В v1 autoupdate только уведомляет; в v2 global update=auto может установить файл, но не перезапускает server. Поэтому управляющий код различает available/selected/installed/running version и активирует обновление новым процессом. Локальная установка DevCoveer тоже входит в эту политику; текущий факт active server без semantic version не аттестует свежесть.

После all-eligible-free quota exhaustion запускается stop/fresh последовательность. Перед остановкой полезные stage payload передаются через normal ingress/journal. Новый executor получает continuation незавершённых стадий, не весь уже принятый job. Модельный catalogue и capability/availability probe обновляются отдельно от бинарника. Реальный send, включая internal retries/subagents, должен проходить общий admission/accounting; если native OpenCode не предоставляет нужный hook, это явная интеграционная задача R4, а не скрытый обход authority.

## 8. Пакеты исполнения и зависимости

| Приоритет / work package | Вход и owned scope | Завершённый результат | Зависит от |
|---|---|---|---|
| P0 / W0 решения и аудит | Документы текущего пакета; immutable source snapshots | Opus может проверить проект с доступом MDH+RKB | Выполняется сейчас |
| P0 / W1 общий runtime | kaggle-runtime/src/tests + thin adapters | R2/R3 с двумя consumers без duplicate core | Начальный R1 уже реализован |
| P0 / W2 durable MDH | Existing control ledger, master coordinator, connector durability, PG profile | Exact command replay и verified checkpoint по RKB | ADR journal; R3/R5 interface |
| P0 / W3 raw/capture | Аддитивные SQL миграции и regional_monitoring ingress | Реальный полный raw сохраняется при сбое LLM, capture cursor восстанавливается | Existing master + W2 durability |
| P0 / W4 OpenCode executor | Shared bootstrap/updater/provider adapter + MDH roles | Current OpenCode, no inherited keys, quota→fresh continuation | W1, common resource authority |
| P1 / W5 significant novelty | Chunk/BGE, prior-state lookup, semantic change apply | Повтор не новое, новое имеет evidence/время/место | W3/W4 |
| P1 / W6 MCP и продуктовая проверка | Existing catalog/API/ACL + fixtures/реальный corpus | Подборка и карточка доступны пользователю, срок/coverage видны | W5 |
| P2 / W7 расширение | People/projects/POI, trends/guests, additional consumers | Новые результаты на том же корпусе/общем runtime | Принят первый продукт |

Один интегратор владеет общей веткой поставки каждого репозитория. Исполнитель не меняет чужой live runtime и не создаёт параллельный controller того же kernel. Перенос РКБ идёт небольшими совместимыми изменениями; текущий книжный импорт не является испытательным стендом для первого MDH workload.

## 9. Приёмка без бюрократического второго проекта

Проверки выбираются для реально изменённой границы. Уже успешная проверка неизменённого кода не повторяется ради нового статуса.

| Сценарий | Что отличает правильный результат |
|---|---|
| Прочитан реальный пост, LLM недоступна | Raw и capture cursor не потеряны; analysis lag отдельно |
| Тот же материал пришёл повторно/другим ingress | Один source revision, прежний apply outcome |
| Два сайта перепечатали известный факт | Не возникает две независимые новинки |
| Новый факт об известном объекте | Описано конкретное изменение и предыдущий контекст |
| Исчерпаны free роли | Старый executor stopped, новый exact execution продолжает хвост, keys не наследованы |
| Потерян start/stop ACK | Сверка прежнего intent, нет параллельного второго execution |
| Kernel master потерян после PG apply до Dataset | Dataset base + Dev journal возвращают принятый факт без повторного inference |
| Поздний результат старого worker | Не меняет canonical state; immutable result можно прочитать/переиспользовать |
| Checkpoint создался, ответ потерян | Exact publication readback, без второй версии из-за timeout |
| Stable OpenCode обновился | Новые attempts используют проверенный релиз, старый pin неизменён; running version подтверждена |

Первый source-only пакет проверен unit tests; эти сценарии целиком требуют будущей интеграционной и живой проверки. Для живого принятия исполнитель фиксирует конкретные источники, интервал, полученные материалы, полезную подборку и восстановленный результат. Красивый README, import или HTTP200 не являются такой приёмкой.

## 10. Совместимость, переход и возврат

Миграции только аддитивные и последовательные. Новый source_revision/chunk search contract не переопределяет blogger_compact_v1 и не меняет смысл hub.project. До подтверждения полного corpus path Region Talk migration и publication остаются в текущем состоянии.

Переход adapter выполняется на одном controller/pool: остановить выдачу новых claims старому владельцу, завершить/reconcile его операции, передать подтверждённое состояние, затем активировать нового. Нет двух writers одной canonical revision. Возврат к прежней версии adapter требует чтения сохранённых операций того же формата; нельзя откатывать schema удалением новых raw/evidence.

Package upgrades ограничены declared compatibility. Schema/native-handler revision pin для replay меняется только через предусмотренную migration boundary; старые exact commands не применяются другим handler без явной совместимости. Секреты и production данные не попадают в audit snapshot.

## 11. Риски для решения аудитора

1. RKB current full physical capture и hard size caps ограничивают рост сильнее общего желания <100 GB. Нужен размерный профиль и доказанный путь, не обещание incremental backup из docs.
2. MDH старое metadata-only описание нужно согласовать с exact payload journal РКБ. Самовольная вторая бизнес-БД на Dev недопустима.
3. Native OpenCode retries/subagents должны учитываться common admission; нынешний pure package этого не делает.
4. Transport compatibility: RKB WSS, MDH SSH data plane, DAEP HTTPS long polling — разные существующие границы. Новый общий пакет не должен ломать их ради одинакового имени.
5. Fresh Kaggle может не дать пригодной модели. Это проверяется при запуске; корректное recovery не объявляет результат модели или quota reset заранее.
6. Доказательства из чужих репозиториев в audit snapshot датированы и не означают их deployment. Изменение этих источников требует обновить только затронутую часть snapshot.

У этих пунктов есть технические владельцы и проверяемые исходы в W1–W6. Они не превращаются в повторный список вопросов владельцу о SQL, backup или роли миграций.
