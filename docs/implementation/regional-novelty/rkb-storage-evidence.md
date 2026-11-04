# РКБ: доказательства lifecycle и Dataset durability для общего runtime

Статус: read-only анализ исходников; предназначен для самостоятельного аудита Opus и реализации общего пакета. Выполнен 2026-11-04. Проверенный GitHub main: **0b7590628dcb29a270727f3920607084dde30720** (коммит 2026-11-04T10:49:08Z). Повторная проверка HEAD не выявила изменения относительно предыдущего аудита.

Runtime, Kaggle, PostgreSQL, внешние публикации и тесты не запускались. Доказательства ниже устанавливают поведение кода, а не успешный production-прогон. Код РКБ остаётся источником переносимых механизмов; RKB-domain contracts должны оставаться адаптерами.

## 1. Выводы для реализации

1. Действующий useful-idle донора РКБ — **30 минут**, не 37. Он считается на Dev по wall clock от полезного спроса. В документации РКБ также указаны 30 минут. Ранее владелец явно потребовал **37 минут** для создаваемого решения; это требование не отменено. Поэтому MDH profile задаёт `useful_idle_seconds=2220` как явное инженерное переопределение при унаследованном механизме РКБ. Число 2220 не является найденной константой донора.
2. Реальная последовательность не «PG, затем придумать журнал»: **точная команда в Dev control spool → journal acceptance → атомарный PG apply и native outcome → пакетный Dataset checkpoint → native readback → подтверждённое Saved**.
3. В Dataset-primary режиме journal между checkpoint хранится в существующей SQLite queue на Dev. Это bounded recovery/control state, **не удалённая сохранность Dataset и не вторая каноническая бизнес-БД**. Он содержит точные payload/outcome bytes, а не только hashes.
4. Потеря Kaggle kernel при сохранной Dev SQLite допускает восстановление Dataset base и replay принятого хвоста. Одновременная потеря Dev pending journal и kernel оставляет только последнее подтверждённое Dataset-покрытие. Saved нельзя выдавать заранее.
5. Текущий код делает **полный physical PG backup**, а не инкрементальный WAL upload: pg_basebackup со streamed WAL, pg_verifybackup, обычный несжатый tar, прямой upload из Kaggle, публикация Dataset, скачивание архива обратно в notebook для проверки размера/hash.
6. Текущая checkpoint policy: quiet 15 минут, интервал 30 минут, planned stop и отдельные принудительные доменные/схемные границы. Это отличается от фразы в docs об отдельных control checkpoint 15 минут / PG base 4 часа.
7. У current конфигурации есть **жёсткие верхние пределы 8 GiB archive / 16 GiB unpacked / 900 секунд recovery**. Поддержка БД 100 ГБ не реализована автоматически; нужен bounded size/time policy через весь путь и отдельная проверка ёмкости. Не следует подменять это новой системой backup.

## 2. Точные часы и lifecycle

### 2.1 Useful-idle

В `src/regional_knowledge/bge_queue.py`:

- L21: `IDLE_SECONDS = 1800`.
- L22: `ROTATE_SECONDS = 8 * 3600 + 15 * 60`.
- L23: `MAX_LIFETIME = 11 * 3600`.
- L24: `HEARTBEAT_SECONDS = 120`.
- L60–62: `BgeQueue(..., clock=time.time)`; этот clock используется для durable timestamps.
- L177–183: run истекает, если достигнут максимальный возраст, не-draining run достиг `useful + IDLE_SECONDS`, потеряна heartbeat liveness или run failed. Стартовый notebook до завершённого bootstrap имеет отдельные 900 секунд.
- L194–215: ready/dispatched run с точной provider version, активным неконфликтующим каналом, живым heartbeat и без pending writer переводится в draining по useful-idle.
- L247–265: `record_request_activity()` обновляет useful timestamp по пользовательскому намерению; изменения агрегируются до минуты. Время `time.monotonic()` в этом методе служит измерению задержки SQLite, не TTL.

Heartbeat и полезный спрос раздельны. Служебный Ping/Pong не делает runtime бессрочным. Документация подтверждает тот же useful-idle 30 min: `docs/configuration.md`:36–39 и `docs/operations/bge-kaggle.md`:56. [S1, S2, S3]

Draining — не доказательство остановки провайдера. В L171–174 и L204–229 queue удерживает reservation/channel до точного provider terminal proof. Локальная lease, закрытый socket и STOP ACK не могут сами освободить удалённую ёмкость. Это переносимый lifecycle-инвариант.

### 2.2 Checkpoint clocks

`SourceDatasetCheckpoints` получает `interval_seconds=1800, quiet_seconds=900, clock=time.monotonic, wall_clock=time.time`. Таймеры отдельны от useful-idle. В текущем startup создаётся `SourceDatasetCheckpoints(self)` без переопределения этих defaults; maintenance `refresh_seconds` по умолчанию 5 секунд. [S4, S5]

После API restart интервал не начинается заново: возраст вычисляется относительно provider timestamp последней подтверждённой Dataset version. Если timestamp прочитать нельзя, code выбирает capture_now. Это не новый отдельный ledger времени. [S4:303–358]

## 3. Что именно находится на Dev, в Kaggle и Dataset

| Место | Содержимое и authority | Чего это не доказывает |
|---|---|---|
| PostgreSQL в Kaggle | Каноническая бизнес-проекция, `rkb_v4.applied_ops`, `recovery_state.applied_seq`, writer generation | PG COMMIT ещё не означает Dataset durability |
| Существующая Dev SQLite queue | `rkb_control_operations`, точные команды и native outcomes, request/operation identities, sequence, apply/seal/outcome state, Dataset publication intent и pointer | Local commit не является external Saved |
| `rkb_dataset_recovery_objects` в той же Dev SQLite | Canonical journal objects с bytes, CAS token, `covered_version`; неподтверждённые строки имеют NULL | Имя RecoveryJournal/AcceptedSeal не делает эти bytes внешне сохранёнными |
| Версия Kaggle Dataset | Один physical PG archive, `base-manifest.json`, `source-control.json`, `journal-control.json` | Provider status/version/listing без native archive readback недостаточны для promotion |

`ExactCommand` включает sequence, operation_key, request_id, person_id, route, logical_time, schema/function/executor revisions, ids, expected_revisions и **payload bytes**. Encoding сохраняет payload_b64/hash/length. `read_query` исключён из durable command kinds. [S6:131–173]

`ControlSpool` сохраняет command_bytes (до 1 MiB), dependency bytes, handler hash, apply/outcome bytes и phase states. Defaults: 512 MiB raw spool / 4 часа raw-retention. До acceptance резервируется место для outcome и метаданных, чтобы новое принятие не оставалось без места для завершения. Конфликт operation_key с другими bytes/generation/scope отклоняется. [S7:221–261,624–647]

`DatasetRecoveryStore` недвусмысленно документирует pending local control; он не держит PG archive. Каждая запись ограничена 1 MiB, суммарный pending control — 512 MiB; CAS token имеет вид pending:..., а `covered_version` остаётся NULL. `remote_durable(key,data)` истинно только для совпадающих bytes с ненулевым covered_version. [S8:1–5,221–255]

**Инженерный ADR для MDH:** сохранить запрет канонических business tables и PGDATA на Dev. Разрешить ограниченный, операционный, неканонический recovery journal с точными командами и native outcomes, необходимыми для replay после kernel loss. Это явное инженерное уточнение, а не создание второго источника истины или декларация «там только metadata». Привязать его к существующей durable queue MDH; не создавать параллельный receipt/audit сервис. Бизнес-запросы читают PG; Dev spool не используется для поиска, аналитики или канонического редактирования сущностей. Удаление pending bytes допустимо только после доказанного покрытия/завершения соответствующей операции; четыре часа нельзя трактовать как разрешение потерять UNKNOWN.

## 4. Последовательность mutation и точный смысл ACK

### 4.1 До PG

ControlSpool сначала фиксирует exact command, его scope/generation и reserve budget, затем journal seal. `apply_accepted()` проверяет acceptance и точный hash команды **до** открытия PG transaction. В Dataset-primary store такая acceptance может быть локальным journal admission; отсутствие remote coverage нельзя маскировать историческим именем AcceptedSeal. [S6, S7, S8, S9]

### 4.2 PG apply

В одной транзакции:

1. Проверяется текущая database/writer generation.
2. Ищется прежняя запись operation_key или sequence.
3. Повтор принимается только при точном совпадении command bytes/hash/generation; возвращается существующий outcome.
4. Для новой операции sequence должен быть строго следующим.
5. Handler разрешается через pinned registry, проверяются полный набор precondition keys и SQL function contract.
6. Изменение предметной области, запись native outcome в applied_ops и продвижение applied_seq фиксируются атомарно.

Receipt возвращается лишь после настоящего commit ACK. Потеря соединения даёт `ApplyUnknown` с требованием reconcile той же operation в исходной database generation, а не повторной генерации результата моделью. [S9:350–460]

### 4.3 После PG

`RecoveryJournal.record_outcome()` сохраняет exact outcome и проверяет readback. В Dataset-primary режиме local outcome без covered_version вызывает `JournalUnknown('outcome_pending_dataset_checkpoint')`. ControlSpool возвращает такой известный pending в NOT_SENT, а не объявляет DURABLE. Только `_durable()` записывает remote receipt и событие saved. [S6:698–749, S7:850–951]

| Наблюдение/ответ | Допустимая интерпретация |
|---|---|
| Local staged / journal admission | Запрос удерживается имеющимся контроллером; внешняя сохранность ещё pending |
| Native PG APPLIED | Каноническая транзакция выполнена в текущей PostgreSQL |
| Dataset published | Provider version создана/найдена; окончательный native readback ещё может ожидаться |
| Verified Dataset + exact covered outcome | Можно сообщать Saved для покрытых bytes |
| UNKNOWN | Удерживать исходные identities и читать состояние; не объявлять failure/success и не создавать новую operation |

Никаких новых receipt levels поверх существующих в переносе не требуется. Требуется корректно отображать уже существующие состояния.

## 5. Когда и как происходит checkpoint

### 5.1 Группировка и условия

`_refresh()` сначала возобновляет retained publishing/published intent после API restart **без второго capture и второго create/version effect**. При отсутствии грязной applied/control/outcome части checkpoint не создаётся.

Новое сохранение запрашивается, если:

- work-cycle полностью применён: local staged sequence == applied_sequence == accepted_sequence, и 15 минут нет изменений;
- либо наступил planned_stop;
- либо возраст грязного состояния достиг 30-минутного интервала.

Dirty clock учитывает timestamp последнего verified Dataset. Непрерывная работа не должна бесконечно отодвигать запрос checkpoint. Но 30 минут — **порог инициирования**, не гарантированное время завершения upload и не универсальный RPO SLA: capture всё равно требует допустимого prefix, текущего writer и успеха provider/readback. [S4:303–393]

Planned stop включает draining, возраст >= 8h45m и наличие successor у текущего run. `before_planned_stop()` ждёт существующий task или запускает принудительный refresh только при текущем binding; общий STOP бюджет ограничен оставшимся временем до девяти часов. [S4:247–260,303–326; S5:573–603]

Есть дополнительные **RKB-specific** forced checkpoints: terminal disagreement/case outcome и schema barrier. Их нельзя автоматически применять к каждой статье, SQL row, токену или ответу OpenCode. [S5:417–433; S4:271–302]

### 5.2 Capture и публикация

Короткий source-intake lock фиксирует exact prefix и source heads. Он снимается до большого backup/upload. `_WorkCycleCapture` сохраняет проверки owner, exact native outcomes и prefix и не держит весь intake под сетью. Более поздние команды могут продолжать работу. [S4:28–125; S10:137–208]

Native capture:

- проверяет PG17, writer, database generation, pgvector и dedicated cluster;
- вызывает pg_basebackup с `--wal-method=stream --checkpoint=fast --manifest-checksums=SHA256`;
- запускает pg_verifybackup;
- создаёт tar всех файлов с hash/size/inventory и fsync;
- загружает archive **прямо из Kaggle** по scoped upload ticket;
- возвращает проверенные blob size/hash и manifest.

Это полный физический образ с согласованным WAL, а не journal-only/incremental backup. Реальный восстановленный cursor архива может быть новее заявленного lower bound; такой concurrent commit сам по себе не ошибка. [S11:335–391,197–210]

Dev загружает ограниченные manifest/control/journal bytes, фиксирует publication intent в SQLite **до** create/version. Publisher reconciles exact expected version, operation description, ready status и полный список файлов. При потерянном ответе он делает readback, а не слепой повтор. Старые версии текущая реализация не удаляет (`delete_old_versions=False`). [S10:175–219; S12:217–286]

### 5.3 Проверка и promotion

После публикации notebook получает `dataset_verify` и **скачивает опубликованный архив** по scoped ticket через NativeSnapshotTransfer. Ticket должен соответствовать Dataset/version/database generation/name/size/hash. Это native archive byte readback, а не полный новый restore drill на каждом checkpoint. После проверки временный download удаляется. [S4:181–205; S13:265–287]

`DatasetRecoveryStore.promote()` проверяет:

- точный native readback;
- монотонную Dataset version/base sequence и ту же database generation/owner/slug;
- manifest, journal и control из этой версии;
- неизменный predecessor и текущего writer;
- для каждого covered object совпадение **data и CAS token**.

Только такие строки получают covered_version в той же транзакции, что и latest pointer. Записи после capture не получают ложного Saved. Повтор уже успешной promotion не покрывает позднейшие local writes. `saved_seq` читает только latest verified Dataset base_sequence. [S8:130–197; S4:216–218]

## 6. Kernel loss и восстановление

1. Dataset checkpoint явно pin-ит owner/slug/version, manifest/archive/control/journal hash и sizes, database generation и base sequence.
2. При создании DatasetRecoveryStore bounded control/journal импортируются из pinned версии. Сохранившиеся Dev pending rows не заменяются неявным «latest».
3. До READY создаётся один retained restore owner. Dataset transport/tickets выбираются вместо прежнего S3 branch.
4. Native owner проверяет archive, manifest, версии и исходную PG writer identity, запускает отдельный candidate PG.
5. PhysicalSnapshotRecovery строит хвост journal от snapshot boundary к текущему head. Это непрерывный sequence; ancestry, byte/command budgets и gaps проверяются.
6. Replay использует **точные команды и handler fingerprints**, прежний PG writer для replay; не просит модель повторно придумать результат.
7. Native finish проверяет exact receipts/cursor/control, retired old writer и new writer, schema и readiness. Только затем control writer активируется.
8. Replay outcome, ещё не включённый в Dataset, остаётся pending до следующего checkpoint; восстановление PG не фабрикует Saved. [S14, S15, S11:594–660]

API-only restart при уже активированной прежней authority не восстанавливает старую базу поверх живой PG: rebuild maintenance facade, потом обычная history verification. [S14:188–220]

### Failure matrix

| Отказ | Что остаётся | Результат |
|---|---|---|
| WSS disconnect / API restart, kernel и Dev диск целы | PG и queue/spool | Reconcile прежние identities; не создавать новый бизнес-эффект |
| Kernel потерян, Dev spool цел | Verified Dataset + точный pending journal | Восстановить base и replay хвоста; pending outcomes дождутся следующего Dataset |
| Dev процесс потерян, SQLite цела | Publication intent, pointer, exact journal | Возобновить readback/capture ownership |
| Потеря kernel и Dev pending SQLite | Только внешне покрытая Dataset version | Новейшие ещё pending операции нельзя считать сохранёнными |
| Lost provider upload/version ACK | Retained publication intent | Readback/reconcile; не повторять create/version без доказательства |
| Writer сменился во время upload/readback | Старый capture не имеет права promote | Pointer не продвигается устаревшим владельцем |

Защита replay не является обещанием произвольного объёма хвоста: SnapshotLimits defaults — 2048 tail commands / 16 MiB tail bytes / 1 MiB control, 2 млн receipts; исчерпание бюджета fail-closed, а не бесконечный replay. [S11:93–107; S15:344–359]

## 7. 100 ГБ и расхождение docs/code

### 7.1 Текущие ограничения

| Уровень | Проверенное ограничение |
|---|---|
| SnapshotLimits | defaults archive 8 GiB, unpacked 16 GiB, timeout 900s |
| SnapshotRecoveryConfig | **валидатор запрещает** archive >8 GiB, unpacked >16 GiB, seconds >900 |
| SourceSnapshotConfig | bounded archive/unpacked до тех же пределов |
| SourceRestoreConfig | archive <=8 GiB |
| Archive creation | обычный uncompressed tar, bounded write |
| Native capture workspace | запрашивает free-space budget `max_unpacked_bytes*2` |
| Publication | все version files заново; старые версии сохраняются |
| Native verification | полный download опубликованного archive |

[S11:93–107,197–210,335–384; S16:14–34; S17:22–40; S14:14–40; S12:249–286; S13:265–287]

**Нельзя принять фичу “100 ГБ поддерживаются” одним увеличением константы.** Для копирования 100 ГБ несжатой базы нужны затраты на backup + tar, upload полного archive и его readback; живой PGDATA остаётся отдельно. Конкретная доступная Kaggle ёмкость, upload/download time и успешный 100-ГБ restore не проверены. Эти показатели — implementation/acceptance gate, а не вопрос к владельцу о выборе нового backup.

Нужен единый bounded size/time profile, проходящий через launch config, native factory, snapshot tickets/transfer, restore config, staging/free-space check и STOP deadline. Границы должны расширяться согласованно; превышение должно возвращать понятное pending/capacity состояние и не выдавать Saved.

### 7.2 Два описания нельзя смешивать

В `docs/operations/bge-kaggle.md`:70 написано: control checkpoint 15 min, changed PG base 4 h или раньше по pressure/planned handover. Это **не то поведение, которое следует из нынешнего SourceDatasetCheckpoints и native capture**.

Для реализации и аудита текущий code path имеет приоритет: SourceDatasetCheckpoints создаётся с 1800/900 и ведёт к full PG capture. Нельзя из документации вывести, что current код уже умеет лёгкую 15-минутную journal-only Dataset version и редкий 4-часовой PG base. Такая оптимизация в прочитанном пути не установлена.

Внешний weekly backup упоминается в старой документации, но по текущему объёму задачи **не переносится и не проектируется**.

## 8. Конкретный вариант переноса в MDH

### 8.1 Общий пакет

Извлекать механизмы, сохраняя существующие состояния и authority:

- `recovery_journal.py`: ExactCommand, bounded segments/head, exact acceptance, outcomes, idempotency.
- `dataset_recovery_store.py`: local pending objects, exact coverage tokens, verified pointer.
- `kaggle_dataset_checkpoint.py` + `kaggle_dataset_seed.py`: один checkpoint owner, coalescing, clocks, retained publication, planned-stop path.
- `kaggle_dataset_publisher.py` + `kaggle_dataset_tickets.py`: scoped direct transfer и provider reconciliation.
- `kaggle_pg_snapshot_native.py`, `pg_snapshot_recovery.py`, `kaggle_restore_startup.py`: physical PG base, exact replay, native readiness и writer handover.
- `queue_writer.py`: current writer/publication fence; адаптировать к существующему MDH ledger, не добавлять вторую очередь.
- `bge_queue.py`: activity/idle/drain/UNKNOWN reservation как отдельную lifecycle policy, не копируя RKB singleton/BGE domain.

### 8.2 MDH adapter

MDH сохраняет канонический PostgreSQL в Kaggle и существующую Dev SQLite authority. Adapter предоставляет schema/route registry, domain command payload и SQL handlers, existing job identity, PG readiness, Dataset config и active-writer binding.

Данные “значимое новое” и решения моделей должны становиться точными accepted mutations; replay воспроизводит их bytes/SQL outcome, а не запускает свежий OpenCode для повторного анализа. OpenCode executors не получают dataset-provider credentials; scoped blob tickets остаются control-owned. Политика free-model exhaustion/замены executor относится к исполнителям, а не даёт права повторять PG effects или делать новый saved ledger.

Для начального переноса использовать механизм и явно описанные defaults РКБ; MDH useful-idle переопределить как `useful_idle_seconds=2220` по раннему явному требованию владельца о 37 минутах. Отдельно вынести bounded capacity profile для 100 ГБ. Publication — на групповых границах/интервале/planned stop, не на каждой статье, строке SQL, heartbeat или модельном токене. Если реальная частота full capture для 100 ГБ оказывается неприемлемой, это конкретный blocker текущего решения и предмет оптимизации существующего Dataset path; не нужно молча обещать уже реализованный incremental режим.

### 8.3 Проверяемые implementation gates для Opus

- Раздельно зафиксированы donor useful-idle 1800s и MDH override 2220s; разные clocks, service heartbeat не продлевает useful demand.
- Единственная canonical PG; Dev journal явно ограничен и не обслуживает business reads.
- Duplicate operation возвращает прежний native outcome; конфликт bytes/sequence/generation отклоняется.
- PG success остаётся pending до verified Dataset coverage.
- Late local write не покрывается старым checkpoint; retired writer не promotes.
- Lost publication ACK возобновляется без повторной version creation.
- Kernel loss воспроизводит exact journal tail; API restart не перезаписывает живую PG старым base.
- Size/time limits проходят весь путь согласованно; 100 ГБ не marked supported до измеренной capture/upload/readback/restore capacity.
- Нет per-row/per-token upload, новой backup системы и параллельного receipt store.
- RKB domain routes/readiness/Case forcing не попадают в универсальный core.

## 9. Реестр pinned источников

Все ссылки относятся к SHA `0b7590628dcb29a270727f3920607084dde30720`.

[S1]: https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/0b7590628dcb29a270727f3920607084dde30720/src/regional_knowledge/bge_queue.py
[S2]: https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/0b7590628dcb29a270727f3920607084dde30720/docs/configuration.md
[S3]: https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/0b7590628dcb29a270727f3920607084dde30720/docs/operations/bge-kaggle.md
[S4]: https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/0b7590628dcb29a270727f3920607084dde30720/src/regional_knowledge/kaggle_dataset_checkpoint.py
[S5]: https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/0b7590628dcb29a270727f3920607084dde30720/src/regional_knowledge/kaggle_service_startup.py
[S6]: https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/0b7590628dcb29a270727f3920607084dde30720/src/regional_knowledge/recovery_journal.py
[S7]: https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/0b7590628dcb29a270727f3920607084dde30720/src/regional_knowledge/control_spool.py
[S8]: https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/0b7590628dcb29a270727f3920607084dde30720/src/regional_knowledge/dataset_recovery_store.py
[S9]: https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/0b7590628dcb29a270727f3920607084dde30720/src/regional_knowledge/kaggle_apply.py
[S10]: https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/0b7590628dcb29a270727f3920607084dde30720/src/regional_knowledge/kaggle_dataset_seed.py
[S11]: https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/0b7590628dcb29a270727f3920607084dde30720/src/regional_knowledge/kaggle_pg_snapshot_native.py
[S12]: https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/0b7590628dcb29a270727f3920607084dde30720/src/regional_knowledge/kaggle_dataset_publisher.py
[S13]: https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/0b7590628dcb29a270727f3920607084dde30720/src/regional_knowledge/kaggle_recovery_phases.py
[S14]: https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/0b7590628dcb29a270727f3920607084dde30720/src/regional_knowledge/kaggle_restore_startup.py
[S15]: https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/0b7590628dcb29a270727f3920607084dde30720/src/regional_knowledge/pg_snapshot_recovery.py
[S16]: https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/0b7590628dcb29a270727f3920607084dde30720/src/regional_knowledge/kaggle_recovery_composition.py
[S17]: https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/0b7590628dcb29a270727f3920607084dde30720/src/regional_knowledge/kaggle_snapshot_startup.py

### Быстрые переходы к ключевым строкам

| Вопрос | Прямое доказательство |
|---|---|
| Idle 30min и clock | [bge_queue.py, L21–62](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/0b7590628dcb29a270727f3920607084dde30720/src/regional_knowledge/bge_queue.py#L21-L62) |
| Useful activity и draining | [bge_queue.py, L171–265](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/0b7590628dcb29a270727f3920607084dde30720/src/regional_knowledge/bge_queue.py#L171-L265) |
| Payload в журнале | [recovery_journal.py, L131–173](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/0b7590628dcb29a270727f3920607084dde30720/src/regional_knowledge/recovery_journal.py#L131-L173) |
| Локальный journal, не remote persistence | [dataset_recovery_store.py, L1–54](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/0b7590628dcb29a270727f3920607084dde30720/src/regional_knowledge/dataset_recovery_store.py#L1-L54) |
| PG атомарность и commit ACK | [kaggle_apply.py, L350–460](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/0b7590628dcb29a270727f3920607084dde30720/src/regional_knowledge/kaggle_apply.py#L350-L460) |
| Exact coverage promotion | [dataset_recovery_store.py, L130–197](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/0b7590628dcb29a270727f3920607084dde30720/src/regional_knowledge/dataset_recovery_store.py#L130-L197) |
| Checkpoint defaults и Saved | [kaggle_dataset_checkpoint.py, L158–218](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/0b7590628dcb29a270727f3920607084dde30720/src/regional_knowledge/kaggle_dataset_checkpoint.py#L158-L218) |
| Quiet/max interval/restart/planned stop | [kaggle_dataset_checkpoint.py, L303–393](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/0b7590628dcb29a270727f3920607084dde30720/src/regional_knowledge/kaggle_dataset_checkpoint.py#L303-L393) |
| Full physical backup | [kaggle_pg_snapshot_native.py, L335–391](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/0b7590628dcb29a270727f3920607084dde30720/src/regional_knowledge/kaggle_pg_snapshot_native.py#L335-L391) |
| Uncompressed tar | [kaggle_pg_snapshot_native.py, L197–210](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/0b7590628dcb29a270727f3920607084dde30720/src/regional_knowledge/kaggle_pg_snapshot_native.py#L197-L210) |
| Direct upload, retained intent | [kaggle_dataset_seed.py, L137–219](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/0b7590628dcb29a270727f3920607084dde30720/src/regional_knowledge/kaggle_dataset_seed.py#L137-L219) |
| Provider reconciliation | [kaggle_dataset_publisher.py, L217–286](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/0b7590628dcb29a270727f3920607084dde30720/src/regional_knowledge/kaggle_dataset_publisher.py#L217-L286) |
| Full archive readback | [kaggle_recovery_phases.py, L265–287](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/0b7590628dcb29a270727f3920607084dde30720/src/regional_knowledge/kaggle_recovery_phases.py#L265-L287) |
| Base + exact journal replay | [pg_snapshot_recovery.py, L129–232](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/0b7590628dcb29a270727f3920607084dde30720/src/regional_knowledge/pg_snapshot_recovery.py#L129-L232) |
| Hard8/16GiB limits | [kaggle_recovery_composition.py, L14–34](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/0b7590628dcb29a270727f3920607084dde30720/src/regional_knowledge/kaggle_recovery_composition.py#L14-L34) |
| Документированный иной график15min/4h | [docs/operations/bge-kaggle.md, L64–70](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/0b7590628dcb29a270727f3920607084dde30720/docs/operations/bge-kaggle.md#L64-L70) |


