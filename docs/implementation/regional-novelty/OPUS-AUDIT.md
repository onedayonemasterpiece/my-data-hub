# My Data Hub: пакет для глубокого аудита проекта реализации

**Начинать аудит с этого файла.** Дата: 04.11.2026, Europe/Kaliningrad. Получатель: Opus с доступом только к **my-data-hub и regional-knowledge-base**. Все необходимые сведения из EventsBot, DAEP, Live, ai-resource-control и нового общего репозитория включены в этот каталог. Внешние ссылки служат provenance; отсутствие доступа к ним не блокирует чтение пакета.

## 1. Что проверять и какой результат нужен

Владелец поручил создать общий репозиторий Kaggle/WSS, начать реализацию и полностью подготовить проект первого продукта MDH. Первый продукт — **значимое новое в Калининградской области** с объяснением изменения, временем, местом/объектом, первоисточниками и сравнением с наблюдённым прошлым корпусом. Цель свежести — около шести часов. От пользователя не требуются SQL, миграции, выбор резервного хранилища или ручное управление notebook.

Нужен аудит связного продукта и минимального общего решения: достаточно ли проекта для исполнения, переиспользованы ли реальные механизмы РКБ, не возник ли второй controller/ledger, сохраняется ли полезный результат при quota exhaustion и kernel loss. Не превращать аудит в разработку нового framework или набор дополнительных формальных gates.

**Что выполнено сейчас:** создан приватный `onedayonemasterpiece/kaggle-runtime`; в нём начальный исполняемый package с generic codec, release/freshness и stop/fresh policies. Проверены 16 unit tests и compileall. Подготовлен настоящий проект следующей реализации, включая конкретные модули/миграции/API и перенос RKB durability. **Что не заявлено:** production adoption двумя потребителями, готовый Kaggle driver/WSS service, установленный новый OpenCode или работающий novelty продукт. Эти результаты имеют отдельные этапы и проверки.

Начальный общий source commit: **`52644f2748b602f54304e56424eee720d12a691b`**. Полный доступный здесь [снимок исходников и тестов](shared-runtime-source.md) позволяет проверить его без доступа к kaggle-runtime. Версия пакета 0.1.0 — source version, не сообщение об опубликованном wheel или rollout.

## 2. Решения владельца — не повторять прежние вопросы

| Тема | Решение для проекта |
|---|---|
| Приоритет | Первым делать значимое новое; другие виды аналитики развивать на том же корпусе позже |
| Free quota | Остановить исчерпавший среду Kaggle executor, запустить свежий и продолжить незавершённое |
| Credentials | По умолчанию не передавать OpenCode provider keys/auth; если ключ обязателен — только заранее настроенные scoped bindings, возможно несколько |
| Возврат доступности | Проверить фактически; новое окружение не выдаётся за доказанный сброс provider/account quota |
| Сохранение | Переживать исчезновение Kaggle по готовой схеме РКБ, не обновлять Dataset после каждой записи |
| SQL/DDL | Инженерная обязанность; пользователю нужен результат через продуктовый интерфейс |
| Размер | Active DB с индексами/данными менее 100 GB; первый пилот не обязан иметь такой размер |
| Внешние копии | Возможно позже на диск/другое облако; полностью вне текущей поставки |
| Общий код | Создать отдельный repo, общий versioned package и тонкие adapters, как организационно сделано у Миры |
| OpenCode | Постоянно актуальные версии во всех средах, per-attempt pin и управляемая активация |

Полная материализация указаний: [owner-decisions.md](owner-decisions.md). Она заменяет прежние открытые вопросы исследования. Раннее требование useful window 37 мин сохранено как MDH profile 2220s; у RKB фактически 1800s. Это разные consumer settings того же механизма, не числовая загадка для владельца.

## 3. Порядок чтения

| Порядок | Документ | Что он даёт аудитору |
|---|---|---|
| 1 | [Решения владельца](owner-decisions.md) | Обязательная продуктовая цель, scope, устранённые вопросы |
| 2 | [Полный проект реализации](implementation-plan.md) | Размещение, owners, общий код, исполнение, этапы W0–W7, recovery, acceptance |
| 3 | [Схема РКБ: code evidence](rkb-storage-evidence.md) | Точная цепочка Dev spool→PG→Dataset→replay, clocks, ACK/Saved, размерные пределы, docs/code различия |
| 4 | [Первый novelty slice](novelty-implementation.md) | Реальные gaps MDH, конкретные модули, additive migrations, typed inputs/results, MCP/query acceptance |
| 5 | [Контракты недоступных проектов](cross-project-evidence.md) | Самодостаточный EventsBot/Ultra/DAEP/Live/ARC packet, source excerpts, совместимость и ограничения |
| 6 | [Исходники общего пакета](shared-runtime-source.md) | Полный code/test snapshot, exact source commit и file hashes; ничего не требуется скачивать извне |
| 7 | [Provenance и проверка первого кода](runtime-code-evidence.md) | Что извлечено, что новое,16 tests, границы pure policies |
| 8 | [ADR-0020](../../adr/0020-shared-runtime-regional-monitoring.md) | Явное согласование RKB payload-bearing journal и существующих MDH инвариантов |
| 9 | [Первоначальное исследование](../../architecture/regional-monitoring-shared-kaggle-runtime-20261104.md) | Более широкий продуктовый контекст и требования people/projects/trends/guests/POI; вопросы §11 уже закрыты новыми решениями |

Если проверяется конкретный риск, переходить сразу к его строке в §6 ниже, затем к исходнику. Не требуется повторно перечитывать весь старый источник ради каждой строки нового кода.

## 4. Authority и изученные версии

Иерархия MDH: прямые решения владельца → exact imported research → corrective ADR-0016 → invariants/append-only contracts → derived docs/code/tests. [AGENTS](../../../AGENTS.md), [source of truth](../../00-source-of-truth.md), [invariants](../../../architecture/invariants.yaml), [ADR-0016](../../adr/0016-kaggle-postgresql-master-architecture-reset.md). Exact imported research не изменён.

| Источник | Проверенная версия |
|---|---|
| MDH до этого implementation пакета | `77a62c53c881120164f525a226ec296dcb3c77b3` |
| РКБ | `0b7590628dcb29a270727f3920607084dde30720` |
| Shared runtime source | `52644f2748b602f54304e56424eee720d12a691b` |
| EventsBot | `4600e9978a7eb9b22234590bfc76258feba0a905` |
| Ultra PR747 | OPEN / merged=false, `406826d9cae0b51e229dc1f28d9fdd4683a4f31f` |
| DAEP master | `d3559d731bb74fbb1262114b9aad15687bb54701` |
| live-interaction | `e710e8e8143a3724d16250a6e439ccd178c279ca` |
| ai-resource-control | `f359f6b6326f4ee11f851b3d451adf210e5259dd` |

Снимки устанавливают поведение кода. Предыдущий MDH endpoint показывал control plane ready/master ABSENT; это не live acceptance PG/novelty. Локальный OpenCode server был active, но semantic version не удалось аттестовать. Никакая строка git HEAD/README не используется как доказательство исполняемой версии.

## 5. Основной дизайн в одном месте

**Данные:** normal connector→existing landing/typed committer→full immutable source revision→детерминированные chunks/FTS/BGE→novelty assessment с prior evidence→одна master transaction/result/outbox→предметный MCP. Source capture продолжается при недоступности смысловой стадии. Event-only фильтр не выбрасывает из MDH мнения, изменения проектов и несобытийные тексты.

**Исполнение:** общий пакет устанавливается в независимые controllers. PG master отделён от сменяемых OpenCode executors. Каждый logical job/attempt/generation связан с точным provider account/kernel/execution. Старый generation не меняет canonical progress; законченный immutable output прежней попытки сохраняется и может быть переиспользован текущим владельцем.

**Долговечность:** bounded exact journal в существующей Dev control SQLite переживает потерю kernel; canonical PG и PGDATA только в Kaggle. Пакетный Dataset содержит base/control/journal; Saved только после native readback и точного coverage. Никакого дополнительного внешнего backup service. Одновременная потеря kernel и ещё не покрытого Dataset Dev journal остаётся границей этой схемы.

**Обновление:** release feed перечисляется по явной major; active attempt pin неизменяем. Fresh process подтверждает реально исполняемую версию. OpenCode v1 autoupdate только уведомляет; v2 auto install не перезапускает server. GitHub latest во время проверки указывал на v1.19.5 при доступном v2.2.5. Установщик должен учитывать обе особенности.

**Общие ресурсы:** существует ARC authority; её consumer SDK не перехватывает OpenCode sends автоматически. Внутренние retries/subagents должны участвовать в admission/accounting. Reservation retry после crash не должен создавать новый UUID и выдавать старый unknown за свободный budget. Это concrete integration task, не необходимость новой limiter БД.

## 6. Маршруты глубокого аудита

| Вопрос аудита | Где читать в MDH | Где читать в РКБ / приложении | Что будет дефектом |
|---|---|---|---|
| Один owner master и direct data plane | `orchestrator/master/coordinator.py`, `runtime_sdk/client.py`, `docs/operations/master-tunnel-broker.md` | RKB bge_queue/channel, §2 RKB evidence | Два controllers одного kernel, WSS объявлен автоматической заменой PG tunnel |
| Kernel loss без per-row Dataset | `connectors/durability.py`, `connectors/checkpoint_control.py`, `checkpoints/restore.py`, ADR0020 | recovery_journal/control_spool/dataset_recovery_store/kaggle_apply | COMMIT выдан за Saved; journal с payload назван metadata-only; модель повторяет уже принятый результат |
| Capture и promotion | `checkpoints/publisher.py`, `checkpoints/registry.py`, `orchestrator/master/coordinator.py` | kaggle_dataset_checkpoint/seed/publisher/recovery_phases | Старый writer promotes, поздние writes объявлены covered прежним Dataset |
| Restore/capacity | Existing master recovery, capacity plan | kaggle_pg_snapshot_native/recovery_composition/snapshot_startup/restore_startup | Лимит изменён в одном месте,100GB объявлено поддержанным без рабочего restore |
| Canonical ingestion | `connectors/contracts.py`, `connectors/runtime.py`, `connectors/postgres.py` | Novelty §2–6 | Новый envelope есть, committer всё ещё только daily statistics |
| Work/result identity | `sql/migrations/0004_orchestration.sql`, `sql/migrations/0005_sync_and_outbox.sql` | Novelty §6 | Один stage_run переиспользуется для несвязанных revisions, требуется удалить uniqueness ради результатов |
| Full source и BGE | `sql/migrations/0002_core_content.sql`, `sql/migrations/0003_analysis.sql`, `embeddings/models.py`, `embeddings/documents.py` | Novelty §5–7; RKB tokenizer | body_excerpt считается полным corpus; article подставлен в blogger contract; truncation теряет текст |
| Что такое новое | Planned regional_monitoring contracts/retrieval/novelty | Novelty §7–8 | Новый URL равен новому факту, operator scope выдан за evidence, reposts считаются независимыми |
| Quota→fresh executor | Shared rotation/provider snapshot | Owner decisions §2; cross-project §§3–5 | Timeout освобождает старый kernel; credentials наследуются; notebook creation считается quota reset |
| Постоянная актуальность | Shared releases snapshot и план updater | Implementation §7 | Только GitHub latest, обновление файла без процесса, постоянный старый pin |
| Shared reuse | Shared source snapshot и extraction manifest | Cross-project §2, RKB §8 | Копии модулей становятся четырьмя независимыми реализациями |

Все пути MDH в таблице приведены относительно `src/my_data_hub/`, если не начинаются с `sql/` или `docs/`; перед утверждением конкретной функции сверять фактическое дерево. Точные pinned источники и диапазоны — в evidence приложениях. Новые planned paths не следует считать отсутствующей реализованной фичей: аудит должен оценить их достаточность, а не объявить production regression.

## 7. Конкретные риски, уже найденные при подготовке

1. **Документация РКБ расходится с текущим snapshot path.** Старое описание control 15min/base4h не доказывает journal-only incremental Dataset. Current код делает full physical capture при policy 30min/quiet15min/planned stop. Audit должен оценить применимость к выбранному рабочему объёму.
2. **Размерные hard caps 8 GiB/16GiB/900s** находятся в нескольких validators. Цель <100 GB не подменяет этот код и не означает, что первый пилот должен работать на 100 GB.
3. **RKB journal содержит payload/outcome.** Нужна явная операционная граница в ADR0020 при сохранении запрета canonical business DB на Dev.
4. **Новый MDH ingress требует dispatcher**, поскольку имеющаяся сборка делает PostgresDailyStatisticsCommitter.
5. **BGE policies различаются:** модель MDH допускает 8192, RKB полный input 512. Новый content representation использует 512 с overhead; существующий model contract не переписывается молча.
6. **ARC reservation ограничена и имеет собственную identity.** Generic OpenCode turn нельзя считать одним учтённым send; повтор создания reservation после crash не является resume.
7. **Ultra ещё проект**, DAEP — дизайн. Их будущие модули не используются как существующая dependency.
8. **Начальный shared package — небольшие pure primitives.** Его16 tests не доказывают WSS/provider/PG живую интеграцию; следующая поставка должна дать двух настоящих consumers и один product path.

## 8. Как вернуть результат аудита

Аудитор возвращает один `.md` с вердиктом готовности **проекта к реализации**, отдельной оценкой начального shared source и точными находками. Для каждой существенной находки: продуктовый вред, concrete path/contract, минимальное исправление, связанный work package, различающая проверка. Разделять блокирующий дефект, необходимую реализационную задачу и необязательное улучшение.

Проверить: (а) принятые решения владельца сохранены; (б) storage/rotation не теряют принятые результаты; (в) scope не превратился во второй RKB/framework; (г) common code действительно может стать одним owner; (д) полный raw и evidence позволяют давать полезную новизну; (е) пакет можно исполнить без доступа к другим закрытым repo. Не требовать от владельца ответов про SQL, migrations, внешний backup или уже выбранный первый продукт.

Предпочтительные категории вывода: `READY_WITH_IMPLEMENTATION_TASKS` либо `CHANGES_REQUIRED` с конкретными изменениями. Эти метки относятся к документу и исходникам, не являются production readiness. Аудит не должен запускать Kaggle, модели, менять данные или публиковать что-либо без отдельной задачи на такой результат.
