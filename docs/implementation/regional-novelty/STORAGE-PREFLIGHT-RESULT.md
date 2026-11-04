# BASE + WAL: результат предварительных проверок

Дата: 04.11.2026. Статус: **П1–П2 выполнены; передачи 100 МБ / 1 ГБ / 10 ГБ выполнены; ёмкость/квоты П3 и длительность П4 подтверждены частично. Реализация хранения и продуктовая приёмка не завершены.**

Решение владельца: [STORAGE-BASE-WAL.md](STORAGE-BASE-WAL.md), исходник [MDH@bcd7acfb6d0cbe635a90528dc30ed1f22c2f2ae2](https://github.com/onedayonemasterpiece/my-data-hub/blob/bcd7acfb6d0cbe635a90528dc30ed1f22c2f2ae2/docs/implementation/regional-novelty/STORAGE-BASE-WAL.md). Применяется [devserver-simplicity](https://github.com/onedayonemasterpiece/codex-devcoveer-mcp/blob/main/skills/devserver-simplicity/SKILL.md).

Источник наблюдений: [storage-preflight-20261104.json](evidence/storage-preflight-20261104.json). Живые действия выполнены интегратором ChatGPT на отдельных синтетических ресурсах; успешные проверки не повторялись без причины. Хэши внутри существующих provider receipts — поля действующего протокола, не предложение новых хэшей WAL или manifest.

## 1. Что реально выполнено

| Проверка | Наблюдение | Результат для реализации |
|---|---|---|
| П1: наследование файлов новой версией | v1 содержала только `segment-a.txt` (81 байт); v2, загруженная только с `segment-b.txt` (80 байт), содержала только B. Оба metadata-read вернули ожидаемую версию и `private=true`; оба каталога имеют `complete=true`, `next_cursor=null` | В проверенном пути публикации старый файл не наследуется. Каждая версия WAL Dataset должна включать полный необходимый хвост текущей базы |
| П2: удаление Dataset | `delete_dataset`, `outcome=applied`, одна попытка; последующий полный inventory: `complete=true`, `probe_present=false` | Штатное удаление работает; отдельный сборщик мусора не требуется |
| П4: отдельный CPU notebook | Запуск завершён с `run_state=complete`, `terminal=true`; JSON-результат прочитан полностью. Notebook затем удалён: `delete_notebook`, `outcome=applied` | Подтверждён короткий CPU probe и видимость ресурсов, не 12-часовой прогон и не размещение 100 ГБ |

Dataset: `zigomaro/mdh-base-wal-probe-20261104-6da37c`; task UUID `6083e5f6-bb4a-46e1-8b50-27cebac72e1d`.

| Действие | Operation UUID | Effect UUID | Наблюдённая длительность |
|---|---|---|---|
| Create v1 | `856958d0-5ef8-54c2-ac9b-496d796c93ef` | `b1c15e7f-2b24-4f02-8fa8-56dc3ff536f5` | 15.741 с |
| Create v2 | `4ed3577e-07ed-5de1-85b4-a9bf2e208c66` | `245e2ac6-47f7-4ac3-8558-bf74dfb93e23` | 10.931 с |
| Delete | `249bf2a4-16bd-5627-a7bc-ba5a90857d4f` | `5a563d65-8976-4bd8-8b7f-39f0e97015e8` | 4.088 с |

Это микрозамеры полного вызова, включая MCP/protocol overhead, для файлов порядка 80 байт. Они **не являются upload/ready SLA**, пропускной способностью больших загрузок или основанием экстраполировать время 100 ГБ. Каталог приложения может отражать его протокол; его размер не устанавливает нативный лимит файлов Kaggle.

## 2. CPU probe и границы П4

Notebook: `zigomaro/mdh-wal-capacity-20261104-3d57ae`, run reference `zigomaro/mdh-wal-capacity-20261104-3d57ae/1`, kernel ID `142774470`.
Task UUID: `8f426120-abbb-4d26-ab12-8c4a8df27a82`; run UUID: `82b329d3-bc56-43f6-9622-43b57c7cc343`.
Run operation: `2f595c42-6d6f-599f-ba7f-3870b4ee2c09`; delete operation: `8c1270e4-59dd-5213-ba1d-7f3ce4ea9ef6`.
Наблюдение статуса: `2026-11-04T22:06:07.386023+00:00`. CPU без ускорителя, internet выключен; получены 4 CPU, 33,658,159,104 байта MemTotal. Загрузок Dataset этот notebook не делал.

| Путь | Наблюдение `statvfs` | Что это доказывает |
|---|---|---|
| `/kaggle/working` | Всего 20,957,446,144 байта ≈20.957 GB; доступно 20,940,562,432 | Видимая ёмкость этого файлового пространства существенно меньше целевых 100 ГБ |
| `/tmp` | Доступно 1,038,219,055,104 байта ≈1038.219 GB; другой device | Видимость свободного места; не выделенная notebook квота, не резервирование и не доказательство возможности записать 100 ГБ |

Все GB в этой таблице десятичные. Никакого allocation/write-теста на 100 ГБ и испытания длительности сессии не выполнено. Для размещения PGDATA, базы, локального WAL и упаковки нужен фактически доступный рабочий запас; сумма `statvfs` не подменяет это подтверждение.

[Документация Kaggle Notebooks](https://www.kaggle.com/docs/notebooks) описывает до 12 часов CPU-сессии и 20 GB дискового output. Это опубликованные условия, а не измеренная длительность данного запуска. При непрерывной работе с полными 12-часовыми сессиями расчёт — `365 × 24 / 12 = 730` плановых основных версий в год, плюс аварийные/досрочные завершения и редкая ротация базы. Фактические квоты аккаунта, доступность такого расписания и годовой итог **UNKNOWN**: официальный MCP не дал авторизованных сведений о квотах.

## 3. П3: измеренная передача и оставшаяся ёмкость

**Ряд 100 МБ / 1 ГБ / 10 ГБ выполнен. П3 в целом остаётся частичным:** 100-ГБ размещение, фактический остаток private quota и учёт истории версий ещё не подтверждены.

Private Dataset: `zigomaro/mdh-p3-5b1a3527200e`; run UUID `5b1a3527-200e-4705-9a5f-912b7a7f9c3e`. Времена в секундах, размеры десятичные.

| Объём | PUT | Finalize → ready | Начало PUT → ready | Версия / состояние |
|---|---:|---:|---:|---|
| 100 МБ (100,000,000 байт) | 3.699 | 14.104 | 42.617 | 1 / ready |
| 1 ГБ (1,000,000,000 байт) | 10.334 | 17.931 | 52.780 | 2 / ready |
| 10 ГБ (10,000,000,000 байт) | 211.114 | 145.914 | 382.143 | 3 / ready |

Это синтетические несжимаемые байты, созданные в `/tmp` Kaggle; credentials Kaggle в notebook не передавались. PUT идёт непосредственно из Kaggle по разовой upload capability; файлы удаляются из scratch перед завершением producer. На Dev прочитаны только тестовые 100 МБ для существующей проверки владения ресурсом при cleanup, а не PGDATA или WAL. Путь использует действующий `KaggleProviderAdapter` из чистого checkout deployed **f7eb3c5f674b772751c815aa055a52f1f52e1828**, установленный Kaggle SDK 2.2.4 и существующие provider contracts. Это standalone operator preflight из [canary runbook](../../operations/brokered-checkpoint-upload.md); production guards и deployed HTTP gateway не менялись.

PUT — только передача. «Finalize → ready» начинается перед вызовом финализатора и включает его API/reconciliation. «Начало PUT → ready» также включает завершение producer, получение его результата и опрос; это не сумма первых двух столбцов. Генерация payload не входит в эти три интервала. Один sample на размер не устанавливает p95, SLA или линейную оценку времени 100 ГБ.

После ready v1 первый диагностический readback завершился `HTTPError`. Отдельное чтение той же v1 прошло успешно; причина первого сбоя не установлена. Продолжение использовало сохранённый intent/receipt и настоящий readback identity, не повторяя PUT или публикацию v1. У v3 финализатор вернул неоднозначный исход (`finalizer_ambiguous=true`); последующие read-only status и metadata reconciliation подтвердили именно версию 3 с ожидаемым файлом. Повторной финализации не было; причина неоднозначного ответа этим замером не устанавливается. Все три producer notebook и Dataset удалены через штатный adapter с зарегистрированными claims; в evidence сохранены applied receipts. Сырые upload URL и credentials в evidence не включены.

Исходник одноразового замера: [storage-publication-probe.py](evidence/storage-publication-probe.py). Это артефакт измерения, не новый production publisher или checkpoint framework. Первоначальный runner: SHA-256 `7f76ff22787f349e7fc77f86bff9bfeb65146d93dda8dc47579e5248144026ef`; продолжение: `9ec3cba5725041ac9f4eaea465572af325c1790b7e701d0826c6e0e02ed6c145`.

| Уровень | Наблюдаемый контракт / опубликованное условие | Следствие |
|---|---|---|
| MCP transport | 64 MiB на файл в `src/my_data_hub/mcp/provider_schemas.py`, 256 MiB суммарно в `control_plane/provider_uploads.py` | Этот интерфейс не предназначен для всей физической базы; его bounds не мешают разрешённому operator preflight |
| Native transport contract | 10 GiB на blob: `providers/kaggle/adapter.py`, `MAX_BROKERED_BLOB_BYTES`; 20 GiB суммарно: `checkpoints/brokered_upload.py` | Это ограничения реализации, не лимиты Kaggle. Для 100 ГБ адаптировать существующий streaming путь; не заводить второго владельца публикации |
| Kaggle Datasets, общая документация | До 200 GB Dataset, 200 GB private storage, 50 файлов верхнего уровня | Не доказывает остаток конкретного аккаунта или возможность хранить current/previous, временный WAL и историю |

Источники: [Kaggle Datasets](https://www.kaggle.com/docs/datasets), [разъяснение Kaggle Staff об uncompressed size](https://www.kaggle.com/product-feedback/462847). Условия распаковки/учёта объёма не заменяют подтверждение правил квоты для старых версий. SDK `quota_view` возвращает GPU/TPU runtime quota, а per-version file summary не сообщает charged account usage. Фактический остаток private quota и способ учёта истории — **UNKNOWN**; сумма размеров latest files не выдаётся за свободное место аккаунта.

Начальная настройка: **N = 15 минут**, `archive_timeout = 300s`; редкая online-база при длительности полной публикации хвоста **свыше 3 минут**. Это инженерные начальные значения для измерения на настоящем WAL, а не доказанная гарантия 15–20 минут. Учесть upload/processing и фактическое отставание в MCP. Прохождение ряда до 10 ГБ не отменяет требование вместимости ~100 ГБ.

## 4. Что показало чтение кода

Исследованный source: [MDH@8a0ec2014e12d62600ee830c6b9fd0d1bd0c234d](https://github.com/onedayonemasterpiece/my-data-hub/tree/8a0ec2014e12d62600ee830c6b9fd0d1bd0c234d). Это source findings, не сообщение о внесённых исправлениях.

| Участок | Необходимое изменение в принятой схеме |
|---|---|
| `master_runtime/postgres.py` | Генератор сейчас задаёт `archive_mode=off`; `write_configuration()` перезаписывает `postgresql.auto.conf`. Archive/recovery параметры должны сохраняться через этот же генератор |
| `master_runtime/bootstrap.py` и notebook lifecycle | Сохранить restore до start; дождаться выхода из recovery до записи. Для холодной базы порядок STOP: прекратить работу → успешный fast-stop → упаковка → публикация ready → удаление старого хвоста |
| `checkpoints/archive.py`, `manifest.py`, provider mapping | Сейчас создаются physical base и второй полный `pg_dump`; logical artifact обязателен. Убрать дублирующий dump вместе с его требованиями, сохранив нужную читаемость прежних физических checkpoints |
| Existing provider/publisher и streaming upload | Сохранить транспорт и одного владельца публикации. Отсоединение обязательного verifier/acceptance не означает удаление рабочего streaming транспорта |

Использовать существующие **PostgreSQL 18.4 + pgvector 0.8.6**. Прежнее упоминание PG17 исправляется в документации; новый storage scope не требует смены версии runtime.

## 5. Текущее состояние и оставшиеся блокеры

Наблюдавшийся MDH master — **ABSENT**, deployed commit `f7eb3c5f674b772751c815aa055a52f1f52e1828`. Синтетический CPU notebook не был master и не меняет этот статус. Production BASE+WAL код не внедрён, live РКБ не изменён.

1. Подтвердить реальное место для PGDATA + сжатой базы + WAL, остаток private quota и учёт истории версий. Передачи до 10 ГБ уже измерены; не повторять их без изменения условий и не выдавать statvfs за 100-ГБ проверку.
2. На реальном WAL проверить начальные N=15 минут и порог публикации 3 минуты; подтвердить расписание длинных сессий с учётом следующего сбора в существующем lifecycle. 730 версий — расчёт, не годовой замер; 12-часовая сессия не была измерена.
3. После П1–П4 внести узкие изменения существующего lifecycle/publisher, выполнить ровно три реальных PG18.4 + pgvector0.8.6 сценария из [storage contract](STORAGE-BASE-WAL.md), затем приёмку первой карточки.

**Все три PostgreSQL recovery-теста и восстановление продуктовой карточки — NOT_RUN.** Их нельзя заменить успешным Dataset CRUD или CPU probe. Документы и результаты synthetic preflight не являются реализацией продукта; новых инфраструктурных механизмов этот отчёт не вводит.

## 6. Выполнение и воспроизводимость

Замер выполнен через штатные DevCoveer project action/job aliases в изолированных source handoff: исходный рабочий checkout MDH не переключался и его tracked files не изменялись. Временная недоступность read-only `direct_ops_v2` устранена использованием поддерживаемых typed aliases и compatibility carrier; это не отказ в разрешении и не текущий блокер П3. Состояние работы сохранено в существующем work record. Production storage-код, live РКБ и voice deployment не менялись.

Прочитанные [provider-real.yml](../../../.github/workflows/provider-real.yml) и [ci.yml](../../../.github/workflows/ci.yml) не являются готовым независимым runner этого замера: первый привязан к self-hosted devstand и полной operational matrix, второй не имеет произвольного dispatch. Специальных Actions dispatch и изменений секретов не выполнялось. Три реальных PostgreSQL recovery-сценария относятся к следующему изменению storage-кода, а не к этому preflight.
