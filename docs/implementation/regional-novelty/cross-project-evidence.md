# Приложение для исполнителя: недоступные проекты и переносимые контракты

Дата: 2026-11-04. Это самодостаточный evidence packet для исполнителя, которому доступны только `my-data-hub` и РКБ. Открывать перечисленные закрытые репозитории для понимания требований не требуется: существенные ограничения, контракты и короткие фрагменты приведены здесь. URL нужны для provenance. Это проект реализации и проверенные исходники, а не отчёт о выполненной интеграции.

## 1. Проверенные версии и статус

| Репозиторий | Точный SHA | Что действительно есть |
|---|---|---|
| `my-data-hub/main` | `77a62c53c881120164f525a226ec296dcb3c77b3` | Код предыдущего `91f7d9b...` плюс опубликованный аналитический отчёт и его индекс; compare показал только два документа |
| `events-bot-new/main` | `4600e9978a7eb9b22234590bfc76258feba0a905` | Kaggle provider, registry, source adapters, event apply, Live adapter; нет готового Ultra runtime |
| EventsBot Ultra PR747 | `406826d9cae0b51e229dc1f28d9fdd4683a4f31f` | PR повторно проверен: **open, merged=false**; проект и отчёт, не merged implementation |
| `my-daep/master` | `d3559d731bb74fbb1262114b9aad15687bb54701` | README, `docs/distributed-mode.md`, `docs/implementation-task.md`; исполняемый DAEP отсутствует |
| `live-interaction/main` | `e710e8e8143a3724d16250a6e439ccd178c279ca` | Общая библиотека Live/audio и тонкие consumer adapters |
| `ai-resource-control/main` | `f359f6b6326f4ee11f851b3d451adf210e5259dd` | Общая resource authority, SDK `WorkloadAdmission`, атомарный budget/ownership |

Refs повторно прочитаны в этой подготовке. Между ранее исследованным EventsBot `79f8b43374dd9b4b531533d7145f4c04974e61b5` и текущим main — шесть commits: guide output budget, документация, сохранение operator scope в parser. Перечисленные ниже Kaggle/Live/status/apply доноры в этой дельте не изменялись. Значимая свежая parser-дельта описана в § 3.4.

**Более новое решение владельца имеет приоритет над старым проектом:** запуск свежего Kaggle executor при исчерпании free quota; по умолчанию без OpenCode provider keys, при обязательной авторизации — несколько явно настроенных bindings; использовать готовую схему долговечности РКБ/private Dataset, не вводить внешние backups в этот проект; первым продуктом сделать значимое новое в регионе. SQL и миграции выполняют инженеры. Ни одно из этих решений не означает обнуление provider/account quota сменой IP.

## 2. Что извлекается в общую зависимость

| Переносимый механизм | Донор и точный code path | Ограничение переноса |
|---|---|---|
| Kaggle Dataset/kernel API | EventsBot `video_announce/kaggle_client.py` | Извлечь provider primitives create/version/status/output/push; класс содержит video scoring и ranking, их не переносить |
| Pre-submit intent, ambiguous launch reconciliation | EventsBot `kaggle_registry.py` | Сохранить barrier и reconciliation; убрать предположения «ровно два Dataset» и job identity `job_type:kernel_ref` |
| Callback/run authentication, dedup, heartbeat, leases | EventsBot `kaggle_status.py` | Это status ledger, не хранилище полного фактического/аналитического результата |
| Повторяемый bootstrap | EventsBot `source_parsing/telegram/service.py`, `guide_excursions/kaggle_service.py` | Общим сделать config → inputs visible → intent → push → output identity; scheduler/cursor/rendering оставить потребителю |
| Source revision и raw capture до cursor | EventsBot `vk_source_envelope.py`, `models.py:VKSourcePacket`, `vk_intake.py:_persist_vk_source_packet` | Переиспользовать форму source packet/provenance; не переносить event-only admission в MDH |
| Thin WSS adapter pattern | EventsBot `static_site_live_socket.py`, Live `src/live_interaction/socket_transport.py` | Переиспользовать форму общего package + adapter, не называть Live/audio протокол готовым Kaggle task protocol |
| Admission и account budgets | ARC `src/ai_resource_control/workload.py`, `client.py`, migrations 009–016 | Подключить SDK к существующей authority; не копировать её ledger в каждый process |
| Job/attempt/code checkpoint модель | DAEP `docs/distributed-mode.md` | Это дизайн. Branch/commit merge и GitHub App broker относятся только к coding-профилю, не обязательны для MDH extraction |

Точные ссылки: [E-Kaggle], [E-Registry], [E-Status], [E-TG], [E-Guide], [E-VK], [E-Live], [A-Code], [D-Design].

**Общий runtime не владеет** канонической бизнес-БД, моделью Event, семантикой региональной новизны, source allowlist, публикациями, POI, SQL-миграциями MDH или планом code merge DAEP. Он владеет совместимыми provider/transport/executor contracts, физической identity попытки и доставкой, но получает business result sink от consumer.

Согласованные ограничения профиля РКБ/MDH: hard limit одной фактически encoded `ExactCommand` — 1 MiB; начальный raw batch budget ≤512 KiB требует проверки полного envelope с overhead. Большой материал передаётся bounded landing parts с replay либо через применимый durable artifact path; ссылка на исчезающий Kaggle artifact долговечность не обеспечивает. Existing bounded recovery journal на Dev сохраняет exact payload/outcome bytes для восстановления, но не становится второй canonical/query БД. Новый content BGE representation v1 ограничивает полный input 512 tokens, включая special tokens/prefix; существующий базовый model max 8192 остаётся фактом прежнего контракта. Цель менее 100 ГБ относится к active БД без внешних backup-копий, а capacity private Dataset/checkpoints считается отдельным инженерным бюджетом; внешние backup-сервисы вне этой поставки.

## 3. EventsBot: работающие механизмы и недостающий Ultra

### 3.1. Submit, UNKNOWN и resource ownership

`register_launch_intent` записывает intent до remote push, делает fsync файла и каталога, затем читает registry обратно. Если ответ submit потерян, `mark_launch_intent_indeterminate` сохраняет намерение; reconciliation ищет именно ожидаемые input datasets и новую remote revision. Это защищает от второго запуска после неоднозначного исхода. Registry — process-local JSON с lock, **не** готовый распределённый coordinator. [E-Registry]

Короткие точные выражения из реализации, пропущенные строки обозначены многоточием:

```python
intent_id = f"{job_type}:{run_id}"
...
_save_registry(data)
persisted = _load_registry()
```

Текущая проверка требует `len(dataset_slugs) == 2`. Для общей библиотеки это product-specific ограничение заменяется явным manifest произвольного валидного набора inputs. Job identity должна включать отдельно логический job и физический execution:

```text
job_id → attempt_id + generation
physical_execution = provider_account + kernel_ref + observed_kernel_version/run_id
controller_id + resource_scope + current_owner/fence
```

Это целевой контракт, не названия уже реализованных dataclasses. Один kernel reference может использоваться снова, поэтому совпадения имени notebook недостаточно для принятия output.

`kaggle_status.py` хранит hash run token, использует constant-time compare, dedup `event_uid`, последовательность сообщений, короткие SQLite transactions и leases. Переносить бизнес-SQLite в runtime не нужно; общий transport получает persistence adapter. Heartbeat/terminal status не доказывает сохранение payload. [E-Status]

**Не переносить опасную эквивалентность «истёк локальный timeout = старый notebook остановлен».** В `remote_telegram_session.py` старые transient lookup errors после default 390 минут перестают блокировать новый запуск, хотя terminal evidence может отсутствовать. AGENTS требует более строгого отношения к UNKNOWN. Это выявленное расхождение механизма и контракта, не новая рекомендация. [E-Session]

Для нескольких controllers выбирать непересекающиеся пулы либо один resource owner с явной передачей и fencing. Resource authority моделей не доказывает владение Kaggle kernel или Telethon session. Нельзя разделить одну эксклюзивную Telegram-сессию между старым UNKNOWN и новым worker.

### 3.2. Полезный результат, apply и публикация — разные стадии

Текущий `smart_event_update(...)` принимает candidate и параметры очереди/lease. Готовый facts/writer payload не является полноценным входным контрактом; обычная facade может вызвать LLM preparation снова. `SmartUpdateAttempt` хранит IDs/status/reason; обновляемый `candidate_payload` не является immutable stage history. [E-Apply], [E-State]

PR747 предлагает:

- одну additive таблицу `smart_update_stage_result` в существующей EventsBot SQLite;
- `smart_update_prepared.py` с `PreparedUpdate`;
- `smart_update_stage_store.py` со save/readback;
- `ultra_runtime/` как Python worker с официальным OpenCode;
- **один общий domain apply** для legacy и Ultra подготовки.

Эти модули **не обнаружены в изученном main**. Нельзя импортировать их в начальный runtime как готовую реализацию. [U-Design]

Минимальный stage result contract из проекта, изложенный как спецификация переноса:

| Поле | Значение |
|---|---|
| `delivery_key` | Один законченный output, одинаков при сетевом повторе |
| `work_key, consumer, stage` | Адрес логической работы, без обязательного Event ID |
| `source_revision, input_fingerprint` | Какой сохранённый материал обработан |
| `predecessor_result_ids` | Какие ранее принятые стадии использованы |
| `payload` | Полный полезный структурированный результат, не summary |
| `contract/prompt/model` | Версии контракта, prompt и реально использованной модели |
| `attempt_id, generation` | Происхождение и полномочие на изменение progress |
| `domain_operation_id/revision` | Отдельный исход применения, если уже произошёл |

Exact delivery retry возвращает прежний receipt. Тот же ключ с другим содержимым — конфликт. ACK после записи payload; потеря ответа означает readback, не повтор inference. Старая generation может сохранить допустимое immutable evidence своей попытки и прочитать receipt, но не вправе изменить authoritative progress/cursor/domain effect. Действующий owner решает, пригоден ли результат.

`PreparedUpdate` ссылается на результаты, input/target/catalog revision и intent. Domain apply проверяет auth/scope, stale dependencies и предметные invariants; extraction/writer не запускаются повторно «на всякий случай». При stale пересчитывается только зависимая стадия. Domain operation/outbox и external publication receipt различаются. EventsBot сохраняет ownership canonical DB, scheduler, queue, Event apply и публикаций.

**Для MDH:** этот принцип реализуется в существующих PG `analysis.result`, orchestration и semantic outbox. SQLite EventsBot и Event schema не копируются. MDH остаётся single-writer PG в Kaggle, с готовой политикой Dataset РКБ. Требование сохранять по готовности не позволяет объявлять RAM/file executor долговечным хранилищем.

### 3.3. Отсутствие событий и retry

В main `source_parse_contract.py` различает результаты:

```python
class SourceDisposition(str, Enum):
    EVENTS_FOUND = "EVENTS_FOUND"
    CONFIRMED_NO_EVENT = "CONFIRMED_NO_EVENT"
    LIFECYCLE_ONLY = "LIFECYCLE_ONLY"
    MIXED = "MIXED"
    RETRY_REQUIRED = "RETRY_REQUIRED"
```

Техническая ошибка, incomplete evidence и malformed output не являются `CONFIRMED_NO_EVENT`. Новое требование владельца от 03.11: слабый отрицательный вердикт должен подтвердить второй qualified model; составные расписания передаются сильной роли/Google; writer выбирается по качеству. В current main verifier вызывается по contradiction facts, а не безусловно после каждого отрицательного ответа. PR-реализацию требуется дополнить. [E-Parse], [U-Design]

Это **event consumer policy**. MDH сохраняет тексты без событий и не удваивает анализ всего корпуса только ради правил Ultra.

### 3.4. Свежая delta main: scope не является свидетельством

В `vk_intake.py` исправлена передача контекста:

```python
source_channel=source_name,
source_context={"operator_extra": operator_extra} if operator_extra else None,
```

Verifier и terminal adjudicator теперь учитывают операторский import scope на каждой стадии: нельзя извлекать остальные сеансы из общего расписания, если оператор попросил одну ticket card. Инструкция оператора ограничивает задачу, но не превращается в внешний факт. Это полезно MDH: region/time/source scope и фактические публикации — разные входы; enrichment не должен изобретать evidence из задания. [E-Delta]

## 4. DAEP: дизайн coding-профиля

В `my-daep` нет исполняемого coordinator/worker. README фиксирует именно постановку: распределённый coding-mode, 1–4 Kaggle notebooks, supervisor управляет ресурсами и recovery независимо от LLM/открытого окна. GitHub хранит source/base SHA, attempt commits, integration branch и один итоговый PR; queue/heartbeat/full session dumps не должны храниться как git business state. [D-Readme], [D-Design]

Для общего runtime переносимы:

- job/attempt не исчезает при смене executor;
- bootstrap получает точные версии inputs/dependencies и актуальный OpenCode;
- завершённые части и checkpoint переживают executor recovery;
- каждый callback/result привязан к attempt и физическому run;
- supervisor работает самостоятельно; ChatGPT и DevCoveer не обязательны для RunCoveer controller.

GitHub App PEM, branch policy, commit integration, code conflict resolution — отдельный coding profile. В первоначальном DAEP PEM оставлялся на DevCoveer; это **не** обязательная новая зависимость MDH или любого RunCoveer controller. MDH может исполнять extraction без GitHub write token вообще.

Текущее решение владельца уточняет recovery: при free-quota exhaustion сохранить доступную законченную стадию, пометить попытку, создать **свежий Kaggle executor** и продолжить с сохранённого результата. PG master при этом не перезапускается. Новое окружение не гарантирует новую доступную provider quota. Если новый executor получает тот же account/provider refusal, действует shared cooldown/retry-later, не бесконечный launch loop. Смена notebook/IP не обнуляет известный общий бюджет.

## 5. Live: образец реального повторного использования

EventsBot adapter делегирует библиотеке protocol loop; product JWT/origin/resource policy остаётся в продукте. Точные ключевые строки из `static_site_live_socket.py`:

```python
from live_interaction.socket_transport import SOCKET_PROTOCOL, serve_socket, socket_ticket
ticket = socket_ticket(offered)
binding = host.open_socket(session_id=session_id, resource_id=resource_id, ticket=ticket)
await serve_socket(binding, receive=receive, send=send, close=close)
```

Один исправленный transport используется через одну versioned dependency. В EventsBot зависимость Live закреплена как `v0.3.7-rc.1` и wheel digest. Runtime Kaggle должен воспроизвести **этот способ поставки**: общий source/package, tagged compatibility, thin adapter, удаление прежнего локального transport после миграции. Не копировать 55 строк adapter как собственный fork всей библиотеки. [E-Live], [E-Requirements], [L-Readme]

Live/audio protocol, аудиобуферы и Gemini connection не входят в общий Kaggle protocol. Новая библиотека может иметь несколько WSS endpoints и worker affinity; маршрутизация должна проверять consumer/job/attempt/resource, а не глобальный «текущий socket».

## 6. Ai-resource-control: единая authority без DevCoveer в call path

`WorkloadAdmission` обращается через `Control` к уже существующей authority. По проектному контракту scope — фактический provider/account/model, а consumer/binding задают отдельные границы. Несколько aliases одного аккаунта не дают нескольких независимых лимитов. [A-Docs], [A-Code], [A-Client]

Публичные методы из точного SDK:

```python
class WorkloadAdmission:
    def __init__(self, control: Control, scope: str): ...
    async def __call__(self, binding, workload): ...

class WorkloadLease:
    async def before_send(self, _metadata): ...
    async def finalize(self, usage, status='completed'): ...
```

Это выдержка сигнатур; многоточия не являются готовой реализацией. Consumer вызывает admission до внешней работы, `before_send` перед фактической отправкой и `finalize` по наблюдаемому исходу. Вызовы/retries/subagents внутри OpenCode должны проходить соответствующий hook/transport. Установка SDK или один reserve на весь долгий OpenCode session сами по себе не доказывают полное применение бюджета.

В текущем SDK одна reservation допускает `max_provider_sends` от 1 до 4 и резервирует `estimated_tokens × steps`; это предел одной порции admission, не общий предел исследования. Длинный OpenCode turn требует корректного разбиения на admitted portions. Каждый вызов `WorkloadAdmission.__call__` создаёт новые request/owner UUID: после crash нельзя считать повторный вход в helper восстановлением прежнего reservation. Сначала reconcile сохранённый attempt/неизвестную отправку; новая reservation не отменяет старую `unknown` capacity. SDK сам не маршрутизирует модели, не доставляет provider keys и не исполняет inference.

SQL reserve сериализует конкурирующие consumers:

```sql
PERFORM pg_advisory_xact_lock(617320260926001);
```

Он проверяет idempotency, scope policy, cooldown и combined Live/workload capacity. `active` и `unknown` держат capacity до deadline. Неотправленный запрос может вернуть reservation; неизвестный отправленный не объявляется бесплатным. Migrations 011/014/016 делают некоторые локальные ceilings опциональными для явно разрешённых маршрутов; это не отменяет provider quota, concurrency или unknown-send fencing. Estimates — рабочие budgets, не доказанная стоимость/entitlement provider. [A-SQL], [A-Docs]

RunCoveer использует тот же установленный SDK и authority URL напрямую; DevCoveer не нужен в каждом вызове. Это существующий служебный ledger ресурсов, **не** общая БД бизнеса MDH/EventsBot. Копировать limiter в каждый процесс с отдельным state нельзя: два хоста потратят один и тот же account budget дважды.

Существование API в ARC main не доказывает rollout каждого consumer: изученный EventsBot Dockerfile ещё закреплял wheel `0.1.4`. При внедрении нужен совместимый установленный artifact и фактическое подключение нужных hooks, без новой общей quota-службы.

**Ключи:** отсутствие OpenCode provider keys по умолчанию не отменяет transport auth, Kaggle credentials или authority client credential. Не требовать платный/персональный OpenCode key ради запуска; если выбранный provider требует авторизацию, использовать только явно настроенные bindings. Несколько bindings одного provider account делят quota scope. Credentials не попадают в payload, Dataset manifests, исходники и отчёты.

## 7. Контракты, которые исполнитель обязан сохранить

| Событие | Действие | Недопустимая потеря смысла |
|---|---|---|
| Ответ submit потерян | Exact reconciliation intent/input/run | Второй запуск по одному timeout |
| Quota exhaustion | Сохранить готовое, сменить executor согласно решению владельца; общий scope/cooldown сохраняется | Restart PG master или обещание reset quota |
| Потерян ACK stage commit | Читать результат по прежнему delivery identity | Повторять законченный inference |
| Старый worker после takeover | Разрешённое immutable evidence/readback; запрет cursor/domain effects | Удалить полезный результат либо дать старому владельцу authority |
| Модель не ответила | pending/retry с причиной | «Нового нет» или «события нет» |
| Независимые RunCoveer/DevCoveer | Отдельные endpoints/controllers, явный resource ownership | Process-local locks как глобальный coordinator |
| Результат сохранён в PG | Отразить существующий durability lifecycle до verified Dataset | Новая внешняя backup-платформа или ложный DURABLE_COMPLETE |
| Подготовлен semantic result | Один product apply + existing outbox | Второй inference pipeline или публикация из общего runtime |

## 8. Источники для provenance

[E-Kaggle]: https://github.com/onedayonemasterpiece/events-bot-new/blob/4600e9978a7eb9b22234590bfc76258feba0a905/video_announce/kaggle_client.py
[E-Registry]: https://github.com/onedayonemasterpiece/events-bot-new/blob/4600e9978a7eb9b22234590bfc76258feba0a905/kaggle_registry.py
[E-Status]: https://github.com/onedayonemasterpiece/events-bot-new/blob/4600e9978a7eb9b22234590bfc76258feba0a905/kaggle_status.py
[E-TG]: https://github.com/onedayonemasterpiece/events-bot-new/blob/4600e9978a7eb9b22234590bfc76258feba0a905/source_parsing/telegram/service.py
[E-Guide]: https://github.com/onedayonemasterpiece/events-bot-new/blob/4600e9978a7eb9b22234590bfc76258feba0a905/guide_excursions/kaggle_service.py
[E-VK]: https://github.com/onedayonemasterpiece/events-bot-new/blob/4600e9978a7eb9b22234590bfc76258feba0a905/vk_source_envelope.py
[E-Session]: https://github.com/onedayonemasterpiece/events-bot-new/blob/4600e9978a7eb9b22234590bfc76258feba0a905/remote_telegram_session.py
[E-Apply]: https://github.com/onedayonemasterpiece/events-bot-new/blob/4600e9978a7eb9b22234590bfc76258feba0a905/smart_event_update.py
[E-State]: https://github.com/onedayonemasterpiece/events-bot-new/blob/4600e9978a7eb9b22234590bfc76258feba0a905/smart_update_state.py
[E-Parse]: https://github.com/onedayonemasterpiece/events-bot-new/blob/4600e9978a7eb9b22234590bfc76258feba0a905/source_parse_contract.py
[E-Delta]: https://github.com/onedayonemasterpiece/events-bot-new/compare/79f8b43374dd9b4b531533d7145f4c04974e61b5...4600e9978a7eb9b22234590bfc76258feba0a905
[E-Live]: https://github.com/onedayonemasterpiece/events-bot-new/blob/4600e9978a7eb9b22234590bfc76258feba0a905/static_site_live_socket.py
[E-Requirements]: https://github.com/onedayonemasterpiece/events-bot-new/blob/4600e9978a7eb9b22234590bfc76258feba0a905/requirements.txt
[U-Design]: https://github.com/onedayonemasterpiece/events-bot-new/blob/406826d9cae0b51e229dc1f28d9fdd4683a4f31f/docs/architecture/smart-update-ultra-targeted-implementation.md
[D-Readme]: https://github.com/onedayonemasterpiece/my-daep/blob/d3559d731bb74fbb1262114b9aad15687bb54701/README.md
[D-Design]: https://github.com/onedayonemasterpiece/my-daep/blob/d3559d731bb74fbb1262114b9aad15687bb54701/docs/distributed-mode.md
[L-Readme]: https://github.com/onedayonemasterpiece/live-interaction/blob/e710e8e8143a3724d16250a6e439ccd178c279ca/README.md
[A-Code]: https://github.com/onedayonemasterpiece/ai-resource-control/blob/f359f6b6326f4ee11f851b3d451adf210e5259dd/src/ai_resource_control/workload.py
[A-Client]: https://github.com/onedayonemasterpiece/ai-resource-control/blob/f359f6b6326f4ee11f851b3d451adf210e5259dd/src/ai_resource_control/client.py
[A-Docs]: https://github.com/onedayonemasterpiece/ai-resource-control/blob/f359f6b6326f4ee11f851b3d451adf210e5259dd/docs/research-workloads.md
[A-SQL]: https://github.com/onedayonemasterpiece/ai-resource-control/blob/f359f6b6326f4ee11f851b3d451adf210e5259dd/migrations/202610050010_shared_workload_capacity.sql

