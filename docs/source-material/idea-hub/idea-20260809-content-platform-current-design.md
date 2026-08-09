---
schema_version: 1
idea_id: idea-20260809-content-platform-current-design
title: "Контентная платформа: PostgreSQL в Kaggle, оркестрация, семантический поиск E5/BGE, MCP и автотесты"
context_id: portfolio.inbox
idea_kind: research
status: proposed
source_event_ids:
  - attachment-d4746fcb-65d3-4cfd-a206-8fb04fc10664
created_at: 2026-08-09T00:00:00Z
updated_at: 2026-08-09T00:00:00Z
analysis_status: source_research
---

# Контентная платформа: PostgreSQL в Kaggle, оркестрация, семантический поиск E5/BGE, MCP и автотесты

> Исходное исследование сохранено без содержательной переработки. Это proposed-материал для серии PoC и нагрузочных испытаний, а не утверждённый план реализации.

<details>
<summary>Полный текст исследования</summary>

Контентная платформа: PostgreSQL в Kaggle, оркестрация, семантический поиск E5/BGE, MCP и автотесты

Статус документа

Proposed / требуется серия PoC и нагрузочных испытаний.

Документ фиксирует текущую проработку отдельной проектируемой контентной платформы.

idea-hub в этой архитектуре — только репозиторий для хранения идеи, исследований и ADR. Это не название платформы.

0. Краткий вывод

Целевая система на текущем этапе выглядит так:

В активной рабочей сессии существует один writable PostgreSQL-primary, запущенный в Kaggle Notebook.

PostgreSQL хранит единый каталог авторов, изданий, аккаунтов, публикаций, статей, проектов, конвейеров, результатов обработки, поисковых документов и активных vector indexes.

Dev-сервер предоставляет стабильный публичный домен, MCP endpoint и control plane.

Оркестратор управляет Kaggle-запусками, registry, leases, fencing, recovery и сервисным обнаружением, но не обязан быть маршрутизатором массивного трафика между внутренними workers.

Kaggle workers после service discovery подключаются к master или model service напрямую по data-plane каналу.

Внешние модели и агенты работают через стабильный MCP endpoint на dev-сервере.

E5 и BGE являются внешними embedding-моделями, а не функциями PostgreSQL.

PostgreSQL/pgvector хранит vectors и выполняет nearest-neighbor search, но сам не превращает текст запроса в vector.

Термин primary encoder исключается как двусмысленный. Вместо него используются:

active embedding space;

default dense retriever;

active retriever set.

MCP-поиск проектируется recall-first:

точный SQL/FTS;

E5;

BGE;

объединение кандидатов;

optional reranking;

крупная, но компактная выборка;

курсор для продолжения;

явный отчёт о том, какие retrievers действительно участвовали.

По умолчанию нельзя молча ограничивать агента только одной embedding-моделью, если в production доступны несколько полезных пространств.

BGE и E5 не обязаны работать в одном Notebook. Фактический опыт Region Talk уже закрепил их раздельное исполнение как invariant.

Corpus embeddings создаются отдельными workers. На DB-master допустим только лёгкий query encoder — и только если нагрузочный тест докажет безопасность.

Фактический events-bot-new уже содержит почти готовый reusable runtime:

private Kaggle datasets;

readiness и exact-file readback;

kernel source binding;

status callbacks из Notebook наружу;

heartbeat;

resource leases;

durable event ledger;

event UID deduplication;

bounded polling;

recovery по свежему output при сбое status API;

output retries;

запрет fire-and-forget в CherryFlash.

Новый runtime не следует писать с нуля. Нужна его предметно нейтральная адаптация.

Оркестратор с первого дня должен строиться через ports/adapters и иметь FakeKaggle, deterministic clock и полноценную state-machine test suite.

Документированный Kaggle лимит сохранённого /kaggle/working output — до 20 GB, а CPU/GPU session — до 12 часов. Это не равнозначно надёжному размеру активного PGDATA.

До реального disk/checkpoint PoC вводится консервативная цель:

зелёная зона PostgreSQL cluster: до 6 GiB;

рабочая цель MVP: до 8 GiB;

8–10 GiB: жёлтая зона;

выше 10 GiB новые крупные derived indexes не создаются до пересмотра topology.

Ограничение относится не к PostgreSQL как СУБД, а к Kaggle lifecycle:PGDATA + WAL + temp + checkpoint + upload staging + reserve.

Операционное временное состояние получает TTL и удаляется после утраты полезности. Долговечными остаются только предметные данные, provenance, значимые решения и агрегаты.

1. Терминология: что такое encoder, index и retriever

1.1. Encoder не встроен в PostgreSQL

Embedding encoder — это ML-модель, которая преобразует текст в числовой vector:

"неудобно добираться без машины"
          │
          ▼
E5 или BGE
          │
          ▼
[0.013, -0.027, ..., 0.041]

PostgreSQL с pgvector выполняет другую работу:

готовый query vector
          │
          ▼
pgvector index
          │
          ▼
ближайшие vectors документов

То есть компоненты разделены:

Компонент

Ответственность

E5/BGE encoder runtime

токенизация, inference, получение vector

PostgreSQL

реляционные данные, фильтры, транзакции, FTS

pgvector

хранение vector values и exact/ANN retrieval

search planner

выбор retrievers, candidate budgets, fusion

reranker

более дорогая переоценка пары query + candidate

MCP

удобный предметный инструмент для модели-агента

Никакого встроенного E5 или BGE в PostgreSQL нет.

1.2. Почему термин primary encoder неудачен

В предыдущей проработке под ним подразумевалась «модель, используемая по умолчанию». Это могло выглядеть как встроенный компонент PostgreSQL или как обязательное единственное пространство поиска.

Фиксируем новые термины:

embedding model
    конкретная модель и её exact revision

embedding space
    corpus vectors, созданные одной моделью по одному contract

query encoder service
    сервис, создающий query vector

corpus embedding worker
    batch worker, создающий document vectors

dense retriever
    query encoder + соответствующий vector index

active retriever set
    набор retrievers, допущенных в production search

default search profile
    политика вызова retrievers для конкретного MCP-инструмента

У платформы может быть одновременно два активных dense retriever:

E5 dense retriever
BGE dense retriever

Они не считаются одной системой координат.

1.3. Нельзя искать E5-query-vector в BGE-index

Каждая embedding-модель создаёт собственное пространство.

Правило:

E5(query)  → E5(document vectors)
BGE(query) → BGE(document vectors)

Нельзя:

E5(query) → BGE(document vectors)

Нельзя также напрямую складывать raw cosine scores E5 и BGE: распределения score различаются.

Объединять нужно rankings, например через Reciprocal Rank Fusion:

score_rrf(document) =
    Σ 1 / (k + rank_retriever(document))

Параметр k и веса retrievers определяются benchmark-ом.

2. Поиск через MCP: recall-first, а не single-model-first

2.1. Принцип

MCP используют языковые модели. Для них лишние релевантные кандидаты обычно безопаснее, чем скрытая потеря нужной публикации.

Следовательно, default search contract должен оптимизироваться не под минимальный ответ из 10 строк, а под:

высокую полноту
+ компактные карточки
+ прозрачность покрытия
+ возможность продолжить выборку

Это не означает бесконечно отправлять в контекст весь каталог. Нужно разделить:

candidate generation;

compact result page;

получение полных карточек выбранных объектов.

2.2. Основной поисковый профиль

Предлагаемый default:

recall_first_hybrid_v1

Он выполняет:

exact/natural-key lookup
        +
PostgreSQL FTS
        +
E5 vector retrieval
        +
BGE vector retrieval
        +
deduplication
        +
rank fusion
        +
optional reranking

Если один retriever недоступен, ответ не должен выглядеть как полный.

В ответ включается:

{
  "coverage": {
    "requested_profile": "recall_first_hybrid_v1",
    "retrievers_requested": ["exact", "fts", "e5", "bge"],
    "retrievers_completed": ["exact", "fts", "e5"],
    "retrievers_unavailable": [
      {
        "name": "bge",
        "reason": "service_cold",
        "retryable": true
      }
    ],
    "is_complete": false,
    "index_revisions": {
      "e5": 184,
      "bge": 181
    }
  }
}

Молчаливая деградация запрещена.

2.3. Candidate budgets

Начальные значения для benchmark, а не окончательные константы:

exact lookup               все совпадения в разумном hard cap
FTS                         top 200
E5                          top 200
BGE                         top 200
union после дедупликации    до 600
reranker                    top 100–200
первая MCP-страница         50–100 compact cards

Модель может запросить продолжение через cursor.

Компактная карточка:

{
  "content_id": "...",
  "kind": "article",
  "title": "...",
  "summary": "...",
  "author": "...",
  "source": "...",
  "published_at": "...",
  "projects": ["..."],
  "matched_by": ["fts", "e5", "bge"],
  "ranks": {
    "fts": 14,
    "e5": 3,
    "bge": 8,
    "fusion": 2
  }
}

Полная история конвейера и все features подгружаются отдельным вызовом.

2.4. MCP не должен заставлять модель знать инфраструктуру

Плохой контракт:

search_with_e5(...)
search_with_bge(...)

Он перекладывает инфраструктурные решения на ChatGPT/другую модель.

Лучший контракт:

search_content(
    queries,
    completeness,
    filters,
    page_size,
    cursor,
    include_trace
)

Пример:

{
  "queries": [
    {
      "text": "публикации о сложностях путешествия без автомобиля",
      "weight": 1.0
    },
    {
      "text": "редкий общественный транспорт и неудобные пересадки",
      "weight": 0.7
    }
  ],
  "completeness": "recall_first",
  "filters": {
    "region_ids": ["kaliningrad-oblast"],
    "published_from": "2024-01-01"
  },
  "page_size": 75,
  "include_trace": true
}

Search planner сам решает:

какие spaces доступны;

какие services нужно разогреть;

какие candidate budgets использовать;

нужен ли reranker;

как объединить rankings.

2.5. Режимы остаются, но fast не является default

exact
    natural keys, URL, platform IDs

lexical
    FTS и trigram

semantic
    все активные dense retrievers

recall_first
    exact + FTS + все активные semantic retrievers

deep
    recall_first + reranker + расширенные budgets

fast
    FTS + один тёплый retriever, только по явному запросу

Для агента default — recall_first.

2.6. Холодный BGE

Если BGE Notebook выключен, есть три варианта.

Вариант A. Дождаться запуска

search_content(...)
→ operation_id
→ orchestrator запускает BGE query service
→ get_search_operation(...)
→ полный результат

Это наиболее честный режим для deep.

Вариант B. Вернуть предварительный результат

E5 + FTS result
coverage.is_complete = false
continuation_operation_id = ...

После BGE модель запрашивает дополнение.

Вариант C. Активная search session

Перед серией запросов:

open_search_session(
    completeness="recall_first",
    expected_duration_minutes=60
)

Оркестратор заранее держит E5/BGE query services тёплыми и завершает их после idle timeout.

Для длительной агентской сессии это предпочтительно.

2.7. FTS всё равно нужен

Даже если большинство запросов смысловые, FTS не является ненужным fallback.

Он лучше dense search для:

точных фамилий;

Telegram username;

названия издания;

географического топонима;

точной цитаты;

хештега;

URL;

внешнего ID;

названия события;

редкого собственного имени.

Поэтому recall-first означает семантика плюс lexical, а не «vector only».

3. E5 и BGE: роли и topology

3.1. Кандидаты

intfloat/multilingual-e5-base

multilingual;

embedding size 768;

retrieval contract требует query: и passage: prefixes;

хороший компактный infrastructure baseline.

intfloat/multilingual-e5-large-instruct

embedding size 1024;

query instruction является частью model contract;

дороже по памяти и диску;

quality candidate.

BAAI/bge-m3

multilingual;

dense, sparse и multi-vector modes;

поддерживает длинные входы до 8192 tokens;

фактический опыт уже показывает, что его нужно отделять от E5 по Notebook memory lifecycle.

Поскольку платформа хранит короткие смысловые карточки, преимущество BGE-M3 на длинных документах нельзя считать автоматически полезным. Оно проверяется предметным benchmark-ом.

3.2. Разделяем query encoding и corpus encoding

Corpus encoding

новые/изменённые search documents
→ embedding queue
→ E5/BGE batch Notebook
→ vectors
→ transactional import

Это тяжёлая пакетная работа.

Query encoding

один или несколько коротких пользовательских запросов
→ query encoder
→ vectors

Это небольшая, но latency-sensitive работа.

Один model Notebook может поддерживать оба режима, но query requests имеют приоритет над background corpus batches.

3.3. PostgreSQL-master — DB-first workload

На master находятся:

PostgreSQL;

FTS;

pgvector indexes;

транзакции;

job queue;

MCP data access;

checkpoint agent.

На master не выполняется массовое corpus encoding.

Допустимое исключение:

PostgreSQL + E5 query-only service

Только после benchmark.

3.4. Тест co-location E5 + PostgreSQL

Проверяются:

PostgreSQL only;

PostgreSQL + E5-base CPU;

PostgreSQL + E5-base quantized runtime;

PostgreSQL + E5-large-instruct;

E5 отдельным Notebook — control group.

Размеры тестового PGDATA:

2 GiB
5 GiB
8 GiB
10 GiB

Mixed workload:

FTS;

vector search;

короткие writes;

queue claims;

autovacuum;

checkpoint;

query inference.

Co-location принимается только если:

нет OOM;

DB p95 не деградирует больше согласованного порога;

query latency пригодна для MCP;

после прогрева остаётся безопасный RAM headroom;

checkpoint стабильно завершается;

модель не вытесняет полезный PostgreSQL page cache.

До теста предположение «E5 лёгкий, значит точно поместится» не считается решением.

3.5. BGE запускается отдельно

Начальная topology:

bge-corpus-worker
    запускается по backlog/schedule
    опустошает очередь
    выключается

bge-query-service
    запускается для active/deep search session
    держится до idle timeout

На первом этапе один Notebook может выполнять обе роли, если scheduler даёт query priority.

3.6. Reranker как альтернатива второму corpus index

Нужно сравнить:

FTS + E5 corpus index + BGE reranker

с:

FTS + E5 corpus index + BGE corpus index

Reranker не требует хранить второй vector для каждого документа, но требует прогнать top-N пар query + document.

Для recall это не полная замена второму retriever: reranker не вернёт документ, который не попал в candidate set. Поэтому benchmark должен проверить две метрики отдельно:

candidate recall;

final ranking quality.

4. Что фактически даёт Region Talk

4.1. Что доказано

Region Talk зафиксировал и частично реализовал важные invariants:

Kaggle runtime не переписывается, а переиспользуется из events-bot-new.

E5 и BGE-M3 работают в разных kernels.

Один общий lifecycle adapter используется для разных стадий.

Workers не должны напрямую менять canonical state.

Есть contracts, schema, тестовая методика и release gates.

В репозиторий перенесены:

worker-side status client;

generic dataset/status/output client;

provenance exact source SHA.

Есть regression tests:

private dataset defaults;

create/version/delete;

exact file readback;

kernel status/output;

callback token redaction;

запрет второго прямого Kaggle API client.

Локально зафиксирован результат 16 tests passed.

Есть базовые deterministic orchestrator tests:

terminal reconciliation блокирует новые launches;

product work выше discovery;

один Telegram auth scope не переиспользуется параллельно.

4.2. Что Region Talk пока не доказал

README и implementation status прямо фиксируют, что пока не завершены:

actual Candidate/E5 worker;

actual BGE worker;

Image/Profile workers;

state/run-history datasets;

полный host ledger/registry port;

первый полный CPU pipeline run;

production scheduler;

publisher.

Поэтому Region Talk не является доказательством:

фактической E5/BGE throughput;

качества их fusion;

стабильного совместного PostgreSQL runtime;

production SLA;

максимального размера БД;

готового master service discovery.

Он полезен как:

архитектурный прототип
+ reuse contract
+ regression-test seed
+ перечень доказанных runtime-механик

Не как finished production reference.

4.3. Что следует перенести в новую платформу

Переносится:

запрет второго Kaggle transport;

pinned source provenance;

exact dataset readiness;

exact file readback;

exact source binding;

status callbacks;

local event log;

resource lease;

output recovery;

bounded retries;

durable attempt;

no fire-and-forget;

secret scanning;

separate E5/BGE stages;

state-machine/property testing.

Не переносится буквально:

SQLite-specific canonical state;

Region Talk domain states;

blanket stale-base rejection;

Telegram-specific auth scopes;

product-specific progress phases;

GitHub Actions как единственное место долгого polling.

5. Фактический reusable опыт events-bot-new

5.1. Telegram Monitoring

Фактический lifecycle включает:

durable run ID и operation record;

проверку занятости Telegram session;

stage-scoped secrets;

private временные Kaggle datasets;

ожидание ready;

проверку exact required files;

подготовку kernel;

включение status helper;

exact dataset-source binding;

dynamic timeout;

bounded handling SSL/network/429/5xx;

polling;

fallback на свежий output при сбое status API;

проверку run_id внутри output;

output download retries;

идемпотентный import;

cleanup/recovery receipt.

Критически полезный опыт:

Статус Kaggle API не является единственным источником истины. Свежий output с совпадающим run_id может доказать завершение, когда status endpoint временно недоступен.

5.2. CherryFlash

CherryFlash добавляет:

durable session row до remote launch;

запрет --no-wait;

проверку активной render session;

persisted retry cap;

heartbeat/terminal ledger;

resource/video lanes;

exact remote handoff;

exact dataset-source binding;

ожидание terminal state через durable state;

serialization по реально используемому Kaggle kernel slug.

Последний пункт важен для model services: два логических профиля могут случайно указывать на один физический Kaggle slug. Оркестратор должен блокировать конкуренцию по resolved runtime identity, а не только по логическому имени.

5.3. Уже существует outbound channel из Notebook

kaggle/kaggle_status_client.py:

находит kaggle_run.json;

читает callback_url, run_id, token;

выполняет HTTP POST;

отправляет:

event;

event UID;

phase;

status;

произвольный progress object;

optional resource;

message;

ведёт локальный kaggle_status_events.jsonl;

редактирует token в локальном логе;

запускает heartbeat thread;

позволяет heartbeat progress_provider добавлять произвольные данные.

Host-side kaggle_status.py:

проверяет run token;

хранит только hash token;

дедуплицирует event_uid;

ведёт append-only event ledger;

обновляет latest projection;

coalesces частые heartbeat;

продлевает resource leases;

использует bounded writer-lock retries.

То есть запрошенная функция:

«отправлять произвольную полезную информацию из Notebook оркестратору»

уже существует в минимальном виде.

5.4. Что нужно изменить

Не следует бесконечно перегружать поле progress.

Нужно выделить предметно нейтральный runtime event protocol поверх существующего transport.

6. Универсальный outbound event protocol

6.1. Envelope

{
  "schema": "content-runtime-event/v1",
  "event_id": "uuid",
  "run_id": "uuid-or-durable-key",
  "attempt_id": "uuid",
  "service_instance_id": "uuid",
  "event_type": "service.ready",
  "emitted_at": "2026-08-09T18:30:00Z",
  "local_sequence": 17,
  "epoch": 42,
  "phase": "serve",
  "status": "ready",
  "data": {
    "service_kind": "bge-query",
    "capabilities": ["query_embedding"],
    "model_id": "bge-m3@exact-revision"
  },
  "artifact_refs": [],
  "metrics": {
    "queue_depth": 0,
    "rss_bytes": 123456789
  },
  "token": "per-run-secret"
}

6.2. Event types

runtime.created
runtime.started
runtime.heartbeat
runtime.progress
runtime.draining
runtime.terminal
runtime.failed

service.announced
service.ready
service.unavailable
service.endpoint_changed

job.claimed
job.progress
job.result_available
job.completed
job.failed

resource.acquire
resource.renew
resource.release

checkpoint.started
checkpoint.candidate_uploaded
checkpoint.verified
checkpoint.failed

6.3. Service announcement

{
  "event_type": "service.ready",
  "epoch": 42,
  "data": {
    "service_kind": "postgres-master",
    "endpoint": "private-or-tunnel-endpoint",
    "protocol": "postgresql+tls",
    "tls_fingerprint": "sha256:...",
    "capabilities": ["sql", "fts", "pgvector"],
    "canonical_revision": 1842,
    "schema_version": 17,
    "lease_until": "2026-08-09T18:35:00Z"
  }
}

Credentials не передаются в событии.

6.4. Large payloads

Через callback не передаются:

vectors batch;

большие result sets;

model artifacts;

database snapshots;

полные logs.

В событии передаётся locator:

{
  "event_type": "job.result_available",
  "artifact_refs": [
    {
      "kind": "kaggle-notebook-output",
      "locator": "owner/notebook/version/file.parquet.zst",
      "sha256": "...",
      "size_bytes": 123456789
    }
  ]
}

6.5. Ограничения

Начальные:

максимум event body: 64 KiB
event ID: обязателен
schema version: обязателен
run token: per-run
token хранится host-side только как hash
HTTPS: обязателен
unknown fields: сохраняются в raw envelope, но не влияют на state без schema support
secret scan: до записи в observability logs

Heartbeat coalescing обязательно, иначе event ledger сам станет источником раздувания БД.

6.6. Три источника истины о runtime

1. outbound events/heartbeat
2. Kaggle platform status polling
3. exact output readback

Ни один из них не является абсолютно достаточным.

Правила:

heartbeat доказывает недавнюю активность;

platform status сообщает мнение Kaggle;

свежий exact output доказывает завершённый результат;

terminal state признаётся только при формализованном evidence policy.

7. Оркестратор: control plane, а не внутренний data router

7.1. Стабильная точка входа

На dev-сервере:

mcp.content.<domain>
control.content.<domain>

DNS позволяет заменить сервер без перенастройки MCP clients.

Dynamic Kaggle endpoints в публичный DNS не помещаются.

7.2. Ответственность оркестратора

Оркестратор делает:

start/stop Kaggle runs;

durable attempts;

state machine;

leases;

fencing epochs;

service registry;

capability registry;

callback ingestion;

heartbeat expiry;

Kaggle status polling;

output recovery;

checkpoint coordination;

MCP endpoint;

short-lived credential issuance;

audit.

Оркестратор не обязан проксировать:

bulk SQL reads workers;

embeddings batches;

image payloads;

large output artifacts;

direct worker-to-master data.

7.3. Data plane

Kaggle worker
    → orchestrator: resolve service
    → direct connection: PostgreSQL/master API/model service

Для внешнего MCP:

LLM host
    → stable MCP on dev server
    → DB/model services

Здесь dev-сервер является внешней точкой входа, что нормально. Ограничение относится к внутреннему worker-to-worker bulk traffic.

7.4. Master state machine

ABSENT
  → REQUESTED
  → STARTING
  → RESTORING
  → REGISTERING
  → ACTIVE
  → DRAINING
  → CHECKPOINTING
  → STOPPED

Отдельные terminal/error states:

FAILED
FENCED
CHECKPOINT_FAILED
ORPHANED

Любой переход сохраняется durable до следующего внешнего side effect.

7.5. Lease и fencing

Проблема:

Master A потерял heartbeat
→ Master B запущен
→ A ожил
→ два writable primary

Решение:

master epoch
+ renewable lease
+ DB gate

Правила:

Новый master получает monotonically increasing epoch.

Каждый heartbeat продлевает lease.

При истечении lease master прекращает принимать writes.

Service registry возвращает только active epoch.

Direct credentials имеют короткий TTL и привязку к epoch.

Старый master закрывает DB gate или переводит PostgreSQL read-only.

Восстановившийся старый master не может самовольно снова стать active.

7.6. Internal control API

Минимум:

POST /internal/runtime/events
POST /internal/runs/request
GET  /internal/runs/{run_id}
POST /internal/runs/{run_id}/cancel

GET  /internal/services/resolve
POST /internal/services/{kind}/ensure
POST /internal/services/{instance_id}/drain

POST /internal/jobs/{job_id}/claim
POST /internal/jobs/{job_id}/heartbeat
POST /internal/jobs/{job_id}/complete

GET  /internal/checkpoints/head
POST /internal/checkpoints/request

7.7. Idempotency

Любой внешний effect:

create dataset
create dataset version
push kernel
request master
request model service
publish checkpoint

получает idempotency key и durable operation row.

Повтор после crash:

тот же result
или
доказанный no-op

8. PostgreSQL-master и долговечность

8.1. Во время active session

Это обычный PostgreSQL single-primary:

MVCC;

row/table locks;

constraints;

concurrent transactions;

FTS;

pgvector;

queues через FOR UPDATE SKIP LOCKED.

Сложный offline merge для обычных workers больше не нужен.

8.2. Direct DB roles

platform_readonly
catalog_ingest
analysis_writer
embedding_writer
workflow_worker
mcp_service
platform_admin

Workers получают минимум прав.

Connection pools маленькие:

1–5 connections per Notebook

Bulk import:

COPY → staging → validated MERGE/upsert

8.3. Checkpoints

Хранятся:

current verified checkpoint
previous verified checkpoint
portable logical backup реже

Новая версия становится canonical только после:

consistent backup;

manifest;

upload;

exact-version readback;

hash verification;

restore smoke;

HEAD update.

8.4. Две версии не защищают последние транзакции

Если master упал после checkpoint, последние commits потеряются, если нет:

WAL archive;

standby;

durable command log.

Поскольку внутренние workers могут писать прямым SQL, orchestrator не видит все semantic commands. Поэтому для минимального RPO предпочтительный PoC:

Kaggle PostgreSQL
    → WAL stream/archive
    → dev-server storage

Тогда восстановление:

last checkpoint + WAL

Если WAL transport не проходит PoC, нужно честно определить ненулевой RPO либо запретить прямые durable writes вне API.

8.5. Ротация по лимиту сессии

До приближения к 12 часам:

DRAINING;

остановить новые writes;

завершить активные transactions;

checkpoint;

readback/verify;

завершить master;

запустить новый;

restore;

получить новый epoch;

открыть writes.

Blue-green handoff — поздняя оптимизация.

9. Данные, embeddings и pgvector

9.1. Embedding identity

search_document_id
+ representation_kind
+ model_id
+ exact model revision
+ encoder contract version
+ input_hash

Один и тот же документ в десяти проектах не получает десять одинаковых vectors.

9.2. Model registry

CREATE TABLE search.embedding_model (
    model_id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    model_key              text NOT NULL,
    model_revision         text NOT NULL,
    runtime_image_digest   text NOT NULL,
    dimensions             integer NOT NULL,
    max_tokens             integer,
    storage_type           text NOT NULL,
    distance_metric        text NOT NULL,
    normalization          text NOT NULL,
    query_contract         jsonb NOT NULL,
    document_contract      jsonb NOT NULL,
    status                 text NOT NULL,
    created_at             timestamptz NOT NULL DEFAULT now(),
    UNIQUE (model_key, model_revision, runtime_image_digest)
);

Статусы:

candidate
shadow
active
retired
archived

9.3. Vector tables

Разделение по размерности:

CREATE TABLE search.embedding_768 (
    search_document_id uuid NOT NULL,
    model_id            bigint NOT NULL,
    representation_id  bigint NOT NULL,
    input_hash          bytea NOT NULL,
    embedding           halfvec(768) NOT NULL,
    is_current          boolean NOT NULL DEFAULT true,
    created_at          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (
        search_document_id,
        model_id,
        representation_id,
        input_hash
    )
);

CREATE TABLE search.embedding_1024 (
    search_document_id uuid NOT NULL,
    model_id            bigint NOT NULL,
    representation_id  bigint NOT NULL,
    input_hash          bytea NOT NULL,
    embedding           halfvec(1024) NOT NULL,
    is_current          boolean NOT NULL DEFAULT true,
    created_at          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (
        search_document_id,
        model_id,
        representation_id,
        input_hash
    )
);

В production можно partition by model_id.

9.4. Размер raw vector payload

Для halfvec:

2 × dimensions + 8 bytes

Нижняя оценка без tuple и index overhead:

Vector

На объект

На 1 млн

halfvec(768)

1 544 bytes

~1.44 GiB

halfvec(1024)

2 056 bytes

~1.91 GiB

E5-768 + BGE-1024

3 600 bytes

~3.35 GiB

Это ещё не полный размер:

heap tuples;

PK;

model IDs;

visibility map;

WAL;

HNSW;

bloat.

HNSW multiplier нельзя фиксировать теоретически — он измеряется на реальном layout.

9.5. Несколько vector spaces разрешены

Архитектура не должна запрещать:

E5 summary vectors;

BGE summary vectors;

author profile vectors;

product description vectors;

visual CLIP vectors;

atlas entity vectors.

Но каждый новый space требует:

use case;

owner;

benchmark;

capacity estimate;

retention policy;

active/shadow status.

Запрещено создавать vector «на всякий случай» для каждого промежуточного pipeline result.

9.6. Не каждый space получает HNSW

active:
    online HNSW

shadow:
    ограниченный subset или временный index

retired:
    HNSW удалён

archived:
    vectors вынесены из master

9.7. Binary-quantized index PoC

Сравнить:

halfvec HNSW

и:

binary-quantized HNSW
→ expanded candidate set
→ rerank по original halfvec

Измерять:

Recall@K against exact search;

index size;

query p95;

build time;

WAL;

checkpoint size.

10. Embedding queue

10.1. Идемпотентность

Job key:

search_document_id
+ representation_kind
+ model_id
+ input_hash

Повтор является no-op.

10.2. Состояния

pending
leased
running
succeeded
retryable_failed
dead
cancelled

10.3. Worker flow

claim batch
→ fetch compact documents
→ encode
→ upload vectors to staging
→ validate dimensions/finiteness/norm
→ transactional upsert
→ mark jobs succeeded
→ release lease

10.4. Query priority

Если Notebook одновременно обслуживает query и corpus:

query queue priority > corpus queue

Corpus batch прерывается только в безопасной границе batch.

11. Экологичное хранение и retention

11.1. Три класса данных

Долговечные

авторы;

аккаунты;

издания;

публикации/статьи;

URLs;

компактные карточки;

project membership;

provenance;

pipeline decisions;

processor/model version;

значимые analysis results;

publish/review audit;

daily product metrics;

atlas entities/relations.

Производные, но online

FTS projection;

active embeddings;

active ANN indexes;

current features;

current search cards.

Могут быть перестроены, но нужны для online работы.

Временные

heartbeat;

job leases;

succeeded queue rows;

query cache;

staging;

verbose worker logs;

raw model output после extraction;

transient search analytics;

temp artifacts;

failed attempts после diagnostic window.

11.2. Начальная retention matrix

Данные

Retention

heartbeat details

coalesced latest + 7–30 дней событий

worker registration

terminal + 7 дней

resource lease

expiry + 7 дней

succeeded embedding jobs

7 дней

failed attempts

30–90 дней

verbose worker logs

14–30 дней

raw model response

7–30 дней

query vector cache

часы/дни

raw search events

30 дней

hourly metrics

90 дней

daily rollups

долговечно

staging

транзакция/сессия

old model HNSW

удалить после rollback window

old vectors

export или delete по model lifecycle

WAL

пока не покрыт проверенным checkpoint + safety window

11.3. Удаление

Большие append-only tables partition by time.

Старые данные удаляются:

DROP/DETACH PARTITION

а не миллионными DELETE.

UNLOGGED допустим для:

query cache;

rebuildable transient staging;

non-durable runtime projection.

Не допустим для:

manual edits;

canonical jobs;

checkpoint ledger;

publish audit;

resource ownership, если потеря приводит к двойному side effect.

12. Целевой размер master-БД в Kaggle

12.1. Что документировано

Kaggle документирует:

до 20 GB сохранённого Notebook output в /kaggle/working;

до 12 часов CPU/GPU Notebook session.

Это не гарантирует:

что весь writable filesystem равен ровно 20 GB;

что /kaggle/tmp имеет стабильный отдельный лимит;

что upload Dataset не требует staging;

что online checkpoint можно сделать без второй копии;

что один layout одинаков во всех session types.

Поэтому размер master определяется экспериментом.

12.2. Peak formula

P + W + T + C + U + M + R <= D

Где:

P — active PGDATA;

W — WAL headroom;

T — PostgreSQL temp/index build;

C — checkpoint staging;

U — compression/upload staging;

M — local model/cache;

R — reserve;

D — фактический writable disk.

Если checkpoint требует ещё одну полную копию:

C ≈ P

12.3. Предварительный guardrail

До disk PoC:

Зона

Physical PostgreSQL cluster

зелёная

≤ 6 GiB

рабочая цель MVP

≤ 8 GiB

жёлтая

8–10 GiB

gate

> 10 GiB

При входе в жёлтую зону:

запрет новых experimental ANN indexes;

cleanup;

export retired vectors;

capacity report;

обязательный checkpoint rehearsal.

Это не предел PostgreSQL.

12.4. Что выносится первым

retired vector indexes;

shadow vectors;

experimental embeddings;

verbose operational history;

raw model outputs;

high-frequency metrics;

альтернативные ANN structures.

Canonical catalog выносится последним.

12.5. Runtime probe

Каждый capacity run сохраняет:

df -h
df -i
free -h
du -sh /kaggle/working
du -sh <PGDATA>
nvidia-smi

Измеряется peak во время:

restore;

start;

HNSW build;

vacuum;

checkpoint;

compression;

upload;

exact readback.

13. Автотестируемая архитектура оркестратора

13.1. Принцип ports/adapters

Core orchestrator не импортирует Kaggle SDK напрямую.

from typing import Protocol

class KaggleRuntime(Protocol):
    async def ensure_private_dataset(self, spec): ...
    async def wait_dataset_ready(self, ref, required_files): ...
    async def push_kernel(self, spec): ...
    async def get_kernel_status(self, kernel_ref): ...
    async def download_output(self, kernel_ref, destination): ...
    async def cancel_kernel(self, kernel_ref): ...

class Clock(Protocol):
    def now(self): ...
    async def sleep(self, seconds: float): ...

class RuntimeEventStore(Protocol):
    async def append(self, event): ...
    async def latest_state(self, run_id): ...

class ServiceRegistry(Protocol):
    async def acquire_epoch(self, service_kind): ...
    async def announce(self, instance): ...
    async def resolve(self, service_kind): ...

Production adapter использует proven Kaggle client.

Tests используют FakeKaggleRuntime.

13.2. Scripted fake Kaggle

Fake должен уметь сценарии:

dataset:
    creating → ready
    missing required file
    delayed visibility
    version conflict
    upload failure
    exact readback mismatch

kernel:
    queued → running → complete
    queued → error
    unknown status
    transient SSL
    429
    stale output
    matching fresh output
    no output

callback:
    duplicate event ID
    out-of-order event
    lost heartbeat
    invalid token
    delayed terminal event

lifecycle:
    process crash
    orchestrator restart
    two launch requests
    two master candidates

Пример:

fake = FakeKaggleRuntime(
    status_script=[
        TransientError("SSL"),
        {"status": "RUNNING"},
        TransientError("503"),
    ],
    output_script=[
        NoOutput(),
        Output("result.json", {"run_id": expected_run_id}),
    ],
)

13.3. Deterministic time

Никаких реальных sleep(60) в unit tests.

Fake clock:

clock.advance(seconds=300)

Позволяет тестировать:

lease expiry;

heartbeat freshness;

retry backoff;

idle shutdown;

12-hour rotation;

terminal grace.

13.4. Test pyramid

Unit tests

pure transition functions;

idempotency;

retry classifier;

event validation;

search coverage;

fusion;

retention selection;

capacity guard.

State-machine/property tests

Hypothesis генерирует:

duplicate ticks;

crash at every boundary;

out-of-order callbacks;

two masters;

stale endpoint;

retries;

output/status contradictions;

repeated MCP calls.

Инварианты:

не больше одного ACTIVE writable master epoch
previous canonical checkpoint не теряется
повтор event/job/request не создаёт второй side effect
неполный search не маркируется complete
старый master не принимает writes
один poison run не блокирует независимые runs

Integration tests

real PostgreSQL in container;

fake Kaggle HTTP;

real callback handler;

real migrations;

queue claims;

pgvector where available;

checkpoint manifest logic.

Contract tests

frozen sanitized responses Kaggle API;

exact kaggle_run.json;

current runtime event schema;

backward compatibility с перенесённым KaggleStatusClient;

exact output run ID validation.

Real Kaggle smoke

Gated/manual/scheduled:

private dataset;

exact required files;

callback to dev orchestrator;

heartbeat;

service announcement;

output;

cleanup;

no secrets.

13.5. Обязательная матрица тестов оркестратора

Launch и idempotency

два одинаковых ensure_service → один run;

orchestrator crash после Dataset create → reuse;

crash после kernel push → recover by identity;

repeated callback → one event;

same physical kernel slug → serialized.

Service discovery

service ready → resolve;

expired lease → no resolve;

endpoint changed → latest epoch only;

old epoch heartbeat → rejected/fenced;

credentials never appear in event ledger.

Master fencing

A active;

heartbeat A lost;

B gets epoch+1;

A returns;

A cannot renew;

direct gate A closes writes;

registry resolves B only.

Status ambiguity

Kaggle status error;

fresh output with matching run ID;

terminal complete.

stale output with another run ID;

not complete.

Callbacks

valid token;

invalid token;

event ID duplicate;

payload too large;

schema mismatch;

heartbeat coalescing;

arbitrary structured data preserved;

secret redaction.

Resource leases

acquire;

renew by heartbeat;

conflict;

expire;

release;

dead worker recovery.

Checkpoint

candidate upload succeeds, readback fails → old HEAD;

hash mismatch → old HEAD;

restore smoke fails → old HEAD;

two publish attempts → one canonical;

WAL replay to target LSN.

MCP

cold master → operation ID;

master active → search;

BGE cold → incomplete coverage or awaited operation;

recall-first result reports all retrievers;

pagination does not repeat objects;

repeated write tool with idempotency key → no duplicate;

tool output conforms to output schema.

Embeddings

duplicate job → one vector;

document hash changed → old result stale;

wrong dimensions → reject;

NaN/Inf → reject;

model revision mismatch → reject;

E5 query never searches BGE space;

RRF deterministic;

index lag exposed in coverage.

14. Пример автотеста split-brain

async def test_old_master_is_fenced_after_new_epoch(
    orchestrator,
    fake_clock,
    fake_kaggle,
):
    master_a = await orchestrator.ensure_master()
    assert master_a.epoch == 1

    await orchestrator.accept_runtime_event(
        heartbeat(master_a, epoch=1)
    )

    fake_clock.advance(orchestrator.master_lease_ttl + 1)
    master_b = await orchestrator.ensure_master()

    assert master_b.epoch == 2
    assert (await orchestrator.resolve_service("postgres-master")).instance_id == master_b.id

    response = await orchestrator.accept_runtime_event(
        heartbeat(master_a, epoch=1)
    )

    assert response.status == "fenced"
    assert not await fake_kaggle.db_gate_accepts_writes(master_a.id)

15. Пример теста recovery по output

async def test_matching_output_proves_completion_when_status_api_fails(
    orchestrator,
    fake_kaggle,
):
    fake_kaggle.status_script = [
        TemporaryNetworkError(),
        TemporaryNetworkError(),
    ]
    fake_kaggle.output = {
        "result.json": {
            "run_id": "run-42",
            "status": "complete",
        }
    }

    result = await orchestrator.reconcile_run("run-42")

    assert result.status == "complete"
    assert result.completion_source == "fresh_output_after_status_error"

16. Пример теста полноты MCP

async def test_recall_first_never_hides_missing_retriever(search_service):
    search_service.bge.set_state("cold")
    result = await search_service.search(
        query="сложно путешествовать без машины",
        completeness="recall_first",
    )

    assert result.coverage.retrievers_requested == {"exact", "fts", "e5", "bge"}
    assert result.coverage.retrievers_completed == {"exact", "fts", "e5"}
    assert result.coverage.is_complete is False
    assert result.continuation_operation_id is not None

17. Retrieval benchmark

17.1. Корпус

Минимум:

реальные Region Talk posts/articles;

travel bloggers;

региональные СМИ;

смешанные регионы;

рекламы;

новости;

похожие топонимы;

hard negatives.

17.2. Запросы

Не менее 300, лучше 500–1000:

точные авторы;

точные издания;

география;

транспорт;

туризм;

впечатления;

критика;

цены;

визуальная пригодность;

продуктовые связи;

pipeline history;

запросы без совпадающих слов;

запросы с ложными lexical совпадениями.

17.3. Relevance labels

0 — нерелевантно
1 — слабая связь
2 — релевантно
3 — ключевой результат

17.4. Сравнение

FTS;

E5;

BGE;

FTS + E5;

FTS + BGE;

FTS + E5 + BGE;

FTS + E5 + reranker;

FTS + E5 + BGE + reranker.

17.5. Метрики

Recall@20
Recall@50
Recall@100
nDCG@10
nDCG@50
MRR
unique relevant candidates
complementary recall E5 vs BGE
latency p50/p95
cold-start latency
index size
generation throughput
checkpoint impact

Главная метрика с учётом требований — Recall@100 и complementary recall, а не только top-10 precision.

17.6. Decision rules

Второй active corpus index сохраняется, если он:

находит устойчивый класс релевантных объектов, пропускаемых первым;

даёт измеримый complementary recall;

укладывается в disk/checkpoint budget;

не разрушает interactive latency.

Нельзя отказаться от BGE только потому, что average nDCG близок: он может давать редкие, но ценные уникальные находки.

18. Capacity benchmark

На 100k и 1m search documents:

relational core;

FTS;

E5 halfvec;

BGE halfvec;

E5 HNSW;

BGE HNSW;

binary HNSW;

combined checkpoint.

Измерять PostgreSQL:

SELECT pg_database_size(current_database());

SELECT
    n.nspname,
    c.relname,
    pg_table_size(c.oid) AS table_bytes,
    pg_indexes_size(c.oid) AS index_bytes,
    pg_total_relation_size(c.oid) AS total_bytes
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'm')
ORDER BY total_bytes DESC;

Измерять:

build time;

WAL generated;

peak temp;

restore;

startup;

backup;

compression;

upload;

exact readback;

query p95;

insert throughput;

vacuum/reindex.

19. MCP tool set

Search

search_content
continue_content_search
get_content_batch
get_content_trace
search_actors
search_sources

Writes

submit_discovery_batch
attach_objects_to_project
enqueue_pipeline
record_processing_results
propose_content_card

Operations

platform_status
ensure_database_session
ensure_search_session
get_operation
get_embedding_backlog
get_index_coverage

Tool annotations

Read-only tools маркируются readOnlyHint.

Идемпотентные additive writes получают idempotentHint, но сервер всё равно проверяет idempotency key.

Structured output

Каждый search result должен иметь output schema. Модели проще корректно использовать:

coverage;

cursor;

compact records;

operation IDs;

canonical revision.

20. Межпродуктовый атлас и метрики

Та же PostgreSQL может содержать верхнеуровневый atlas:

product
service
repository
dataset
pipeline
channel
owner
metric definition
dependency
data flow

Отношения:

product uses service
pipeline produces dataset
repository implements pipeline
service feeds product
metric describes product

Хранить нужно агрегаты:

daily active sources;

найдено материалов;

обработано;

прошло фильтры;

опубликовано;

latency pipeline;

failure rate;

embedding backlog;

search coverage;

DB/vector size.

Не хранить в canonical DB каждую низкоуровневую метрику каждую секунду.

21. План реализации

Этап 0. Зафиксировать reusable baseline

exact source commit/blob events-bot-new;

перечислить переносимые файлы;

создать compatibility tests;

запретить второй Kaggle SDK adapter.

Этап 1. Orchestrator core без Kaggle

state machines;

Postgres/control schema;

fake clock;

fake Kaggle;

event ingestion;

leases;

fencing;

service registry;

unit/property tests.

Критерий:

все failure scenarios проходят без реального Kaggle

Этап 2. Generic runtime SDK

Из существующего status client:

versioned event envelope;

arbitrary structured events;

local JSONL fallback;

heartbeat;

resource lease;

secret redaction;

backward-compatible adapter.

Этап 3. Real Kaggle lifecycle smoke

private dataset;

readiness;

exact files;

push;

source binding;

callbacks;

output;

cleanup;

recovery.

Этап 4. PostgreSQL master Notebook

restore;

service announce;

DB gate;

lease watchdog;

direct internal connection;

checkpoint;

clean rotation;

capacity probes.

Этап 5. MCP baseline

stable domain;

status;

exact/FTS search;

read tools;

idempotent writes;

async operation pattern.

Этап 6. E5

corpus worker;

query service;

model registry;

768/1024 table;

queue;

quality benchmark;

co-location test.

Этап 7. BGE

separate Notebook;

corpus worker;

query service;

fusion;

complementary recall benchmark;

optional reranker.

Этап 8. Durability

current/previous checkpoint;

exact readback;

WAL receiver/archive PoC;

recovery drill.

Этап 9. Production canary

bounded corpus;

one active project;

manual observation;

no automatic external publication until gates pass.

22. Решения

GO

один active PostgreSQL primary;

stable MCP/control domain;

internal direct data plane;

outbound runtime events;

reuse events-bot-new lifecycle;

E5/BGE separate model runtimes;

recall-first MCP;

FTS + semantic ensemble;

explicit search coverage;

cursor pagination;

fake Kaggle;

deterministic orchestrator tests;

current/previous verified checkpoint;

TTL для operational data.

CONDITIONAL GO

E5 query encoder рядом с PostgreSQL;

BGE query service always warm;

два active corpus vector indexes;

WAL stream из Kaggle на dev server;

binary-quantized HNSW;

master DB больше 8–10 GiB.

Только после PoC.

NO-GO

считать encoder встроенным в PostgreSQL;

один жёстко зашитый semantic model в MCP;

silent fallback с BGE на E5;

raw cosine score fusion между моделями;

массовое corpus encoding на PostgreSQL master;

E5 и BGE в одном production Notebook без отдельного доказательства;

новый Kaggle client рядом с proven adapter;

fire-and-forget Kaggle launch;

признание completion только по platform status;

бесконечное хранение heartbeat/jobs/logs;

HNSW для каждой экспериментальной модели;

размер master по Dataset limit без disk/checkpoint measurement.

23. Открытые вопросы

Какая точная E5 variant использовалась в Region Talk experiments?

Какой runtime BGE дал memory conflict с E5?

Какой фактический RAM/VRAM profile у этих notebooks?

Какой прямой network endpoint безопасно публиковать из Kaggle?

Работает ли WAL streaming стабильно через доступный egress?

Каков фактический writable disk layout текущего Kaggle runtime?

Можно ли checkpoint stream-upload без второй полной local copy?

Какой complementary recall дают E5 и BGE на реальном корпусе?

Нужен ли BGE corpus index или достаточно BGE reranker?

Как быстро model Notebook стартует из pre-attached model Dataset?

Какова допустимая cold-start latency для MCP?

Нужен ли отдельный vector service раньше достижения disk gate?

24. Обязательные PoC gates

Платформа не считается ready до прохождения:

P0 orchestrator fake/state-machine suite
P1 real Kaggle callback/recovery smoke
P2 master fencing test
P3 PostgreSQL restore/checkpoint/readback
P4 WAL/RPO decision
P5 E5 co-location test
P6 BGE separate service test
P7 retrieval benchmark
P8 1m vector storage benchmark
P9 MCP recall/coverage contract
P10 10–12 hour rotation rehearsal

25. Проверенные исходники и документация

Сохранённые исследования в idea-hub

ideas/portfolio.inbox/idea-20260809-consolidated-content-db-architecture.md

ideas/portfolio.inbox/idea-20260809-offline-first-transactional-databases.md

Region Talk

[README](https://github.com/onedayonemasterpiece/region-talk/blob/main/README.md)

[Kaggle runtime reuse](https://github.com/onedayonemasterpiece/region-talk/blob/main/docs/kaggle-runtime-reuse.md)

[Implementation status](https://github.com/onedayonemasterpiece/region-talk/blob/main/docs/implementation-status.md)

[Testing and debugging](https://github.com/onedayonemasterpiece/region-talk/blob/main/docs/testing-debugging.md)

[Runtime reuse tests](https://github.com/onedayonemasterpiece/region-talk/blob/main/tests/test_kaggle_runtime_reuse.py)

[Orchestrator tests](https://github.com/onedayonemasterpiece/region-talk/blob/main/tests/test_orchestrator.py)

events-bot-new

[Worker status client](https://github.com/onedayonemasterpiece/events-bot-new/blob/main/kaggle/kaggle_status_client.py)

[Host status ledger](https://github.com/onedayonemasterpiece/events-bot-new/blob/main/kaggle_status.py)

[Kaggle client](https://github.com/onedayonemasterpiece/events-bot-new/blob/main/video_announce/kaggle_client.py)

[Telegram Monitoring service](https://github.com/onedayonemasterpiece/events-bot-new/blob/main/source_parsing/telegram/service.py)

[CherryFlash scenario](https://github.com/onedayonemasterpiece/events-bot-new/blob/main/video_announce/scenario.py)

[CherryFlash runner test](https://github.com/onedayonemasterpiece/events-bot-new/blob/main/tests/test_cherryflash_live_runner.py)

Models and search

[multilingual-e5-base](https://huggingface.co/intfloat/multilingual-e5-base)

[multilingual-e5-large-instruct](https://huggingface.co/intfloat/multilingual-e5-large-instruct)

[BGE-M3](https://huggingface.co/BAAI/bge-m3)

[FlagEmbedding](https://github.com/FlagOpen/FlagEmbedding)

[pgvector](https://github.com/pgvector/pgvector)

Platforms and protocol

[Kaggle Notebooks](https://www.kaggle.com/docs/notebooks)

[Kaggle Datasets](https://www.kaggle.com/docs/datasets)

[MCP specification](https://modelcontextprotocol.io/specification/2026-07-28)

[MCP tools](https://modelcontextprotocol.io/specification/2026-07-28/server/tools)

26. Финальная формулировка ADR

Контентная платформа использует один эфемерный PostgreSQL-primary в Kaggle во время активной сессии. Dev-сервер предоставляет стабильный MCP и control plane, а внутренние Kaggle workers работают по прямому data plane после service discovery. Оркестратор переиспользует доказанный runtime Telegram Monitoring и CherryFlash: private datasets, readiness/readback, exact source binding, durable attempts, callbacks, heartbeat, leases, bounded polling, output recovery и cleanup. Worker-side callback обобщается в versioned runtime event protocol для произвольной компактной телеметрии и service announcements.

Semantic search не ограничивается одной моделью. PostgreSQL/pgvector хранит и ищет vectors, а E5/BGE работают как внешние encoder services. MCP использует recall-first search planner: exact lookup, FTS и все активные semantic retrievers, затем rank fusion и при необходимости reranking. Ответ всегда сообщает фактическое покрытие retrievers и поддерживает продолжение выборки. E5 и BGE исполняются раздельно; corpus embeddings создаются batch workers, а совместное размещение E5 query encoder с PostgreSQL допускается только после нагрузочного PoC.

Master-БД хранит только canonical data и доказавшие полезность online projections. Временные jobs, heartbeats, caches и verbose logs удаляются по TTL. До измерения Kaggle disk/checkpoint lifecycle рабочая цель physical PostgreSQL cluster ограничивается 6–8 GiB, а 8–10 GiB считается жёлтой зоной. Архитектура заранее допускает вынос vector indexes в отдельный derived search layer без изменения canonical catalog и MCP contract.

Оркестратор создаётся test-first: Kaggle SDK скрыт за adapter, FakeKaggle моделирует задержки, ошибки, stale outputs и split-brain, deterministic clock исключает реальные ожидания, а property/state-machine tests доказывают single-master, idempotency, fencing, recovery и честное search coverage.
</details>
