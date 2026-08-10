# Источники истины и именование

## 1. Каноническое имя

**`my-data-hub`** — финальное имя проекта, репозитория, Python package,
PostgreSQL database, service units и MCP server.

`content-platform` — раннее рабочее имя того же продукта. Оно допустимо
только в ссылке на исходное исследование и не должно порождать отдельную
сущность, ветку архитектуры или второй набор документов.

## 2. Каноническое исходное видение

Первичный архитектурный материал находится в `idea-hub`:

```text
ideas/portfolio.inbox/idea-20260809-content-platform-current-design.md
source commit: 0c3fcf7 (Add research record)
```

Этот документ — не «сырая карточка идеи». Он содержит предварительную
проработку и целевое видение, на основании которого строится этот репозиторий.

До появления его точной копии в `my-data-hub` действует правило:

1. исходный файл в `idea-hub` определяет продуктовые цели и целевое видение;
2. ADR и implementation docs этого репозитория уточняют способ реализации;
3. при противоречии реализация не молча переписывает исходное видение, а
   создаёт ADR с явным diff, причиной и решением владельца;
4. историческое имя не используется как аргумент для создания второго
   проекта.

## 3. Что кодовый агент должен импортировать

При наличии доступа агент должен:

1. скопировать исходный Markdown **без смысловых правок** в
   `docs/source-material/idea-hub/`;
2. записать source repository, path, source commit и SHA-256;
3. не заменять исходный текст пересказом;
4. оставить этот файл immutable, а любые уточнения вносить через ADR.

Шаблон provenance находится в
[`docs/source-material/source-manifest.yaml`](source-material/source-manifest.yaml).

## 4. Иерархия доноров

| Источник | Роль | Что не делать |
|---|---|---|
| Исходное целевое видение в `idea-hub` | продуктовая и архитектурная основа | не называть вводной заметкой или обычной идеей |
| `events-bot-new/private_events_mcp` | проверенные MCP security/protocol patterns | не переносить event-domain и лишние social tools |
| текущий Region Talk | первый workload, данные и product semantics | не переносить YDB/SQLite/Supabase как целевую БД |
| предыдущая Region Talk migration design | требования к provenance, idempotency, shadow и cutover | не считать старый backend target обязательным |
| Joplin | будущая authoring/projection surface | не делать Joplin SQLite канонической БД |

## 5. Терминология

- **Hub** — единый каталог и каноническое состояние.
- **Project** — контекст использования объектов, например Region Talk.
- **Pipeline** — версионированный граф обработки.
- **Orchestrator** — планирование, leases, retries, ingestion результатов,
  observability и side-effect outbox.
- **Worker / notebook** — вычислительная стадия без права прямой мутации
  canonical tables.
- **MCP** — предметная агентская поверхность над Hub/Orchestrator.
- **Artifact** — immutable файл/набор файлов с hash и manifest; это не БД.

## 6. Принятое инфраструктурное дополнение

ADR-0009–ADR-0015 и документы `15`–`22` уточняют реализацию исходного видения:

- PostgreSQL supervised/always-on; Kaggle не является master DB;
- data connectors — отдельная idempotent ingress boundary;
- `mcp-datahub.kenigevents.ru` — canonical remote MCP URL;
- default semantic MCP и privileged operator MCP разделены;
- Kaggle resources имеют registry control classes;
- infrastructure/test workflows precede Region Talk migration;
- shared data uses explicit platform/project/pipeline/project-pipeline scopes;
- persistent relation, scoped workflow state, append-only usage and policy decision are
  independent facts;
- Region Talk raw lineage and normalized/deduplicated target relations are mandatory.

Это не второй проект и не замена исходного idea-hub документа. При конфликте с точным
импортом исходника требуется новый ADR с явным сравнением и решением владельца.

## 7. Дополнительная терминология

- **Data connector** — versioned producer/adapter с batch identity, hash, receipt,
  watermark и retry semantics.
- **Intake** — authenticated acceptance/staging boundary; не canonical application.
- **Operator MCP** — отдельно разрешённый профиль broad bounded reads/controlled DML под
  restricted PostgreSQL role.
- **Kaggle control class** — registry-enforced provider authorization независимо от
  resource name.
- **Exchange package** — private TTL/hash-manifested artifact transfer; не canonical data.
- **Scope** — устойчивый platform/project/pipeline/project-pipeline контекст для relation,
  state, usage или policy; не свободный tag.
- **Object-scope relation** — persisted membership/management/reference/origin fact; не
  execution status и не policy allow.
- **Scoped state** — exact namespaced workflow/domain state объекта в одном scope, с
  normalized class только для общих отчётов.
- **Pipeline usage** — append-only факт обработки объекта конкретным run/stage; не
  membership.
- **Policy decision/evaluation** — versioned authorization fact и immutable receipt его
  эффективного вычисления для exact action/scope/revisions.
