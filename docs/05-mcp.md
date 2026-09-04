# MCP boundary

Status: `CONTRACT PRESERVED / DYNAMIC MASTER BINDING DEFERRED`

Stable MCP belongs on the lightweight devstand control plane. It never embeds a local
canonical PostgreSQL. For a data operation it ensures/resolves the latest ACTIVE Kaggle
master, binds the receipt to master instance/epoch/canonical revision/idempotency identity,
and uses a bounded epoch credential. When no master exists it reports cold-start operation
state rather than fake success.

The default profile remains typed and semantic; it exposes no generic SQL, shell,
filesystem or provider secret access. Segregated reader/editor/migration profiles preserve
ADR-0012 limits and use restricted roles inside the master PostgreSQL. No remote profile
receives owner, superuser, DDL, BYPASSRLS, server-file or program execution rights.

Public Streamable HTTP, OAuth and the stable hostname are later gates. PR-A keeps remote
MCP and all MCP writes disabled and makes no DNS/VPN/443 change. The database-free control
status endpoint is not claimed as the final MCP gateway.

## IdeaHub Showcase constructor boundary

IdeaHub Showcase is a bounded content-publication MCP surface, not a general IdeaHub
search API. The model obtains IdeaHub/voice context through an available external
read-only source, then begins Showcase MCP work from an owner-approved manifest. Its
MVP surface is the eight methods recorded in
[`ideahub-showcase.md`](ideahub-showcase.md); `apply(dry_run=true)` is the validation
preview. Repository registration does not establish live availability: OAuth,
discovery, remote execution, and receipts remain unaccepted until the dedicated runtime
evidence checklist passes.

## Preserved detailed contract — bound by ADR-0016

The detailed material below is retained where topology-neutral. Any reference to a database, role, committer, backup or connector application is executed inside/against the latest ACTIVE Kaggle master; devstand execution claims are superseded.

## Назначение и границы доверия

MCP предоставляет агентам несколько явно разделённых профилей доступа к каталогу,
оркестратору, коннекторам, Kaggle и миграции. **Реализованный bootstrap v0.1** пока
содержит только bounded semantic tools. Принятое дополнение проектирует отдельный
operator profile для широкого bounded-чтения и контролируемого DML; это ещё не
реализованный PostgreSQL-superuser proxy.

Транспортный слой использует MCP Python SDK v2 через `MCPServer`. Business tools
вызывают `HubService`, поэтому transport/auth меняются без переноса SQL и доменной
логики в обработчики MCP.

## Реализованная поверхность bootstrap v0.1

Tools регистрируются только при наличии требуемого scope. Запрещённый tool не только
завершается ошибкой, но и не появляется в surface данного процесса. Узкое исключение
для incremental OAuth discovery: существующий unified owner/operator grant с
`provider:write` видит схемы включённых `showcase.*` actions, даже если grant был
выдан до появления Showcase scopes. Вызов всё равно требует точный
`showcase:read`/`showcase:write` и возвращает OAuth `insufficient_scope`; reader grants
эти actions не видят.

| Tool | Scope | Режим | Контракт |
|---|---|---|---|
| `hub.health` | `hub:read` | read | canonical/schema revision и write gate |
| `hub.project.list` | `hub:read` | read | не более 100 проектов |
| `hub.content.search` | `hub:read` | read | PostgreSQL FTS, query ≤500 символов, limit ≤50, timeout |
| `hub.content.get` | `hub:read` | read | compact content object и ≤20 assets |
| `hub.trace.get` | `hub:read` | read | exact UUID, ≤100 provenance events |
| `region_talk.queue.summary` | `region-talk:read` | read | агрегаты очереди, без payload dump |
| `region_talk.plan.preview` | `region-talk:read` | read | plan-only, без dispatch/side effects |
| `region_talk.migration.status` | `migration:read` | read | export batches, quarantine, `cutover_ready` |
| `region_talk.migration.accounting` | `migration:read` | read | bounded counts и accounting gates |
| `region_talk.work.enqueue` | `region-talk:write` | semantic write | allowlisted stage; `dry_run=true` по умолчанию |
| `hub.command.submit` | `hub:write` | semantic write | typed idempotent command, без raw SQL |

Mutation tools создаются только если одновременно включён server-side write gate и
присутствует write scope. Production publication tool отсутствует.

## Принятые целевые профили

### 1. `semantic_default`

Текущая предметная поверхность. Предпочтительна для повторяемых продуктовых операций,
поскольку кодирует invariants, idempotency и domain receipts.

### 2. `data_reader`

Широкие bounded `SELECT` и schema/catalog inspection по allowlisted application schemas:

- отдельная read-only PostgreSQL role;
- одна statement/transaction на вызов;
- AST validation;
- statement/transaction/lock timeouts;
- row/byte caps и явная truncation;
- запрет DML/DDL/COPY/CALL/DO/SET, unsafe functions и sensitive catalogs.

### 3. `data_editor`

Контролируемые `INSERT`/`UPDATE`/`DELETE` через preview → short-lived receipt → apply:

- отдельная PostgreSQL role без ownership/superuser/BYPASSRLS;
- allowlisted schemas/tables/columns;
- expected canonical revision/effect bounds;
- idempotency key;
- recent backup/restore gate;
- pre-change checkpoint для bulk/high-impact;
- immutable audit/commit receipt;
- database-layer prohibition for protected tables.

### 4. `migration_operator`

Typed tools для inventory/export/landing/mapping/quarantine/reconciliation/shadow/
cutover/rollback. Агент может управлять Region Talk migration, но raw DML не может
фальсифицировать accounting, `cutover_ready` или publication state.

### 5. `kaggle_operator`

Provider tools, фильтруемые PostgreSQL registry control class:

- inventory всех visible notebooks/private datasets;
- status-only для `orchestrator_protected`;
- lifecycle для `mcp_managed`;
- private TTL/hash exchange packages для `mcp_exchange`;
- metadata/status-only для `external_read_only`.

Полные требования:

- [`17-kaggle-control-plane.md`](17-kaggle-control-plane.md)
- [`18-mcp-operator-and-database-access.md`](18-mcp-operator-and-database-access.md)

## Target scope/participation read surface

После реализации ADR-0015 semantic/read profiles должны давать независимые bounded
ответы, а не один смешанный `status`:

| Tool | Scope | Режим | Контракт |
|---|---|---|---|
| `hub.object.context.get` | `hub:read` | read | catalog object + lifecycle, без payload dump |
| `hub.object.relations.list` | `hub:read` | read | active/history project/pipeline relations |
| `hub.object.states.list` | `hub:read` | read | exact namespaced state + normalized class per scope |
| `hub.object.usage.list` | `orchestrator:read` | read | bounded append-only run/stage usage facts |
| `hub.object.policy.explain` | `hub:read` | read | effective outcome, exact decisions, traversal and freshness |
| `connector.batch.list_applications` | `connector:read` | read | independent consumer applications/receipts |

Scope is resolved from server-side registries and FKs. Tools must not infer membership or
permission from schema name, latest work item, producer hint or free-form metadata. Generic
`data_reader` may inspect approved views; typed semantic tools remain preferred for stable
contracts and explainability.

Mutation of relation/state/policy is allowed only through typed commands with namespace/
writer authority, expected revision, reason/evidence and immutable audit. The generic editor
cannot rewrite append-only relation/state/policy histories or policy-evaluation receipts.
Canonical model: [`22-data-scope-and-pipeline-participation.md`](22-data-scope-and-pipeline-participation.md).

## Транспортные профили

### Локальный агент: stdio

Профиль по умолчанию:

```bash
export MY_DATA_HUB_DATABASE_URL='postgresql://...'
export MY_DATA_HUB_MCP_SCOPES='hub:read,orchestrator:read,region-talk:read,migration:read,sync:read'
my-data-hub mcp serve --transport stdio
```

Database URL и scopes передаются supervisor/защищённым environment, а не client JSON
или repository.

### Локальный Streamable HTTP для отладки

Development bearer разрешён только на loopback:

```text
MY_DATA_HUB_MCP_REMOTE_ENABLED=true
MY_DATA_HUB_MCP_AUTH_MODE=development-token
MY_DATA_HUB_MCP_HOST=127.0.0.1
MY_DATA_HUB_MCP_DEVELOPMENT_TOKEN=<random secret>
```

Конфигурация отклоняет non-loopback bind, дополнительно проверяет Host, Origin, bearer,
body size и SDK transport security.

### Production remote

Канонический URL:

```text
https://mcp-datahub.kenigevents.ru/mcp
```

Контур:

```text
ChatGPT / remote agent
  → TLS edge
  → OAuth 2.1 resource/audience validation
  → MCP Host/Origin/admission checks
  → profile/scoped tools
  → application services / restricted PostgreSQL roles / provider adapters
```

Development token на public listener запрещён. Порядок настройки и acceptance:
[`20-remote-mcp-endpoint.md`](20-remote-mcp-endpoint.md).

## Semantic write contract

Каждая предметная mutation включает или сервер выводит:

- principal/client identity и scopes;
- session ID и idempotency key;
- versioned command type;
- target identity, exact object/project-pipeline scope and expected revision/preconditions;
- bounded payload, exact state/policy namespace and reason/evidence;
- dependency IDs;
- dry-run/preview, где применимо.

Business mutation, required scope relation/state/usage, command receipt and semantic outbox
фиксируются одной PostgreSQL transaction. Внешний side effect не выполняется MCP handler.
Side-effect intent references a fresh policy-evaluation receipt and input fingerprint; stale
policy evidence fails closed before provider dispatch.

## Operator write contract

Generic DML является отдельной привилегированной функцией, а не расширением
`hub.command.submit`. Обязательны:

```text
parse and authorize
→ preview under restricted DB role
→ bind short-lived receipt
→ revalidate backup/revision/effects
→ apply one transaction
→ audit + commit receipt
```

Break-glass DDL/roles/extensions выполняются только локально и не входят в normal remote
profile.

## Data connectors are not MCP calls

Боты и сервисы передают регулярные данные через `/intake/v1` по versioned connector
envelope. У них отдельная service identity, limits, receipt и outage spool. MCP только
наблюдает, приостанавливает и разрешает connector quarantine, а также показывает
independent per-consumer applications. См.
[`16-data-connectors.md`](16-data-connectors.md).

## Всё ещё запрещено

- remote PostgreSQL owner/superuser;
- shell, arbitrary filesystem и secret/session dump;
- raw YDB/provider credentials;
- unbounded query/export;
- public Kaggle dataset creation;
- mutation `orchestrator_protected` Kaggle resources;
- direct notebook canonical writes;
- `publish_now` без отдельного ADR, exact revision и dispatcher gate;
- обход migration accounting/cutover через generic editor DML;
- producer-assigned authoritative project/platform scope;
- вывод membership/policy из work status, schema name или provenance text;
- изменение append-only scope/state/policy evidence generic editor-ом.

## Донор

`events-bot-new/private_events_mcp` остаётся donor для OAuth,
resource/audience validation, admission control, bounded responses, no-store headers,
correlation IDs и provider isolation. Его event-domain/SQLite code не определяет
архитектуру `my-data-hub`.

## Release order

1. devstand/backup/test evidence;
2. production OAuth and remote read-only semantic tools;
3. ADR-0015 scope/catalog/relation/state/usage/policy foundation and read views;
4. connector status + synthetic multi-consumer connector;
5. Kaggle inventory read-only;
6. data-reader profile;
7. MCP-managed Kaggle provider canary;
8. data-editor in disposable schema;
9. allowlisted application DML;
10. migration-operator tools and Region Talk migration;
11. publication remains a separate future gate.
