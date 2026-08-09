# MCP к my-data-hub

## Назначение и граница доверия

MCP предоставляет агентам предметный интерфейс к каталогу, оркестратору,
миграции Region Talk и semantic command layer. MCP **не** является SQL proxy,
оболочкой над provider SDK или способом читать секреты и произвольные файлы.

Транспортный слой реализован на MCP Python SDK v2 через `MCPServer`. Бизнес-tools
вызывают `HubService`, поэтому transport/auth можно заменить без переноса SQL и
доменной логики в обработчики MCP.

## Реализованная поверхность bootstrap v0.1

Tools регистрируются только при наличии требуемого scope. Это значит, что
запрещённый tool не только завершится ошибкой, но и не появится в surface данного
процесса.

| Tool | Scope | Режим | Контракт |
|---|---|---|---|
| `hub.health` | `hub:read` | read | canonical/schema revision и write gate |
| `hub.project.list` | `hub:read` | read | не более 100 проектов |
| `hub.content.search` | `hub:read` | read | PostgreSQL FTS, query ≤500 символов, limit ≤50, statement timeout |
| `hub.content.get` | `hub:read` | read | один compact content object и не более 20 assets |
| `hub.trace.get` | `hub:read` | read | exact subject UUID, не более 100 provenance events |
| `region_talk.queue.summary` | `region-talk:read` | read | агрегаты очереди, без payload dump |
| `region_talk.plan.preview` | `region-talk:read` | read | plan-only, без dispatch и side effects |
| `region_talk.migration.status` | `migration:read` | read | последние export batches, quarantine и `cutover_ready` |
| `region_talk.migration.accounting` | `migration:read` | read | bounded per-kind counts, `fully_accounted` и `cutover_ready` |
| `region_talk.work.enqueue` | `region-talk:write` | semantic write | allowlisted stage, bounded URL/priority; `dry_run=true` по умолчанию |
| `hub.command.submit` | `hub:write` | semantic write | typed idempotent command, без raw SQL |

Оба mutation tools создаются только если одновременно выполнены два условия:

1. `MY_DATA_HUB_MCP_WRITE_ENABLED=true`;
2. в `MY_DATA_HUB_MCP_SCOPES` есть соответствующий write scope.

Production publication tool в v1 отсутствует. Добавление scope само по себе не
может включить публикацию.

## Транспортные профили

### Локальный агент: stdio

Профиль по умолчанию:

```bash
export MY_DATA_HUB_DATABASE_URL='postgresql://...'
export MY_DATA_HUB_MCP_SCOPES='hub:read,orchestrator:read,region-talk:read,migration:read,sync:read'
my-data-hub mcp serve --transport stdio
```

Пример client configuration:

```json
{
  "mcpServers": {
    "my-data-hub": {
      "command": "/opt/my-data-hub/venv/bin/my-data-hub",
      "args": ["mcp", "serve", "--transport", "stdio"]
    }
  }
}
```

Database URL и scopes должны передаваться process supervisor-ом или защищённым
client environment, а не храниться в репозитории либо client JSON.

### Локальный Streamable HTTP для отладки

Development bearer profile разрешён только на loopback:

```text
MY_DATA_HUB_MCP_REMOTE_ENABLED=true
MY_DATA_HUB_MCP_AUTH_MODE=development-token
MY_DATA_HUB_MCP_HOST=127.0.0.1
MY_DATA_HUB_MCP_DEVELOPMENT_TOKEN=<random secret>
```

Конфигурация отклоняет `0.0.0.0` и другие non-loopback bind addresses для
`development-token`. Сервер дополнительно проверяет `Host`, `Origin`, bearer
token и размер тела запроса; MCP SDK transport security остаётся включённой.

### Удалённый агент / телефон / другая машина

Development-token profile для этого не используется. Целевой контур:

```text
client
  → TLS reverse proxy
  → OAuth resource/audience validation
  → MCP Host/Origin/admission checks
  → scoped tools
  → HubService
  → PostgreSQL
```

Production remote HTTP намеренно fail-closed до переноса и интеграционной
проверки OAuth boundary из проверенного MCP-контура `events-bot-new`.

## Semantic write contract

Каждая mutation должна включать или выводить сервером:

- principal/client identity и scopes;
- session ID и idempotency key;
- versioned command type;
- target identity и expected revision/preconditions;
- bounded payload и reason/evidence;
- dependency IDs, если операция причинно зависит от другой;
- dry-run, где операция допускает preview.

Business mutation, command receipt и semantic outbox записываются одной
PostgreSQL-транзакцией. Внешний side effect не выполняется обработчиком MCP.

## Запрещённая поверхность

- `execute_sql`, `query_any_table`, database shell;
- arbitrary filesystem read или secret/session dump;
- raw YDB/provider mutation;
- unbounded export/search;
- generic content patch без domain preconditions;
- `publish_now` без exact revision approval и отдельного dispatcher gate.

## Донор и что из него переносится

`events-bot-new/private_events_mcp` используется как donor для OAuth,
resource/audience validation, admission control, bounded responses, no-store
headers, correlation IDs и provider isolation. Его event-domain и
SQLite-specific storage code не определяют архитектуру `my-data-hub`.

## Следующие MCP этапы

После deployment и Region Talk migration добавляются только поверх тех же
service/repository boundaries:

- exact candidate/revision read;
- review decision по immutable revision fingerprint;
- run/task diagnostics;
- Joplin link/sync tools;
- conflict resolution с expected conflict revision;
- production release tool — только после отдельного ADR и canary acceptance.
