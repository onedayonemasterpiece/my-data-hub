# MCP architecture

## Boundary

MCP is a scoped application protocol over `HubService`; it is not a database,
provider or filesystem protocol. Tool handlers contain validation and service calls,
not free-form SQL or provider credentials.

## Layers

```text
MCP client
  → stdio or Streamable HTTP transport
  → transport authentication / Host / Origin / body limits
  → tool registration by scope
  → bounded input validation
  → HubService
  → PostgreSQL repositories / semantic command unit of work
  → receipts and audit
```

## Transport profiles

- **stdio** — default for a local trusted agent process; scopes are supplied by its
  supervised environment.
- **development Streamable HTTP** — loopback-only bearer profile for local testing.
- **production Streamable HTTP** — TLS + OAuth resource/audience validation +
  admission controls. It remains disabled until the donor OAuth boundary is ported
  and integration-tested.

The MCP SDK/protocol version is pinned by `pyproject.toml` and imported in CI. The
HTTP host application enters the SDK session-manager lifespan explicitly.

## Authorization model

A tool is registered only when its required scope is present. Mutation tools also
require the independent server-side `MCP_WRITE_ENABLED` gate. Publishing requires a
separate future `hub:publish` profile and cannot be enabled by a generic write scope.

Current exact tools and limits are canonical in [`../05-mcp.md`](../05-mcp.md) and
`src/my_data_hub/mcp/scopes.py`.

## Mutation model

Writes are semantic commands with idempotency, preconditions and bounded payloads.
The command, business state transition and transactional-outbox records share one
PostgreSQL transaction. External effects are later executed by dedicated dispatchers
only after canonical commit and exact approval.

## Security invariants

- no arbitrary write SQL or table browser;
- no secret/provider/session tools;
- bounded rows, bytes, execution time and response content;
- explicit Host/Origin checks for HTTP;
- loopback-only development bearer transport;
- OAuth required for future production remote access;
- correlation/audit data never includes bearer tokens;
- destructive and publication capabilities require additional gates and evidence.

`events-bot-new/private_events_mcp` is a donor for authentication, admission control,
response bounding and provider isolation. Its domain/storage implementation is not copied
as the `my-data-hub` architecture.
