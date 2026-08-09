# MCP architecture

## Boundary

MCP is a scoped operator/application protocol over services and restricted database/
provider adapters. The default surface is semantic. A separately enabled operator
surface permits broad bounded reads and controlled DML, but never a PostgreSQL
owner/superuser, shell, filesystem or secret protocol.

## Layers

```text
MCP client
  → stdio or Streamable HTTP transport
  → OAuth / Host / Origin / body limits
  → profile and tool registration by scope
  → bounded input validation
  → semantic service OR operator/provider adapter
  → restricted PostgreSQL role / Kaggle registry policy
  → receipts and immutable audit
```

## Transport profiles

- **stdio** — local trusted agent; supervised environment supplies scopes/credentials.
- **development Streamable HTTP** — loopback-only bearer profile.
- **production Streamable HTTP** — TLS + OAuth resource/audience at
  `https://mcp-datahub.kenigevents.ru/mcp`.

The HTTP host enters the SDK session-manager lifespan explicitly. Internal ports are not
published to the internet.

## Authorization model

A tool is registered only when profile, scope and server-side feature gate allow it.
Dynamic authorization also evaluates target, environment, provider control class,
PostgreSQL role, preview/lease/revision and backup evidence.

Profiles:

- semantic default;
- data reader;
- data editor;
- migration operator;
- Kaggle operator;
- local break-glass administration outside remote MCP.

Current and planned tools are canonical in [`../05-mcp.md`](../05-mcp.md).

## Mutation models

### Semantic

Typed command, idempotency and expected revision. Business transition, command receipt
and transactional outbox commit together.

### Database operator

AST-validated, allowlisted DML follows preview → short-lived bound receipt → one
transaction apply → audit/commit receipt. The DB role prevents prohibited targets even
if application validation fails.

### Provider operator

Kaggle mutation follows local resource registry, control class, expected provider
fingerprint, lease, idempotency and provider reconciliation after ambiguous outcomes.

## Security invariants

- no remote owner/superuser, DDL/roles/extensions or server file access;
- no secret/provider credential tools;
- bounded rows, bytes, execution time and response content;
- explicit OAuth resource/audience, Host and Origin checks;
- loopback-only development bearer transport;
- protected Kaggle and migration state enforced at DB/service layer;
- correlation/audit data never includes bearer tokens;
- production publication requires a separate future gate/ADR.

`events-bot-new/private_events_mcp` remains the donor for authentication, admission,
response bounding and provider isolation. Its domain/storage implementation is not the
`my-data-hub` architecture.
