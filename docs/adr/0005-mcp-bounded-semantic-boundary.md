# ADR-0005: MCP is a bounded semantic boundary

- Status: Accepted
- Date: 2026-08-09

## Decision

MCP exposes typed catalog, pipeline, review and migration tools with strict
schemas, scopes, limits and audit. It never exposes arbitrary SQL, shell,
filesystem, provider clients or secrets. Local code agents use stdio first;
remote Streamable HTTP remains disabled until the proven OAuth/host/origin and
admission-control boundary is ported from `events-bot-new`.

## Consequences

Tool handlers remain thin adapters over application services. New mutations
require an explicit semantic command and tests. Production publishing is not
part of the v1 MCP surface and cannot be enabled by adding a scope alone.
