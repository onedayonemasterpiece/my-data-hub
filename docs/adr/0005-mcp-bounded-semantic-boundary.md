# ADR-0005: MCP default surface is a bounded semantic boundary

- Status: Accepted; amended by ADR-0012
- Date: 2026-08-09

## Decision

The default MCP profile exposes typed catalog, pipeline, review and migration tools with
strict schemas, scopes, limits and audit. It does not expose arbitrary SQL, shell,
filesystem, provider clients or secrets. Local code agents use stdio first; remote
Streamable HTTP remains disabled until the proven OAuth/host/origin and
admission-control boundary is ported from `events-bot-new`.

ADR-0012 adds a separately configured and authorized database-operator profile. It does
not weaken this default semantic surface and does not expose a PostgreSQL owner or
superuser.

## Consequences

Tool handlers remain thin adapters over application services. New recurring product
mutations should use explicit semantic commands and tests. The privileged operator
profile has separate OAuth scopes, PostgreSQL roles, preview/apply receipts, backup
gates and audit. Production publishing is not part of the v1 MCP surface and cannot be
enabled by adding a generic scope alone.
