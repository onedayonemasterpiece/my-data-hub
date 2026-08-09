# ADR-0001: Project name and vision authority

- Status: Accepted
- Date: 2026-08-09

## Context

The target vision was originally recorded in `idea-hub` under the rough working
name `content-platform`. The dedicated implementation repository is named
`my-data-hub`. Both names refer to the same product, not competing initiatives.

## Decision

`my-data-hub` is the canonical product, repository, package, database, service
and MCP namespace. The original research document is an architectural source,
not a disposable inbox note. Its exact source bytes and provenance must be
imported before deployment; conflicts are resolved through an ADR rather than
silent reinterpretation.

## Consequences

The historical name may appear only in provenance/search metadata. New runtime
identities use `my-data-hub` / `my_data_hub`; no compatibility namespace or
second project is created.
