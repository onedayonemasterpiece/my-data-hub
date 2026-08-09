# ADR-0013: Remote MCP is published at mcp-datahub.kenigevents.ru

- Status: Accepted
- Date: 2026-08-09

## Context

ChatGPT and remote agents need a stable HTTPS endpoint. Development bearer tokens and
direct exposure of the application port are insufficient for a high-privilege data
operator surface.

## Decision

The canonical public endpoint is:

```text
https://mcp-datahub.kenigevents.ru/mcp
```

It uses Streamable HTTP behind TLS termination and an OAuth 2.1 resource-server
boundary. The exact audience/resource identifier is the public MCP URL. Development
bearer mode remains loopback-only and is never enabled on the public listener.

The Yandex Cloud edge/reverse proxy:

- exposes only TCP 443 publicly;
- redirects or rejects plaintext HTTP;
- uses a managed or automatically renewed certificate;
- forwards to a private/loopback upstream;
- applies body, connection and coarse rate limits.

The MCP process still validates identity, scopes, Host, Origin, resource/audience,
per-tool limits and correlation IDs. The proxy is not the sole authorization boundary.

The same hostname may temporarily expose `/intake/v1` for service connectors, but it is
a separate upstream/auth policy. Contracts must allow that route to move to a dedicated
ingest hostname without changing producer payloads.

## Consequences

- The endpoint can be connected to ChatGPT without exposing PostgreSQL or port 8765.
- Read-only semantic tools are enabled first; operator and Kaggle mutation tools appear
  only after their release gates pass.
- DNS, certificate, OAuth metadata, reverse proxy and MCP admission tests become part
  of deployment evidence.
