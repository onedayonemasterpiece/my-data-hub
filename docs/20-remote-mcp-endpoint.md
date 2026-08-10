# Remote MCP endpoint: mcp-datahub.kenigevents.ru

Status: `R1 RESOURCE SERVER IMPLEMENTED / SAME-HOST TLS AND OAUTH PENDING`
Date: 2026-08-10
Related decision: ADR-0013

Implemented in R1: a private Streamable HTTP backend profile, asymmetric JWKS JWT
verification, exact issuer/audience/resource/scope/time claims, append-only PostgreSQL
revocation, RFC 9728 protected-resource metadata, `WWW-Authenticate` discovery,
Host/Origin/trusted-proxy validation, no-store/correlation headers, bounded
request/response/concurrency/rate/timeout controls, and read-only tool discovery.
The permanent backend is the current `DevCoveer` server. Cloud DNS/TLS/edge deployment is
not yet claimed; no Compute instance or ALB is required, and the hostname currently has
no DNS record.

## 1. Canonical URL

```text
https://mcp-datahub.kenigevents.ru/mcp
```

This is the stable resource identifier used by ChatGPT and other remote MCP clients.
Do not expose the internal application port or PostgreSQL directly.

## 2. Edge topology

```text
Internet client
  → Yandex Cloud DNS
  → public HTTPS listener / TLS certificate
  → same-host nginx SNI/TLS edge (sharing 443 with existing Xray)
  → loopback MCP upstream
  → MCP Streamable HTTP server
  → scoped application/database/provider services
```

Only TCP 443 is public. The upstream MCP port binds to loopback or a private network and
accepts traffic only from the trusted proxy path.

A minimal public liveness route may exist, but readiness, database details, scopes,
provider identity and queue data require authentication.

## 3. DNS and certificate

Use the existing Yandex Cloud DNS zone to point the hostname at `188.227.84.107`, then:

1. locate the authoritative Cloud DNS zone for `kenigevents.ru`;
2. add the required `A`/`AAAA` or `CNAME` record for `mcp-datahub`;
3. request or attach a certificate covering the exact hostname;
4. complete DNS or HTTP validation;
5. attach the certificate to the same-host nginx HTTPS listener;
6. verify renewal ownership and expiration monitoring;
7. record zone, record set, certificate and listener IDs in a deployment receipt.

Do not commit those IDs if they reveal protected infrastructure context; the public
hostname and non-secret certificate fingerprint may be documented.

## 4. Transport and routes

### MCP

```text
POST /mcp
```

Use Streamable HTTP. Support only the methods/content types required by the selected MCP
SDK transport. Enforce request body and connection limits at both proxy and application.

### Health

```text
GET /health/live
GET /health/ready    # authenticated or private
```

Public liveness reveals only that the edge/upstream responds. It must not expose
versions, database state, scopes or internal hostnames.

### Connector intake

During the bootstrap, the same hostname may route:

```text
POST /intake/v1/batches
GET  /intake/v1/batches/{batch_id}/receipt
```

This is a separate upstream or route policy with service identity, not MCP OAuth user
scopes. The connector base URL must be configuration so it can later move to, for
example, a dedicated ingest hostname without changing the envelope contract.

## 5. Authentication

Production remote MCP uses OAuth 2.1 resource-server semantics. Requirements:

- exact resource/audience binding to the public MCP URL;
- issuer and signing-key validation;
- expiry/not-before validation;
- client/principal identity and scope extraction;
- revocation or short token lifetime;
- no token in query string;
- `Cache-Control: no-store` on sensitive responses;
- correlation ID on every request;
- separate profiles/clients for read-only, operator and provider capabilities.

Development-token mode remains loopback-only. It cannot be enabled on
`mcp-datahub.kenigevents.ru` by environment drift.

The preferred implementation is to adapt the already proven OAuth/resource/audience
boundary from the `events-bot-new` MCP donor rather than invent another security stack.

## 6. Host and Origin controls

The MCP server validates:

```text
Host: mcp-datahub.kenigevents.ru
Origin: approved MCP client origins where Origin is sent
```

Missing Origin is handled according to the MCP client/transport contract; an unexpected
present Origin is rejected. Proxy-rewritten forwarding headers are trusted only from
the proxy address and normalized once.

Host/Origin checks supplement OAuth. They do not replace it.

## 7. Tool release profiles

### Profile A — remote read-only

Initially expose:

- hub health/catalog/search/trace;
- orchestrator and Region Talk summaries;
- migration accounting/status;
- connector status after implementation;
- Kaggle inventory with control-class filtering;
- bounded database reader after its own gate.

### Profile B — MCP-managed Kaggle

Add only after provider canary and protected-resource denial tests:

- MCP-managed notebook lifecycle;
- private MCP-managed dataset lifecycle;
- exchange package tools.

### Profile C — data editor and migration operator

Add only after:

- separate DB roles;
- SQL AST and grant tests;
- preview/apply and audit receipts;
- backup freshness and restore gates;
- disposable-schema canary;
- protected-table negative tests.

Production publication remains absent until a separate ADR and canary decision.

## 8. Proxy and application limits

At minimum configure:

- TLS-only listener and HTTP redirect/rejection;
- bounded request body;
- request/header timeout;
- upstream connect/read timeout;
- connection and coarse IP/client rate limits;
- no directory listing or static file exposure;
- sanitized error bodies;
- access log without bearer tokens or sensitive payloads;
- health and certificate monitoring.

The application independently enforces tool-specific row/byte/time/concurrency budgets.
An edge limit cannot authorize a tool or database target.

## 9. ChatGPT connection acceptance

Before connecting the high-privilege profile:

1. test the endpoint with MCP Inspector or equivalent protocol client;
2. verify OAuth discovery/login and exact resource/audience;
3. verify tool discovery under a read-only token;
4. run health and one bounded data read;
5. prove wrong audience, expired token, unapproved scope, Host and Origin failures;
6. revoke the test client/token and prove access stops;
7. connect ChatGPT using the exact `/mcp` URL;
8. keep operator/Kaggle mutation tools disabled for the first connection;
9. archive non-secret request IDs and authorization receipts.

## 10. Yandex CLI implementation acceptance

The accepted implementation uses the existing same-host reverse proxy. It is acceptable
only if evidence shows:

- DNS resolves globally to the intended edge;
- certificate chain and hostname validation pass;
- only 443 is externally reachable;
- upstream application port and PostgreSQL are not public;
- service survives host/process restart;
- certificate renewal is monitored;
- OAuth and negative MCP tests pass through the public hostname;
- rollback to the prior proxy/config is documented and rehearsed.

The architecture does not require a specific load-balancer product if these properties
are met and the choice is recorded.
