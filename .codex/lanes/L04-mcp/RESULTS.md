# L04-mcp results

## Scope

- Lane: `L04-mcp`
- Requirement: `R07` — dynamic master-resolver MCP / OAuth contracts
- Base SHA: `5b92886f319b17b0a121df00da6bd366c9017c60`
- Tested implementation head SHA: `8d413dd828f0d0821accd24e2379f8cdd15b0d7c`
- Execution effort: high; security-sensitive OAuth/write-gate changes received negative-path and full-suite validation.

## Outcome

Implemented inside the assigned MCP/OAuth surface:

- Replaced `HubService`'s static PostgreSQL URL/`psycopg.connect` boundary with injected `MasterResolver`, `MasterSessionBroker`, exact-epoch `MasterSession`, control gateway, audit sink, and `WriteGate` protocols.
- Kept `platform.status` and `master.status` healthy at `master_state=ABSENT`; data tools durably call `ensure_master` and return `operation_id` rather than inventing a local database or false success.
- Bound service authorization, session role, actor/client audit identity, epoch and revision to the verified per-request token identity propagated by HTTP admission.
- Added reader and owner/operator scope catalogs. `tools/list` is identity-filtered; a reader cannot discover or call owner write/provider-management tools.
- Added all required operations/status, blogger list/get/search/provenance/statistics, bounded data query/change, migration preview/apply, and provider status/managed-resource contracts.
- Added MCP `2026-07-28` stateless Streamable HTTP construction and top-level/per-tool OAuth `securitySchemes`, legacy `_meta` mirroring, truthful read-only/destructive/idempotent/open-world annotations, plus `_meta["mcp/www_authenticate"]` linking challenges.
- Removed PostgreSQL-backed OAuth revocation and introduced a master-independent control-ledger protocol for revocation, client enablement/scope policy, OAuth audit, authorization-code/PKCE exchange, refresh rotation and revocation adapters.
- Kept JWT verification on PyJWT's asymmetric JWKS implementation with exact HTTPS issuer/audience/resource/JWKS policy; OAuth discovery contracts advertise authorization code + refresh, PKCE `S256`, CIMD/static/DCR-compatible endpoints and token auth methods.
- Preserved/extended exact Host and Origin checks, trusted-forwarder handling, no-store/no-cache headers, body/header/response/time/rate/concurrency bounds, and request-scoped identity cleanup.
- Added a `pglast` AST boundary that rejects multiple statements, DDL, DML in reads, COPY/CALL/DO/SET, locking/SELECT INTO, system/non-allowlisted relations, unsafe functions, secondary write relations/subqueries, unbounded UPDATE/DELETE, literals, discontinuous parameters and non-allowlisted write targets.
- Made remote writes fail closed without an injected, nonexpired principal/client/tool/epoch/revision-bound permit. Apply requires exact preview binding; destructive actions require a verified pre-change checkpoint; successful apply must return a durable operation ID in the checkpoint lifecycle.
- Provider mutations reject `orchestrator_protected`, `external_read_only`, unknown control classes and every public-resource request before gateway dispatch; only private `mcp_managed`/`mcp_exchange` contracts can be permitted.
- Added committed JSON schemas and sanitized examples for OAuth provider metadata and MCP security catalogs.

No deployment, OAuth credentials, provider resources, secrets, or production data were created by this lane.

## External contract research

Implementation was checked against primary sources before coding:

- MCP specification `2026-07-28`, Streamable HTTP, authorization and tools: `https://modelcontextprotocol.io/specification/2026-07-28`
- MCP 2026 release/header/stateless changes: `https://blog.modelcontextprotocol.io/posts/2026-07-28/`
- OpenAI plugin authentication, `securitySchemes`, PKCE/CIMD and `mcp/www_authenticate`: `https://developers.openai.com/plugins/build/auth`
- RFC 9728 protected resource metadata: `https://www.rfc-editor.org/rfc/rfc9728.html`
- RFC 8414 authorization server metadata: `https://www.rfc-editor.org/rfc/rfc8414.html`
- pglast AST/visitor API: `https://pglast.readthedocs.io/en/v7/usage.html`

## Evidence and commands

All final commands below ran against tested implementation head `8d413dd828f0d0821accd24e2379f8cdd15b0d7c` in `/home/dev/.codex/worktrees/my-data-hub/operational-l04-mcp`:

```text
.venv/bin/python -m compileall -q src tests
(exit 0)

.venv/bin/ruff check src tests
All checks passed!

.venv/bin/pytest -q
294 passed

.venv/bin/python scripts/validate_repository.py
{"checks": 2452, "errors": [], "notes": [], "ok": true}

.venv/bin/python scripts/create_notebooks.py --check
{"drift": [], "mode": "check", "written": []}

git diff --check
(exit 0)
```

Targeted MCP/OAuth/security coverage is 87 collected tests across `tests/mcp`, `tests/test_mcp_oauth.py`, `tests/test_mcp_oauth_runtime.py`, `tests/test_mcp_sdk_v2_contract.py`, and `tests/test_mcp_service.py`.

## Integration / configuration needs

Root integration must perform these shared-scope changes; this lane intentionally did not edit the forbidden files:

1. Replace/remove `MY_DATA_HUB_MCP_REVOCATION_DATABASE_URL` in `config.py`, `control_plane/app.py`, `.env.example`, service-identity/provisioning scripts and config tests. It must not point to canonical PostgreSQL.
2. Adapt the L02 durable control ledger to `OAuthControlLedger`, `ControlPlaneReader`, `MCPAuditSink`, and the write-permit policy. Revocation/client/audit lookup errors remain authentication failures.
3. Adapt the L03 master registry/tunnel components to `MasterResolver` and `MasterSessionBroker`; issued roles must be short-lived, restricted and epoch-bound, and master results must return their exact epoch/revision.
4. Update shared configuration scope allowlists/defaults to `READER_PROFILE_SCOPES` for `chatgpt-reader` and `OWNER_OPERATOR_PROFILE_SCOPES` only for the separately enabled owner profile. Do not configure all scopes as an HTTP admission requirement; authorization is per tool.
5. Build remote HTTP with `create_streamable_http_app(settings, dependencies=..., validator=...)`. The legacy `serve(streamable-http)` path now intentionally fails closed until dependencies are injected.
6. Supply an established external IdP/authorization library matching `OAuthProviderMetadata`; create and safely store credentials only in the root deployment lane. Run real issuer/JWKS/PKCE/resource/revocation/ChatGPT tests there.
7. Back `data.query` and change sessions with PostgreSQL read-only transactions, statement/lock/transaction/idle timeouts, row/byte caps and real role grants. The MCP AST classifier is defense in depth, not a replacement for database grants.
8. Back write permits with signed short-lived preview receipts and the L03 pre/post verified-checkpoint lifecycle; the MCP service will reject non-durable immediate success.

## Risks / blockers

- Real remote OAuth/ChatGPT, control-ledger persistence, master cold start, tunnel sessions and durable checkpoint writes cannot be proven in this isolated lane because L02/L03 adapters, deployment configuration and credentials are separate ownership. The implemented boundaries remain fail closed.
- MCP Python SDK 2.0 retains extension security metadata under `_meta`; `ToolSecurityMetadataMiddleware` adds the OpenAI-required top-level mirrors to bounded JSON `tools/list` responses. Keep the compatibility test until the SDK serializes this field natively, then remove the adapter rather than double-writing it.
- SQL policy is intentionally restrictive. New canonical views/functions/write targets require explicit reviewed allowlist changes plus database grants; they must never be opened dynamically from client input.
- No production readiness, row counts, checkpoint identity, OAuth client identity, or external availability is claimed here.

## Changed files

- `.codex/lanes/L04-mcp/RESULTS.md`
- `src/my_data_hub/auth/{__init__,context,control,metadata}.py`
- `src/my_data_hub/mcp/{admission,catalog,contracts,oauth,oauth_jwt,server,service,sql_policy,transport}.py`
- removed `src/my_data_hub/mcp/oauth_postgres.py`
- `schemas/mcp/security-catalog.v1.schema.json`
- `schemas/oauth/provider-metadata.v1.schema.json`
- `examples/mcp/reader-security-catalog.v1.json`
- `examples/oauth/provider-metadata.v1.json`
- `tests/mcp/test_dynamic_contracts.py`
- `tests/mcp/test_oauth_control_metadata.py`
- `tests/mcp/test_sql_policy.py`
- `tests/mcp/test_transport_metadata.py`
- `tests/test_mcp_oauth.py`
- `tests/test_mcp_oauth_runtime.py`
- `tests/test_mcp_sdk_v2_contract.py`
