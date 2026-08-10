# L06-oauth results

## Scope

- Lane: `L06-oauth`
- Requirement: `R06` — OAuth 2.1 resource-server authentication and HTTP admission primitives
- Base SHA: `0b6b7311081bdfecdd4f3004e5d6842a42f64253`
- Tested implementation SHA: `652bdba1e7b03450334b5053cc126b9521da23ef`

## Outcome

Implemented:

- strict bearer parsing and verified-token decoder boundary;
- exact issuer, audience and requested-resource checks;
- required `sub`, `client_id`, `jti`, `iat`, `nbf`, `exp` and bounded lifetime checks;
- allowed/required scope enforcement;
- fail-closed token/client/principal revocation lookup contract suitable for a bounded PostgreSQL implementation, with no SQLite fallback;
- OAuth principal propagation through ASGI state;
- exact Host and Origin admission with bracketed IPv6 support;
- duplicate security-header rejection and trusted-proxy-only forwarded header normalization/removal;
- query-string bearer rejection;
- bounded headers, request body, response body, concurrency queue, request duration, process rate buckets and rate-key cardinality;
- server-generated correlation IDs and enforced `no-store`/security headers on success and failure;
- sanitized bounded admission errors and development bearer reuse of the same transport boundary.

## Evidence and commands

All commands were run from `/home/dev/.codex/worktrees/my-data-hub/l06-oauth`.

```text
uv run --extra dev ruff check src/my_data_hub/mcp/oauth.py src/my_data_hub/mcp/admission.py src/my_data_hub/mcp/http_security.py tests/test_mcp_oauth.py tests/test_http_security.py
All checks passed!

uv run --extra dev pytest -q
........................................................................ [ 57%]
.....................................................                    [100%]

uv run --extra dev python -m compileall -q src tests
(exit 0)

git diff --check
(exit 0)
```

Targeted OAuth/admission/security suite: 41 passed. Full repository suite: 125 passed.

## Changed files

- `src/my_data_hub/mcp/oauth.py`
- `src/my_data_hub/mcp/admission.py`
- `src/my_data_hub/mcp/http_security.py`
- `tests/test_mcp_oauth.py`
- `tests/test_http_security.py`
- `.codex/lanes/L06-oauth/RESULTS.md`

## Risks / integration notes

- `VerifiedTokenDecoder` deliberately requires a cryptographic JWT/JWKS or introspection adapter; it never decodes an unsigned token itself. The production wiring must provide that adapter.
- `RevocationStore` is a fail-closed PostgreSQL-friendly boundary. Its durable table/query implementation belongs with the separately owned migrations/data-access lane; store failures deny authentication.
- Trusted proxy IPs must be configured exactly. Forwarded headers are rejected from every other peer and stripped before downstream dispatch.
- The response bound buffers a complete finite ASGI response. A future long-lived SSE profile needs a separately designed bounded streaming budget rather than bypassing this admission layer.
