# External implementation references

Checked: **2026-08-09**.

These references constrain implementation details and dependency compatibility. They do
not replace the product authority in `idea-hub` or accepted ADRs.

## PostgreSQL and pgvector

- PostgreSQL 18 documentation: <https://www.postgresql.org/docs/18/>
- Database roles: <https://www.postgresql.org/docs/current/database-roles.html>
- Role attributes: <https://www.postgresql.org/docs/current/role-attributes.html>
- Role membership and `SET ROLE`:
  <https://www.postgresql.org/docs/current/role-membership.html>
- Client/session timeouts:
  <https://www.postgresql.org/docs/current/runtime-config-client.html>
- pgvector source/releases: <https://github.com/pgvector/pgvector>
- pgvector Docker tags: <https://hub.docker.com/r/pgvector/pgvector/tags>

Bootstrap target: PostgreSQL 18 plus pgvector pinned in Compose/CI to
`pgvector/pgvector:0.8.6-pg18-bookworm`. Deployment records actual image digest and
extension versions.

The operator implementation must rely on PostgreSQL roles/grants as the primary boundary
and set per-session/local statement, transaction, lock and idle-in-transaction limits.
No remote role receives superuser, `BYPASSRLS`, role/database creation or schema ownership.

## Model Context Protocol

- MCP Python SDK: <https://github.com/modelcontextprotocol/python-sdk>
- MCP documentation: <https://modelcontextprotocol.io/>
- OAuth 2.1 authorization tutorial:
  <https://modelcontextprotocol.io/docs/2026-07-28/tutorials/security/authorization>
- Security best practices:
  <https://modelcontextprotocol.io/docs/2025-06-18/tutorials/security/security_best_practices>

The bootstrap uses stdio by default. Production remote Streamable HTTP requires OAuth
resource/audience, Host/Origin, body/timeout and admission controls. Development bearer
mode remains loopback-only.

## OpenAI / ChatGPT remote MCP

- ChatGPT Developer mode:
  <https://developers.openai.com/api/docs/guides/developer-mode>
- Building MCP servers for ChatGPT integrations:
  <https://developers.openai.com/api/docs/mcp>
- Developer mode and MCP apps help:
  <https://help.openai.com/en/articles/12584461-developer-mode-and-mcp-apps-in-chatgpt>

Current official documentation describes full MCP read/write tools in Developer mode and
remote streaming HTTP/SSE with OAuth/no-auth/mixed authentication. `my-data-hub` uses
OAuth for its high-privilege endpoint and connects using the exact `/mcp` URL only after
read-only and negative security tests.

## Kaggle CLI and provider behavior

- Official Kaggle CLI: <https://github.com/Kaggle/kaggle-cli>
- Kernels/notebooks commands:
  <https://github.com/Kaggle/kaggle-cli/blob/main/docs/kernels.md>
- Datasets commands:
  <https://github.com/Kaggle/kaggle-cli/blob/main/docs/datasets.md>
- Kernel metadata:
  <https://github.com/Kaggle/kaggle-cli/blob/main/docs/kernel-metadata.md>
- Authentication:
  <https://github.com/Kaggle/kaggle-cli/blob/main/docs/authentication.md>

The official CLI supports listing, creating/updating/running/pulling/status/output/deleting
kernels/notebooks and listing/creating/versioning/downloading/deleting datasets. Dataset
creation is private unless explicitly made public; the MCP wrapper omits public creation
entirely and verifies privacy after provider write.

No cancellation tool is exposed until a supported provider operation is present and
proven by integration tests. Provider web UI behavior is not enough.

Kaggle hosts the fenced master Notebook and private verified checkpoints; Datasets alone are not a live database. Every provider
resource is governed by the local registry/control class and exact receipts.

## Yandex Cloud DNS, certificates and HTTPS edge

- Cloud DNS CLI/tutorial examples using `yc dns zone add-records`:
  <https://yandex.cloud/en/docs/tutorials/web/blue-green-canary-deployment>
- Application Load Balancer quickstart:
  <https://yandex.cloud/en/docs/application-load-balancer/quickstart>
- Certificate Manager:
  <https://yandex.cloud/en/docs/certificate-manager/>
- Application Load Balancer CLI:
  <https://yandex.cloud/en/docs/cli/cli-ref/application-load-balancer/cli-ref/>

The code agent may use an existing reverse proxy or Yandex Application Load Balancer,
provided DNS, managed/renewed TLS, public 443 only, private upstreams and rollback evidence
meet [`20-remote-mcp-endpoint.md`](20-remote-mcp-endpoint.md).

## Joplin

- Joplin Data API: <https://joplinapp.org/help/api/references/rest_api/>
- Joplin plugin API: <https://joplinapp.org/api/references/plugin_api/>

The desktop bridge uses a supported API on loopback. It does not read or write Joplin's
internal SQLite database. Android participates through normal Joplin synchronization.

## YDB migration source

- YDB Python SDK documentation: <https://ydb.tech/docs/en/dev/ydb-sdk/>
- YDB Python SDK source: <https://github.com/ydb-platform/ydb-python-sdk>
- Transaction modes: <https://ydb.tech/docs/en/concepts/transactions>

The migration exporter uses read-only credentials and a consistent snapshot where the
actual source/query supports it. Final migration records endpoint/database/table identity,
SDK/source revisions, consistency mode, counts and hashes.
