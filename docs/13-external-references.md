# External implementation references

Checked: **2026-08-09**.

These references constrain implementation details and dependency compatibility. They do
not replace the product authority in `idea-hub` or the ADRs in this repository.

## PostgreSQL and pgvector

- PostgreSQL current documentation: <https://www.postgresql.org/docs/current/>
- PostgreSQL 18 documentation: <https://www.postgresql.org/docs/18/>
- pgvector source, extension releases and SQL usage: <https://github.com/pgvector/pgvector>
- pgvector Docker images/tags: <https://hub.docker.com/r/pgvector/pgvector/tags>

Bootstrap target: PostgreSQL 18 plus pgvector, pinned in Compose and CI to
`pgvector/pgvector:0.8.6-pg18-bookworm`. A deployment must still record the actual image
digest and extension versions rather than trusting a mutable local cache.

## Model Context Protocol

- MCP Python SDK: <https://github.com/modelcontextprotocol/python-sdk>
- MCP specification/documentation: <https://modelcontextprotocol.io/>

The bootstrap targets the stable Python SDK v2 API, uses stdio by default and permits
Streamable HTTP only behind explicit transport/admission controls. Production HTTP remains
fail-closed until OAuth resource/audience validation is ported and integration-tested.

## Joplin

- Joplin Data API reference: <https://joplinapp.org/help/api/references/rest_api/>
- Joplin plugin API: <https://joplinapp.org/api/references/plugin_api/>

The desktop bridge uses a supported API on loopback. It does not read or write Joplin's
internal SQLite database. Android participates through normal Joplin synchronization; the
phone is not assumed to expose the desktop Data API.

## YDB migration source

- YDB Python SDK documentation: <https://ydb.tech/docs/en/dev/ydb-sdk/>
- YDB Python SDK source: <https://github.com/ydb-platform/ydb-python-sdk>
- YDB transaction modes, including Snapshot Read-Only:
  <https://ydb.tech/docs/en/concepts/transactions>

The migration exporter uses read-only credentials and a consistent snapshot where the
actual source table/query supports it. The final migration must record endpoint/database/
table identity, SDK version, source code revision, consistency mode, counts and hashes.

## Kaggle artifacts and notebook execution

- KaggleHub source and API documentation: <https://github.com/Kaggle/kagglehub>
- Kaggle API/CLI source: <https://github.com/Kaggle/kaggle-api>

Kaggle is a compute and private-artifact lane, not a direct canonical writer. Notebook
outputs are accepted only through exact manifests/hashes and PostgreSQL reconciliation.
Encrypted backups/checkpoints require readback verification and separate retention tests.
