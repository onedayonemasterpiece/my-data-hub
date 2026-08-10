# Secrets and configuration

## Secret classes

- PostgreSQL owner/app/orchestrator/connector/reader/editor/migration/backup credentials;
- MCP OAuth signing/resource-server credentials;
- Kaggle orchestrator, MCP sandbox and canary credentials;
- Google/LLM provider keys and shared limiter credentials;
- Telegram API/session/bot credentials;
- temporary read-only YDB credentials;
- Joplin desktop API token;
- backup encryption keys.

## Placement

- Devstand: root-restricted OS secret store/environment injected into services.
- GitHub: protected environment-scoped secrets only for required workflows.
- Kaggle: Kaggle User Secrets for notebook runtime only; never notebook source/dataset.
- Windows/Joplin: OS credential store or protected local environment.
- Backup encryption key: separate channel from the off-host artifact/provider.

## Separation

Prefer separate Kaggle identities/tokens for orchestrator production, MCP-managed sandbox
and canary. Remote MCP never returns provider/database credentials. OAuth revocation must
stop agent access without rotating database owner credentials.

The MCP data editor uses its own restricted PostgreSQL login/role. Owner/migrator and
break-glass credentials are local only.

The devstand uses a separate root-owned, mode-0600 environment file and Unix account for
each service: `api.env`, `orchestrator.env`, `mcp.env`, `committer.env`, `backup.env`,
`migrator.env`, `verify.env` and `monitoring.env`. Each contains only that process's restricted database
URL and directly required settings/secrets. `/etc/my-data-hub/admin.env` contains only
`MY_DATA_HUB_ROLE_ADMIN_DATABASE_URL` and is consumed solely by short-lived root
role-bootstrap/provision/probe services. `identity-verify.env` is another root-only
oneshot input containing the URLs needed to prove LOGIN/group isolation; it is never
loaded by a long-running process. Deployment runs `my-data-hub-identity-verify.service`
before starting application services.

`connector-canary.env` is loaded only by the bounded, short-lived synthetic canary and
contains four distinct restricted URLs: connector intake, canonical committer, MCP
reader and monitoring verification. It never contains owner, role-admin or migrator
credentials and is never loaded by an API, orchestrator, MCP,
committer timer or backup process. The post-deploy OAuth canary token remains in the
protected GitHub environment; only its already-public issuer, JTI and expiry are passed
to a root transient unit which appends the revocation row using `admin.env`.

`compose.yaml` and a single local `.env` are development conveniences only. They are not
the production supervision or secret-isolation model; `deploy/systemd/install.sh`
requires the per-service files above before it installs or enables any unit.

## Lifecycle

- YDB secrets exist only during migration/rollback window.
- Publication credentials are unavailable to discovery/embedding workers and generic
  database operator.
- Connector principals are bound to connector/data-product IDs.
- Rotate provider/OAuth/database secret after suspected exposure; deleting a log is not
  remediation.
- Exchange packages contain no secrets; encrypted sensitive payload key travels through a
  separate channel.
- Development bearer token is loopback-only and never reused for the public hostname.
