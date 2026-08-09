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

On the devstand, `/etc/my-data-hub/my-data-hub.env` supplies distinct restricted LOGIN
URLs for application, connector intake, orchestrator, MCP reader and OAuth revocation
lookup plus the local migrator URL. `/etc/my-data-hub/admin.env` contains only
`MY_DATA_HUB_ROLE_ADMIN_DATABASE_URL`, is mode 0600/root-owned, and is consumed solely by
the short-lived root oneshot role-bootstrap/provision services. Long-running API,
orchestrator and MCP units must never receive the admin URL. Deployment runs
`my-data-hub-identity-verify.service` before starting them.

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
