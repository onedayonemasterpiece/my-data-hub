# Secrets and configuration

## Secret classes

- PostgreSQL application/owner/backup credentials;
- MCP signing/authentication credentials;
- Kaggle API token and private dataset identifiers where sensitive;
- Google/LLM provider keys and shared limiter credentials;
- Telegram API/session/bot credentials;
- temporary read-only YDB credentials;
- Joplin desktop API token.

## Placement

- Devstand: root-restricted environment/secret store, injected into containers.
- GitHub: environment-scoped secrets only for required workflows.
- Kaggle: Kaggle User Secrets; never notebook source or dataset files.
- Windows/Joplin: OS credential store or local protected environment.

## Lifecycle

YDB secrets exist only during migration and rollback window. Publication credentials are not
provided to discovery/embedding workers. Database owner credentials are not used by MCP or
normal orchestrator runtime. Rotate any credential that appears in logs or artifacts; do not
only delete the log.
