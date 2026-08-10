# Local development

## Requirements

- Python 3.12+
- Docker/Compose
- Git

## Setup

```bash
cp .env.example .env
# Replace the integration-only placeholder password.
make integration-up
python -m venv .venv
. .venv/bin/activate
pip install -e ".[dev]"
python scripts/validate_repository.py
python -m pytest
make integration-down
```

This profile is disposable test infrastructure: PostgreSQL uses tmpfs, has no named
volume or restart policy, and `make integration-down` always runs Compose cleanup with
`-v --remove-orphans`. It is not a production/devstand deployment path. The database
port is bound to `127.0.0.1`; do not change it to `0.0.0.0` for convenience.

## Useful commands

```bash
my-data-hub db status
my-data-hub db verify
my-data-hub orchestrator plan
my-data-hub mcp serve --transport stdio
```

Runtime integration tests that need secrets or provider access must be separate from unit
and PostgreSQL contract tests and must default to disabled.
