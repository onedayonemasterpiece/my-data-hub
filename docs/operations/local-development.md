# Local development

## Requirements

- Python 3.12+
- Docker/Compose
- Git

## Setup

```bash
cp .env.example .env
# Replace the local placeholder password.
docker compose up -d postgres
docker compose run --rm api db migrate
python -m venv .venv
. .venv/bin/activate
pip install -e ".[dev]"
python scripts/validate_repository.py
python -m pytest
```

The database port is bound to `127.0.0.1` by default. Do not change it to `0.0.0.0` for
convenience.

## Useful commands

```bash
my-data-hub db status
my-data-hub db verify
my-data-hub orchestrator plan
my-data-hub mcp serve --transport stdio
```

Runtime integration tests that need secrets or provider access must be separate from unit
and PostgreSQL contract tests and must default to disabled.
