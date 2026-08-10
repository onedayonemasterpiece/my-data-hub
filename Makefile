.PHONY: up down logs install migrate status verify validate test lint notebooks bundle backup restore-check

PYTHON ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)
PYTEST ?= $(if $(wildcard .venv/bin/pytest),.venv/bin/pytest,pytest)
RUFF ?= $(if $(wildcard .venv/bin/ruff),.venv/bin/ruff,ruff)

up:
	docker compose up -d postgres
	docker compose run --rm api db migrate
	docker compose up -d api orchestrator

down:
	docker compose down

logs:
	docker compose logs -f --tail=200

install:
	$(PYTHON) -m pip install -e '.[dev]'

migrate:
	docker compose run --rm api db migrate

status:
	docker compose run --rm api db status

verify:
	docker compose run --rm api db verify

validate:
	$(PYTHON) scripts/validate_repository.py

test: validate
	$(PYTEST)

lint:
	$(RUFF) check .

notebooks:
	$(PYTHON) scripts/create_notebooks.py --check

bundle:
	bash scripts/build_release_bundle.sh

backup:
	bash scripts/backup_postgres.sh

restore-check:
	@echo "Set MY_DATA_HUB_RESTORE_DATABASE_URL and MY_DATA_HUB_RESTORE_CONFIRM, then run scripts/restore_postgres.sh explicitly."
