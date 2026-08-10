.PHONY: up down integration-up integration-down integration-logs integration-migrate integration-status integration-verify control-config install validate test lint notebooks bundle

PYTHON ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)
PYTEST ?= $(if $(wildcard .venv/bin/pytest),.venv/bin/pytest,pytest)
RUFF ?= $(if $(wildcard .venv/bin/ruff),.venv/bin/ruff,ruff)

up:
	@echo "FORBIDDEN: devstand does not run PostgreSQL; use integration-up for disposable tests" >&2
	@exit 78

down:
	@echo "Use integration-down; production control-plane deployment is separately approved" >&2
	@exit 78

integration-up:
	docker compose up -d postgres
	docker compose run --rm api db migrate
	docker compose up -d api orchestrator

integration-down:
	docker compose down -v --remove-orphans

integration-logs:
	docker compose logs -f --tail=200

integration-migrate:
	docker compose run --rm api db migrate

integration-status:
	docker compose run --rm api db status

integration-verify:
	docker compose run --rm api db verify

control-config:
	docker compose -f compose.control-plane.yaml config

install:
	$(PYTHON) -m pip install -e '.[dev]'

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
