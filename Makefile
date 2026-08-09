.PHONY: up down logs install migrate status verify validate test lint notebooks bundle backup restore-check

up:
	docker compose up -d postgres
	docker compose run --rm api db migrate
	docker compose up -d api orchestrator

down:
	docker compose down

logs:
	docker compose logs -f --tail=200

install:
	python -m pip install -e '.[dev]'

migrate:
	docker compose run --rm api db migrate

status:
	docker compose run --rm api db status

verify:
	docker compose run --rm api db verify

validate:
	python scripts/validate_repository.py

test: validate
	pytest

lint:
	ruff check .

notebooks:
	python scripts/create_notebooks.py --check

bundle:
	bash scripts/build_release_bundle.sh

backup:
	bash scripts/backup_postgres.sh

restore-check:
	@echo "Set MY_DATA_HUB_RESTORE_DATABASE_URL and MY_DATA_HUB_RESTORE_CONFIRM, then run scripts/restore_postgres.sh explicitly."
