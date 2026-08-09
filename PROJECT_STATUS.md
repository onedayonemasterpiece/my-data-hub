# Project status

Date: 2026-08-09
Status: `BOOTSTRAP_IMPLEMENTED / RUNTIME_NOT_DEPLOYED`

## Зафиксировано и реализовано

- финальное имя `my-data-hub`; рабочее имя `content-platform` признано историческим alias той же системы;
- каноническое архитектурное основание указано явно и защищено exact-import gate;
- Region Talk назначен первым обязательным migration/cutover контуром, включая накопленные YDB-данные;
- PostgreSQL 18 + pgvector — единственная прикладная canonical СУБД;
- созданы core, analysis, orchestration, sync, Region Talk, migration и Joplin schemas;
- созданы append-only migrations `0001`–`0009`, idempotent bootstrap и live PostgreSQL verification script;
- создан предметный MCP v2 без arbitrary SQL, shell, filesystem и secret-reading tools;
- создан короткий catch-up orchestrator с durable queue, leases, retry и plan-only safety mode;
- созданы typed notebook input/result contracts, immutable HTTP result inbox и базовые Region Talk notebooks;
- создан lossless YDB export/landing/mapping/reconciliation/cutover контур;
- создан handoff для локального devstand, отладки, backup/restore и auto-start;
- production publication и remote MCP по умолчанию выключены.

## Локально доказано

На bootstrap-снимке выполнены:

```text
pytest -q                                      90 passed, 1 skipped (MCP SDK absent locally)
python scripts/validate_repository.py         1025 checks / 0 errors
python -m compileall -q src tests scripts     PASS
python scripts/create_notebooks.py --check    PASS
```

Тесты покрывают contract validation, tamper detection, deterministic notebook generation,
MCP scope/write guards, loopback HTTP/Joplin boundaries, orchestration priorities,
safe-by-default CLI и lossless YDB bundle semantics.

## В CI предусмотрено, но в текущем окружении ещё не исполнено

- установка полного `.[dev]` dependency set;
- Ruff;
- `pglast` parsing всех PostgreSQL migrations;
- настоящий PostgreSQL 18 + pgvector service;
- live fixture cycle landing → replay → quarantine blocker → resolution → reconciliation;
- двукратное применение migrations как idempotency gate;
- database health/bootstrap verification;
- импорт и создание MCP SDK v2 server object.

Причина разделения зафиксирована явно: в текущем рабочем окружении отсутствовали Docker,
`psql`, MCP SDK, `psycopg`, `pglast` и Ruff, а package registry был недоступен. Это не
подменяется заявлением о выполненной server integration.

## Не считается выполненным

- локальный git bootstrap не равен push в `onedayonemasterpiece/my-data-hub/main`;
- точный target-vision source пока не импортирован из Git object `0c3fcf7`;
- schema/bootstrap не равны развёрнутой и обслуживаемой PostgreSQL instance;
- dry-run importer не доказывает полноту реального YDB mapping;
- MCP server object не доказывает production OAuth, reverse-proxy и network hardening;
- notebook contracts не доказывают совместимость ещё не перенесённых Region Talk model adapters;
- unit tests не заменяют shadow comparison со старым Region Talk;
- никакая Telegram/VK production publication не включена.

## Следующий release gate

`R1 — PostgreSQL + MCP + Region Talk migrated shadow`:

1. bootstrap отправлен в `main`, точный source material импортирован и provenance checksum принят;
2. devstand PostgreSQL развёрнут, migrations и live bootstrap checks прошли;
3. backup/readback и полный restore drill подтверждены evidence;
4. MCP доступен локальному агенту с least-privilege scopes;
5. YDB inventory и final export имеют counts, ordered hashes и source attestation;
6. 100% строк учтены, reconciliation report имеет `passed=true`, а `quarantined=0` перед cutover;
7. Region Talk worker adapters перенесены с exact fixtures и contract tests;
8. не менее трёх representative shadow cycles сопоставлены со старым Region Talk;
9. pipeline до review работает на PostgreSQL без production publication;
10. private-channel canary прошёл exact-revision и idempotency gates;
11. rollback rehearsal выполнен;
12. только затем разрешён controlled production cutover.

Подробный delivery receipt: [`docs/14-bootstrap-delivery.md`](docs/14-bootstrap-delivery.md).
