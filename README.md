# my-data-hub

`my-data-hub` — PostgreSQL-first ядро личной контентной базы, предметного MCP
и управляемых конвейеров данных. Один общий каталог хранит авторов, аккаунты,
материалы, provenance и результаты обработок; проекты вроде Region Talk
подключаются к этим объектам отношениями и собственными проекциями, а не
создают дублирующие базы.

## Статус

Текущий репозиторий — **реализационный bootstrap, ещё не развёрнутый runtime**.
В нём зафиксированы и связаны кодом:

- PostgreSQL как единственная каноническая серверная СУБД;
- общий каталог, FTS/pgvector evidence, проекты и provenance;
- durable orchestration: pipeline, stage, work item, run, attempt, event и lease;
- semantic changesets, transactional outbox, commit receipts и conflict quarantine;
- bounded MCP без arbitrary SQL, shell и прямого доступа к секретам;
- typed notebook input/result contracts и изолированные Region Talk lanes;
- lossless YDB landing, нормализация, reconciliation, cutover и rollback;
- HTTP inbox для immutable notebook results: worker не пишет canonical tables;
- будущая двусторонняя Joplin-интеграция через официальный Data API, без чтения
  внутренней SQLite-базы Joplin;
- Docker, systemd, backup/restore и CI/handoff каркас;
- live PostgreSQL CI-gate, который доказывает landing, idempotent replay, quarantine blocker и успешный reconciliation после явного resolution.

Production publication по умолчанию выключена. Pipeline зарегистрирован в
состоянии `paused`; включение требует миграционных и shadow gates.

## Каноническое имя и архитектурный источник

Финальное имя — **`my-data-hub`**. `content-platform` было черновым именем той
же системы и сохраняется только в provenance исходного документа.

Каноническое целевое видение:

```text
onedayonemasterpiece/idea-hub
ideas/portfolio.inbox/idea-20260809-content-platform-current-design.md
source commit: 0c3fcf7
```

Точный импорт исходника пока помечен `pending_authenticated_import`, а не
подменён пересказом. Текущее состояние provenance находится в
[`docs/source-material/source-manifest.yaml`](docs/source-material/source-manifest.yaml).

## Первый полный migration contour: Region Talk

Region Talk — не «пример интеграции», а первый обязательный workload и первый
полный перенос накопленных данных:

```text
YDB read-only inventory + consistent export
  → immutable JSONL bundle + manifest + hashes
  → lossless migration.raw_record landing
  → explicit mapping / normalization / deduplication
  → Region Talk projections over the shared catalog
  → per-kind and per-identity reconciliation
  → shadow pipeline with side effects disabled
  → exact-revision private canary
  → controlled write freeze and cutover
```

Каждая исходная строка должна получить один disposition:
`normalized`, `deduplicated`, `intentionally_excluded`, `retained_raw` или
`quarantined`. SQL-представление accounting отдельно показывает
`undispositioned`, `quarantined`, `fully_accounted` и `cutover_ready`; ненулевой карантин,
неразобранные строки или расхождение с manifest блокируют cutover.

Старые YDB/SQLite решения Region Talk используются только как доноры
поведения, данных, fixtures и operational evidence. Ни YDB, ни SQLite не
становятся частью новой canonical architecture.

## Локальный запуск через Docker

```bash
cp .env.example .env
# задать POSTGRES_PASSWORD и MY_DATA_HUB_DATABASE_URL для docker network
make up
make verify
make test
```

`make up` поднимает PostgreSQL, применяет append-only migrations, регистрирует
Region Talk pipeline и затем запускает API и plan-only orchestrator.

После запуска:

- API liveness: `http://127.0.0.1:8080/health/live`
- API readiness: `http://127.0.0.1:8080/health/ready`
- MCP stdio: `my-data-hub mcp serve --transport stdio`
- PostgreSQL публикуется только на loopback host interface.

Порядок установки на devstand, smoke/shadow проверки и systemd auto-start:
[`docs/12-code-agent-handoff.md`](docs/12-code-agent-handoff.md).

## Работа с YDB export

Реальный export выполняется только с read-only credentials в защищённом
migration environment:

```bash
my-data-hub region-talk export-ydb \
  --endpoint "$YDB_ENDPOINT" \
  --database "$YDB_DATABASE" \
  --table "$MY_DATA_HUB_REGION_TALK_YDB_TABLE" \
  --output-root /private/region-talk-export

my-data-hub region-talk validate-ydb-export \
  --manifest /private/region-talk-export/<bundle>/manifest.json

# По умолчанию — только validate/dry-run
my-data-hub region-talk import-ydb-export \
  --manifest /private/region-talk-export/<bundle>/manifest.json

# После backup и review
my-data-hub region-talk import-ydb-export \
  --manifest /private/region-talk-export/<bundle>/manifest.json \
  --apply
```

Экспортированные данные, Telegram sessions, Joplin token и credentials в
публичный GitHub не помещаются.

## Документы

1. [`docs/00-source-of-truth.md`](docs/00-source-of-truth.md)
2. [`docs/01-project-charter.md`](docs/01-project-charter.md)
3. [`docs/02-target-architecture.md`](docs/02-target-architecture.md)
4. [`docs/03-data-model.md`](docs/03-data-model.md)
5. [`docs/04-orchestrator.md`](docs/04-orchestrator.md)
6. [`docs/05-mcp.md`](docs/05-mcp.md)
7. [`docs/06-notebooks.md`](docs/06-notebooks.md)
8. [`docs/07-joplin-integration.md`](docs/07-joplin-integration.md)
9. [`docs/08-security.md`](docs/08-security.md)
10. [`docs/09-observability.md`](docs/09-observability.md)
11. [`docs/10-release-plan.md`](docs/10-release-plan.md)
12. [`docs/migrations/region-talk/README.md`](docs/migrations/region-talk/README.md)
13. [`docs/12-code-agent-handoff.md`](docs/12-code-agent-handoff.md)
14. [`docs/13-external-references.md`](docs/13-external-references.md)
15. [`BOOTSTRAP_VALIDATION.md`](BOOTSTRAP_VALIDATION.md)
16. [`docs/14-bootstrap-delivery.md`](docs/14-bootstrap-delivery.md)

## Неподлежащие ослаблению инварианты

- PostgreSQL — единственная каноническая серверная СУБД.
- Business write и его semantic outbox operations фиксируются одной SQL-транзакцией.
- Notebook/Kaggle worker возвращает typed immutable result; canonical apply делает
  только локальный reconciler/committer.
- External side effect разрешён только после canonical commit, по exact approved
  revision и с idempotency key.
- MCP предоставляет предметные команды и bounded reads, но не arbitrary write-SQL,
  shell, filesystem browser или secret reader.
- Неизвестная строка YDB не отбрасывается и не считается автоматически мигрированной.
- Joplin синхронизируется через Data API и explicit note mapping; его внутреннее
  хранилище не читается напрямую.
- GitHub хранит код, contracts, coordination manifests и receipts, но не production data.
