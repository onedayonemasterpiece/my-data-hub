# my-data-hub

`my-data-hub` — PostgreSQL-first ядро личной контентной базы, предметного и
операторского MCP, управляемых конвейеров и data connectors. Один общий каталог хранит
авторов, аккаунты, материалы, provenance и результаты обработок; проекты вроде Region
Talk подключаются отношениями и собственными проекциями, а не создают отдельные базы.

## Статус

Репозиторий содержит реализационный bootstrap. Владелец сообщил, что проект развёрнут на
devstand; фактический runtime пока должен пройти infrastructure-first verification и
получить deployment/backup/restore/autotest receipts.

Реализованный bootstrap включает:

- PostgreSQL 18 + pgvector как единственную каноническую server-side СУБД;
- общий каталог, FTS/vector evidence, projects и provenance;
- durable orchestration: pipelines, stages, work, runs, attempts, events и leases;
- semantic changesets, transactional outbox, receipts и conflict quarantine;
- bounded semantic MCP v0.1;
- typed notebook input/result contracts и Region Talk worker lanes;
- lossless YDB landing/mapping/reconciliation/cutover scaffold;
- Joplin integration boundary;
- Docker/systemd/backup/CI scaffold.

Принятое документальное дополнение, ещё требующее реализации, добавляет:

- infrastructure/test-first порядок вместо немедленной тяжёлой миграции;
- versioned data connector intake и первый продукт `events-bot.daily-statistics.v1`;
- remote MCP `https://mcp-datahub.kenigevents.ru/mcp` через TLS/OAuth;
- Kaggle inventory, protected/MCP-managed/exchange control classes;
- широкий bounded database reader и preview/apply DML под отдельными roles;
- agent-operated Region Talk migration через typed gates;
- nightly/provider/restore/autotest workflows.

Production publication выключена. Region Talk pipeline остаётся `paused` до evidence
и миграционных gates.

## Каноническое имя и источник

Финальное имя — **`my-data-hub`**. `content-platform` было черновым именем той же
системы и сохраняется только в provenance исходного исследования.

Каноническое целевое видение:

```text
onedayonemasterpiece/idea-hub
ideas/portfolio.inbox/idea-20260809-content-platform-current-design.md
source commit: 0c3fcf71b2ee8ba8afa49624bef4b779873802f7
source SHA-256: c7efb28231223caa6fd02fcc001a38e0f16bcc3fa4c4cd53e744721b2eac0852
```

Точный файл импортирован и проверен побайтно; это не пересказ. Состояние provenance:
[`docs/source-material/source-manifest.yaml`](docs/source-material/source-manifest.yaml).

## Архитектурные решения дополнения

- Canonical PostgreSQL supervised/always-on; Kaggle не является master DB/failover.
- При недоступности devstand push-connectors сохраняют exact batch в durable local spool
  и повторяют тот же idempotency key.
- Default MCP остаётся semantic; operator MCP — отдельный профиль/роль/процесс с
  preview/apply, limits, backup и audit gates.
- Orchestrator-owned Kaggle resources доступны remote MCP только как status.
- MCP-owned private notebooks/datasets могут управляться через проверенный provider
  lifecycle.
- Private `mcp_exchange` служит для передачи файлов/кода/документов, но не является
  canonical storage.
- Region Talk migration может управляться агентом только через typed accounting,
  quarantine, shadow, backup, cutover и rollback gates.

## С чего начать на devstand

Не начинать с полного YDB-переноса. Последовательность:

```text
зафиксировать deployment facts и выключенные gates
→ проверить clean/upgrade migrations и split roles
→ выполнить backup + off-host readback + isolated restore
→ настроить PR/post-deploy/nightly/provider workflows
→ поднять remote read-only MCP
→ доказать synthetic connector
→ доказать Kaggle protected/MCP-managed canary
→ доказать DB operator в disposable schema
→ начать Region Talk inventory/export
```

Полный план: [`docs/15-infrastructure-first-plan.md`](docs/15-infrastructure-first-plan.md).

## Локальный запуск через Docker

```bash
cp .env.example .env
# задать POSTGRES_PASSWORD и MY_DATA_HUB_DATABASE_URL для docker network
make up
make verify
make test
```

`make up` поднимает PostgreSQL, применяет append-only migrations, регистрирует paused
Region Talk pipeline и запускает API/plan-only orchestrator.

После запуска:

- liveness: `http://127.0.0.1:8080/health/live`
- readiness: `http://127.0.0.1:8080/health/ready`
- MCP stdio: `my-data-hub mcp serve --transport stdio`
- PostgreSQL публикуется только на loopback host interface.

## Region Talk

Region Talk — первый обязательный migration workload и первый полный перенос
накопленных данных:

```text
YDB read-only inventory/export
→ immutable JSONL + manifest + hashes
→ migration.raw_record landing
→ explicit mapping / normalization / deduplication
→ Region Talk projections over shared catalog
→ reconciliation
→ shadow/canary
→ controlled cutover/rollback
```

Каждая исходная строка получает disposition: `normalized`, `deduplicated`,
`intentionally_excluded`, `retained_raw` или `quarantined`. Неразобранные строки,
manifest mismatch или quarantine блокируют cutover.

Read-only exporter получает фактическую source table только через защищённую переменную
`MY_DATA_HUB_REGION_TALK_YDB_TABLE`; значение не фиксируется и не угадывается в публичном
репозитории.

## Основные документы

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
12. [`docs/11-deployment.md`](docs/11-deployment.md)
13. [`docs/12-code-agent-handoff.md`](docs/12-code-agent-handoff.md)
14. [`docs/13-external-references.md`](docs/13-external-references.md)
15. [`docs/14-bootstrap-delivery.md`](docs/14-bootstrap-delivery.md)
16. [`docs/15-infrastructure-first-plan.md`](docs/15-infrastructure-first-plan.md)
17. [`docs/16-data-connectors.md`](docs/16-data-connectors.md)
18. [`docs/17-kaggle-control-plane.md`](docs/17-kaggle-control-plane.md)
19. [`docs/18-mcp-operator-and-database-access.md`](docs/18-mcp-operator-and-database-access.md)
20. [`docs/19-test-first-rollout.md`](docs/19-test-first-rollout.md)
21. [`docs/20-remote-mcp-endpoint.md`](docs/20-remote-mcp-endpoint.md)
22. [`docs/21-infrastructure-addendum-delivery.md`](docs/21-infrastructure-addendum-delivery.md)
23. [`docs/operations/first-deploy-template.md`](docs/operations/first-deploy-template.md)
24. [`docs/migrations/region-talk/README.md`](docs/migrations/region-talk/README.md)
25. [`BOOTSTRAP_VALIDATION.md`](BOOTSTRAP_VALIDATION.md)

## Неподлежащие ослаблению инварианты

- PostgreSQL — единственная canonical server-side СУБД и supervised live head.
- Kaggle backup/notebook/dataset не становится master DB или canonical pointer.
- Business write и required outbox фиксируются одной SQL-транзакцией.
- Notebook worker возвращает typed immutable result; canonical apply делает локальный
  committer.
- Connector batch принимается идемпотентно и отделён от canonical application.
- External side effect разрешён только после canonical commit по exact approved revision.
- Default MCP не предоставляет generic SQL; operator SQL — отдельный restricted profile,
  без owner/superuser/DDL и с preview/apply/backup/audit gates.
- Orchestrator-protected Kaggle resources не мутируются remote MCP.
- Неизвестная YDB строка не отбрасывается и не считается мигрированной.
- Joplin используется через supported API, а не внутреннюю SQLite.
- GitHub хранит код/contracts/receipts, но не production data/secrets.
