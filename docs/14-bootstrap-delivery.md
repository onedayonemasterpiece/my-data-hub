# Bootstrap delivery receipt

Date: 2026-08-09
Repository: `onedayonemasterpiece/my-data-hub`
Delivery state: `implementation bootstrap complete; environment deployment pending`

## 1. Что является источником архитектуры

Финальное имя системы — `my-data-hub`. `content-platform` — раннее имя той же
системы, сохранившееся в имени исходного документа.

Canonical target vision:

```text
repository: onedayonemasterpiece/idea-hub
path: ideas/portfolio.inbox/idea-20260809-content-platform-current-design.md
commit: 0c3fcf7
role: canonical_target_vision
```

Bootstrap не воспроизводит предполагаемое содержимое исходника по памяти. Его exact bytes
должен импортировать `scripts/import_source_material.py` из указанного Git object; SHA-256
затем фиксируется в `docs/source-material/source-manifest.yaml`. До этого manifest честно
остаётся `pending_authenticated_import`.

## 2. Что уже создано

### PostgreSQL platform core

- PostgreSQL 18 + pgvector runtime profile;
- append-only migrations и checksum-aware runner;
- shared content/actor/account/project catalog;
- provenance, analysis results, embeddings и model identity;
- durable pipelines, stages, work items, runs, attempts, dependencies, leases и events;
- semantic commands, transactional outbox, changesets, receipts, conflicts и checkpoints;
- Region Talk projections, review decisions, exact revision fingerprints, publication plans
  и fail-closed dispatcher state;
- Joplin mapping, revision and conflict tables;
- idempotent bootstrap and live database verification script.

### Region Talk as first migration workload

Region Talk реализован не как demo namespace, а как первый полный перенос:

```text
read-only YDB snapshot export
→ immutable JSONL files + manifest + ordered hashes
→ lossless migration.raw_record landing
→ versioned mapping and identity normalization
→ shared catalog + Region Talk projections
→ row/identity reconciliation
→ shadow processing
→ private canary
→ controlled cutover
```

Migration accounting формализует обязательное равенство:

```text
raw = normalized + deduplicated + intentionally_excluded + retained_raw + quarantined
```

`undispositioned > 0`, `quarantined > 0` или расхождение raw/manifest блокируют
cutover. Unknown row kinds сохраняются в raw landing до явного disposition; они не теряются
и не объявляются успешно мигрированными автоматически. `fully_accounted` означает
lossless accounting, а отдельный `cutover_ready` требует ещё и нулевого карантина.

### MCP

Создан bounded semantic MCP SDK v2 server со следующей фактической поверхностью:

```text
hub.health
hub.project.list
hub.content.search
hub.content.get
hub.trace.get
region_talk.queue.summary
region_talk.plan.preview
region_talk.migration.status
region_talk.migration.accounting
region_talk.work.enqueue           # gated write
hub.command.submit                 # gated write
```

Инварианты:

- stdio — default local-agent transport;
- write tools требуют scope и `MY_DATA_HUB_MCP_WRITE_ENABLED=true`;
- development HTTP требует bearer token, loopback bind, Host/Origin/body limits;
- arbitrary SQL, shell, filesystem browser и secret reader отсутствуют;
- production remote HTTP остаётся выключенным до OAuth donor-port и integration evidence.

### Orchestrator and notebooks

- short-tick orchestration без долгого polling;
- durable work ownership, lease/retry/terminal outcomes;
- strict pipeline registry and stage contracts;
- deterministic notebook generation;
- отдельные Region Talk lanes для Candidate, E5, BGE-M3, ImageDiagnostic,
  FinalVerifier, SourceProfile, Writer и migration reconciliation;
- immutable typed worker result bundle;
- authenticated bounded HTTP inbox;
- local reconciler remains the only canonical apply boundary.

Notebook adapters, зависящие от ещё не импортированного donor implementation, fail closed
с `PROCESSOR_ADAPTER_NOT_PORTED`; skeleton не выдаёт фиктивный model result.

### Joplin boundary

Заложена будущая интеграция через официальный desktop Data API/plugin boundary:

- desktop bridge по умолчанию loopback-only;
- explicit notebook/note mappings and revision fingerprints;
- no direct access to Joplin internal SQLite;
- Android получает данные обычным Joplin sync path;
- удалённый агент в будущем подключается к authenticated hub API/MCP, а не к exposed
  desktop token.

### Operations and delivery

- Docker image and Compose;
- systemd API/orchestrator units;
- backup and restore scripts/runbooks;
- secrets and observability runbooks;
- code-agent deployment/cutover handoff;
- CI contract and real PostgreSQL integration jobs.
- live Region Talk migration fixture gate: lossless landing, exact replay, quarantine block and explicit resolution to a validated reconciliation report.

## 3. Проверки bootstrap

Локально выполнено:

| Проверка | Результат |
|---|---:|
| `pytest -q` | 90 passed, 1 skipped (MCP SDK absent locally) |
| repository validator | 1025 checks, 0 errors |
| Python compileall | PASS |
| deterministic notebook drift check | PASS |

Repository validator проверяет структуру, source authority, migrations, schemas, stage
registry, notebook contract alignment, migration naming, Compose/systemd/CLI coherence,
fail-closed publication flags, CI PostgreSQL profile and live verification commands.

## 4. Что намеренно не объявляется завершённым

- push/merge в удалённый GitHub;
- exact authenticated source import;
- настоящий devstand PostgreSQL deployment;
- выполнение migrations на PostgreSQL server в текущем окружении;
- live MCP SDK process and production OAuth transport;
- реальный YDB inventory/export/import;
- port exact Region Talk workers, prompts, fixtures and provider clients;
- shadow cycles, private canary, cutover or rollback drill;
- Joplin desktop installation/bridge test;
- production publication.

## 5. Следующая исполняемая последовательность

1. Push bootstrap to `main` and run CI.
2. Import exact target vision and donor provenance.
3. Deploy PostgreSQL on devstand; apply migrations twice and run live verification.
4. Execute backup/readback/restore drill before migration data is admitted.
5. Start local stdio MCP and plan-only orchestrator under systemd.
6. Inventory and export YDB with read-only credentials.
7. Land raw records; implement row-kind transformers; reach complete accounting and zero quarantine.
8. Port Region Talk adapters using exact fixtures and source commit receipts.
9. Run shadow cycles with all external side effects disabled.
10. Run private exact-revision canary, rollback rehearsal and only then cut over.

The environment-specific completion contract is
[`docs/12-code-agent-handoff.md`](12-code-agent-handoff.md).
