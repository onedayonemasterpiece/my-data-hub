# Notebook workers

This contract covers ordinary compute notebooks. ADR-0016 defines one explicit exception:
the fenced Kaggle master Notebook is the PostgreSQL database runtime, not a worker result
producer.

## Принцип

Notebook — вычислительный worker, а не владелец state. Он не получает
PostgreSQL credentials и не выполняет прямые writes.

## Общий input

`task-envelope.v1` фиксирует:

- task/run/pipeline/stage identity;
- workload/project;
- code and contract version;
- idempotency key;
- input payload;
- input artifact locators + SHA-256;
- deadline and resource budget.

## Общий output

`task-result.v1` содержит:

- exact input identity/hash;
- terminal status;
- output payload;
- artifacts с hashes;
- metrics/provider usage;
- model and code versions;
- structured error/zero-result reason.

`result_id` immutable. Повторный upload допустим только с тем же hash.

## Region Talk notebooks

| Notebook | Роль | Не делает |
|---|---|---|
| `source_discovery.ipynb` | находит/проверяет новые источники и exact links | не меняет queue order |
| `candidate_e5.ipynb` | cheap text gate/embedding | не загружает BGE-M3 |
| `bge_m3.ipynb` | independent semantic enrichment | не загружает E5 production model |
| `image_diagnostic.ipynb` | album/media scoring | не принимает final publication decision |
| `writer.ipynb` | формирует versioned review copy | не одобряет и не публикует |

Bootstrap notebooks deliberately produce `blocked/NOT_IMPLEMENTED` outside
explicit dry-run, чтобы пустой stage не выглядел успешным production result.

## Required evidence bundle

```text
run-manifest.json
stdout.log
stderr.log
events.jsonl
resource-samples.jsonl
provider-usage.jsonl
result.json
artifact-manifest.json
exception.json        # only on failure
files.sha256
```

Orchestrator проверяет schema, signatures/checksums, secret scan и exact task
identity до DB transaction.

## Kaggle integration

- kernels private и CPU-only по умолчанию;
- model revisions pinned;
- E5 и BGE не co-located в одном production notebook;
- task input/result передаются как immutable artifacts;
- notebook output не становится canonical state;
- credentials для providers минимальны и не возвращаются в logs/result;
- terminal output readback обязателен до task success.
