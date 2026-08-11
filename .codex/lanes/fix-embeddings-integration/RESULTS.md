# Lane fix-embeddings-integration Results

## Status
committed

## Requirement IDs
- R-EMBED/H6

## Branch
agent/operational-mvp/fix-embeddings-integration

## Worktree
/home/dev/.codex/worktrees/my-data-hub/fix-embeddings-integration

## Base SHA
b095f28c845251d1724cdc0e8bd7bfd44eb30549

## Head SHA
Branch tip containing this results file; exact SHA is reported in the lane handoff.

## Files changed
- `.codex/lanes/fix-embeddings-integration/RESULTS.md`
- `src/my_data_hub/embeddings/__init__.py`
- `src/my_data_hub/embeddings/importer.py`
- `notebooks/templates/embedding_workers/bge_m3_runtime.py`
- `notebooks/06-bge-m3-blogger-embedding-worker/worker.ipynb`
- `notebooks/06-bge-m3-blogger-embedding-worker/kernel-metadata.example.json`
- `tests/embeddings/test_postgres_importer.py`

## Commands run
- `.venv/bin/pytest -q tests/embeddings`
- `.venv/bin/ruff check src/my_data_hub/embeddings notebooks/templates/embedding_workers tests/embeddings`
- `.venv/bin/pytest -q`
- `.venv/bin/python scripts/validate_repository.py`
- `.venv/bin/python scripts/create_notebooks.py --check`
- `.venv/bin/python -m compileall -q src tests`

## Tests / verification
- Embedding tests: 26 passed.
- Full suite: passed with 2 expected environment-gated skips.
- Repository validator: 2,782 checks, zero errors/notes.
- Notebook generator: no drift.
- Ruff and compileall: passed.
- Fake-PostgreSQL tests prove E5/BGE table routing, atomic job/revision/outbox flow,
  late-result staling without vector insertion, exact replay without writes, and
  immutable-result conflict rollback before revision/outbox changes.

## Risks
- The importer binds model UUIDs to the append-only registry identities seeded by
  migration 0012; future approved models require an explicit code/schema update.
- No disposable PostgreSQL run was requested/enabled; the transaction protocol is
  covered with a stateful fake DB and remains integration-capable through the normal
  psycopg connection boundary.

## Merge notes
- Cherry-pick the lane commit as one unit.
- The two generated BGE notebook outputs are intentional and were explicitly
  authorized after template drift was observed; `scripts/create_notebooks.py` was
  not edited.
