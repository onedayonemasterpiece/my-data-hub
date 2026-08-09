# Bootstrap validation

Date: **2026-08-09**
Scope: repository/bootstrap validation only; **not** a deployment or completed migration.

## Passed in the build environment

```text
python scripts/validate_repository.py
  PASS — 1025 structural/schema/layout/security checks
  NOTE — pglast unavailable, therefore PostgreSQL AST parsing was skipped locally

python scripts/create_notebooks.py --check
  PASS — 0 generated notebook drift

pytest -o addopts='' -q
  PASS — 90 passed; 1 MCP SDK contract module skipped because the SDK is unavailable locally

python -m compileall -q src tests scripts
  PASS
```

The test environment used Python 3.13.5. The project runtime contract remains Python
3.12+ and CI installs the declared development dependencies on Python 3.12.

## Deliberately not claimed

The build environment did not contain Docker, `psql`, `psycopg`, MCP SDK, `pglast` or
`ruff`, and it had no YDB/Kaggle/Joplin credentials. Consequently this local validation
does **not** prove:

- creation of a live PostgreSQL 18 database or pgvector indexes;
- clean migration replay and idempotent pipeline registration against PostgreSQL;
- live MCP stdio/Streamable HTTP behavior;
- complete YDB inventory/export/import or semantic mapping;
- Kaggle notebook execution and artifact readback;
- Joplin Desktop Data API integration;
- systemd/Compose autostart, backup or restore;
- Region Talk shadow equivalence or production cutover.

Those checks are mandatory in `docs/12-code-agent-handoff.md` and the migration acceptance
package. Production publication remains disabled.
