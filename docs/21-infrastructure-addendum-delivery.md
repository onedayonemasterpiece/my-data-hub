# Infrastructure, connectors and operator MCP addendum — delivery record

Status: `R1 REPOSITORY IMPLEMENTED / SAME-HOST RUNTIME PROOF IN PROGRESS`
Date: 2026-08-10

Historical note: ADR-0015 and
[`22-data-scope-and-pipeline-participation.md`](22-data-scope-and-pipeline-participation.md)
were accepted on 2026-08-10 after this validation snapshot. Their schema/runtime work and
new tests are implementation-pending and are not covered by the counts below.

## 1. Scope delivered

This addendum converts the first devstand deployment into an infrastructure-first
implementation plan and adds the missing architectural boundaries for:

- versioned data connectors and producer-side durable delivery;
- supervised canonical PostgreSQL availability;
- remote MCP at `mcp-datahub.kenigevents.ru`;
- Kaggle notebook, dataset and exchange-package control;
- broad but bounded PostgreSQL read/write operator profiles;
- agent-operated Region Talk migration through typed gates;
- PR, post-deploy, nightly, provider-canary and restore workflows.

The R1 repository capabilities were implemented after the historical snapshot below.
They are not yet claimed as verified same-host runtime capabilities: the permanent host
must still produce installation, backup, recovery, auth and restart receipts. ADR-0015
remains design-only.

## 2. Accepted decisions

1. PostgreSQL on the devstand remains the only canonical live head. Kaggle does not host
   or fail over the master database.
2. Push producers submit an idempotent envelope and retain exact bytes in a durable local
   spool during outage. A separate optional Yandex availability controller may start the
   host, but it is outside the orchestrator and never accepts canonical data.
3. Producers do not write shared canonical tables directly. HTTPS intake is the default;
   trusted direct PostgreSQL access is limited to a connector-owned landing schema or
   narrow procedure with the same envelope/receipt semantics.
4. Remote MCP starts read-only. Semantic tools remain the default profile; broad database
   reads, preview/apply DML and migration operation are separate profiles backed by
   separate PostgreSQL roles and explicit safety gates.
5. Kaggle authorization is determined by a local resource registry, not provider names:
   `orchestrator_protected`, `mcp_managed`, `mcp_exchange` and `external_read_only`.
6. Private Kaggle backup generations improve recoverability but never grant permission to
   perform a change. Broad writes require fresh backup/readback and restore-drill evidence.
7. Region Talk remains the first migration workload, but read-only inventory begins only
   after infrastructure, recovery, connector, MCP and provider-control gates pass.

## 3. Canonical documents

- [`15-infrastructure-first-plan.md`](15-infrastructure-first-plan.md)
- [`16-data-connectors.md`](16-data-connectors.md)
- [`17-kaggle-control-plane.md`](17-kaggle-control-plane.md)
- [`18-mcp-operator-and-database-access.md`](18-mcp-operator-and-database-access.md)
- [`19-test-first-rollout.md`](19-test-first-rollout.md)
- [`20-remote-mcp-endpoint.md`](20-remote-mcp-endpoint.md)
- [`operations/first-deploy-template.md`](operations/first-deploy-template.md)
- ADR-0009 through ADR-0014 in [`adr/`](adr/) for this historical delivery; ADR-0015 is
  the later scope/participation addendum.

Machine-readable design contracts:

- [`../schemas/data-connector-envelope.v1.schema.json`](../schemas/data-connector-envelope.v1.schema.json)
- [`../schemas/kaggle-exchange-manifest.v1.schema.json`](../schemas/kaggle-exchange-manifest.v1.schema.json)
- matching examples in [`../examples/contracts/`](../examples/contracts/)

## 4. Repository validation at delivery

The documentation/contracts snapshot passed:

```text
python scripts/validate_repository.py
  1280 checks, 0 errors

pytest -ra
  90 passed, 1 skipped

python -m compileall -q src tests scripts
  PASS

python scripts/create_notebooks.py --check
  no drift

git diff --check
  PASS

relative Markdown link check
  PASS
```

The skipped test requires the MCP Python SDK, and PostgreSQL AST validation in the
repository validator requires `pglast`. The code agent must install the complete `.[dev]`
dependency set and rerun the same gates on CI/devstand; this document does not convert a
locally skipped dependency check into a pass.

## 5. Immediate implementation release

At the time of this historical delivery the combined assignment was called **R1
Infrastructure and Workflow**. The current release plan splits the same pre-migration work
across R1–R4; the dependency order remains:

1. copy the first-deploy template and record observed devstand facts;
2. keep scheduler, publication, remote MCP and operator writes disabled;
3. prove clean/upgrade migrations, split roles and negative grants;
4. prove encrypted backup, provider readback and isolated restore;
5. create PR/post-deploy/nightly/Kaggle/restore workflows with receipts;
6. publish read-only OAuth MCP at the accepted hostname;
7. implement/prove ADR-0015 scope, backfill and policy foundations;
8. prove a synthetic multi-consumer connector including outage, exact replay and conflict
   handling;
9. prove Kaggle protected-resource denial and disposable MCP-managed lifecycle;
10. prove database reader/editor in a disposable schema;
11. then begin Region Talk read-only inventory/export.

The executable handoff is [`12-code-agent-handoff.md`](12-code-agent-handoff.md).
