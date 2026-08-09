# Handoff to the code agent

## Goal

Turn this bootstrap into a running single-node production on the devstand,
then migrate Region Talk from YDB with complete evidence. Do not redesign the
project around Region Talk's old backend.

## Phase A — repository and source provenance

1. Push this bootstrap commit to `onedayonemasterpiece/my-data-hub/main`.
2. Import the exact target-vision document from `idea-hub` at commit `0c3fcf7`
   into `docs/source-material/idea-hub/`; update SHA-256 manifest.
3. Pin current source commits of `events-bot-new` and Region Talk.
4. Curate/import donor MCP modules and Region Talk docs/code with a manifest;
   do not copy `.env`, sessions, DB exports or unrelated events-bot runtime.
5. Reconcile any target-vision conflict through ADR, not silent edits.

## Phase B — PostgreSQL deployment

1. Install Docker Compose or native PostgreSQL 18 + pgvector.
2. Create split DB roles and strong credentials.
3. Run `my-data-hub db migrate` on a clean database; migration `0009` creates the idempotent bootstrap rows/views and the command registers the paused Region Talk pipeline.
4. Verify the implemented migration version tracking and idempotency against a live
   PostgreSQL 18 instance; document rollback policy rather than replacing the
   migration runner.
5. Run `python scripts/verify_postgres_bootstrap.py` and then `python scripts/verify_region_talk_migration_flow.py`; archive both JSON outputs.
6. Run API/orchestrator locally; install and verify the supplied systemd/autostart units.
7. Configure nightly backup and execute a full restore drill with the supplied scripts.
8. Record commands, versions, run IDs and evidence in `docs/operations/first-deploy.md`.

## Phase C — complete repositories/adapters

1. Run live PostgreSQL integration tests for the existing MCP repositories and
   semantic command path; fill only gaps exposed by those tests.
2. Port/adapt the OAuth HTTP MCP boundary from `events-bot-new`; keep stdio for
   the local code agent and retain the existing bounded tool catalog.
3. Implement the missing Kaggle launch/status/download adapter using proven shared clients from `events-bot-new`.
4. Port the actual Region Talk processors behind the existing worker contracts;
   keep Candidate/E5, BGE-M3, ImageDiagnostic, FinalVerifier and Writer in
   separate workers.
5. Add artifact signature, size, archive, decompression and secret-scan gates
   around the implemented immutable result intake.
6. Harden the existing short scheduler tick with provider dispatch/reconciliation;
   do not add long polling or a second queue.

## Phase D — YDB inventory/export

1. Create protected read-only migration credentials.
2. Enumerate actual tables/row kinds and code references.
3. Fill `inventory.json`; include counts, key order, caps and semantic owner.
4. Run deterministic bounded export; verify repeat logical hashes.
5. Land all rows in `migration.raw_record`.
6. Remove/rotate temporary credentials after final export.

## Phase E — transform and reconcile

1. Implement versioned row-kind transformers.
2. Preserve all useful source/post/result/review/publication/history data.
3. Repair queue into immutable `queue_seq`, separate priority and explicit
   scheduler lanes.
4. Rebuild current eligibility using one versioned contract.
5. Generate row/identity/queue/candidate reconciliation reports.
6. Reach 100% accounting; unresolved active rows block cutover.

## Phase F — shadow and cutover

1. Port current Region Talk stages behind new task/result contracts.
2. Run at least three representative shadow cycles.
3. Explain every candidate/readiness/review drift.
4. Run private review/render canary.
5. Freeze legacy writers, final delta import, backup, switch scheduler.
6. Keep production publishing off until separate owner approval.
7. Keep YDB read-only through rollback window; do not delete automatically.

## Required final report

- repository commit/PR links;
- source provenance manifest;
- deployment/service status and versions;
- migration inventory and export manifest hashes;
- row accounting and quarantine totals;
- queue before/after integrity;
- shadow run IDs and semantic diffs;
- backup restore evidence;
- MCP tools/scopes and test evidence;
- exact remaining blockers;
- explicit confirmation that production publishing is disabled/enabled and why.
