# R1 integration report

Generated: 2026-08-09 UTC  
Base: `0b6b7311081bdfecdd4f3004e5d6842a42f64253`  
Integration branch: `integration/r1-infrastructure-workflow`

This report distinguishes implemented code, disposable local proof, and external
deployment/provider proof. `PASS` never means "a configuration file exists" when the
requirement asks for runtime evidence.

## Lane reconciliation

| Lane | Integration outcome | Evidence |
|---|---|---|
| L01 provenance / R10 | merged | exact source commit and SHA-256; `.codex/lanes/L01-provenance/RESULTS.md` |
| L02 recovery / R04 | merged | encrypted/off-host/restore implementation and tests; no live off-host receipt |
| L03 connectors / R07 | merged | intake, PostgreSQL repository, canonical committer, spool and live disposable flow |
| L03b events-bot producer / R07 | separate PR | [events-bot-new PR #478](https://github.com/onedayonemasterpiece/events-bot-new/pull/478), default-off |
| L04 Kaggle / R08 | merged | provider-neutral policy/contracts; concrete provider adapter blocked |
| L05 operator / R09 | merged | bounded engine plus live PostgreSQL disposable canary |
| L06 OAuth / R06 | merged | admission primitives plus JWKS/revocation/RFC9728 production wiring |
| L07 integration / R01–R05/R11 | in progress | workflows, central migration/grants/API/MCP/deploy evidence |
| L08 review / R12 | remediation in progress | maximum-available High audit found critical issues; fixes are in this branch and require re-review |

No worker change was dropped. Shared migrations, config, MCP catalog and workflows were
reconciled serially by the integration owner.

## Requirement closure

| ID | Status | Proof / exact gap |
|---|---|---|
| R01 observed devstand | **BLOCKED** | `docs/operations/first-deploy.md`; YC auth restored, but both authorized folders contain zero Compute instances/ALBs, required DNS is absent, and runner has no installed service |
| R02 reproducible baseline | **PASS (local)** | exact Make commands use the checked-in `.venv` when present and pass with compileall; PR CI remains the independent proof |
| R03 PostgreSQL | **PARTIAL** | privileged extension bootstrap followed by owner-scoped clean migration, 12 group roles, 66 strict-SQLSTATE probes and object ownership pass on PostgreSQL 18.4/pgvector 0.8.6; restricted LOGIN creation and host reboot/private listener proof require devstand secrets/access |
| R04 recovery | **PARTIAL** | encrypted streaming, sanitized adapter environment, strict age-key mode/owner, exact readback, post-restore object/outbox/MCP verification, durable `recovery.evidence` recorder/provider and receipt tests pass; real off-host artifact/readback/restore evidence remains blocked |
| R05 workflows | **PARTIAL** | all five workflow definitions and receipt schema exist; PR CI can run without secrets, but deploy/nightly/restore/provider run IDs require absent environments/secrets/runner/backend |
| R06 remote MCP | **PARTIAL** | JWT/JWKS, exact claims, PostgreSQL revocation, RFC9728 metadata, Host/Origin/proxy/body/response/rate/timeout tests and read-only catalog pass; DNS/TLS/issuer/backend/Inspector/ChatGPT connection blocked |
| R07 connectors | **PASS (disposable)** | intake → exact acceptance → single CAS committer/outbox → MCP read → replay → conflict quarantine plus durable outage/restart/eventual delivery; events-bot producer shape now has an exact target normalizer test, while merge/live credential/canary remain blocked |
| R08 Kaggle | **PARTIAL** | four classes, conservative inventory policy, protected denials, leases/fingerprints, exchange and canary receipt contracts pass offline; account inventory and private dataset/notebook lifecycle/cleanup blocked by missing tested adapter/credentials |
| R09 DB operator | **PARTIAL** | apply DML + durable receipt/idempotency now commit in one PostgreSQL transaction; journal failure rolls back, replay is durable, high/bulk impact requires checkpoint, and live disposable apply/replay passes. Production remains empty/remote-undiscoverable until real recovery evidence exists |
| R10 Region Talk | **PASS (bounded R1 scope)** | exact vision imported; donor entries remain explicitly pending; fixture only; pipeline paused and publication disabled; no YDB mutation/cutover |
| R11 PR/merge/deploy | **IN PROGRESS** | [primary PR #1](https://github.com/onedayonemasterpiece/my-data-hub/pull/1) and events-bot PR #478 are open; security remediation must be pushed/green/re-reviewed before merge |
| R12 security review | **PARTIAL** | maximum-available High checklist review completed with a not-ready verdict; critical/high findings were remediated locally and a second review is mandatory before merge |

## Local PostgreSQL evidence

- image digest: `sha256:691673308c99d2161ba298736f3147f1f22d79de2fb7ec93ae9b4afcab870b62`;
- PostgreSQL `18.4`; pgvector `0.8.6`;
- clean migration: 1–10; repeat: zero; upgrade: 9→10 then zero repeat;
- password-free roles: 12; strict role/ownership probes: 66 PASS;
- process-kill: Docker restart count 1 plus PostgreSQL WAL recovery;
- host reboot: not performed/claimed.

## Connector receipt

- batch: `04c6230c-647e-5b9c-aef2-65329a97444d`;
- acceptance receipt: `aebded4b-5a17-4672-9112-ede0b3912e78`;
- conflict quarantine: `d7fda5b7-584a-426e-9aa9-f363331b55c0`;
- semantic outbox: `3e97a75c-fe50-4789-b141-a47740610cf3`;
- outage/restart eventual batch: `b251b189-2747-50ad-a06e-809d7455d535`;
- canonical revision after two commits: 2;
- MCP read observed two committed batches.

## Operator receipt

- disposable schema: `operator_disposable_r1`;
- role: `mdh_mcp_editor`;
- preview/apply affected rows: 1/1;
- apply receipt SHA-256:
  `bdb0ff6f21efc2a2dc37b23f4c006d0966a5d41056c3e9a27fe495e3a808fb4c`;
- replay: idempotent;
- PostgreSQL DDL denial: PASS;
- cleanup: schema dropped and canary grants revoked;
- apply receipt/idempotency insertion occurred in the same transaction as DML;
- the freshness object was synthetic and explicitly cannot open a production gate.

## Exact external blockers and verification commands

### Devstand / remote MCP

Required: existing devstand resource/host identity, pinned SSH host key, protected SSH
credential, OAuth issuer/JWKS/client configuration, and an owner decision if new billable
compute/ALB/certificate resources must instead be created.

```bash
yc compute instance get "$DEVSTAND_INSTANCE_ID" --folder-id "$FOLDER_ID"
gh workflow run devstand-deploy.yml -f commit="$MERGE_SHA"
curl -i https://mcp-datahub.kenigevents.ru/.well-known/oauth-protected-resource/mcp
```

### Recovery

Required: private off-host upload/readback adapters and credentials, age recipient and
isolated PostgreSQL 18 recovery runner/target.

```bash
gh workflow run restore-drill.yml
gh run watch "$RUN_ID" --exit-status
```

### Kaggle

Required: a separately reviewed concrete adapter providing private dataset and notebook
create/readback/delete primitives plus `KAGGLE_CANARY_USERNAME` and
`KAGGLE_CANARY_KEY` in the protected environment.

```bash
gh workflow run kaggle-canary.yml
gh run watch "$RUN_ID" --exit-status
```

### Events-bot producer

Required: merge both repository PRs, enable the paused connector/product registry entry,
inject a dedicated connector token and exact deployed events-bot SHA, prove `/data`
spool restart/capacity/backup, then run accept/replay/outage/conflict/auth canary.

## Non-negotiable gates

- `MY_DATA_HUB_SCHEDULER_ENABLED=false`;
- `MY_DATA_HUB_PRODUCTION_PUBLISH_ENABLED=false`;
- `MY_DATA_HUB_MCP_WRITE_ENABLED=false`;
- remote catalog contains semantic/status reads only;
- Kaggle protected resources are status-only;
- Region Talk and `publication_dispatch` remain paused/disabled;
- no production dump, credential, OAuth token, provider key, or private source data is in git.
