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
| L08 review / R12 | second remediation in progress | maximum-available High second audit found no Critical and identified remaining High issues; current fixes require final re-review |

No worker change was dropped. Shared migrations, config, MCP catalog and workflows were
reconciled serially by the integration owner.

## Requirement closure

| ID | Status | Proof / exact gap |
|---|---|---|
| R01 observed devstand | **BLOCKED** | `docs/operations/first-deploy.md`; YC auth restored, but both authorized folders contain zero Compute instances/ALBs, required DNS is absent, and runner has no installed service |
| R02 reproducible baseline | **PASS (local)** | exact Make commands use the checked-in `.venv` when present and pass with compileall; PR CI remains the independent proof |
| R03 PostgreSQL | **PARTIAL** | privileged extension bootstrap followed by owner-scoped clean migration, 12 group roles, 90 strict-SQLSTATE probes and object ownership pass on PostgreSQL 18.4/pgvector 0.8.6; ten distinct service LOGINs pass exact direct/transitive-membership verification locally, while host reboot/private listener proof requires devstand access |
| R04 recovery | **PARTIAL** | encrypted streaming, sanitized adapter environment, strict age-key mode/owner, exact readback, post-restore object/outbox/MCP verification and exact hash/locator/version-bound receipt→checkpoint→operator-gate wiring pass; freshness uses original backup time and high/bulk requires the expected canonical revision; real off-host evidence remains blocked |
| R05 workflows | **PARTIAL** | all five workflow definitions and receipt schema exist; deploy now encodes role/identity/connector/process-kill/reboot/live OAuth/MCP/revocation/listener evidence and nightly fails closed on queue/cadence/recovery/inventory, but external run IDs require absent environments/secrets/backend |
| R06 remote MCP | **PARTIAL** | JWT/JWKS, exact claims, PostgreSQL revocation, RFC9728 metadata, Host/Origin/proxy/body/response/rate/timeout tests and read-only catalog pass; DNS/TLS/issuer/backend/Inspector/ChatGPT connection blocked |
| R07 connectors | **PASS (disposable)** | intake → exact acceptance → single CAS committer/outbox → MCP read → replay → conflict quarantine plus durable outage/restart/eventual delivery; deterministic poison reaches idempotent terminal quarantine; a live row lock reaches bounded SQLSTATE 55P03 and commits after release; events-bot merge/live canary remains blocked |
| R08 Kaggle | **PARTIAL** | four classes, conservative inventory policy, protected denials, leases/fingerprints, exchange and canary receipt contracts pass offline; account inventory and private dataset/notebook lifecycle/cleanup blocked by missing tested adapter/credentials |
| R09 DB operator | **PARTIAL** | apply DML + durable receipt/idempotency now commit in one PostgreSQL transaction; journal failure rolls back, replay is durable, high/bulk impact requires checkpoint, and live disposable apply/replay passes. Production remains empty/remote-undiscoverable until real recovery evidence exists |
| R10 Region Talk | **PASS (bounded R1 scope)** | exact vision imported; donor entries remain explicitly pending; fixture only; pipeline paused and publication disabled; no YDB mutation/cutover |
| R11 PR/merge/deploy | **IN PROGRESS** | [primary PR #1](https://github.com/onedayonemasterpiece/my-data-hub/pull/1) and events-bot PR #478 are open; second-audit remediation must be pushed/green/finally reviewed before merge |
| R12 security review | **PARTIAL** | two maximum-available High reviews completed; the second found no Critical but retained High findings. Current remediation requires a final review before merge |

## Local PostgreSQL evidence

- image digest: `sha256:691673308c99d2161ba298736f3147f1f22d79de2fb7ec93ae9b4afcab870b62`;
- PostgreSQL `18.4`; pgvector `0.8.6`;
- clean migration: 1–10; repeat: zero; upgrade: 9→10 then zero repeat;
- password-free roles: 12; strict role/ownership probes: 90 PASS;
- process-kill: Docker restart count 1 plus PostgreSQL WAL recovery;
- host reboot: not performed/claimed.

## Connector receipt

- batch: `44855fdd-7bb7-5b00-b25d-b04b47aac8c7`;
- acceptance receipt: `f6454369-f9b6-4d32-a1f5-045c006ba5c6`;
- conflict quarantine: `92975ca4-714a-470a-a94f-a3af1b34d35b`;
- semantic outbox: `38e15e90-7f1d-46ec-93ea-a35e5f0a14be`;
- outage/restart eventual batch: `7e20e6f1-d140-51c5-90ef-8cbd05e44ae1`;
- canonical revision after the two latest commits: 4;
- MCP read observed four committed batches in the reusable disposable database.

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

---

# Operational MVP continuation — 2026-08-11

Integration branch: `integration/operational-mvp`

This continuation preserves the Kaggle-only writable PostgreSQL topology. It is
merge-safe implementation evidence, not an operational-completion claim.

| Lane / correction | Requirement area | Integration outcome | Integrated evidence |
|---|---|---|---|
| FINAL-M1 | scheduled connector/probe/restore/rotation controls | merged and corrected | `b6e633f`, `a8b7fed`, `4f4455f`, `9ce8ce2`, `a7524eb` |
| FINAL-BLOGGER | bounded YDB import, checkpoint, cold restore, public MCP projection | merged and corrected | `c895b4b`..`e4d78dd`, `502a042`, `9ce8ce2`, `a7524eb` |
| FINAL-MATRIX | real-provider driver | merged as platform smoke only; mandatory operational matrix remains blocked | `b52e62f`, `09f1e36`, `34085aa` |
| FINAL-EMBED | production closure boundary | merged fail-closed; worker/prerequisite binding hardened; live control/MCP implementation still missing | `89355c7`, `a7524eb` |
| Final integration | durable replay, deadlines, terminal polling, callback-outage regression | merged; full local gates pass | `a7524eb` |

No lane is represented as live acceptance. The machine-readable verdict remains
`MY_DATA_HUB_OPERATIONAL_MVP_BLOCKED` in
`docs/operations/evidence/2026-08-11-operational-mvp/operational-mvp-acceptance-blocked.json`.
Exact open implementation and external gates are listed there and in
`docs/operations/2026-08-11-operational-mvp-progress.md`.
