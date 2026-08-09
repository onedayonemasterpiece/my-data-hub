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
| L08 review / R12 | in progress | separate maximum-available reviewer audit |

No worker change was dropped. Shared migrations, config, MCP catalog and workflows were
reconciled serially by the integration owner.

## Requirement closure

| ID | Status | Proof / exact gap |
|---|---|---|
| R01 observed devstand | **BLOCKED** | `docs/operations/first-deploy.md`; YC auth restored, but both authorized folders contain zero Compute instances/ALBs, required DNS is absent, and runner has no installed service |
| R02 reproducible baseline | **PASS (local)** | `make validate`, `make test`, `make lint`, `make notebooks`, compileall all pass; 186 tests and 1433 repository checks at the last documentation run |
| R03 PostgreSQL | **PARTIAL** | clean/repeat/9→10 upgrade, 10 roles, 39 role probes, bootstrap and process-kill recovery pass on PostgreSQL 18.4/pgvector 0.8.6; host reboot/private devstand listener proof blocked |
| R04 recovery | **PARTIAL** | encrypted streaming, exact independent readback contract, fresh isolated restore and receipt implementation/tests pass; real off-host credential, live encrypted artifact/readback SHA and isolated restore receipt blocked |
| R05 workflows | **PARTIAL** | all five workflow definitions and receipt schema exist; PR CI can run without secrets, but deploy/nightly/restore/provider run IDs require absent environments/secrets/runner/backend |
| R06 remote MCP | **PARTIAL** | JWT/JWKS, exact claims, PostgreSQL revocation, RFC9728 metadata, Host/Origin/proxy/body/response/rate/timeout tests and read-only catalog pass; DNS/TLS/issuer/backend/Inspector/ChatGPT connection blocked |
| R07 connectors | **PASS (disposable)** | intake → exact acceptance → commit/outbox → MCP read → replay → conflict quarantine plus durable outage/restart/eventual delivery; events-bot producer PR exists but live credential/canary blocked |
| R08 Kaggle | **PARTIAL** | four classes, conservative inventory policy, protected denials, leases/fingerprints, exchange and canary receipt contracts pass offline; account inventory and private dataset/notebook lifecycle/cleanup blocked by missing tested adapter/credentials |
| R09 DB operator | **PASS (R1 disposable only)** | AST/allowlist/limits/preview/signed binding/apply/backup gate/idempotency tests plus live PostgreSQL read/preview/apply/replay/DDL-denial/cleanup; production-empty and remote undiscoverable |
| R10 Region Talk | **PASS (bounded R1 scope)** | exact vision imported; donor entries remain explicitly pending; fixture only; pipeline paused and publication disabled; no YDB mutation/cutover |
| R11 PR/merge/deploy | **IN PROGRESS** | primary PR/check/merge not yet created at this report revision; events-bot PR #478 open |
| R12 security review | **IN PROGRESS** | separate read-only reviewer launched with maximum available high reasoning; findings must be resolved before merge |

## Local PostgreSQL evidence

- image digest: `sha256:691673308c99d2161ba298736f3147f1f22d79de2fb7ec93ae9b4afcab870b62`;
- PostgreSQL `18.4`; pgvector `0.8.6`;
- clean migration: 1–10; repeat: zero; upgrade: 9→10 then zero repeat;
- role probes: 39 PASS;
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
  `ec38ae8a5495bf38355439b786ea48faa94c1b0654ca35037782b0cf3f53f133`;
- replay: idempotent;
- PostgreSQL DDL denial: PASS;
- cleanup: schema dropped and canary grants revoked;
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
