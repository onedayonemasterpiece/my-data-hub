# Operational MVP integration report

This report is the integration ledger for
`MY_DATA_HUB_ONE_PASS_COMPLETION_PROMPT.md`. It is deliberately not an
acceptance receipt and cannot establish `MY_DATA_HUB_OPERATIONAL_MVP_COMPLETE`.

## Exact integration baseline

- Branch: `integration/operational-mvp`
- Audited base: `6b1cebdd1e81541669b66f63e6369905c58dcc11`
- PR #4: merged as `de657d63e4662e69dfb7169bc67aa65e8a9bda71`
- PR #5: open; exact-base hosted contracts and PostgreSQL integration checks
  passed, but merge is intentionally withheld while internal and live gates
  remain open.
- Current integrated implementation head: `HEAD` of `integration/operational-mvp`;
  exact SHA is recorded at each gate run and will be frozen only after remaining
  live evidence lands.

## Owner-approved Kaggle authority

The production topology reuses the existing events-bot/CherryFlash pattern:

- one control-owned `KaggleProviderAdapter`;
- the already automated legacy `KAGGLE_USERNAME`/`KAGGLE_KEY` credential pair;
- one private exact-version status Dataset per attempt containing the bounded
  `kaggle_run.json`, helper, configuration and one-time callback token;
- only the token hash in the control ledger and redacted runtime spooling;
- typed custom-state callbacks, heartbeat and resource leases;
- no Kaggle provider credential, manual browser session, OAuth refresh token or
  second direct provider client in a Notebook or remote MCP process.

## Observed live evidence

- A real private Kaggle run started through the single central legacy-auth
  adapter and proved PostgreSQL 18.4 plus pgvector 0.8.6, exact custom-state
  events, exact output readback, and claim-bound cleanup:
  `docs/operations/evidence/2026-08-11-operational-mvp/kaggle-pg18-runtime-canary-live.json`.
- This diagnostic evidence is not a matrix scenario or a `LIVE_PASS`.
- A second, independent live canary exercised the completed brokered checkpoint
  data path with one central adapter: a credential-free private producer
  Notebook PUT 4,096 bytes directly to Kaggle blob storage, the central adapter
  finalized private Dataset version `1`, and a credential-free verifier read the
  exact numeric version and matched the SHA-256. Both Notebooks and the Dataset
  were then deleted and inventory absence was observed. The sanitized receipt is
  `docs/operations/evidence/2026-08-11-operational-mvp/broker-live-canary-observed.json`.
- The canary observed two real Kaggle runs (producer kernel `130485704` and
  verifier kernel `130485733`), one central adapter instance, zero checkpoint
  bytes through devstand, and no Kaggle credential, signed upload URL or blob
  token in the receipt/state evidence. This closes the disposable broker POC;
  it does not by itself satisfy the 15-run/24-scenario matrix.
- A live read-only YDB aggregate observed 266 source rows and 266 distinct source
  identities across 14 batches/files (202 confirmed, 64 review). The subsequent
  full ordered export exhausted its bounded retries with YDB error `200803 /
  CLIENT_RESOURCE_EXHAUSTED`: the source database has RCU throttling enabled with
  the effective limit set to zero. No YDB setting or source row was changed.
- The public MCP, OAuth issuer and OIDC discovery endpoints currently return
  HTTP 502; no production deployment acceptance is claimed.

## Requirement closure

| Requirement | Status | Exact evidence or blocker |
|---|---|---|
| Gate A | CODE PASS | Single-transport validator and 10,000 deterministic provider histories; lane `gates-a-b`. |
| Gate B | CODE PASS | Poisoned operation isolation and retry-safe effect preservation; lane `gates-a-b`. |
| Gate C | CODE PASS | Durable lease payload and event redaction; lane `gates-c-e`. |
| Gate D | CODE PASS / BROKER POC LIVE PASS / PHYSICAL CHECKPOINT PENDING | The disposable signed-PUT/private-Dataset/exact-verifier/cleanup POC passed live. Physical PostgreSQL checkpoint publication, restore and HEAD promotion remain unobserved. |
| Gate E | CODE PASS | Exact generated E5/BGE worker assets and runtime-bound pins; lane `gates-c-e`. |
| Gate F | AUDIT IN PROGRESS | Read-only exact-head audit lane. |
| Gate G | AUDIT IN PROGRESS | Read-only exact-head audit lane. |
| Gate H | AUDIT IN PROGRESS | Read-only exact-head audit lane. |
| Gate I | CODE PASS / LIVE PENDING | OAuth/OIDC readiness and negative canaries; lane `gates-i-m-readiness`. |
| Gate J | AUDIT IN PROGRESS | Read-only exact-head audit lane. |
| Gate K | AUDIT IN PROGRESS | Read-only exact-head audit lane. |
| Gate L | CODE PASS / LIVE PENDING | Connector durability requires exact `DURABLE_COMPLETE` before spool deletion; lane `gate-l-connectors`. |
| Gate M | CODE PASS / LIVE PENDING | Deployment/post-deploy evidence contracts; lane `gates-i-m-readiness`. |
| Gate N | CODE PASS / LIVE PENDING | Typed acceptance dispatch and staged provider-real recovery; lane `acceptance-matrix-runtime`. |
| Live matrix | PARTIAL | Two broker POC Kaggle runs are observed and reconciled; the mandatory >=15-run/24-scenario operational matrix remains open. |
| Security/fault matrix | AUDIT IN PROGRESS | Exact-head audit lane. |
| Deployment | BLOCKED | PR #5 is not merged; local OAuth/MCP listeners are absent, the root tunnel-broker service is not installed, and public MCP/OAuth routes return 502. |
| Final acceptance | BLOCKED | Physical checkpoint/restore, full matrix, full YDB export/import, OAuth/MCP deployment and post-deploy evidence are absent. |

## Brokered checkpoint topology implemented

The owner explicitly approved Kaggle's one-file signed `create_url` as a
temporary upload capability, not an account credential or second lifecycle
client. The central adapter alone starts blob uploads, encrypts and retains
opaque blob tokens, finalizes the private Dataset version, launches the exact
numeric-version restore verifier and advances HEAD by CAS. The master contains
no Kaggle credential/SDK/CLI and streams each file directly to Kaggle storage;
checkpoint bytes never traverse devstand. Deterministic full-suite acceptance
passes. The disposable live upload/verifier/cleanup canary has now passed with
observed provider IDs and complete cleanup. The next checkpoint gate is a real
PostgreSQL checkpoint publication, exact-version restore verification and
single CAS HEAD promotion; the canary must not be counted as that proof.

## Final gates

The final exact-head validation, hosted checks, merge, deployment, 24-scenario
live matrix and sanitized final acceptance receipt must be recorded here only
after they actually occur.
