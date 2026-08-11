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
- The public MCP, OAuth issuer and OIDC discovery endpoints currently return
  HTTP 502; no production deployment acceptance is claimed.

## Requirement closure

| Requirement | Status | Exact evidence or blocker |
|---|---|---|
| Gate A | CODE PASS | Single-transport validator and 10,000 deterministic provider histories; lane `gates-a-b`. |
| Gate B | CODE PASS | Poisoned operation isolation and retry-safe effect preservation; lane `gates-a-b`. |
| Gate C | CODE PASS | Durable lease payload and event redaction; lane `gates-c-e`. |
| Gate D | CODE PASS / LIVE PENDING | Central brokered direct upload, bounded reconcile, exact verifier and CAS HEAD; lanes `checkpoint-adapter-primitives` and `checkpoint-broker`. |
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
| Live matrix | MISSING | No synthetic or diagnostic carrier is counted. |
| Security/fault matrix | AUDIT IN PROGRESS | Exact-head audit lane. |
| Deployment | BLOCKED | PR #5 is not merged; public endpoints return 502. |
| Final acceptance | BLOCKED | Required live evidence and deployment are absent. |

## Brokered checkpoint topology implemented

The owner explicitly approved Kaggle's one-file signed `create_url` as a
temporary upload capability, not an account credential or second lifecycle
client. The central adapter alone starts blob uploads, encrypts and retains
opaque blob tokens, finalizes the private Dataset version, launches the exact
numeric-version restore verifier and advances HEAD by CAS. The master contains
no Kaggle credential/SDK/CLI and streams each file directly to Kaggle storage;
checkpoint bytes never traverse devstand. Deterministic full-suite acceptance
passes. The disposable live upload/verifier/cleanup canary is now the next gate,
not an internal topology blocker.

## Final gates

The final exact-head validation, hosted checks, merge, deployment, 24-scenario
live matrix and sanitized final acceptance receipt must be recorded here only
after they actually occur.
