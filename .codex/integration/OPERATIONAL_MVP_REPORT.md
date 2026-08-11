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
| Gate A | AUDIT IN PROGRESS | Read-only exact-head audit lane. |
| Gate B | AUDIT IN PROGRESS | Read-only exact-head audit lane. |
| Gate C | AUDIT IN PROGRESS | Read-only exact-head audit lane. |
| Gate D | AUDIT IN PROGRESS | Read-only exact-head audit lane. |
| Gate E | AUDIT IN PROGRESS | Read-only exact-head audit lane. |
| Gate F | AUDIT IN PROGRESS | Read-only exact-head audit lane. |
| Gate G | AUDIT IN PROGRESS | Read-only exact-head audit lane. |
| Gate H | AUDIT IN PROGRESS | Read-only exact-head audit lane. |
| Gate I | AUDIT IN PROGRESS | Read-only exact-head audit lane. |
| Gate J | AUDIT IN PROGRESS | Read-only exact-head audit lane. |
| Gate K | AUDIT IN PROGRESS | Read-only exact-head audit lane. |
| Gate L | AUDIT IN PROGRESS | Read-only exact-head audit lane. |
| Gate M | AUDIT IN PROGRESS | Read-only exact-head audit lane. |
| Gate N | AUDIT IN PROGRESS | Read-only exact-head audit lane. |
| Live matrix | MISSING | No synthetic or diagnostic carrier is counted. |
| Security/fault matrix | AUDIT IN PROGRESS | Exact-head audit lane. |
| Deployment | BLOCKED | PR #5 is not merged; public endpoints return 502. |
| Final acceptance | BLOCKED | Required live evidence and deployment are absent. |

## Known integration blocker under research

The normal master is intentionally rejected before provider mutation with
`CENTRAL_CHECKPOINT_UPLOAD_PATH_UNAVAILABLE`. The existing checkpoint writer
creates a second Kaggle client inside the master Notebook so it can version a
private checkpoint Dataset. That would require copying the central provider
credential into the Notebook and violates the owner-approved single-adapter
topology. Exact official-source research of pinned `kaggle==2.2.4`, current
`kagglesdk==0.1.37` and current `kagglehub==1.0.2` found no credential-free
server-side Notebook-output-to-Dataset copy operation. Dataset create/version
uploads caller-local files; kernel output is downloaded to the caller; kernel
sources attach completed output only to another Notebook. `kagglehub`'s
Notebook-default auth is an injected bearer session and would still be a
second lifecycle client.

The smallest technically plausible split-upload seam would let the central
adapter request resumable blob upload URLs, retain the resulting opaque file
tokens, and send only the short-lived signed upload URLs to the exact fenced
master. Bytes would travel master-to-provider and only central control would
create the Dataset version. Those URLs are nevertheless bearer upload-session
capabilities. The owner has explicitly rejected a new Notebook auth/session
mechanism, so this exception is **not implemented**. Unless Kaggle exposes a
supported server-side copy API, one of the stated constraints must be relaxed;
this is a topology decision, not missing `KAGGLE_USERNAME`/`KAGGLE_KEY`.

## Final gates

The final exact-head validation, hosted checks, merge, deployment, 24-scenario
live matrix and sanitized final acceptance receipt must be recorded here only
after they actually occur.
