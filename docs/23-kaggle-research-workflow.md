# Durable Kaggle research workflow — implementation design

Status: `READY FOR IMPLEMENTATION`
Design date: 2026-08-25
Audit base: `main@38d3b19e9825dde265973025ff64c32d6a22ed1a`
Observed deployment: `38d3b19e9825dde265973025ff64c32d6a22ed1a`
Owner decision authority: this design records the decisions supplied for the durable Kaggle research workflow.
Implementation boundary: documentation and design only; no production code or provider resource was changed by this stage.

This document is the canonical implementation design for durable, agent-operated research on
Kaggle. For this workflow it refines the generic provider model in
[`17-kaggle-control-plane.md`](17-kaggle-control-plane.md), while preserving ADR-0016 and
all boundaries around the Kaggle PostgreSQL master.

## 1. Decision summary

The target is a compositional research workflow:

```text
discover Dataset
→ inspect metadata, license, versions and files
→ pin an exact source version
→ create or resume a durable research session
→ find or create a Notebook
→ preserve and revise Notebook source
→ attach provider-native Dataset inputs
→ start an idempotent run
→ reconcile status, events and logs without a connected MCP client
→ validate a compact output package
→ resume by research ID, alias or Dataset ref
→ iterate without erasing prior source, inputs, runs or outputs
→ export results and provenance
```

The implementation MUST satisfy these decisions:

1. Research orchestration state lives in the existing local SQLite-WAL `ControlLedger`.
   It is operational metadata, not canonical business data.
2. Every research operation MUST work while `master_state=ABSENT`.
3. Research operations MUST NOT call `master.ensure`, the master session broker,
   `data.query`, or the Kaggle PostgreSQL master.
4. The normal data path is provider-native Dataset attachment to a Kaggle Notebook.
   Full Dataset download to ChatGPT or devstand is optional and exceptional.
5. A durable `research session` is the user-facing aggregate. Claims, effects, provider
   kernel IDs and hashes remain internal implementation identities.
6. Existing MCP endpoint, OAuth issuer/resource/audience, client registrations, refresh
   families, tool names and valid payloads remain compatible.
7. New semantic tools are preferred, but a bounded façade through an already-visible tool
   is mandatory because current ChatGPT tool-catalog refresh cannot be treated as reliable.
8. Kaggle competition terms or licenses that require affirmative acceptance are never
   accepted automatically.
9. No Kaggle credentials, provider download credentials or signed provider URLs are
   returned to a client.
10. Provider operations are advertised only when the pinned official SDK or a live canary
    proves them.

## 2. Authority and non-goals

Authority order remains:

1. explicit owner decisions;
2. exact imported source/research;
3. corrective ADR-0016;
4. machine-readable architecture invariants;
5. derived documentation, code and tests.

This design does not:

- start or change the Kaggle PostgreSQL master;
- put research state in canonical PostgreSQL;
- create a second domain database;
- make Dataset download mandatory;
- introduce one monolithic `analyze_dataset` operation;
- expose provider credentials;
- accept competition rules;
- change the public MCP URL or OAuth identity;
- remove or rename existing MCP tools;
- claim that mock tests prove a live provider workflow;
- claim that this docs-only stage has implemented the design.

## 3. Confirmed current state

### 3.1 Audit evidence

The audit read the current versions of:

- `AGENTS.md`, `docs/00-source-of-truth.md`, ADR-0011 and ADR-0016;
- `architecture/invariants.yaml`;
- `docs/17-kaggle-control-plane.md`;
- remote-MCP, provider-only deployment and ChatGPT CIMD OAuth documents;
- MCP catalog, schemas, server, service, runtime and control gateway;
- `ControlLedger`, migrations `001` through `034`, provider-effect journal,
  provider registry, resource leases and OAuth grant tables;
- provider models, Kaggle contracts, adapter and recovery code;
- tests and checked-in canary/recovery evidence;
- the pinned official `kaggle==2.2.4` command and metadata contracts;
- the pinned `mcp==2.0.0` dependency and the MCP tool-list change protocol;
- the live `my-data-hub` MCP `platform.status`, provider inventory, provider registry
  projection and the tool catalog visible to this ChatGPT client.

No GitHub Actions workflow was invoked. The merge SHA has no attached combined-status or
workflow-run evidence in GitHub, so this design does not restate local PR validation as CI.

### 3.2 Git and deployment identity

| Item | Observed value |
|---|---|
| Repository | `onedayonemasterpiece/my-data-hub` |
| Default branch | `main` |
| Audited `main` SHA | `38d3b19e9825dde265973025ff64c32d6a22ed1a` |
| Live `deployed_commit` | `38d3b19e9825dde265973025ff64c32d6a22ed1a` |
| Live `control_plane_ready` | `true` |
| Live `master_state` | `ABSENT` |
| Live control ledger | `sqlite-wal` |
| Canonical database location | `kaggle-master-only` |

There was no SHA drift at audit time. `ABSENT` is a healthy state for this workflow.

### 3.3 Live capability matrix

“Code primitive” means a class, method or contract exists at the audited SHA.
“Live” means the operation is directly callable from the current ChatGPT tool catalog and
was checked without mutation.

| User operation | Existing code primitive | Current ChatGPT live surface | Result |
|---|---|---|---|
| Read platform/deployed SHA/master state | `platform.status` | yes | confirmed |
| List owner-visible Kaggle resources | adapter mine-only inventory | `provider.inventory.live` | confirmed, 40 rows |
| Read registered provider projections | `provider_resources` projection | `provider.resources.status` | confirmed, 12 rows |
| Create private managed/exchange Dataset | provider effect journal + adapter | visible | code/live tool exists; not invoked in this audit |
| Create exact Dataset version | claim-bound provider effect | visible | code/live tool exists; not invoked |
| Run private managed Notebook | source + Dataset input adapter | visible | code/live tool exists; not invoked |
| Read claim-bound Dataset/Notebook metadata | claim authorizer | visible | requires internal `claim_sha256` |
| List/download exact managed Dataset files | exact-version manifest readback | visible | Dataset only; 128 KiB chunk fallback |
| Restart-safe provider upload | staging + upload ledger | server allowlist has five tools | absent from this ChatGPT catalog |
| Search public Datasets | pinned SDK can list/search | absent | gap |
| Inspect public Dataset license/version/files | SDK metadata/list-files primitives | absent | gap |
| Inspect owner-private external Dataset | provider rights could allow it | denied by current claim/control model | gap |
| Attach public or owner-private external Dataset without adoption | adapter accepts Dataset source refs but gateway requires managed claims | absent | gap |
| Find Notebook by Dataset ref | SDK `kernels list --dataset` primitive | absent | gap |
| Get exact Notebook source version | SDK exact-version `kernels pull` primitive | absent | gap |
| Create a source revision without starting a run | no durable source aggregate | absent | gap |
| Get run status by durable research identity | latest-run provider status only | absent | gap |
| Get append-only run events | generic ledger events exist, no research projection | absent | gap |
| Get Notebook logs | pinned SDK has log primitives | absent from adapter/MCP | gap |
| List/get Notebook output artifacts | SDK latest-output primitives | live `list` schema is Dataset-only | gap |
| Resume by research ID, alias or Dataset ref | no research aggregate | absent | gap |
| Reconcile in background after client disconnect | provider reconciliation primitives exist | no live proof of an independent research reconciler | gap |
| Continue after devstand restart | ledger is durable | no research startup recovery | gap |
| Preserve old outputs after later runs | provider output API is latest-run oriented | no compact durable intake | gap |
| Same owner through another OAuth client | subject and client are recorded | no research ownership model | gap |
| Principal-bound resumable artifact delivery | chunked Dataset download only | no materialization broker | gap |

The provider-only server allowlist currently contains 17 tools, while this ChatGPT client
shows 12. The missing visible entries are the five upload lifecycle tools. This is direct
evidence that code deployment and the current ChatGPT catalog can diverge.

### 3.4 What exists only in code

The audited implementation already provides valuable primitives:

- SQLite WAL, `synchronous=FULL`, `busy_timeout`, `BEGIN IMMEDIATE` writer
  transactions and checksum-protected contiguous migrations;
- append-only operation/effect/runtime logs and current projections;
- durable provider effect intents and receipts;
- resource leases with fencing;
- OAuth subject, `client_id`, scope, refresh-family and audit metadata;
- exact private Dataset version readback and content-manifest verification;
- private Notebook push/run with intent-before-effect ordering;
- reconciliation after an ambiguous Notebook push;
- exact Notebook source SHA and provider version identities;
- latest-run status projection;
- bounded Dataset file list/download and restart-safe upload;
- control-class enforcement and protected-resource denial.

These primitives are implementation inputs, not a user-facing research model.

## 4. Instacart calibration result

The intended calibration Dataset is:

```text
psparks/instacart-market-basket-analysis
```

Observed Kaggle metadata on 2026-08-25:

- title: `Instacart Market Basket Analysis`;
- owner/slug: `psparks/instacart-market-basket-analysis`;
- visibility: public;
- license: `CC0: Public Domain`;
- known file names:
  - `aisles.csv`;
  - `departments.csv`;
  - `order_products__prior.csv`;
  - `order_products__train.csv`;
  - `orders.csv`;
  - `products.csv`;
  - `sample_submission.csv`.

The current live MCP cannot expose the exact numeric Dataset version or authoritative file
sizes because it has no public Dataset search/inspect/files operation. Those values are
`UNOBSERVABLE_WITH_CURRENT_LIVE_SURFACE`, not assumed absent and not invented here.

The mine-only live inventory and registered-resource projection contain no Dataset,
Notebook or run whose reference/title identifies Instacart. The current ledger has no
research-session table, so “registered Instacart research” is also absent by construction.

The first read-only implementation acceptance MUST therefore:

1. search for the exact Dataset;
2. inspect its current numeric provider version, visibility, license and access state;
3. list the authoritative provider file manifest and sizes;
4. persist the exact observation before any run;
5. search owner Notebooks with the Dataset filter and by title/alias;
6. resolve any existing source/run/output before allowing creation.

“No matching item in current mine-only inventory” MUST NOT be translated into “the public
Dataset does not exist.”

## 5. Target architecture

```text
ChatGPT / authorized agent
        |
        | existing HTTPS MCP endpoint and OAuth identity
        v
remote MCP resource server
  - tool discovery and compatibility façade
  - coarse scope enforcement
  - no Kaggle credentials
  - no canonical database credentials
        |
        | authenticated internal control call
        v
central lightweight control plane on devstand
  - existing ControlLedger (SQLite-WAL)
  - ResearchService
  - Dataset/Notebook/Run adapters
  - background ResearchReconciler
  - artifact materialization broker
  - Kaggle credentials remain server-side
        |
        | official pinned Kaggle SDK
        v
Kaggle
  - public and authorized private Datasets
  - private owner Notebooks
  - provider-native Dataset inputs
  - run status/logs/latest output
```

The background reconciler runs with the central control plane, not inside a ChatGPT
request and not inside the remote MCP process. It continues after the MCP session closes.
It shares the same `ControlLedger` and Kaggle adapter as synchronous operations.

Every research code path MUST assert:

```text
master_state == ABSENT is accepted
no master.ensure
no MasterSessionBroker method
no data.query
no PostgreSQL DSN
no Kaggle PostgreSQL master resource
```

Tests MUST inject sentinels that fail if any forbidden path is touched.

## 6. Durable research aggregate

### 6.1 Public identities

The server exposes:

- `research_id`: stable UUID generated by the control plane;
- exact Dataset ref, for example `psparks/instacart-market-basket-analysis`;
- owner-scoped aliases such as `instacart`;
- stable Notebook provider ref when materialized.

The user is never required to supply:

- `claim_sha256`;
- provider effect IDs;
- task IDs;
- provider kernel IDs;
- internal operation UUIDs.

`research.resume` resolves in this order:

1. exact `research_id`;
2. exact owner-scoped alias;
3. exact Dataset ref linked to one accessible research;
4. normalized title match.

Multiple matches return `AMBIGUOUS_MATCH` with bounded candidates. The server never guesses
between two sessions.

### 6.2 Aggregate invariants

- one owner subject owns a research session;
- `client_id` is audit context, not the ownership boundary;
- one source revision is immutable after `FROZEN`;
- one run references exactly one source revision and immutable input set;
- at most one non-terminal run exists per research session;
- prior source revisions, runs, input pins and accepted artifacts are never overwritten;
- a newer Dataset version never changes an old input row or run;
- every state projection update and corresponding event commit in one SQLite transaction;
- every non-idempotent provider effect has a persisted intent before the call;
- `unknown_outcome` reconciles before any retry;
- output acceptance is separate from provider run success.

## 7. SQLite operational model

### 7.1 Reuse before extension

Reuse these existing facilities:

| Existing facility | Research use |
|---|---|
| `operations`, `operation_log` | client request idempotency and operation replay |
| `effects`, `effect_log` | bounded control-plane effects |
| `provider_effect_intents`, `provider_effect_receipts` | intent-before-provider-call and ambiguous-outcome recovery |
| `provider_resources`, `provider_resource_claims` | exact provider resource projections and managed-resource claims |
| `resource_leases` | per-research and per-provider-resource fencing |
| `runtime_events`, `runtime_projection` | generic runtime evidence where applicable |
| `audit_log` | immutable security/audit decisions |
| OAuth grant tables | subject/client/scope and refresh-family evidence |

Do not add an independent SQLite database, duplicate operation journal, duplicate provider
registry, duplicate lease table or duplicate OAuth ownership store.

### 7.2 Migration 035 — research identity, inputs, source and runs

Add `src/my_data_hub/control_plane/ledger/sql/035_kaggle_research_workflow.sql`.

#### `research_sessions`

Purpose: current owner-facing research projection.

| Column | Contract |
|---|---|
| `research_id TEXT PRIMARY KEY` | UUID; immutable |
| `owner_subject TEXT NOT NULL` | ownership boundary; immutable |
| `created_client_id TEXT NOT NULL` | creation audit; immutable |
| `title TEXT NOT NULL` | 1–200 UTF-8 characters; mutable under revision |
| `goal TEXT NOT NULL` | 1–4000 UTF-8 characters; immutable |
| `state TEXT NOT NULL` | state enum in section 8 |
| `next_action TEXT` | bounded semantic action or null |
| `active_notebook_id TEXT` | FK, nullable |
| `active_run_id TEXT` | FK, nullable |
| `latest_completed_run_id TEXT` | FK, nullable |
| `projection_revision INTEGER NOT NULL` | starts at 1; CAS/fencing |
| `policy_revision INTEGER NOT NULL` | authorization/output policy revision |
| `created_at TEXT NOT NULL` | UTC RFC3339 |
| `updated_at TEXT NOT NULL` | UTC RFC3339 |
| `completed_at TEXT` | nullable |
| `archived_at TEXT` | nullable |

Constraints and indexes:

- `CHECK(projection_revision > 0)`;
- state check constraint;
- index `(owner_subject, state, updated_at DESC)`;
- index `(owner_subject, updated_at DESC)`;
- active IDs are updated only in the same transaction as the matching event;
- owner, creation client, goal and created time are immutable by trigger/store method.

#### `research_aliases`

Purpose: deterministic human resume.

| Column | Contract |
|---|---|
| `owner_subject TEXT NOT NULL` | owner namespace |
| `alias_normalized TEXT NOT NULL` | Unicode-normalized, case-folded, 1–120 chars |
| `alias_display TEXT NOT NULL` | user-facing value |
| `alias_kind TEXT NOT NULL` | `user`, `dataset_ref`, `title` |
| `research_id TEXT NOT NULL` | FK |
| `created_at TEXT NOT NULL` | immutable |

Primary key: `(owner_subject, alias_normalized)`.
An alias is never silently reassigned. A conflict is returned to the caller.

#### `research_inputs`

Purpose: exact Dataset observations and pins.

| Column | Contract |
|---|---|
| `input_id TEXT PRIMARY KEY` | UUID |
| `research_id TEXT NOT NULL` | FK |
| `provider TEXT NOT NULL` | v1: `kaggle` |
| `provider_ref TEXT NOT NULL` | exact `owner/slug` |
| `provider_version INTEGER NOT NULL` | positive exact version |
| `visibility TEXT NOT NULL` | `public`, `owner_private`, `granted_private` |
| `control_class TEXT NOT NULL` | mutation authority |
| `metadata_access INTEGER NOT NULL` | bool |
| `file_manifest_access INTEGER NOT NULL` | bool |
| `content_read_access INTEGER NOT NULL` | bool |
| `notebook_attach_access INTEGER NOT NULL` | bool |
| `license_name TEXT` | observed license |
| `license_url TEXT` | optional provider evidence |
| `terms_acceptance_required INTEGER NOT NULL` | bool |
| `access_evidence_json TEXT NOT NULL` | bounded canonical JSON |
| `provider_identity_json TEXT NOT NULL` | bounded canonical JSON |
| `files_manifest_json TEXT NOT NULL` | exact names/sizes/provider hashes where available |
| `files_manifest_sha256 TEXT NOT NULL` | lowercase SHA-256 |
| `manifest_observed_at TEXT NOT NULL` | UTC RFC3339 |
| `attach_mode TEXT NOT NULL` | `native_exact`, `native_guarded`, `provider_snapshot` |
| `pin_state TEXT NOT NULL` | `OBSERVED`, `PINNED`, `UNAVAILABLE`, `DRIFTED` |
| `created_at TEXT NOT NULL` | immutable |
| `retired_at TEXT` | explicit retirement only |

Unique: `(research_id, provider, provider_ref, provider_version)`.
All exact provider/version/license/manifest fields are immutable after `PINNED`.
A new provider version creates a new row.

#### `research_notebooks`

Purpose: stable Notebook identity independent of one run.

| Column | Contract |
|---|---|
| `notebook_id TEXT PRIMARY KEY` | UUID |
| `research_id TEXT NOT NULL` | FK |
| `provider TEXT NOT NULL` | v1: `kaggle` |
| `provider_ref TEXT` | `owner/slug`, nullable before create |
| `provider_kernel_id TEXT` | provider numeric/string identity |
| `control_class TEXT NOT NULL` | v1 research notebooks: `mcp_managed` |
| `state TEXT NOT NULL` | `PLANNED`, `ACTIVE`, `ABSENT`, `ARCHIVED` |
| `current_source_revision_id TEXT` | FK, nullable |
| `created_at TEXT NOT NULL` | immutable |
| `updated_at TEXT NOT NULL` | projection update |

Unique provider ref when non-null.
A research session may retain multiple historical Notebook identities, but only one is
`ACTIVE`.

#### `research_source_revisions`

Purpose: recoverable Notebook source and inputs after loss of chat context.

| Column | Contract |
|---|---|
| `source_revision_id TEXT PRIMARY KEY` | UUID |
| `notebook_id TEXT NOT NULL` | FK |
| `ordinal INTEGER NOT NULL` | starts at 1 |
| `state TEXT NOT NULL` | source state enum |
| `code_file TEXT NOT NULL` | normalized relative path |
| `kernel_type TEXT NOT NULL` | `script` or `notebook` |
| `language TEXT NOT NULL` | `python`, `r`, `julia` where SDK supports it |
| `source_utf8 TEXT NOT NULL` | max 262144 UTF-8 bytes |
| `source_sha256 TEXT NOT NULL` | lowercase SHA-256 |
| `runtime_options_json TEXT NOT NULL` | canonical bounded JSON |
| `runtime_options_sha256 TEXT NOT NULL` | SHA-256 |
| `input_set_sha256 TEXT NOT NULL` | ordered exact input pins |
| `created_by_subject TEXT NOT NULL` | audit |
| `created_client_id TEXT NOT NULL` | audit |
| `created_at TEXT NOT NULL` | immutable |
| `frozen_at TEXT` | set once |
| `submitted_at TEXT` | set once |

Unique: `(notebook_id, ordinal)`.
After `FROZEN`, source bytes, SHA, runtime options and input-set hash are immutable.
A correction always creates the next ordinal.

Source text is bounded operational state. Credentials, tokens, cookies, private keys,
PostgreSQL dumps and full Dataset contents are rejected before storage.

#### `research_runs`

Purpose: one immutable execution attempt plus mutable provider projection.

| Column | Contract |
|---|---|
| `run_id TEXT PRIMARY KEY` | UUID exposed through research APIs |
| `research_id TEXT NOT NULL` | FK |
| `notebook_id TEXT NOT NULL` | FK |
| `source_revision_id TEXT NOT NULL` | FK |
| `iteration_no INTEGER NOT NULL` | starts at 1 |
| `operation_id TEXT NOT NULL` | FK to existing operation journal |
| `effect_id TEXT` | provider effect identity, nullable before intent |
| `state TEXT NOT NULL` | run state enum |
| `output_state TEXT NOT NULL` | output state enum |
| `provider_kernel_id TEXT` | exact provider identity |
| `provider_run_ref TEXT` | exact durable run identity |
| `provider_source_version INTEGER` | exact Notebook source version |
| `poll_attempt INTEGER NOT NULL DEFAULT 0` | durable backoff |
| `next_poll_at TEXT` | UTC RFC3339 |
| `last_provider_state TEXT` | normalized provider state |
| `last_observed_at TEXT` | UTC RFC3339 |
| `started_at TEXT` | nullable |
| `finished_at TEXT` | nullable |
| `failure_code TEXT` | bounded code |
| `failure_summary TEXT` | redacted, max 2000 chars |
| `projection_revision INTEGER NOT NULL` | CAS/fencing |
| `created_at TEXT NOT NULL` | immutable |
| `updated_at TEXT NOT NULL` | projection update |

Constraints:

- unique `(research_id, iteration_no)`;
- unique `provider_run_ref` when non-null;
- partial unique index allowing at most one state in
  `REQUESTED,EFFECT_PENDING,UNKNOWN_OUTCOME,QUEUED,RUNNING,OUTPUT_PENDING`
  per research;
- `projection_revision > 0`;
- terminal `SUCCEEDED` or `FAILED` cannot be changed.

#### `research_run_inputs`

Purpose: immutable provenance snapshot even if the research input is later retired.

| Column | Contract |
|---|---|
| `run_id TEXT NOT NULL` | FK |
| `input_id TEXT NOT NULL` | FK |
| `ordinal INTEGER NOT NULL` | provider attachment order |
| `provider_ref TEXT NOT NULL` | copied exact ref |
| `provider_version INTEGER NOT NULL` | copied exact version |
| `files_manifest_sha256 TEXT NOT NULL` | copied pin |
| `attachment_ref TEXT NOT NULL` | exact submitted provider source ref |
| `attach_mode TEXT NOT NULL` | copied mode |
| `created_at TEXT NOT NULL` | immutable |

Primary key: `(run_id, input_id)`.
Unique `(run_id, ordinal)`.

### 7.3 Migration 036 — events, artifacts, materialization and grants

Add
`src/my_data_hub/control_plane/ledger/sql/036_kaggle_research_events_artifacts_grants.sql`.

#### `research_events`

Append-only evidence.

| Column | Contract |
|---|---|
| `event_id TEXT PRIMARY KEY` | UUID |
| `research_id TEXT NOT NULL` | FK |
| `run_id TEXT` | FK, nullable |
| `sequence INTEGER NOT NULL` | monotonic per research |
| `event_type TEXT NOT NULL` | bounded enum |
| `previous_state TEXT` | nullable |
| `new_state TEXT` | nullable |
| `provider_state TEXT` | nullable |
| `observed_at TEXT NOT NULL` | provider/event time |
| `recorded_at TEXT NOT NULL` | ledger time |
| `actor_subject TEXT` | null for reconciler |
| `actor_client_id TEXT` | null for reconciler |
| `payload_json TEXT NOT NULL` | canonical, max 64 KiB |
| `payload_sha256 TEXT NOT NULL` | SHA-256 |

Unique `(research_id, sequence)`.
Triggers reject `UPDATE` and `DELETE`.
Provider polls write an event when state changes and a bounded heartbeat no more often
than the configured interval.

#### `research_artifacts`

Exact compact and optional output identities.

| Column | Contract |
|---|---|
| `artifact_id TEXT PRIMARY KEY` | UUID |
| `run_id TEXT NOT NULL` | FK |
| `path TEXT NOT NULL` | normalized relative path |
| `semantic_role TEXT NOT NULL` | manifest role enum |
| `media_type TEXT NOT NULL` | normalized media type |
| `byte_size INTEGER NOT NULL` | non-negative |
| `sha256 TEXT NOT NULL` | lowercase SHA-256 |
| `required INTEGER NOT NULL` | bool |
| `provider_locator_json TEXT NOT NULL` | exact Notebook/source/output identity |
| `retrieval_state TEXT NOT NULL` | `REMOTE`, `MATERIALIZING`, `AVAILABLE`, `EXPIRED`, `FAILED` |
| `acceptance_state TEXT NOT NULL` | `DECLARED`, `VERIFIED`, `REJECTED` |
| `observed_at TEXT NOT NULL` | immutable identity observation |
| `accepted_at TEXT` | set once |

Unique `(run_id, path)`.
Path, role, size, hash and provider locator are immutable after insert.

#### `research_materializations`

Principal-bound resumable delivery metadata. Artifact bytes are never stored in SQLite.

| Column | Contract |
|---|---|
| `materialization_id TEXT PRIMARY KEY` | UUID |
| `artifact_id TEXT NOT NULL` | FK |
| `principal_subject TEXT NOT NULL` | binding |
| `client_id TEXT NOT NULL` | audit |
| `opaque_token_sha256 TEXT NOT NULL` | only token hash stored |
| `source_locator_json TEXT NOT NULL` | exact provider/snapshot locator |
| `expected_sha256 TEXT NOT NULL` | exact artifact hash |
| `total_bytes INTEGER NOT NULL` | exact size |
| `next_offset INTEGER NOT NULL` | resumable progress |
| `state TEXT NOT NULL` | `OPEN`, `COMPLETE`, `EXPIRED`, `REVOKED`, `FAILED` |
| `expires_at TEXT NOT NULL` | default 15 minutes |
| `created_at TEXT NOT NULL` | immutable |
| `updated_at TEXT NOT NULL` | progress |
| `completed_at TEXT` | nullable |

The broker either streams exact provider output or uses private mode-0700 staging outside
SQLite. Staged files are mode 0600, no-follow checked, deleted at completion/expiry, and
never mounted into the remote MCP process.

#### `research_access_grants`

| Column | Contract |
|---|---|
| `grant_id TEXT PRIMARY KEY` | UUID |
| `research_id TEXT NOT NULL` | FK |
| `grantor_subject TEXT NOT NULL` | must be owner |
| `grantee_subject TEXT NOT NULL` | subject boundary |
| `can_read_research INTEGER NOT NULL` | bool |
| `can_read_artifacts INTEGER NOT NULL` | bool |
| `can_edit_source INTEGER NOT NULL` | bool |
| `can_start_run INTEGER NOT NULL` | bool |
| `issued_client_id TEXT NOT NULL` | audit |
| `issued_at TEXT NOT NULL` | immutable |
| `expires_at TEXT` | optional |
| `revoked_at TEXT` | future access denied |
| `revoke_client_id TEXT` | audit |

At most one active grant per `(research_id, grantee_subject)`.
Revocation never deletes or rewrites prior audit/history.

### 7.4 Limits and retention

Initial hard limits:

- 32 aliases per research;
- 16 Dataset inputs per source revision;
- 100 source revisions per research;
- 262144 UTF-8 bytes per source revision;
- 64 KiB per event payload;
- 100 files in the required compact package;
- 500 total artifact rows per run;
- 8 MiB total required compact package;
- 1 MiB each for `metrics.json`, `provenance.json` and `diagnostics.json`;
- 512 KiB for `summary.md`;
- 1 MiB accepted `run.log`.

Retention:

- research sessions, exact inputs, source revisions, runs, run-input snapshots, events,
  accepted artifact metadata and access grants are retained indefinitely in v1;
- archive changes visibility/state but does not delete evidence;
- explicit purge is out of scope until a separate owner-approved retention policy exists;
- materialization authorization expires after 15 minutes by default;
- materialization metadata is retained seven days for replay/audit;
- staged bytes are removed immediately on completion, revoke, expiry or failure;
- optional large provider artifacts remain provider-side and are not copied unless
  explicitly requested.

### 7.5 Concurrency and crash consistency

Reuse the existing ledger settings:

- WAL required;
- `synchronous=FULL`;
- foreign keys enabled;
- `busy_timeout=5000`;
- serialized writers with `BEGIN IMMEDIATE`;
- checksum-verified contiguous migrations;
- file mode 0600.

Additional rules:

1. Acquire `resource_leases` with `resource_kind='research'` before a state-changing
   research operation. Default lease: 60 seconds.
2. Use the fencing token and expected `projection_revision` on every update.
3. Persist operation, run row, run-input snapshot, event and provider-effect intent before
   the non-idempotent Notebook push.
4. Bind provider receipt, provider identity, event and current projection in one
   transaction after the call.
5. A lost response leaves `UNKNOWN_OUTCOME`; it never issues a second push blindly.
6. A partial unique active-run index and lease prevent concurrent next-iteration starts.
7. Startup scans all non-terminal runs whose `next_poll_at` is due or whose lease expired.
8. Reconciler instances compete through the same fenced lease; only one polls/mutates.
9. Each successful poll commits event/projection/backoff before releasing the lease.

## 8. State machines

### 8.1 Research session

```text
DRAFT
  → READY
  → RUNNING
  → REVIEW_REQUIRED
  → READY                    # a new revision/iteration
  → COMPLETED
  → ARCHIVED
```

Side states:

```text
READY/RUNNING → BLOCKED_ACCESS → READY
READY/RUNNING → BLOCKED_INPUT_DRIFT → READY
```

Rules:

- a failed run moves the session to `REVIEW_REQUIRED`, not terminal failure;
- `COMPLETED` requires at least one run with accepted output;
- `ARCHIVED` is explicit and read-only;
- no state erases prior runs.

### 8.2 Source revision

```text
DRAFT → FROZEN → SUBMITTED → EXECUTED
   \        \
    → INVALID
```

`FROZEN`, `SUBMITTED`, `EXECUTED` and `INVALID` source bytes are immutable.
Fixes create a new ordinal.

### 8.3 Run

```text
REQUESTED
  → EFFECT_PENDING
  → QUEUED
  → RUNNING
  → OUTPUT_PENDING
  → SUCCEEDED
```

Failure paths:

```text
EFFECT_PENDING → UNKNOWN_OUTCOME → QUEUED/RUNNING/OUTPUT_PENDING/FAILED
QUEUED/RUNNING → FAILED
OUTPUT_PENDING → FAILED
```

`UNKNOWN_OUTCOME` permits only reconciliation of the exact provider identity/source hash.
It does not permit an automatic second push.

A user-visible `runs.retry` after a terminal failure creates a new run attempt, preserving
the old attempt. It may reuse the same frozen source revision or a new revision. A retry of
a non-terminal unknown outcome only reconciles the existing run.

### 8.4 Output acceptance

```text
UNOBSERVED
  → MANIFEST_PENDING
  → VALIDATING
  → ACCEPTED
```

or:

```text
MANIFEST_PENDING/VALIDATING → REJECTED
```

Provider `complete` is not research success until required output files, hashes,
provenance, source revision and input pins validate.

## 9. Recovery paths

| Failure | Required recovery |
|---|---|
| Connection breaks before `runs.start` response | Same `client_request_id` resolves the existing operation/run. If provider outcome is unknown, reconcile exact Notebook ref, source SHA and provider version before any new push. |
| ChatGPT window closes | Central reconciler continues polling. A new window calls `research.resume` by ID, alias or Dataset ref. |
| Access token expires | Provider run continues. Client refreshes normally; OAuth family and research ownership are independent of one access token. |
| Devstand restarts | SQLite migrations/open checks run, due non-terminal runs are scanned, expired leases are fenced, polling resumes. |
| Kaggle remains queued/running | Durable exponential backoff with jitter, capped interval, `next_poll_at`, state-change events and bounded heartbeat. |
| Notebook fails | Persist terminal failure, exact source/input identities, provider logs and diagnostics. Session becomes `REVIEW_REQUIRED`. |
| Output readback breaks | Run stays `OUTPUT_PENDING`; repeat exact manifest/file fetch or resume materialization at `next_offset`. |
| Dataset disappears or access changes | Keep immutable pin; set `BLOCKED_ACCESS`; never substitute another version. |
| Dataset version advances | Old runs stay bound to old input row. Offer an explicit new input/revision/iteration. |
| Client catalog is stale | `platform.status` publishes capability revision and compatibility instructions. Use the existing `provider.resources.run` façade. |
| Second OAuth client, same subject | Ownership succeeds; `client_id` is appended to audit. Leases serialize writes. |
| Different subject | Deny until explicit grant; apply only granted rights. |
| Two agents start next iteration | One fenced lease/partial unique insert wins; loser receives `RESEARCH_BUSY`, active run and retry hint. |
| Provider reports a run that cannot be matched exactly | Keep `UNKNOWN_OUTCOME`; do not adopt or mutate it automatically. |
| Required artifact hash fails | Mark output `REJECTED`; preserve provider evidence and do not mark research complete. |

## 10. Dataset authority and access

### 10.1 Separate dimensions

`control_class` determines mutation/deletion authority:

- `orchestrator_protected`;
- `mcp_managed`;
- `mcp_exchange`;
- `external_read_only`.

Data access is independent and evaluated as four rights:

- metadata visibility;
- file-manifest visibility;
- content read;
- Notebook attach.

A control class MUST NOT deny content that the authenticated owner is otherwise allowed to
read, except for the explicit `orchestrator_protected` boundary.

### 10.2 Required policy

| Resource | Metadata/files | Content read | Notebook attach | Mutation |
|---|---|---|---|---|
| Public Kaggle Dataset | allowed, bounded | provider/license permitted | allowed after exact pin | never without adoption |
| Owner-private Dataset | allowed | allowed | allowed after exact pin | denied while external; allowed after adoption |
| Other subject private Dataset | only provider/grant permitted | only provider/grant permitted | only provider/grant permitted | denied |
| `external_read_only` | rights follow provider + grants | rights follow provider + grants | rights follow provider + grants | denied |
| `orchestrator_protected` | bounded status only | denied | denied | orchestrator only |
| `mcp_managed` | owner/grant policy | owner/grant policy | allowed | guarded lifecycle |
| `mcp_exchange` | creator/recipient/TTL policy | creator/recipient/TTL policy | explicit purpose only | guarded lifecycle |

Kaggle credentials remain server-side. Competition rules or terms that require user
acceptance return `LICENSE_ACCEPTANCE_REQUIRED` or `TERMS_ACCEPTANCE_REQUIRED`; they are
never accepted by the service.

### 10.3 Exact input pin and provider attachment

Each pin contains:

- exact `owner/slug`;
- positive provider version;
- visibility and access evidence;
- control class and four access decisions;
- license/terms evidence;
- exact file manifest or provider identity;
- observation timestamp;
- manifest SHA-256;
- submitted attachment ref;
- attachment mode.

Provider-native attachment is preferred. The current pinned SDK documents Notebook
`dataset_sources` as `owner/slug`; the repository adapter can render
`owner/slug/<version>`, but no live canary in the audited evidence proves that Kaggle
accepts exact-version Dataset attachment.

Implementation MUST therefore expose capability:

```text
native_exact_dataset_attachment =
  VERIFIED | GUARDED_LATEST | SNAPSHOT_REQUIRED | UNSUPPORTED
```

Rollout gate:

1. run a disposable Dataset version-pin canary;
2. if exact `owner/slug/version` attachment is proven, use `native_exact`;
3. otherwise attach only when the selected version is current immediately before push,
   and require the Notebook bootstrap to emit a mount manifest that matches the pinned
   expected manifest; this is `native_guarded`;
4. any mismatch rejects output as input drift;
5. rerunning a no-longer-current version requires an explicit provider snapshot/mirror;
6. never create a mirror merely to manufacture an internal claim;
7. if reproducibility cannot be proved, fail closed with `EXACT_ATTACHMENT_UNPROVEN`.

## 11. Notebook lifecycle

### 11.1 Proven pinned-SDK primitives

The official pinned `kaggle==2.2.4` surface proves:

- Dataset list/search and mine filters;
- Dataset metadata, status, file listing and exact-version download;
- Notebook list/search and filters by Dataset/user;
- Notebook push, which creates/updates source and starts a run;
- Notebook source pull by exact `owner/slug/version`;
- latest-run status;
- output file listing/latest-output download;
- Notebook logs through SDK log primitives;
- Notebook deletion.

It does not prove cancellation. Cancellation remains absent.

### 11.2 Target lifecycle

1. `notebooks.find` searches accessible owner Notebooks by exact provider ref, alias,
   title and linked Dataset.
2. `notebooks.source.get` returns the stored exact source revision first. It may reconcile
   provider source by exact Notebook version.
3. `notebooks.source.update` creates a local `DRAFT`; it never overwrites a frozen revision.
4. `notebooks.inputs.set` resolves exact input rows and updates only a draft revision.
5. Freezing validates source size, prohibited secrets, paths, runtime options and input
   rights, then writes `FROZEN`.
6. `runs.start` materializes the frozen revision and exact input set, writes intent, then
   calls Notebook push.
7. Status/logs/output are accepted only for the expected exact Notebook/run identity.
8. After terminal provider status, required compact output is ingested immediately because
   provider status/output APIs are latest-run oriented.
9. Old provider source is recoverable by exact Notebook version; old accepted compact
   outputs are retained independently of the provider's latest-output pointer.
10. A correction creates a new source revision and a new run. Prior evidence remains.

## 12. Compact output and artifact contract

### 12.1 Required package

```text
research-output-manifest.json
summary.md
metrics.json
provenance.json
diagnostics.json
run.log
tables/...
figures/...
```

The first six names are reserved. A Notebook MAY omit `run.log` only when the server can
materialize an equivalent provider log and add it before acceptance.

`research-output-manifest.json` contains:

```json
{
  "schema_version": "my-data-hub-research-output.v1",
  "research_id": "uuid",
  "run_id": "uuid",
  "source_revision_id": "uuid",
  "source_sha256": "sha256",
  "input_set_sha256": "sha256",
  "inputs": [
    {
      "provider": "kaggle",
      "provider_ref": "owner/slug",
      "provider_version": 1,
      "files_manifest_sha256": "sha256",
      "attach_mode": "native_exact"
    }
  ],
  "files": [
    {
      "path": "summary.md",
      "semantic_role": "summary",
      "media_type": "text/markdown",
      "byte_size": 1234,
      "sha256": "sha256",
      "required": true,
      "retrieval": {"mode": "provider_output", "path": "summary.md"}
    }
  ]
}
```

Required semantic roles:

- `manifest`;
- `summary`;
- `metrics`;
- `provenance`;
- `diagnostics`;
- `log`.

Optional roles include `table`, `figure`, `model`, `query_result` and `other`.

### 12.2 Retrieval order

Default ChatGPT response returns only parsed/bounded:

1. manifest;
2. summary;
3. metrics;
4. diagnostics;
5. provenance.

Tables, figures and large derived files are listed and requested separately.

The existing 128 KiB chunk contract remains a recovery fallback. Normal UX uses
`artifacts.materialize`:

- TTL-bound;
- principal- and client-bound;
- exact artifact path and run/provider version;
- resumable byte ranges or deterministic offsets;
- final expected SHA-256;
- no provider credentials;
- no filesystem path returned;
- revoke/expiry cleanup.

For durable old-run access, the server retains the compact accepted package in a
non-canonical private artifact snapshot/CAS after hash verification. Large optional
artifacts remain provider-side unless explicitly materialized.

## 13. Semantic MCP surface

### 13.1 Common envelope

Every response includes where applicable:

```json
{
  "capability_revision": "opaque-revision",
  "deployed_commit": "git-sha",
  "research_id": "uuid",
  "operation_id": "uuid",
  "state": "STATE",
  "next_cursor": null,
  "continuation": {},
  "recommended_next_actions": [
    {"operation": "runs.get", "arguments": {"research_id": "uuid", "run_id": "uuid"}}
  ]
}
```

Read lists use opaque cursors, not client-calculated offsets.
State-changing calls require `client_request_id` of 8–300 characters.
Reusing the same ID with different canonical arguments returns `IDEMPOTENCY_CONFLICT`.

Common errors:

- `NOT_FOUND`;
- `AMBIGUOUS_MATCH`;
- `ACCESS_DENIED`;
- `LICENSE_ACCEPTANCE_REQUIRED`;
- `TERMS_ACCEPTANCE_REQUIRED`;
- `INPUT_VERSION_DRIFT`;
- `EXACT_ATTACHMENT_UNPROVEN`;
- `RESEARCH_BUSY`;
- `IDEMPOTENCY_CONFLICT`;
- `PROVIDER_UNKNOWN_OUTCOME`;
- `PROVIDER_RATE_LIMITED`;
- `OUTPUT_NOT_READY`;
- `ARTIFACT_INTEGRITY_FAILED`;
- `CAPABILITY_CATALOG_STALE`.

### 13.2 Operation contracts

| Tool | Required input | Primary output | Idempotency/retry | Authorization |
|---|---|---|---|---|
| `datasets.search` | query, visibility filter, cursor, limit ≤50 | exact refs, title, visibility, current version/license when provider returns it | read retry with cursor | research/provider read |
| `datasets.inspect` | exact `owner/slug`, optional version | version, visibility, license/terms, access decisions, provider identity | read retry | provider rights |
| `datasets.files` | exact ref/version, cursor, limit ≤200 | names, sizes, provider hashes/identity, next cursor | read retry | metadata/files right |
| `research.create` | title, goal, exact input selections, optional aliases, `client_request_id` | durable session/input pins and next action | exactly-once ledger create | research write + input rights |
| `research.get` | research ID/alias/ref | current projection, inputs, active Notebook/run | read retry | owner or read grant |
| `research.list` | state/filter/cursor/limit ≤50 | bounded owner/granted sessions | read retry | subject/grants |
| `research.resume` | one human selector | resolved session, active work and next action | read retry; ambiguous fails | owner or grant |
| `notebooks.find` | research selector, optional exact ref/query | matching owned Notebooks and linked evidence | read retry | research read + provider rights |
| `notebooks.source.get` | research/notebook, source revision or provider version | exact stored source, hashes, inputs, runtime options | read retry | owner/read plus source right |
| `notebooks.source.update` | research/notebook, base revision, source/runtime options, `client_request_id` | new draft/frozen revision | exactly-once revision creation | owner or edit-source grant |
| `notebooks.inputs.set` | draft revision, exact input IDs/order, `client_request_id` | updated draft/input-set hash | exactly-once CAS | edit-source + attach rights |
| `runs.start` | research, frozen source revision, `client_request_id` | existing/new run identity and state | intent-before-effect; same ID returns same run | owner or run grant |
| `runs.get` | research/run | exact projection, provider identity, next poll/action | read retry | research read |
| `runs.events` | research/run, cursor, limit ≤200 | append-only events | read retry | research read |
| `runs.logs` | research/run, cursor/max bytes | bounded persisted/provider log with continuation | read retry; exact run binding | research read |
| `runs.retry` | terminal run, source revision, `client_request_id` | new run attempt | never mutates old run | run grant |
| `artifacts.list` | research/run, role filter, cursor, limit ≤200 | exact artifact manifest rows | read retry | artifact read |
| `artifacts.get` | artifact ID or exact run/path, optional bounded inline | metadata plus inline small content or continuation | read retry | artifact read |
| `artifacts.materialize` | artifact, `client_request_id` | TTL materialization identity, size/hash/continuation | same ID returns same active materialization | artifact read |
| `research.complete` | research, accepted run, `client_request_id` | completed projection/export summary | CAS/idempotent | owner |
| `research.archive` | research, `client_request_id` | archived projection | idempotent | owner |

`research.create` does not create a Notebook automatically.
`notebooks.source.update` does not start a run.
`runs.start` does not hide status/log/output retrieval.
The composition remains visible and resumable.

### 13.3 Mapping to current primitives

| Semantic operation | Reused primitive | Missing implementation |
|---|---|---|
| Dataset search/inspect/files | Kaggle list/metadata/status/list-files | public/owner-private access adapter and MCP contracts |
| Research CRUD/resume | ControlLedger transactions | research tables/service/alias resolver |
| Notebook find | Kaggle kernels list filters | adapter + research linkage |
| Source get | exact-version kernels pull | secure bounded source intake |
| Source update/inputs | current source validation helpers | durable revision model |
| Run start | existing run adapter/effect journal | research run wrapper and idempotency resolver |
| Run status | existing latest status | exact run binding/events/reconciler |
| Logs | pinned SDK log primitive | adapter normalization and bounded persistence |
| Artifacts | existing output/download concepts | output manifest validation and broker |
| Grants | OAuth subject/client/audit tables | research grant table and policy evaluator |

## 14. MCP catalog compatibility without reconnect

### 14.1 Confirmed constraints

The MCP protocol supports the `tools` capability with `listChanged` and
`notifications/tools/list_changed`. The pinned Python SDK exposes the notification
primitive. This is useful for active subscribed sessions, but it is not sufficient for a
deployment boundary.

The current ChatGPT catalog is demonstrably stale relative to the server allowlist:
12 visible tools versus 17 provider-only allowlisted tools. OpenAI's app interface also
documents an explicit app refresh operation, so automatic adoption of new tool names MUST
NOT be a release assumption.

### 14.2 Non-breaking native path

Implementation MUST:

- keep the exact endpoint `https://mcp-datahub.kenigevents.ru/mcp`;
- keep OAuth issuer, resource/audience and CIMD identity;
- keep all existing tools and valid payloads;
- add no newly required field to an existing schema;
- preserve existing refresh families and client registrations;
- publish `listChanged=true` and send tool-list notifications where sessions support them;
- add `capability_revision`, `catalog_sha256`, `deployed_commit` and compatibility metadata
  to `platform.status`;
- keep existing `provider:write` as a superset of all owner research operations;
- add future narrow scopes `research:read`, `research:write`, `research:run` without
  invalidating old tokens.

### 14.3 Mandatory compatibility façade

Use the already-visible `provider.resources.run` tool with this exact reserved outer
identity:

```text
resource_ref = urn:my-data-hub:research-compat:v1
control_class = mcp_managed
private = true
```

The legacy-required payload remains schema-valid:

```json
{
  "kind": "notebook",
  "task_id": "00000000-0000-0000-0000-000000000000",
  "effect_id": "00000000-0000-0000-0000-000000000000",
  "idempotency_key": "client-request-id",
  "task_run_id": "00000000-0000-0000-0000-000000000000",
  "title": "research-compat-v1",
  "code_file": "request.json",
  "kernel_type": "script",
  "language": "python",
  "source_utf8": "{\"protocol\":\"research-compat.v1\",\"operation\":\"research.resume\",\"arguments\":{\"selector\":\"instacart\"},\"known_capability_revision\":\"optional\"}",
  "dataset_inputs": [],
  "disposable": false
}
```

Rules:

1. `source_utf8` is a closed JSON envelope:
   `protocol`, one allowlisted semantic `operation`, `arguments`, optional known revision.
2. One call performs exactly one semantic operation. Composite `analyze_dataset` is
   forbidden.
3. The compatibility dispatcher intercepts the reserved URN before Kaggle validation,
   provider claims, provider effect intent creation or adapter invocation.
4. The outer nil UUIDs/title/code fields are compatibility sentinels and have no provider
   meaning.
5. Authorization and contracts are identical to the native semantic operation.
6. Idempotency is keyed by owner subject + outer idempotency key + canonical envelope hash.
7. Changed replay returns `IDEMPOTENCY_CONFLICT`.
8. `platform.status` returns the protocol version, supported façade operations and a
   recommended call when the client reports an old capability revision.
9. Existing owner tokens with `provider:write` may use the façade. Future narrow-scope
   clients use native tools.
10. Tests MUST prove the Kaggle adapter and provider effect journal are not called for the
    reserved URN.

This guarantees that a new ChatGPT window using the already-connected app can perform the
research workflow even when it still sees only the old catalog. No delete/re-add,
new registration or repeat OAuth is required.

## 15. Access model across clients

Authorization is conjunctive:

1. MCP scope permits the operation class;
2. authenticated `owner_subject` owns the research or has a grant;
3. research grant contains the exact right;
4. provider identity has the underlying Dataset/Notebook right;
5. control class permits mutation;
6. data-access decision permits metadata/content/attach;
7. protected-resource policy has final deny authority.

Scope mapping:

| Scope | Rights |
|---|---|
| existing `provider:read` | legacy provider reads only |
| existing `provider:write` | owner superset: research read/write/run for compatibility |
| future `research:read` | research/session/result reads |
| future `research:write` | create session, aliases, source revisions, inputs, complete/archive |
| future `research:run` | start/retry runs and materialize outputs |

The same owner subject through another OAuth client can find and continue the session.
`client_id` remains in every audit event but does not split ownership.

A different subject receives no private research data until grant. `can_read_research`
does not imply artifact content. `can_edit_source` does not imply run. `can_start_run`
does not imply grant management or archive. Revocation applies to subsequent requests;
historic audit and run provenance remain unchanged.

## 16. Security boundaries

- Remote MCP receives neither Kaggle nor PostgreSQL credentials.
- Central control is the only provider adapter owner.
- Research source and JSON fields are scanned for credential-like material and bounded.
- Dataset contents, large outputs and provider credentials never enter SQLite.
- Protected checkpoint/master resources remain status-only and cannot be selected as
  research inputs.
- Provider refs, paths and archive members are normalized and traversal-free.
- Artifact staging is private, no-follow checked and TTL-reaped.
- Materialization tokens are opaque; only SHA-256 is stored.
- Errors redact provider credentials, local paths and raw provider bodies.
- License/terms evidence is stored; acceptance is never synthesized.
- Every access decision includes subject, client, scopes, research, grant, provider rights,
  control class and policy revision in audit.
- No research operation may cause canonical PostgreSQL autostart.

## 17. Acceptance matrix

Each scenario has four evidence layers where applicable:

1. deterministic unit/contract tests;
2. SQLite crash/concurrency/replay tests;
3. adapter tests using a fake provider;
4. explicit live Kaggle acceptance using disposable resources.

Mock evidence is never reported as provider acceptance.

### A. Public Dataset discovery

From a human query, find Instacart and return exact ref, numeric version, license,
authoritative file list/sizes, access and attach capability. Works with `master_state=ABSENT`.

### B. Private owner Dataset

Find an owner-private external Dataset, read metadata/files and attach it without adoption
or manual claim. Mutation remains denied.

### C. Research creation

Create question/title/aliases and exact input pin in SQLite. Assert no master resolver
ensure, broker or data query call.

### D. Notebook attach and run

Attach provider-native input. Notebook reads expected Kaggle mount files and emits the
standard output package. Run records exact source/input/runtime/provider identities.

### E. Lost response after start

Inject disconnect after provider side effect and before MCP response. Replay same request.
Exactly one provider run exists and the original run identity is returned.

### F. Closed ChatGPT window

Start run, end the MCP client, allow reconciler to continue, open a new window on the
existing app and resume by alias/ref without reconnect or OAuth.

### G. Devstand restart

Restart central control during a non-terminal run. Startup recovery reacquires a fenced
lease and resumes polling from durable `next_poll_at`.

### H. Notebook failure

Return terminal failure, exact source/input identities, logs, diagnostics and next allowed
source-revision action.

### I. Iteration

Recover source, create a new revision, start a new run. Old run/source/output remain
readable.

### J. Output interruption

Interrupt materialization mid-file. Resume at exact offset. Final SHA-256 equals manifest.

### K. Dataset version drift

Publish/observe a newer Dataset version. Old run remains unchanged. New version requires an
explicit new input/revision/iteration.

### L. Same owner, different client

A second OAuth `client_id` with the same subject resumes without grant. Both client IDs are
audited.

### M. Different subject

Deny by default. After read grant, permit only session/result reads. Deny source change,
run and archive.

### N. Protected resource denial

Attempt metadata/content/attach against `orchestrator_protected`. Return bounded status
only and prove no provider content call.

### O. MCP catalog compatibility

Deploy at the same endpoint/issuer/resource. Existing OAuth refresh family remains valid.
Old calls still work. Native new tools appear when client refreshes. A deliberately stale
catalog completes `research.resume` through the reserved compatibility façade. No
delete/re-add is required.

### P. End-to-end Instacart research

Future implementation canary:

1. search exact Dataset;
2. inspect and pin exact version/manifest/license;
3. create or resume research;
4. find or create Notebook;
5. attach Dataset;
6. calculate basic basket, product and reorder metrics;
7. intentionally break the client after start;
8. continue in a new window;
9. read status, events and logs;
10. obtain summary, metrics, diagnostics, provenance and selected figures;
11. verify artifact SHA-256;
12. prove the full Dataset was not transferred to ChatGPT/devstand.

Additional mandatory canary before P:

- prove or reject exact-version `dataset_sources`;
- when rejected, prove guarded mount-manifest matching;
- record capability outcome and fail closed if neither is reproducible.

## 18. Ordered implementation and rollout

### Phase 1 — ledger and pure domain service

1. add migrations 035 and 036;
2. add store methods, models, immutability triggers and indexes;
3. implement research identity/alias resolver, access evaluator and state transitions;
4. test migration from every supported prior schema, rollback-on-error, WAL concurrency,
   lease fencing, idempotency and crash points;
5. keep all features disabled.

### Phase 2 — read-only Dataset/Notebook discovery

1. extend pinned Kaggle protocol/adapter for search, inspect, files, Notebook search,
   exact source pull and logs;
2. implement public/owner-private external access decisions separated from control class;
3. expose read-only semantic services behind a feature flag;
4. run live read-only Instacart discovery and owner-private discovery;
5. persist no credentials or Dataset bytes.

### Phase 3 — durable research/source model

1. expose create/get/list/resume and source/input operations;
2. add aliases and same-subject cross-client tests;
3. add future narrow scopes while preserving provider scopes;
4. implement `platform.status` capability revision and reserved compatibility façade;
5. prove stale-catalog behavior without reconnect.

### Phase 4 — run and reconciliation

1. wrap existing Notebook push/effect journal with research run identity;
2. add central `ResearchReconciler`;
3. wire startup recovery, durable backoff and exact event projection;
4. expose status/events/logs/retry;
5. test lost-response and devstand-restart scenarios.

### Phase 5 — output acceptance and broker

1. implement output package validator and artifact rows;
2. ingest required compact outputs at terminal status;
3. add principal-bound materialization, range resume and SHA verification;
4. retain old compact packages independently of provider latest output;
5. test interruption/tamper/expiry.

### Phase 6 — live provider acceptance

1. use disposable non-production Datasets/Notebooks;
2. run exact-version attachment capability canary;
3. run failure, disconnect, restart and iteration canaries;
4. run end-to-end Instacart scenario only after read/access/version gates pass;
5. clean up only explicitly disposable task-owned resources;
6. preserve sanitized receipts and hashes, never credentials or Dataset contents.

### Deployment order

1. backup/check SQLite ledger and free-space state;
2. deploy central control image with migrations/features disabled;
3. apply migrations on startup and verify `quick_check`;
4. start reconciler disabled, then shadow-read due-run scan;
5. deploy remote MCP with old tools unchanged and capability metadata;
6. enable read-only discovery;
7. enable research CRUD/source;
8. enable run/reconciler;
9. enable artifacts;
10. execute live canaries;
11. enable native research tools;
12. retain compatibility façade permanently for old catalogs.

### Rollback

- feature flags disable native research tools, façade operations, reconciler and run start;
- existing provider tools continue unchanged;
- do not downgrade SQLite migrations;
- new tables are inert when features are disabled;
- non-terminal runs already started remain reconciled read-only until terminal;
- materialization staging is reaped;
- no OAuth/client/endpoint rollback is required;
- provider resources are deleted only with exact task-owned disposable receipts.

## 19. File-by-file implementation handoff

### 19.1 Ledger and research domain

| File | Required change |
|---|---|
| `src/my_data_hub/control_plane/ledger/sql/035_kaggle_research_workflow.sql` | new identity/input/source/run schema and indexes |
| `src/my_data_hub/control_plane/ledger/sql/036_kaggle_research_events_artifacts_grants.sql` | new append-only events, artifact/materialization/grant schema |
| `src/my_data_hub/control_plane/ledger/migrations.py` | register contiguous migrations and checksums |
| `src/my_data_hub/control_plane/ledger/models.py` | typed research rows/state enums |
| `src/my_data_hub/control_plane/ledger/store.py` | CAS projections, append events, aliases, pins, runs, artifacts, grants, recovery scans |
| `src/my_data_hub/research/__init__.py` | new bounded public research package |
| `src/my_data_hub/research/models.py` | semantic request/response and state models |
| `src/my_data_hub/research/access.py` | subject/grant/provider/control/data-access evaluator |
| `src/my_data_hub/research/service.py` | create/get/list/resume/source/run/artifact orchestration |
| `src/my_data_hub/research/reconciler.py` | due-run polling, unknown outcome, startup recovery |
| `src/my_data_hub/research/artifacts.py` | manifest validator, compact intake, materialization broker |

### 19.2 Provider and control gateway

| File | Required change |
|---|---|
| `src/my_data_hub/providers/models.py` | separate data-access decisions from control class; add proven read capabilities |
| `src/my_data_hub/providers/kaggle/contracts.py` | exact SDK protocol for search/metadata/files/kernel filters/source/logs/output |
| `src/my_data_hub/providers/kaggle/adapter.py` | bounded public/private discovery, exact source/log/output wrappers and attachment capability probe |
| `src/my_data_hub/providers/kaggle/control_journal.py` | reuse/extend exact run reconciliation only where existing journal boundary requires it |
| `src/my_data_hub/control_plane/adapters.py` | expose research control reader/writer and capability status |
| `src/my_data_hub/mcp/control_gateway.py` | authenticated native research forwarding and compat envelope forwarding |
| central control service/runtime entry points | construct `ResearchService`, shared adapter and background reconciler; no master dependency |

### 19.3 MCP, OAuth and configuration

| File | Required change |
|---|---|
| `src/my_data_hub/mcp/contracts.py` | research control protocol and status capability fields |
| `src/my_data_hub/mcp/catalog.py` | native semantic tool contracts, scopes, read/write annotations |
| `src/my_data_hub/mcp/server.py` | provider-only/unified allowlists, native registrations, tool-list capability |
| `src/my_data_hub/mcp/service.py` | route research tools and reserved compat URN before provider write path |
| `src/my_data_hub/mcp/runtime.py` | inject research client/service; preserve provider-only master independence |
| `src/my_data_hub/config.py` | feature flags, reconciler intervals, artifact limits/directories and future scopes |
| `src/my_data_hub/oauth_server/models.py` | recognize future research scopes without invalidating old grants |
| `src/my_data_hub/oauth_server/client_metadata.py` | allow narrow future scopes for eligible clients |
| `src/my_data_hub/oauth_server/service.py` | scope issuance/superset compatibility |
| `src/my_data_hub/oauth_server/runtime.py` | startup scope/config wiring without issuer/resource change |

### 19.4 Deployment and evidence

| File | Required change |
|---|---|
| `compose.control-plane.yaml` | central reconciler process/service sharing ledger and provider credentials, never remote MCP |
| `deploy/control-plane/install.sh` | private artifact staging, service wiring, feature flags and safe upgrade |
| `deploy/control-plane/collect_deployment_evidence.py` | capability revision, research/reconciler/SQLite evidence |
| `scripts/verify_post_deploy.py` | no-reconnect catalog/compat checks and master-ABSENT assertions |
| new `scripts/provider/research_instacart_canary.py` | future explicit live scenario P with cleanup receipts |
| `scripts/validate_repository.py` | migration/tool/schema/docs invariants |

### 19.5 Tests

Reuse and extend:

- `tests/control/test_ledger_master.py`;
- `tests/control/test_control_runtime_wiring.py`;
- `tests/control/test_mcp_operator_provider.py`;
- `tests/provider/test_kaggle_adapter.py`;
- `tests/mcp/test_control_gateway.py`;
- `tests/mcp/test_remote_runtime.py`;
- `tests/oauth_server/test_chatgpt_cimd.py`;
- `tests/oauth_server/test_oauth_server_runtime.py`;
- `tests/test_control_plane_deployment.py`;
- `tests/control/test_acceptance_evidence.py`.

Add:

- `tests/research/test_research_ledger.py`;
- `tests/research/test_research_service.py`;
- `tests/research/test_research_access.py`;
- `tests/research/test_research_reconciler.py`;
- `tests/research/test_research_artifacts.py`;
- `tests/mcp/test_research_tools.py`;
- `tests/mcp/test_research_compat.py`;
- `tests/integration/test_research_disconnect_restart.py`;
- `tests/integration/test_research_catalog_compatibility.py`.

Required test order per phase:

1. schema/migration/static repository validation;
2. pure state-machine and authorization tests;
3. crash/idempotency/concurrency tests;
4. fake-provider adapter/service integration;
5. remote MCP/OAuth compatibility tests;
6. post-deploy read-only probes;
7. disposable live Kaggle capability canary;
8. full Instacart acceptance.

## 20. Readiness verdict

No unresolved owner product decision prevents implementation.

The exact current numeric Instacart version and exact-version Dataset attachment behavior
are provider observations, not owner decisions. The implementation has explicit read-only
discovery and fail-closed capability gates for both.

Verdict:

```text
READY_FOR_CODEX
```

This verdict means the implementation design is ready. It does not mean production code,
deployment or the live Instacart research workflow is complete.

## References

Repository authority and implementation evidence:

- `AGENTS.md`;
- `docs/00-source-of-truth.md`;
- `docs/adr/0011-kaggle-resource-control-classes.md`;
- `docs/adr/0016-kaggle-postgresql-master-architecture-reset.md`;
- `architecture/invariants.yaml`;
- `docs/17-kaggle-control-plane.md`;
- `docs/20-remote-mcp-endpoint.md`;
- `docs/operations/provider-only-mcp-deploy.md`;
- `docs/operations/chatgpt-cimd-oauth.md`;
- `src/my_data_hub/control_plane/ledger/**`;
- `src/my_data_hub/providers/kaggle/**`;
- `src/my_data_hub/mcp/**`;
- `src/my_data_hub/oauth_server/**`.

External primary references:

- Kaggle CLI `v2.2.4` Dataset, Kernel and Kernel metadata documentation;
- MCP tools specification and tool-list change notification;
- MCP Python SDK `v2.0.0`;
- OpenAI Developer mode and MCP documentation;
- Kaggle Dataset `psparks/instacart-market-basket-analysis`.
