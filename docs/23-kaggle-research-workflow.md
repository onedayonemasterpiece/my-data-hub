# Durable Kaggle research workflow — implementation specification

Status: `READY_FOR_CODEX`

Audit date: 2026-08-25  
Audit base: `main@38d3b19e9825dde265973025ff64c32d6a22ed1a`  
Observed deployment: `38d3b19e9825dde265973025ff64c32d6a22ed1a`

This is the single canonical implementation specification for lightweight, resumable
research on Kaggle through the existing my-data-hub MCP endpoint. It is intentionally
narrow: no second orchestrator, no second database, no Kaggle PostgreSQL master, no
monolithic analysis tool and no compatibility tunnel hidden inside a legacy tool.

## 1. Required outcome

```text
find a public or owner-private Kaggle Dataset
→ inspect exact version, visibility, license and files
→ create or resume a research record
→ create or resume a private research Notebook
→ attach Dataset input at Kaggle without relaying the Dataset through ChatGPT
→ start an idempotent run
→ inspect durable status and bounded logs
→ recover after a lost response, a new ChatGPT window or a devstand restart
→ save a new Notebook revision and run the next iteration
→ retrieve summary, metrics, output manifest, selected artifacts and provenance
```

Full Dataset download remains an auxiliary bounded operation. The normal path is compute
next to the data in Kaggle.

## 2. Verified baseline

### Repository and deployment

| Item | Verified value |
|---|---|
| Default branch | `main` |
| `main` SHA | `38d3b19e9825dde265973025ff64c32d6a22ed1a` |
| Live `deployed_commit` | `38d3b19e9825dde265973025ff64c32d6a22ed1a` |
| Live control plane | ready |
| Live control ledger | `sqlite-wal` |
| Live `master_state` | `ABSENT` |
| Kaggle SDK | pinned official `kaggle==2.2.4` |
| MCP SDK | `mcp==2.0.0` |

There is no code/deployment SHA drift. `master_state=ABSENT` is the required healthy state
for this workflow.

### Existing usable primitives

The current implementation already has:

- one central Kaggle adapter using server-side credentials;
- private Dataset create/version and private Notebook push/run;
- exact provider resource and Notebook source/run identities;
- latest Notebook run status;
- bounded output declarations and provider output readback primitives;
- persist-before-side-effect provider intents and receipts;
- idempotency keys, resource claims, leases/fencing and audit records;
- restart-safe SQLite-WAL with contiguous checksum-protected migrations;
- stable remote MCP endpoint, OAuth issuer/resource, client registrations and rotating
  refresh-token families;
- a provider-only mode that works while the PostgreSQL master is absent.

These are low-level primitives, not yet a usable research workflow.

### Confirmed live limitations

The current ChatGPT catalog exposes only low-level provider tools. Public Dataset search,
public Dataset metadata/files, owner-private external Dataset access, research identity,
Notebook revision history, run events/logs, artifact manifest retrieval and human resume
are absent.

Current provider calls also expose implementation identities to the caller, including
`claim_sha256`, `task_id`, `effect_id` and `task_run_id`. Public and pre-existing private
Datasets cannot be attached without manufacturing internal claims.

The deployed backend can read a completed Notebook run and reports source/run identities,
runtime options and expected outputs. However, the current ChatGPT schema for
`provider.resources.list`/`download` still accepts Dataset only. The server allowlist also
contains five upload lifecycle tools that this ChatGPT catalog does not show. Therefore
server deployment and ChatGPT action schemas can demonstrably drift.

## 3. Instacart calibration

The calibration Dataset is:

```text
psparks/instacart-market-basket-analysis
```

Verified externally:

- title: `Instacart Market Basket Analysis`;
- visibility: public;
- license: `CC0: Public Domain`;
- logical files include `aisles.csv`, `departments.csv`, `orders.csv`, `products.csv`,
  `order_products__prior.csv` and `order_products__train.csv`.

The exact current numeric Dataset version and authoritative current file sizes are not
available through the present live MCP, because it has no public Dataset inspect/files
operation. They must be obtained by the first read-only acceptance run, not guessed.

The complete owner-only live Kaggle inventory and the control-ledger provider projection
contain no Instacart Dataset, owner Notebook or tracked run. Consequently there is no
existing owner work that can currently be continued. Public example Notebooks do not
become mutable or resumable owner work merely because they are visible.

## 4. Architectural decision

```text
ChatGPT or another authorized MCP client
        |
        | existing HTTPS MCP endpoint and existing OAuth identity
        v
remote MCP process
  - semantic tools and coarse scopes
  - no Kaggle credentials
  - no PostgreSQL credentials
        |
        | existing authenticated internal control gateway
        v
existing central control process on devstand
  - existing SQLite-WAL ControlLedger
  - small KaggleResearchService
  - small in-process due-run reconciler
  - existing central KaggleProviderAdapter
        |
        | official pinned Kaggle SDK
        v
Kaggle Dataset + private research Notebook + provider outputs
```

Research state is operational metadata. It is not canonical business data and does not
belong in the Kaggle PostgreSQL master.

Every research handler and test must enforce:

```text
master_state=ABSENT is accepted
master.ensure is never called
MasterSessionBroker is never called
data.query is never called
no PostgreSQL DSN is required
no Kaggle PostgreSQL master resource is created
```

The reconciler runs inside the existing central control process. A new container, daemon,
queue service or database is not required.

## 5. Minimal SQLite model

Reuse existing `operations`, operation/effect logs, provider-effect intents/receipts,
resource leases, audit records and OAuth tables. Add one contiguous migration with four
research tables only.

### `kaggle_researches`

Current owner-facing projection.

| Column | Contract |
|---|---|
| `research_id TEXT PRIMARY KEY` | server-generated UUID, stable public identity |
| `owner_subject TEXT NOT NULL` | ownership boundary; `client_id` is audit only |
| `alias TEXT` | unique per owner, normalized human resume key |
| `title TEXT NOT NULL` | 1–200 characters |
| `goal TEXT NOT NULL` | 1–4000 characters |
| `state TEXT NOT NULL` | research state below |
| `primary_dataset_ref TEXT NOT NULL` | exact `owner/slug` |
| `notebook_ref TEXT` | private managed Notebook `owner/slug` after creation |
| `current_revision_id TEXT` | latest saved revision |
| `active_run_id TEXT` | nullable |
| `last_completed_run_id TEXT` | nullable |
| `created_at TEXT NOT NULL` | UTC |
| `updated_at TEXT NOT NULL` | UTC |

Indexes:

- unique `(owner_subject, alias)` when alias is not null;
- `(owner_subject, updated_at DESC)`;
- `(owner_subject, primary_dataset_ref)`.

### `kaggle_notebook_revisions`

Immutable research source and exact input snapshot.

| Column | Contract |
|---|---|
| `revision_id TEXT PRIMARY KEY` | server-generated UUID |
| `research_id TEXT NOT NULL` | FK |
| `revision_no INTEGER NOT NULL` | starts at 1 |
| `parent_revision_id TEXT` | previous revision, nullable |
| `state TEXT NOT NULL` | `DRAFT`, `FROZEN`, `SUBMITTED` |
| `code_file TEXT NOT NULL` | normalized relative path |
| `kernel_type TEXT NOT NULL` | `script` or `notebook` |
| `language TEXT NOT NULL` | initially `python` |
| `source_utf8 TEXT NOT NULL` | maximum 262144 UTF-8 bytes |
| `source_sha256 TEXT NOT NULL` | exact source identity |
| `runtime_json TEXT NOT NULL` | bounded accelerator/internet/timeout settings |
| `inputs_json TEXT NOT NULL` | ordered exact Dataset pins |
| `inputs_sha256 TEXT NOT NULL` | hash of the canonical ordered pins |
| `provider_source_version INTEGER` | set after Notebook push |
| `created_at TEXT NOT NULL` | UTC |
| `frozen_at TEXT` | set once |

Unique `(research_id, revision_no)`. Source, runtime and inputs cannot change after
`FROZEN`; a correction creates the next revision.

Each input pin contains:

```json
{
  "provider_ref": "owner/slug",
  "provider_version": 1,
  "visibility": "public|owner_private",
  "license": "observed provider value",
  "terms_acceptance_required": false,
  "files": [{"path": "file.csv", "size": 123, "provider_hash": null}],
  "files_manifest_sha256": "sha256",
  "attach_mode": "native_exact|native_guarded"
}
```

Dataset bytes are never stored in SQLite.

### `kaggle_runs`

One durable execution attempt and its current provider projection.

| Column | Contract |
|---|---|
| `run_id TEXT PRIMARY KEY` | public run UUID |
| `research_id TEXT NOT NULL` | FK |
| `revision_id TEXT NOT NULL` | FK to immutable source/input snapshot |
| `attempt_no INTEGER NOT NULL` | starts at 1 |
| `retry_of_run_id TEXT` | explicit retry only |
| `operation_id TEXT NOT NULL` | existing operation journal identity |
| `effect_id TEXT` | existing provider-effect intent identity |
| `state TEXT NOT NULL` | run state below |
| `provider_run_ref TEXT` | exact Notebook source/run ref |
| `provider_kernel_id TEXT` | provider identity |
| `provider_source_version INTEGER` | exact pushed Notebook version |
| `last_provider_status TEXT` | normalized bounded status |
| `failure_summary TEXT` | redacted, maximum 2000 characters |
| `next_poll_at TEXT` | durable reconciliation schedule |
| `output_manifest_sha256 TEXT` | set after validation |
| `created_at TEXT NOT NULL` | UTC |
| `started_at TEXT` | UTC |
| `finished_at TEXT` | UTC |
| `updated_at TEXT NOT NULL` | UTC |

Rules:

- one initial run per revision: partial unique `(revision_id) WHERE retry_of_run_id IS NULL`;
- one active run per research;
- a deliberate retry creates a new row and preserves the failed row;
- provider identifiers are never accepted from the client.

### `kaggle_artifacts`

Exact output metadata. Bytes are not stored in SQLite.

| Column | Contract |
|---|---|
| `artifact_id TEXT PRIMARY KEY` | server-generated UUID |
| `run_id TEXT NOT NULL` | FK |
| `path TEXT NOT NULL` | normalized output path |
| `role TEXT NOT NULL` | `summary`, `metrics`, `manifest`, `provenance`, `log`, `table`, `figure`, `other` |
| `media_type TEXT NOT NULL` | bounded media type |
| `byte_size INTEGER NOT NULL` | exact size |
| `sha256 TEXT NOT NULL` | exact hash |
| `storage_mode TEXT NOT NULL` | `kaggle` or `local_cache` |
| `cache_relpath TEXT` | private path relative to configured cache root only |
| `created_at TEXT NOT NULL` | UTC |

Unique `(run_id, path)`. Optional cache files are mode 0600 under a mode-0700 directory;
only relative paths are stored. Incomplete `.part` files are resumable and TTL-reaped.

## 6. State machines

### Research

```text
DRAFT → READY → RUNNING → REVIEW_REQUIRED → READY
                              └────────────→ COMPLETED
DRAFT|READY|REVIEW_REQUIRED|COMPLETED → ARCHIVED
```

A failed run moves research to `REVIEW_REQUIRED`; it does not erase history. A new Notebook
revision returns it to `READY`.

### Run

```text
PREPARED
  → SUBMITTING
  → QUEUED
  → RUNNING
  → COLLECTING
  → SUCCEEDED
```

Failure/recovery paths:

```text
SUBMITTING → SUBMISSION_UNKNOWN → QUEUED|RUNNING|COLLECTING|FAILED
QUEUED|RUNNING|COLLECTING → FAILED
```

Terminal rows are immutable except for bounded late artifact metadata that was already
bound to the exact run.

## 7. Idempotency and recovery

### `runs.start`

The public idempotency key is the tuple `(owner_subject, research_id, revision_id)` for the
initial attempt. The client supplies only the research selector and revision number/ID.
It does not supply task IDs, effect IDs, provider kernel IDs or claim hashes.

Inside one SQLite transaction:

1. acquire the existing fenced resource lease for the research;
2. return the existing initial run if one already exists for the revision;
3. insert `kaggle_runs(PREPARED)` and the corresponding existing operation record;
4. snapshot the exact frozen revision and input hashes;
5. persist the provider-effect intent;
6. commit;
7. change the run to `SUBMITTING` and call Kaggle.

After a successful call, bind the exact provider source/run identity and state in one
transaction. Repeating `runs.start` returns the same run.

### Lost response after provider submission

A timeout or connection loss after the Kaggle call becomes `SUBMISSION_UNKNOWN`. The
service must reconcile the exact managed Notebook ref, expected source SHA and provider
source version before another push. A blind second Notebook push is forbidden.

The existing provider-effect journal remains the source of side-effect evidence; the run
row is its semantic projection.

### New ChatGPT window

`research.list` and `research.get` resolve by `research_id`, owner alias or exact Dataset
ref. They return the active revision/run, last provider observation and recommended next
semantic operation. Chat history is not required.

### Devstand restart

On control-process startup, the in-process reconciler scans non-terminal runs with expired
leases or due `next_poll_at`, reacquires a fenced lease and resumes exact status/output
polling. It never starts the PostgreSQL master.

### Iteration

`notebooks.save` creates revision `N+1` from an optional parent. `runs.start` then creates
a new run bound to that revision. All prior revisions, runs and artifact metadata remain
readable.

## 8. Dataset access and exact attachment

Mutation authority and read/attach authority are separate.

| Dataset | Inspect/files | Notebook attach | Mutation/delete |
|---|---|---|---|
| Public Kaggle Dataset | allowed, bounded | allowed after pin | denied |
| Dataset private to authenticated Kaggle owner | allowed | allowed after pin | denied unless separately managed by existing claim-gated tools |
| Other private Dataset | denied unless Kaggle explicitly grants access | provider decision | denied |
| `orchestrator_protected` | status only | denied | orchestrator only |

No semantic discovery/attach operation creates a `claim_sha256`. Claims remain mutation
and cleanup authority for resources created/adopted by my-data-hub.

The preferred attachment source is exact `owner/slug/version`. Before enabling it, a
disposable live canary must prove that the pinned Kaggle SDK and current Kaggle service
accept and mount that exact source.

If exact-version attachment is not supported, v1 may use `native_guarded` only when:

1. Dataset inspection immediately before Notebook push still reports the pinned version;
2. the Notebook writes a mounted-input manifest;
3. the server compares it with `files_manifest_sha256` before accepting outputs.

A mismatch returns `DATASET_VERSION_CHANGED`. The service never silently substitutes the
latest version. Re-running an old unavailable version requires an explicit snapshot/mirror
operation outside the normal path.

Terms that require affirmative user acceptance are reported as
`TERMS_ACCEPTANCE_REQUIRED`; the service never accepts them automatically.

## 9. Notebook and output contract

Research Notebooks are private `mcp_managed` resources. The service may discover existing
owner Notebooks, but it mutates only a Notebook already linked to the research or explicitly
adopted under the existing exact-fingerprint claim policy. Unmanaged Notebooks are
read-only and may be copied into a new managed research Notebook.

Each successful run must produce:

```text
manifest.json
summary.md
metrics.json
```

`manifest.json` declares every output path, media type, byte size, SHA-256 and semantic
role. The server validates it and generates `provenance.json` from trusted ledger data:

- research/run/revision IDs;
- source SHA and provider source version;
- exact ordered Dataset pins and manifest hashes;
- runtime options;
- provider run identity and timestamps;
- output hashes.

Provider `complete` is not semantic success until required outputs and hashes validate.

Default result retrieval returns compact parsed summary, metrics and provenance. Optional
large artifacts remain on Kaggle until requested. Full Dataset content is never part of the
result package.

Artifact reads use `(artifact_id, offset, max_bytes)` and return `next_offset`, total size
and expected SHA-256. A partial local cache may continue after a lost response or restart;
completed bytes are verified before exposure.

## 10. Semantic MCP tools

All tools use closed schemas, bounded outputs and opaque cursors. No tool accepts internal
claim/effect/task/provider IDs from the user.

### Dataset discovery

| Tool | Purpose |
|---|---|
| `datasets.search` | search public and/or owner-private Datasets; return exact refs and bounded metadata |
| `datasets.inspect` | exact version, visibility, license/terms, access, authoritative file manifest and attach capability |
| `datasets.file.read` | optional bounded file/download helper with offset; not the normal analysis path |

### Research

| Tool | Purpose |
|---|---|
| `research.create` | create a durable research record and pin inspected input; does not run a Notebook |
| `research.list` | list owner research records with state and continuation |
| `research.get` | resume by ID, alias or exact Dataset ref and return revisions/runs/history |

### Notebook

| Tool | Purpose |
|---|---|
| `notebooks.find` | find linked managed or owner-visible candidate Notebooks |
| `notebooks.get` | return linked Notebook identity, exact stored source revision and inputs |
| `notebooks.save` | create the next draft/frozen source revision; never starts a run |
| `notebooks.inputs.set` | replace ordered inputs on a draft revision after exact inspection |

### Run

| Tool | Purpose |
|---|---|
| `runs.start` | idempotently start or return the initial run for one frozen revision |
| `runs.get` | exact durable run/provider/output state |
| `runs.logs` | bounded exact-run logs with cursor/offset |
| `runs.retry` | explicit new attempt after a terminal failure; old run remains |

### Artifacts

| Tool | Purpose |
|---|---|
| `artifacts.list` | exact output manifest and compact parsed result metadata |
| `artifacts.read` | bounded inline/chunk read with resumable offset and final hash |

This surface is compositional. There is deliberately no `analyze_dataset`, generic JSON
RPC envelope or hidden dispatcher inside `provider.resources.run`.

## 11. MCP and OAuth compatibility

The implementation must preserve:

- endpoint `https://mcp-datahub.kenigevents.ru/mcp`;
- OAuth issuer, resource/audience and owner subject;
- existing static/CIMD client registrations;
- existing refresh-token families;
- every existing tool name and every previously valid input schema;
- current `provider:read` and `provider:write` authority.

Initial semantic read tools use existing `provider:read`; source/run mutations use existing
`provider:write`. New scopes are not required for v1, so existing grants and refresh tokens
remain sufficient.

The server adds native semantic tools, advertises a catalog revision/hash through
`platform.status`, enables MCP tool-list change notifications where the client supports
them, and keeps old tools unchanged.

ChatGPT keeps an approved action-schema snapshot and does not reliably adopt new actions
from a deployment alone. Release acceptance therefore includes **Refresh actions on the
existing app**, not deletion/re-addition of the app, not MCP reconnection, not a new OAuth
login and not an endpoint change. The existing connection and refresh-token family must
remain valid before and after the refresh.

If a product environment cannot refresh the existing app's action snapshot at all, native
new tools cannot be made visible there without violating the no-monolith requirement; that
is an external catalog limitation, not a reason to add a hidden compatibility tunnel.

## 12. Security and deletion policy

- Kaggle credentials remain only in the central control process.
- Dataset bytes, credentials and large outputs never enter SQLite.
- Source is bounded and rejected when it contains obvious credentials/private keys.
- Paths are normalized, relative and traversal-free.
- Logs and provider errors are bounded and redacted.
- Semantic v1 exposes no Dataset or Notebook delete operation.
- Existing low-level delete remains exact-claim, task-owned, disposable and destructive.
- A research archive changes state only; it does not delete provider resources or evidence.
- Provider resources created by acceptance tests are deleted only with their exact
  disposable claim/receipt.

## 13. Acceptance matrix

| Scenario | Required evidence |
|---|---|
| Public Dataset | `datasets.search/inspect` finds Instacart, records exact numeric version, CC0 license, files/sizes and attach mode while `master_state=ABSENT` |
| Owner-private Dataset | inspect/files/attach succeeds through owner Kaggle credentials without manual claim; mutation remains denied |
| Attach to Notebook | provider-native input appears in the Notebook; mounted manifest matches the exact pin; full Dataset is not relayed through ChatGPT/devstand |
| Lost response after `runs.start` | disconnect after provider side effect; repeated start returns one existing run and provider shows no duplicate push |
| New ChatGPT window | close the original window; open another with the same installed app; resume by alias/Dataset ref and continue |
| Devstand restart | restart during queued/running state; SQLite recovery resumes polling without master startup |
| Logs | exact-run bounded logs are available during/after failure and survive restart through provider/compact cache metadata |
| New Notebook iteration | save revision N+1, run it, and retain revision N, both runs and prior outputs |
| Artifact download continuation | interrupt a multi-chunk read, resume from `next_offset`, verify final SHA-256 |
| No MCP reconnect | same endpoint, app installation, client registration and OAuth refresh family remain valid; only existing-app action refresh is allowed |
| Existing tools | old provider calls and valid schemas remain unchanged after deployment |
| Catalog drift | `platform.status.catalog_revision` matches the refreshed action catalog and schema contract tests detect stale Dataset-only Notebook output schemas |
| Instacart end-to-end | discover/pin → create/resume research → save Notebook → attach → start → recover → logs → summary/metrics/provenance/artifacts; no full Dataset transfer |
| PostgreSQL master absent | every scenario runs with injected sentinels proving no ensure/broker/data-query/DSN path |

Live Kaggle tests use disposable private research Notebooks. They must not invoke GitHub
Actions and must not mutate the public Instacart Dataset.

## 14. Ordered implementation

1. Add the single four-table ledger migration and store/state tests.
2. Extend the Kaggle adapter with bounded public/mine Dataset search/inspect/files,
   Notebook search/source/log/output and exact-attachment capability probe.
3. Add `KaggleResearchService` and in-process startup/due-run reconciler behind disabled
   feature flags.
4. Add native semantic schemas/tools and control-gateway routing; preserve all old tools.
5. Add compact manifest/provenance validation and resumable artifact reads.
6. Deploy with features disabled, migrate SQLite, verify `quick_check` and old tools.
7. Enable read-only discovery; run Instacart and private-Dataset acceptance.
8. Enable research/source/run/reconciler; run lost-response and restart acceptance.
9. Refresh actions on the existing ChatGPT app; verify the same OAuth connection and
   refresh family; do not delete/re-add the app.
10. Run full Instacart acceptance, then enable the semantic surface.

Feature rollback disables new semantic calls and new run starts. Migrations are not rolled
back; new tables remain inert. Already-started runs continue read-only reconciliation until
terminal so provider work is not orphaned.

## 15. File-by-file implementation handoff

| File | Change |
|---|---|
| `src/my_data_hub/control_plane/ledger/sql/<next>_kaggle_research.sql` and mirrored migration | four tables, indexes, checks and immutable/terminal guards |
| `src/my_data_hub/control_plane/ledger/models.py` | research/revision/run/artifact row and state types |
| `src/my_data_hub/control_plane/ledger/store.py` | transactional CRUD, unique start, leases, recovery scan and artifact metadata |
| `src/my_data_hub/control_plane/research.py` | new small service, state transitions, input pins, run orchestration and reconciler |
| `src/my_data_hub/providers/kaggle/contracts.py` | pinned SDK protocol for public/mine search, metadata/files, Notebook source/status/log/output |
| `src/my_data_hub/providers/kaggle/adapter.py` | bounded read adapters, exact attachment probe and exact run/output reconciliation |
| `src/my_data_hub/control_plane/app.py` | control handlers and in-process reconciler lifecycle |
| `src/my_data_hub/control_plane/adapters.py` | internal gateway methods and capability projection |
| `src/my_data_hub/mcp/kaggle_schemas.py` | closed semantic request models; no internal IDs |
| `src/my_data_hub/mcp/catalog.py` | additive tools using current provider scopes |
| `src/my_data_hub/mcp/server.py` | registrations, profile allowlists and tool-list change capability |
| `src/my_data_hub/mcp/service.py` | semantic routing to control gateway; old provider routing untouched |
| `src/my_data_hub/mcp/control_gateway.py` | authenticated internal forwarding |
| `src/my_data_hub/config.py` | feature flags, poll/backoff, source/log/artifact/cache limits |
| `scripts/verify_post_deploy.py` | SHA/master-absent/old-tools/catalog/OAuth/recovery probes |
| `tests/control/test_kaggle_research.py` | migration, state, leases, idempotency, restart and artifact metadata |
| `tests/provider/test_kaggle_research_adapter.py` | public/private discovery, exact pins, logs/output and attachment capability |
| `tests/mcp/test_kaggle_research_tools.py` | schemas, scopes, no internal IDs, separate operations and old-tool compatibility |
| `tests/integration/test_kaggle_research_recovery.py` | lost response, new process, restart, next iteration and chunk resume |
| `tests/oauth_server/test_chatgpt_cimd.py` | same registration/grant/refresh-family acceptance after catalog expansion |

The migration filename must use the next verified contiguous number at implementation time;
no migration number is fabricated by this docs-only stage.

## 16. Readiness verdict

No owner product decision blocks implementation. The exact current Instacart version and
exact-version attachment behavior are live provider observations and are explicitly covered
by read-only discovery/capability gates.

```text
READY_FOR_CODEX
```

This verdict applies to implementation readiness only. Production code, deployment and the
live end-to-end research workflow are not implemented by this documentation PR.
