# Lane H6-MASTER-SCENARIOS Results

## Status
implemented / live execution not claimed

The fixed master-scenario backend and production wiring are committed. This
result is a code/gate result only: the lane did not possess production Kaggle,
host-supervisor, or PostgreSQL credentials and therefore does **not** report a
scenario `LIVE_PASS`.

## Requirement status

- **FM04 — Done (implementation):** the preboot task is durably bound to the
  exact empty-baseline run and the Notebook executes the source-owned revision
  and canonical-relation zero-row probes.
- **FM07 — Done (implementation):** owner-host CAS drives twenty same-key
  ensures and validates a single physical provider run/operation.
- **FM08 — Partial:** task-bound callback suppression persists the exact
  heartbeat/body, keeps retries unacknowledged, survives a real independently
  supervised control restart, records before/after process boot UUIDs, then
  projects the exact duplicate and marks `REPLAYED`. This is not the required
  abrupt *master* termination/recovery with distinct provider runs, so FM08
  remains blocked rather than projecting invented old/new run identities.
- **FM09 — Done (implementation):** exact stored callback replay, revoked-token
  replay, stale-epoch replay, and before/after control-state hashes are fixed.
- **FM10 — Done (implementation):** the runtime ACKs suspension of heartbeat,
  DatabaseGate and tunnel/session renewal; the brokered H1 probe waits for real
  expiry, observes both deferred and fresh immediate SQLSTATE 55000/INERROR
  guards, rolls back, and proves invalidated write authority plus revision/row/
  outbox/audit invariance through a private atomic completion journal.
- **FM11 — Partial (assembly):** the concrete four-denial probe and replacement
  binder are implemented and the unified opt-in fails closed unless a concrete
  old-runtime adapter is injected. Production capture of the protected old
  credential/tunnel context remains a host deployment integration prerequisite;
  no live denial receipt is claimed here.
- **FM12 — Done (implementation):** only a STOPPED operation with its current
  VERIFIED handoff checkpoint can produce the fixed terminal evidence.
- **FM24 — Done (implementation):** FM24 is runtime-only claimed. A private
  runtime journal executes at most one due five-minute step per active-loop
  poll, preserving ordinary heartbeat/drain polling. Each step performs an ACKed
  runtime heartbeat, real DatabaseGate and tunnel renewals, broker credential
  rotation, fixed PostgreSQL read, explicit old-role drop, and real stale-login
  reconnect denial. Completion is accepted only after 12 steps and 3600–5400
  system-monotonic seconds.
- **Unified request/status — Partial (deployment input):** the owner-only
  `acceptance.scenario.request/status` MCP surface and control assembly exist;
  opt-in is hidden/fail-closed without concrete checkpoint launcher/catalog,
  FM08 supervisor, FM10 session directory, and FM11 adapter. The checkpoint
  entrypoint contract is integrated, but this lane did not fabricate a provider
  launch in the absence of its protected production inputs.

## 2026-08-11 remediation

- centralized control Kaggle authentication now accepts either a private access
  token or a complete legacy username/key pair; partial and mixed profiles fail
  closed, and remote MCP/OAuth/Notebook launch metadata receives no legacy key;
- FM10 and FM11 status receipts project newly validated observations. FM11
  requires consecutive epochs, registry resolution to the replacement, and
  distinct exact old/new provider run/kernel identities;
- master carrier status validates exact push source identity and projects real
  terminal file/tree/receipt hashes only when the official-adapter terminal
  recovery audit exists. The actual filename is
  `my-data-hub-master-terminal.json`; active runs retain null output fields;
- MCP `acceptance.scenario.request` now forwards `target_operation_id`.

The production `serve()` assembly remains blocked on two absent concrete
primitives: a dedicated checkpoint-acceptance launch/status authority (it may
not borrow an ACTIVE master runtime token), and a task-keyed FM11 pre-drain
context supervisor providing the four old-epoch denial clients. Enabling the
operator surface without those primitives would either crash startup or
overstate capability, so this lane does not set a deploy opt-in flag.

## Relevant implementation commits

- `346ec58` — durable runtime control directives
- `b96fb6b` / `0049d35` — callback suppression expiry/retry/replay hardening
- `1054756` — brokered FM10 composition
- `b6fcde9` — exact FM11 replacement binding
- `5ab001e7586b890e54bda2d38fdc11d3f328ca61` — cooperative live FM24 runtime
  assembly and fail-closed unified control assembly

Worker implementations integrated by the root lane provide the FM08 host
supervisor, FM10 fixed denial adapter, FM11 denial adapter, and FM24 resumable
port. Migration 020 is mirrored between repository and packaged control SQL.

## Verification on `5ab001e`

- focused acceptance/soak/notebook/supervisor suite: **85 passed**
- full `uv run pytest -q`: **PASS at 100%, 3 skipped**
- Ruff: **PASS**
- repository validator: **3598 checks, 0 errors**
- `python -m compileall -q src tests`: **PASS**
- `git diff --check`: **PASS**
- worktree: clean after commit

## Evidence boundary

No live Kaggle run, disposable PostgreSQL run, host restart, or one-hour soak was
executed by this lane. Consequently this document reports implementation and
local verification, never `LIVE_PASS`. The deployed operator tool remains
unavailable until all fail-closed production dependencies are supplied; FM11's
protected pre-STOPPED context capture is the remaining internal assembly gap.

## 2026-08-11 checkpoint authority closure

- control migration 021 persists one FM05/FM14/FM15 owner-task launch, exact
  request/config hashes, principal/client binding, one-time token hash, exact
  status-Dataset claim, provider run, cleanup receipt and terminal result;
- the provider runtime authenticates with dedicated
  request/task/attempt headers and `acceptance:operate`, never a borrowed master
  instance/epoch;
- the concrete launcher uses the single control-owned Kaggle adapter, persists
  before effects, creates a unique private disposable exact-version status
  Dataset (`kaggle_run.json` plus fixed helper), launches the protected evidence
  Notebook with exact numeric runtime/template/status/verifier inputs, and
  reconciles only the exact official run/output;
- callback/session transport no longer depends on a Kaggle User Secret root.
  The raw one-time token exists only in the provider input Dataset and Notebook
  environment; control stores its SHA-256. Existing Bearer/header transport and
  redacted JSONL behavior are retained;
- stable events-bot-style UIDs and typed runtime events persist custom
  kernel/preflight/alive/report/resource/terminal phases, heartbeat elapsed and
  progress counters, with exact-body duplicate replay/tamper rejection and a
  bounded status projection. Provider COMPLETE alone cannot bypass this event
  evidence;
- the shared evidence Notebook slug is serialized by one exact task resource
  lease included in the status input; terminal cleanup releases only its bound
  lease ID/holder/epoch, while an ambiguous launch safely ages out;
- only provider-side checkpoint API access may use reviewed User Secret names,
  with exactly one access-token name or a complete legacy username/key name
  pair. Values never enter source, Dataset metadata, callbacks, logs or receipts;
- terminal reconciliation deletes the status Dataset by its exact task claim and
  persists the absence receipt. Ambiguous Notebook-push response is terminal
  failure and is never blindly retried or unsafely cleaned;
- operator deployment remains default-off and needs the exact protected-effects
  acknowledgement plus a private deployment document. FM08 remains BLOCKED
  pending real abrupt-master termination/recovery identities; no synthetic
  lifecycle evidence was added.

## 2026-08-11 PostgreSQL-master callback authority closure

- control migration 022 atomically binds one admitted master operation/run/
  attempt to a cryptographically random 256-bit callback token hash, a fixed
  900-second creator claim, and the exact resource-lease fencing token before
  any provider mutation. The raw token is written only to `kaggle_run.json` in
  a private disposable `ORCHESTRATOR_PROTECTED` status Dataset;
- the status Dataset is attached to the master Notebook by exact numeric
  version alongside the reviewed asset Dataset. The single control-owned
  Kaggle adapter uses the legacy-safe pending-runtime-attestation launch path;
  credentials remain central and the runtime cannot become ACTIVE or receive
  database authority until authenticated `service.ready` reports the exact
  push-time executable source SHA-256;
- twenty same-key callers produce one status Dataset and one provider run.
  Followers observe the durable creator claim and never create a replacement
  token or repeat either mutation. An expired create-side-effect ambiguity
  revokes the actual token, fails the operation, releases the exact persisted
  lease, and deletes only a deterministically proven task claim;
- terminal callback processing and the five-second control loop both drive a
  bounded claim-based cleanup reconciler. A lost delete response leaves a
  durable `CLEANING` state and resumes the exact delete after claim expiry;
- the runtime preserves the donor contract through stable `event_uid` values,
  typed kernel/preflight/alive/resource events, ACK-bound heartbeat lease
  renewal, header-only Bearer authentication, and the existing fsynced redacted
  JSONL spool. Callback tokens are absent from source, ledger receipts, logs and
  terminal output;
- obsolete callback-root and callback-User-Secret configuration was removed.
  Provider launch authentication remains the existing centralized automated
  Kaggle access-token or complete legacy credential path and is never copied
  into the PostgreSQL master Notebook.

This is implementation and local-gate evidence only. No live provider mutation
or scenario `LIVE_PASS` was performed. At that checkpoint FM08 remained blocked
on abrupt termination/recovery; the corrective section below closes its
internal action path while live evidence remains absent. FM09 exact
revoked-token replay now requires a runtime-owned exact replay capability; the
control plane intentionally does not persist or reconstruct raw bearer tokens.

## 2026-08-11 FM08 abrupt-master recovery implementation

- control migration 023 persists one task-owned old/recovery plan before any
  termination: exact old operation/run/epoch, deterministic distinct recovery
  Notebook ref/idempotency key, termination receipt, replacement operation/run,
  recovery receipt, and terminal state;
- the official central Kaggle adapter issues exactly one delete for the exact
  numeric source-attested old run. A lost response is reconciled by exact
  absence and never causes a second destructive call;
- after provider absence, one ledger transaction fences the old operation,
  service and attempt and revokes the old callback token. Only then may a
  distinct task-derived Notebook ref enter the next consecutive epoch;
- the recovery uses the normal protected status Dataset and mandatory runtime
  source attestation. Startup reconciliation selects the recovery-specific
  assets from the durable migration row, so it does not revive the deleted run;
- after the real host control-process restart, the already-authenticated
  captured heartbeat is projected by exact task/event/body-hash identity. Raw
  body bytes and the revoked Bearer are neither reconstructed nor exposed;
- typed FM08 evidence now requires distinct old/new operation IDs, provider run
  refs and kernel IDs, consecutive epochs, abrupt-termination and recovery
  receipt hashes, and distinct control boot UUIDs before it can validate.

Focused unit/integration gates cover one-shot lost-response reconciliation,
fence/retry idempotency, revoked-token stored-event projection, supervisor call
ordering, schemas and migration continuity. No live provider mutation or host
restart was performed, so no FM08 `LIVE_PASS` is claimed here.

## 2026-08-11 FM11 production context and denial composition

- control migration 024 journals the task/command-bound pre-STOPPED capture
  intent while the old operation is still ACTIVE, including only the exact old
  binding, runtime-token SHA-256, opaque held-session handle, public tunnel
  certificate digests, fixed 900-second expiry and release receipt;
- `TaskBoundOldEpochDenialFactory` is resolved before the host executor checks
  for STOPPED. It opens the restricted H1 operator session and snapshots the
  exact current tunnel identity once, caches one probe per task, and refuses to
  reconstruct a lost process-private credential context after rotation;
- after clean drain/current VERIFIED checkpoint and a consecutive ACTIVE
  replacement, canonical ledger admission proves the old runtime token revoked
  and registration/renewal fenced, the superseded Directory credential must be
  absent, H1 must return SQLSTATE 55000 in rollback-only state with unchanged
  revision, and the broker must show the original certificate serial revoked;
- the tunnel broker/IPC accepts only two fixed metadata-only FM11 actions:
  current identity snapshot and retired-denial proof. It exposes no generic
  status/action, private key, database credential, payload or caller duration;
- default-off production app assembly now constructs the task factory from the
  existing session directory and existing root-owned tunnel authority, and
  fails startup when that concrete structured authority is unavailable.

Focused tests cover capture-before-STOPPED ordering, opaque handle persistence,
same-task reuse, structured broker revocation, migration continuity and the
existing fixed H1 rollback probe. No real rotation was executed, so this is not
live FM11 PASS evidence.

## 2026-08-11 FM09 hash-only stored replay

- `ControlLedgerStoredReplay` no longer has current or retired bearer fields and
  never asks the event ledger for callback bytes;
- the protected selector returns only one exact ACKed event UUID/body SHA-256;
  immutable `runtime_events` plus `runtime_event_dedup` yield the canonical
  `duplicate` disposition by exact run/attempt/epoch identity;
- the fixed denial projection compares canonical current and revoked token hashes
  internally, verifies the selected operation/current service epoch is ACTIVE,
  and proves both a genuinely retired token and the same attempt at epoch-1 are
  rejected. Hashes, bodies and bearer values are not returned;
- state is hashed before and after all three observations, so a successful FM09
  receipt still requires exact equality and cannot disguise a projection change.

Focused tests use an actual epoch-2 active ledger plus a genuinely revoked prior
identity, validate duplicate/retired/stale results, reject an altered body hash,
and assert the production adapter has no raw-token fields. This is local
implementation evidence only, not live FM09 PASS evidence.
