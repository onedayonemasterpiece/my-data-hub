# Master lifecycle production executor

`my_data_hub.acceptance.master_production` is the production execution layer
for the fixed FM04/FM07/FM08/FM09/FM10/FM11/FM12/FM24 protocol. It does not add
a generic fault API. The operator request contains only the task identity,
closed scenario enum, idempotency key, source revision and (for an already
ACTIVE master) exact operation ID.

## Trust boundaries

There are two non-interchangeable claim identities:

1. The Kaggle runtime continues to claim through the authenticated
   `/internal/runtime/master-acceptance/...` endpoint and can only claim a
   command bound to its exact run, attempt and epoch.
2. Control-host work uses `OwnerBoundAcceptanceClaimPort`. Its implementation
   must require `acceptance:operate`, bind principal and OAuth client, and CAS
   task + command + operation + receipt hash. It must never mint or reuse a
   runtime token. `ControlLedgerOwnerAcceptanceClaims` implements that contract
   on append-only control migration 018. Migration 018 also prevents the
   runtime-token claim from taking a host command.

The unified operator adapter has exactly two surfaces:
`acceptance.scenario.request` and `acceptance.scenario.status`. It is not included
in a reader catalog and has no list endpoint.

## Fixed scenario execution

| Scenario | Concrete execution |
|---|---|
| FM04 | Control ensures the exact empty boot and binds it after ACTIVE. The notebook factory then probes revision zero and a source-pinned relation allowlist; no query comes from the request. |
| FM07 | The control executor performs exactly 20 concurrent same-key ensures through the official Kaggle provider, verifies one operation/epoch and the applied trigger receipt's numeric kernel identity, then completes through the owner claim CAS. |
| FM08 | A narrow supervisor suppresses the next task callback, terminates the exact persisted old Kaggle run once, atomically fences its epoch, admits a distinct source-attested next-epoch run, performs a real process restart with a changed boot UUID, projects the immutable stored callback by task/event/hash identity, and proves the recovery service is ACTIVE. |
| FM09 | `ControlLedgerStoredReplay` selects one canonical ACKed event identity/hash from the protected ledger and hashes operation/service/event state before/after. The immutable dedup row returns the exact duplicate disposition without exporting its body. Canonical token-hash/revocation and attempt/current-epoch state prove that a genuinely retired token and the same attempt at epoch-1 are rejected; no current/retired raw bearer is persisted, reconstructed, accepted by the adapter, or exposed to the request/operator. |
| FM10 | `PostgresH1ExpiredLeaseDenialProbe` validates the restricted H1 operator login and exact epoch, suspends only that runtime's renewal, waits 60–900 real seconds, reads back `lease_until <= clock_timestamp()`, invokes H1's mandatory pre-DML epoch assertion before a fixed bounded UPDATE, observes SQLSTATE 55000 plus PostgreSQL `INERROR`, rolls back, and proves the canonical revision unchanged. No caller SQL is accepted. |
| FM11 | After the old operation is STOPPED, ledger history proves DRAINING preceded terminal, the verified handoff checkpoint is selected, a fixed-key replacement is ensured, and four exact old-binding probes prove renew/register/write/tunnel denial. |
| FM12 | The finalizer accepts only a STOPPED operation whose exact checkpoint is the current VERIFIED candidate. VERIFIED is reachable only after upload readback and independent restore smoke. |
| FM24 | A fixed 3600-second monotonic loop performs twelve 300-second lease+tunnel renewals, credential rotations, bounded reads and stale-session reconnect denials. Duration and counts cannot be supplied by the caller. |

## Notebook hook

`run_master(..., acceptance_effects_factory=...)` builds the factory only after
the exact epoch is ACTIVE and the local PostgreSQL connection exists. Passing
both a prebuilt effect object and a factory is rejected. Direct library callers
must choose explicitly; the production notebook `main()` supplies the fixed
factory.

FM11/FM12 are owner-host claimed while the old epoch is ACTIVE. The exact
runtime can then read only a boolean drain directive from
`/internal/runtime/master-acceptance/{run_id}/{attempt_id}/drain-directive`.
It runs the ordinary gate-close/checkpoint/terminal path; the owner host later
finalizes from STOPPED + current VERIFIED checkpoint evidence. The directive
contains no action, fault, timeout, SQL or payload bytes.

Migration 018 limits runtime claims to FM04, so a notebook cannot race or impersonate the
owner-host executor for FM07/FM08/FM09/FM10/FM11/FM12/FM24.

## Evidence rule

Contract/unit tests are never LIVE_PASS evidence. The task status exposes
`live_pass=true` only after the durable production receipt reaches `PASSED`.
Real Kaggle IDs, PostgreSQL observations, host boot UUIDs and hashes must come
from the official adapters during execution.


## Unified checkpoint launch

The same owner-only pair dispatches FM05, FM14, and FM15 through one injected
`CheckpointAcceptanceLaunchPort`. The public request still accepts only
`task_id`, the fixed scenario enum, `idempotency_key`, and `source_revision`; it
has no SQL, bytes, duration, clock, fault, resource, or arbitrary action field.

The control host persists and compares metadata only: an owner-fixed private
evidence Notebook, task-owned candidate Dataset, exact numeric protected
template Dataset version plus claim/manifest/content hashes, and (only for
FM05/FM15) exact numeric verifier Dataset claim/source hash and a separate
verifier Notebook. The launch identity is a dedicated `acceptance:operate`
service identity, not a fabricated ACTIVE runtime identity. Only the User Secret
name `MY_DATA_HUB_RUN_SECRET` crosses the launch contract; its value never
appears in request or status. Timeout is fixed at 900 seconds.

No `/kaggle` path exists in the control-side launch model. The task-owned
Notebook alone maps claimed inputs into its fixed `/kaggle/working` paths and
runs the entrypoint. Status returns `LIVE_EVIDENCE_READY`, never matrix `PASS`,
only after an official-adapter numeric run locator, bounded output
file/tree/receipt hashes, exact config/result hashes, and the fully validated
checkpoint receipt with initial/final HEAD all reconcile. A missing scoped
acceptance execution authority blocks before provider mutation.

## FM11 old-epoch probe composition handoff

`TaskBoundOldEpochDenialFactory` is the default-off control composition around
`ProductionOldEpochDenialProbe`. It is not an MCP tool. Migration 024 persists
only the owner-task capture intent, exact old binding, bearer hash, opaque held-
session handle, public tunnel certificate identity, fixed expiry and release
receipt; the database credential and live connection remain process-private.

The production composition order is fixed:

1. While the selected old runtime is still ACTIVE, capture an
   `OldRuntimeProbeContext` bound to the acceptance task and exact old
   operation/run/attempt/service/master/epoch. Store only the runtime bearer
   SHA-256, a UUID handle for a protected pre-opened H1 operator session, and
   the old tunnel certificate serial plus principal/public-key digests. Raw
   bearer, DSN, password, certificate and private key remain inside their
   narrow clients.
2. The task factory constructs `ProductionOldEpochDenialProbe` with
   `replacement=None` and returns it to `ProductionControlHostEffects` before
   the latter tests for STOPPED. The runtime directive then performs the
   ordinary drain, verified checkpoint and STOPPED transition.
   Never rotate first and reconstruct an old context from durable state.
3. After `ControlPlaneMasterRuntime.ensure` returns the exact ACTIVE
   replacement, build `ReplacementEpochContext` from its operation/master/epoch
   and the already selected current VERIFIED checkpoint (numeric Dataset
   version and manifest hash), then call the probe's one-shot
   `bind_replacement`. Exact replay is allowed; rebinding is denied. This is the
   post-`ensure` composition handoff in `ProductionControlHostEffects._old_epoch`.
4. The runtime client projects the exact revoked-token/current-epoch decision
   from the same canonical admission state used by callback/renewal routes; the
   credential client additionally proves the superseded Directory credential
   can no longer bind; `PsycopgRetiredBoundedWriteClient` executes only H1's
   fixed `assert_session_write_epoch()` followed by the no-row bounded UPDATE;
   and the tunnel broker's fixed FM11 IPC action verifies the old serial is
   revoked under the exact consecutive active replacement. No generic tunnel
   status/action or arbitrary certificate material is exposed.
5. The adapter requires normalized exact denials, revoked token evidence,
   SQLSTATE `55000`, PostgreSQL `rollback_only`, unchanged canonical revision,
   and the original certificate identity. It then releases the protected
   session/tunnel handle and drops the context reference. The TTL is a fixed
   monotonic 900 seconds and cannot be supplied or widened by a request.

An exact retry after a lost completion response returns the same
`OldEpochDenials` and receipt hashes without repeating physical probes. Only
hashes, UUIDs, numeric epochs/checkpoint versions, denial codes and booleans are
representable in the sanitized internal receipt. Its schema is
`schemas/acceptance/old-epoch-denial-receipt.v1.schema.json`.

The four clients and production `create_app` assembly are concrete and fail
closed if the session directory or structured tunnel authority is absent. Unit
and integration tests remain implementation evidence only; FM11 still needs one
real stopped-old/active-new rotation before any live PASS claim.
