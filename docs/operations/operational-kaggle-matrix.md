# Operational Kaggle acceptance matrix

`scripts/provider/operational_kaggle_matrix.py` is the executable acceptance
contract for the 24 scenarios mandated by the source completion prompt. It does
not reuse the generated-Notebook platform-smoke matrix and it never promotes a
smoke result into operational evidence.

## Fail-closed entry conditions

The `run` command checks for a modern Kaggle API token before it creates a plan,
ledger, adapter, invokes the operational driver, or performs a provider
mutation. If the token is absent, it writes a bounded blocker with
`mutations_started: 0` and exits 78. Legacy `kaggle.json` does not satisfy this
gate.

The repository includes the trusted bounded driver
`scripts/provider/operational_kaggle_driver.py`. The provider workflow selects
it by default:

```text
MY_DATA_HUB_OPERATIONAL_DRIVER_JSON=["python","scripts/provider/operational_kaggle_driver.py"]
```

The value is parsed as a JSON argv array, never as shell text. For each scenario
the runner passes `--request REQUEST.json --result RESULT.json`. The v2 request is
non-secret and binds the matrix/commit/scenario/task-run identity, required
assertions, lifecycle gates, soak bounds, phase, and `resume_only`. The result
validates as `my-data-hub-operational-kaggle-driver-result.v2` and either:

- return `READY` with an exact run locator plus acceptance task/claim and
  output-read receipt for outer reconciliation;
- return `PASS` only for a completed cleanup phase, or for an older scenario
  whose evidence run was owner-managed and has no acceptance cleanup contract;
- return `FAIL` after an action may have started but cannot be reconciled; or
- report `BLOCKED` with an uppercase blocker code and the concrete missing
  integration dependency.

The driver has one exact executor entry for every FM01–FM24 scenario. It checks
the required reader/operator/migration/provider OAuth profile and exact MCP tool
catalog, then performs bounded non-mutating observations until the whole
scenario can be bound to an exact evidence Notebook run. It never starts a
mutation and then returns BLOCKED: every BLOCKED result requires
`mutations_started: 0`. A mutation that was accepted but cannot be reconciled
terminally is a typed `FAIL`, not a blocker.

Existing safe production surfaces are wired now: FM01 Dataset lifecycle,
FM02 Notebook lifecycle, FM03 bounded runtime history, FM05/FM14/FM15 through
the owner-only checkpoint acceptance launcher, FM06 durable restore,
FM16--FM19 and FM21 through the single resumable production data workload,
FM22 Dataset plus Notebook lifecycles, FM23 protected-resource denial, master/
checkpoint/provider status, stale-epoch denial, blogger accounting/statistics,
embedding coverage, claim-gated rotation, and the FM20 signed-host-evidence
cold-start path.
Unresolved mutation/fault interfaces use a
scenario-specific code such as `CHECKPOINT_CORRUPTION_FAULT_API_MISSING` or
`E5_WORKER_SUBMISSION_TOOL_MISSING`; the generic
`OPERATIONAL_DRIVER_INTERFACE_MISSING` blocker is not used by the trusted
driver. This remains a blocked operational state, not a readiness claim.

The current internal gaps are explicit in the executor registry. They include
empty-bootstrap selection, callback/lease/replay fault controls, drain control,
and the accelerated soak controller. Privileged actions
are not attempted until their terminal exact-run evidence contract exists.

### Evidence-plane scenarios and two-phase cleanup

`MY_DATA_HUB_OPERATIONAL_EVIDENCE_DRIVER_JSON` validates as
`my-data-hub-operational-kaggle-evidence-driver.v1`. It supplies the exact
provider owner, the optional FM03 runtime `(run_id, attempt_id, epoch)` key, and
optional owner-fixed production data-workload paths. It
contains no credential or provider locator. Missing/invalid configuration is a
pre-action `BLOCKED` result with `mutations_started: 0`.

FM01 and FM22 derive distinct deterministic Dataset and Notebook subtask UUIDs.
Their Dataset lifecycle performs private create, version, exact readback, and
inline claim-bound cleanup. The evidence Notebook uses `dataset_inputs: []`:
the current provider authority requires an `mcp_managed` input claim to share
the Notebook task ID, which intentionally conflicts with the distinct subtask
identity. The matrix therefore proves both lifecycles but does not claim that
the evidence Notebook consumed the disposable Dataset.

FM03 reads at most 200 metadata-only events and requires every event to match
the configured run, attempt, and epoch, unique event/sequence identities, at
least one `runtime.heartbeat`, and exactly one final `runtime.terminal`. FM23
requires `evaluated=true`, `protected=true`, the exact protected-denial reason,
and `mutation_attempted=false` before its distinct evidence Notebook launches.

For FM01, FM02, FM03, FM06, FM16--FM19, FM21--FM23 the driver returns `READY`, never
PASS, after the acceptance claim is `SUCCEEDED/PENDING`. The outer matrix then:

1. independently reconciles the exact numeric run with its one real
   `KaggleProviderAdapter`;
2. downloads only `operational-result.json` and compares its file and output
   tree hashes with the durable `OUTPUT_READ` receipt;
3. appends a mode-0600 reconciliation fence; and
4. invokes the driver `CLEANUP` phase with the exact task claim, provider run,
   and output-read receipt.

Only a durable claim with `cleanup_state=COMPLETE` produces cleanup PASS and a
PASS scenario receipt. A lost cleanup response is re-read through
`provider.acceptance.claim.get`; any unresolved post-mutation ambiguity is
FAIL, never BLOCKED. A rerun consumes the append-only reconciliation fence and
never launches a second logical run. The fence contains the already validated
receipt draft, so a retry after successful deletion does not attempt to
download the deleted Notebook again.

### Matrix-wide FM16--FM19/FM21 production data workflow

The `data_workload` configuration binds absolute non-symlink paths for one
`operational-data-workload-plan.v1`, one production config, and one durable
mode-0600 state file. The plan matrix UUID and source commit must exactly equal
the operational request. The driver validates the control/reader/operator
credentials and persists the initial metadata-only matrix state before it may
invoke `scripts/provider/data_workload_evidence.py`. The production deadline is
limited to 6,900 seconds so the child entrypoint cannot outlive the outer
7,200-second driver budget.

That single state machine performs FM16 v1 quarantine, explicit owner-authorized
v2 replay and checkpoint; FM17 rotation/restore and logical equality; one shared
FM18/FM19 two-model request and checkpoint; and FM21 fixed insert/checkpoint,
delete/checkpoint, then zero-row preview. The driver accepts only the exact
ordered `EVIDENCE_READY` bundle, requires FM18 and FM19 to share their first
request ID and have distinct worker task IDs, and splits the five bounded
requirement receipts into five task-run-bound evidence Notebooks. Each is then
independently downloaded and cleaned by the normal two-phase protocol.

After the exact v1 quarantine and duplicate-review projections are durably
reconciled, FM16 may return `FM16_AWAITING_OWNER_AUTHORIZATION` as BLOCKED/0 for
the *current* invocation. The matrix writes a distinct append-only
`.owner-pause.json` fence and stops before dependent scenarios; it does not
write or later overwrite the final scenario receipt. A later run validates
that exact pause fence, reuses the existing launch fence and
same state/task, and supplies the owner-created mode-0600 envelope. It never
creates a synthetic decision. Any other capability loss or ambiguous response
after a persisted action phase is FAIL with nonzero mutation accounting, not
BLOCKED.

`EVIDENCE_READY` remains non-live intermediate metadata. PASS still requires a
real provider lifecycle claim, terminal COMPLETE Notebook, independent outer
run/output reconciliation, and durable claim-bound cleanup. No checked-in test
or example is live evidence.

### Claim-gated restore and rotation

FM06 and FM13 can use the real `checkpoint.restore.request`,
`master.rotation.request`, and `operation.get` paths only when an owner has
already launched a disposable evidence Notebook for the exact planned task.
The non-secret keyed claim document in
`MY_DATA_HUB_OPERATIONAL_EVIDENCE_CLAIMS_JSON` validates against
`my-data-hub-operational-kaggle-evidence-claims.v1`. The driver does **not**
trust locator fields from that document: it sends the claim hash to
`provider.resources.read`, requires the returned task UUID, provider ref,
numeric run ref/kernel ID, source version, and source SHA to match the matrix,
and only then builds the checkpoint/epoch/revision-bound action request.

On a launch-fenced `resume_only` invocation, the keyed claim must carry the
previously accepted `operation_id`. The driver first uses `operation.get` to
bind that exact restore/rotation kind, then re-reads the provider claim; it
never recomputes the operation from a possibly newer checkpoint HEAD and never
creates a replacement action. A missing, lost, or mismatched resume identity is
typed `FAIL`, not BLOCKED. For FM06, `DURABLE_COMPLETE` is followed by
`master.status`; the driver requires the exact ACTIVE `provider_run_ref`,
`provider_kernel_id`, epoch, and canonical revision equal to the selected
checkpoint before launching its post-action evidence Notebook. It never uses
the verifier Notebook run as the restored master identity. An accepted action
that fails, is fenced/orphaned, times out, or loses its receipt produces typed
`FAIL` with `mutations_started: 1`; it is never rewritten as external BLOCKED.

This closes the client-side request, polling, and evidence binding only. It does
not claim a live run: the deployed MCP must expose the exact action schemas and
consumer, and the owner must supply claims from real pre-launched verifier
Notebooks. No such provider evidence is checked in.

### Owner-launched checkpoint acceptance

FM05, FM14, and FM15 use the exact operator-only
`acceptance.scenario.request`/`acceptance.scenario.status` pair. The first
status read is a non-mutating preflight. A fresh request binds the planned task
UUID, fixed FM requirement, idempotency key, and exact 40-hex source revision;
the driver then polls the same task for at most 900 seconds. `resume_only`
performs status reconciliation only and never starts a replacement task.

The driver accepts only `LIVE_EVIDENCE_READY` from the official adapter with a
numeric private Notebook run locator, exact source/result/output hashes, and
the typed `checkpoint-acceptance-operational-result.v1`. It checks the fixed
stage sequence and exact HEAD transition: FM05 advances generation/current/
previous through verified restore, while FM14 and FM15 preserve identical HEAD
projections after the expected rejection. A lost request response is reconciled
by the same task; invalid or unresolved post-request evidence is FAIL with
nonzero mutation accounting.

The returned driver PASS is only an evidence locator. The outer matrix
independently reconciles the Kaggle run, compares the downloaded file/tree
hashes, parses the typed checkpoint result, and derives the required assertion
hashes before writing a scenario PASS. The checkpoint launcher has no separate
acceptance cleanup operation, so this path records `cleanup_state=NOT_REQUIRED`
rather than inventing a cleanup receipt. The MCP catalog is owner-only,
requires `acceptance:operate`, and is hidden unless
`MY_DATA_HUB_MCP_ACCEPTANCE_SCENARIOS_ENABLED=true`; production also must inject
the concrete unified launcher. Missing opt-in/injection remains a pre-action
toolset BLOCKED. No checked-in receipt is live evidence.

### FM20 signed reboot and remote cold search

FM20 never initiates a reboot through MCP. An owner first runs the documented
three-stage deployment evidence collector, performs the separately authorized
host reboot, and supplies its fresh Ed25519-signed
`my-data-hub-deployment-evidence.v2` receipt inside the existing
`MY_DATA_HUB_OPERATIONAL_EVIDENCE_CLAIMS_JSON` document. The keyed document must
also contain an FM20 claim for an already-running exact evidence Notebook.

The nested `fm20_evidence` object validates against
`operational-kaggle-fm20-evidence.v1`. It binds the trusted public key/key ID,
owner source identity, independently reviewed source-tree SHA-256, exact
per-service immutable image IDs, and one bounded blogger query. The shared
post-deploy verifier checks the signature, freshness, source/release equality,
expected hashes/images, distinct signed before/after boot IDs, systemd unit,
linger and the exact recovered service set before any MCP mutation.

Only after `provider.resources.read` reconciles the FM20 Notebook locator does
the reader have to report an exact `ABSENT` master. The driver then calls the
existing operator `master.ensure`, requires an unambiguous durable UUID receipt,
waits boundedly for an exact ACTIVE epoch/revision, and performs
`bloggers.search` with `limit: 1`. PASS requires one item and search epoch and
revision equal to the ACTIVE status. Search rows and the query are not emitted
in the driver result; only bounded hashes/counts are retained.

On `resume_only`, the FM20 claim must carry the previously accepted ensure UUID.
The driver first reconciles it with `operation.get` as `ensure_master` and never
calls `master.ensure` again. Missing evidence before action is BLOCKED with zero
mutations. Any ambiguous ensure response, lost post-action observation, failure
to reach ACTIVE, or malformed/empty search result is FAIL with
`mutations_started: 1`.

### Fault/action blockers retained intentionally

- FM08/FM09: callback ingestion accepts exact replay from its per-run bearer,
  but there is no safe callback-suppression or stale-output injection tool.
- FM12: the master drains itself at its natural lifecycle boundary; there is no
  authenticated clean-drain/checkpoint/stop request or credential revocation
  endpoint.
- FM24: status polling alone cannot prove heartbeat, read, checkpoint, recovery,
  or credential-rotation counts. No accelerated soak controller/event stream is
  exposed, so the 3,600–5,400 second scenario remains BLOCKED before mutation.

## What counts as PASS

A driver-reported READY/PASS is only a locator. The runner constructs exactly one
`KaggleProviderAdapter` and reuses it for every scenario. It reconciles the
planned task ID/ref/source hash, compares the exact numeric Kaggle run ref,
kernel ID and source version, requires terminal `complete`, and downloads the
exact `operational-result.json` from that run. The Notebook output must contain
exactly the scenario's required assertions and lifecycle events. Every
assertion is bound to an evidence SHA-256. For two-phase scenarios this is
still insufficient: the exact acceptance cleanup must be durably COMPLETE.

The summary counts unique numeric provider run refs and provider kernel IDs. It
does **not** count internal task UUIDs. PASS requires all 24 scenario receipts,
at least 15 distinct refs and kernel IDs, and all lifecycle gates:

- at least three distinct master boots;
- at least two clean rotations with distinct old/new run refs and consecutive
  epochs;
- an abrupt master termination;
- a control-plane restart during an unfinished operation;
- a host reboot;
- exactly one accelerated soak lasting 3,600–5,400 seconds with positive
  heartbeat, read-query, checkpoint and recovery counts.

Injected/fake adapter paths are hard-coded to produce only BLOCKED receipts, so
unit tests cannot create live PASS evidence.

Driver BLOCKED receipts additionally record hashed capability checks and a
hash of bounded safe observations. Raw business rows, OAuth tokens, database
credentials, or provider output are never copied into the driver receipt.

## Scenario contract

| ID | Operational assertion | Required production boundary |
|---|---|---|
| FM01 | private Dataset create/exact readback/delete | disposable Dataset lifecycle |
| FM02 | private Notebook exact source/run/output/delete | disposable Notebook lifecycle |
| FM03 | callback, heartbeat and terminal event | runtime event observation |
| FM04 | empty PostgreSQL master bootstrap | empty-master action/status |
| FM05 | verified empty checkpoint round trip | publish/readback/restore |
| FM06 | cold master restore | exact checkpoint restore |
| FM07 | 20 concurrent ensures, one physical run | concurrent MCP and inventory |
| FM08 | callback loss recovery | callback fault + control restart |
| FM09 | duplicate/stale callback/output rejection | replay fault injector |
| FM10 | lease-expiry write closure | clock/fault + admission probe |
| FM11 | old epoch return fenced | split-brain resume injector |
| FM12 | clean drain/checkpoint/stop | drain action |
| FM13 | forced new-run/new-epoch rotation | rotation action |
| FM14 | corrupt/hash-mismatch candidate preserves HEAD | checkpoint corruption |
| FM15 | restore-smoke failure preserves HEAD | restore failure injection |
| FM16 | full YDB blogger import and checkpoint | YDB + migration operator |
| FM17 | post-import cold restore equality | counts/logical-hash query |
| FM18 | E5 worker and transactional import | exact E5 worker/import status |
| FM19 | BGE-M3 worker and transactional import | exact BGE-M3 worker/import status |
| FM20 | remote MCP cold-start blogger search | signed v2 host evidence + provider claim + reader/operator MCP |
| FM21 | owner preview/apply/post-checkpoint receipt | controlled row + owner MCP |
| FM22 | MCP-managed Dataset/Notebook lifecycle | provider-operator MCP |
| FM23 | protected resource mutation denial | protected-resource probe |
| FM24 | accelerated session rotation/soak | 60–90 minute soak controller |

## Resume and artifacts

Each scenario gets a mode-0600 launch fence before driver invocation. A rerun
with a fence but no receipt sets `resume_only: true`; the driver must reconcile
the exact run and must not launch another run under the same task identity.
Completed receipts are consumed only when matrix, commit and planned task
identity match.

```bash
python scripts/provider/operational_kaggle_matrix.py preflight
python scripts/provider/operational_kaggle_matrix.py run \
  --ledger artifacts/operational-provider-effects.sqlite3 \
  --plan artifacts/operational-kaggle-plan.json \
  --scenario-receipts artifacts/operational-kaggle-scenarios \
  --receipt artifacts/operational-kaggle-matrix.json
```

Schemas and non-evidence examples live in `schemas/provider/` and
`examples/provider/`. Checked-in examples are BLOCKED/FAIL illustrations and
must never be cited as live provider evidence.
