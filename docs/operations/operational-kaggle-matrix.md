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
the runner passes `--request REQUEST.json --result RESULT.json`. The request is
non-secret and binds the matrix/commit/scenario/task-run identity, required
assertions, lifecycle gates, soak bounds, and `resume_only`. The result must
validate as `my-data-hub-operational-kaggle-driver-result.v1` and either:

- locate an already launched exact Kaggle run (`provider_ref`, numeric
  `provider_run_ref`, `provider_kernel_id`, source version/hash); or
- report `BLOCKED` with an uppercase blocker code and the concrete missing
  integration dependency.

The driver has one exact executor entry for every FM01–FM24 scenario. It checks
the required reader/operator/migration/provider OAuth profile and exact MCP tool
catalog, then performs only bounded non-mutating observations until the whole
scenario can be bound to an exact evidence Notebook run. It never starts a
mutation and then returns BLOCKED: every BLOCKED result requires
`mutations_started: 0`.

Existing safe production surfaces are wired now: master/checkpoint/provider
status, stale-epoch denial, blogger accounting/statistics, embedding coverage,
and protected-resource denial. Unresolved mutation/fault interfaces use a
scenario-specific code such as `CHECKPOINT_CORRUPTION_FAULT_API_MISSING` or
`E5_WORKER_SUBMISSION_TOOL_MISSING`; the generic
`OPERATIONAL_DRIVER_INTERFACE_MISSING` blocker is not used by the trusted
driver. This remains a blocked operational state, not a readiness claim.

The current internal gaps are explicit in the executor registry. They include
exact provider create/run/read/delete payload contracts, runtime event history,
empty-bootstrap selection, checkpoint candidate publication, callback/lease/
replay fault controls, drain control, YDB batch-to-checkpoint binding, blogger
logical hash reads, embedding submission tools, host boot identity, a controlled
business-row fixture, and the accelerated soak controller. Privileged actions
are not attempted until their terminal exact-run evidence contract exists.

## What counts as PASS

A driver-reported PASS is only a locator. The runner constructs exactly one
`KaggleProviderAdapter` and reuses it for every scenario. It reconciles the
planned task ID/ref/source hash, compares the exact numeric Kaggle run ref,
kernel ID and source version, requires terminal `complete`, and downloads the
exact `operational-result.json` from that run. The Notebook output must contain
exactly the scenario's required assertions and lifecycle events. Every
assertion is bound to an evidence SHA-256.

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
| FM20 | remote MCP cold-start blogger search | reader MCP + host reboot |
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
