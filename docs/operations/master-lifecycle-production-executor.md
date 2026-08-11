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

The operator adapter has exactly two surfaces:
`master.acceptance.request` and `master.acceptance.status`. It is not included
in a reader catalog and has no list endpoint.

## Fixed scenario execution

| Scenario | Concrete execution |
|---|---|
| FM04 | Control ensures the exact empty boot and binds it after ACTIVE. The notebook factory then probes revision zero and a source-pinned relation allowlist; no query comes from the request. |
| FM07 | The control executor performs exactly 20 concurrent same-key ensures through the official Kaggle provider, verifies one operation/epoch and the applied trigger receipt's numeric kernel identity, then completes through the owner claim CAS. |
| FM08 | A narrow supervisor port suppresses the next task callback, returns only event ID/hash, performs a real process restart with a changed boot UUID, replays the stored body by ID, and proves the exact service is ACTIVE. |
| FM09 | The protected callback store selects one ACKed event, replays it by ID, performs retired-token and stale-epoch replays, and hashes control state before/after. Callback bytes never cross the acceptance boundary. |
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
