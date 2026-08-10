# ADR-0016: Kaggle Notebook hosts the single writable PostgreSQL master

- Status: Accepted — corrective owner decision
- Date: 2026-08-10
- Supersedes: ADR-0009 and incompatible runtime clauses in ADR-0002, ADR-0006,
  ADR-0010, ADR-0011, ADR-0012 and ADR-0014

## Authority

Conflicts are resolved in this order: owner decisions; exact imported source research;
this corrective ADR; machine-readable invariants; derived documentation, code and tests.
The exact source is
`docs/source-material/idea-hub/idea-20260809-content-platform-current-design.md`
(SHA-256 `c7efb28231223caa6fd02fcc001a38e0f16bcc3fa4c4cd53e744721b2eac0852`).

## Decision

At most one ACTIVE writable PostgreSQL-primary exists, and it runs only inside the
Kaggle master Notebook. It owns canonical catalog, pipeline state, restricted roles,
queues, FTS/pgvector, lease watchdog, closed-by-default write gate and checkpoint agent.
A master without the latest epoch and lease cannot accept writes.

The devstand is a lightweight control plane: stable MCP/control endpoint, lifecycle
adapter, callbacks, operation/idempotency ledger, registry, leases, fencing, checkpoint
metadata, security and audit. It has no production PostgreSQL, PGDATA, canonical business
rows, local canonical committer or master backup service. `master=ABSENT` is a healthy
control-plane state; data operations return an asynchronous operation or fail closed.

Private Kaggle Datasets retain current and previous verified checkpoints plus less
frequent portable logical backup. Promotion requires manifest, exact-version readback,
hash verification, isolated restore smoke and atomic HEAD advance.

After resolution, approved workers/connectors receive short-lived epoch-bound access and
use the direct master data plane. External agents use stable MCP on the devstand, which
resolves the ACTIVE master. Credentials never enter runtime event records.

## Compatibility

Schema, migrations, role contracts, connector envelopes/receipts, bounded MCP contracts,
Region Talk accounting, provider control classes and recovery tooling are preserved but
rebound to the Kaggle master runtime. Disposable PostgreSQL in CI is permitted and must
have no persistent named volume.

## Sequence

PR-A only restores safety. Next are donor compatibility, FakeKaggle state machine,
runtime SDK, real provider smoke, master Notebook PoC, and MCP/connectors against the
dynamic master. Region Talk remains paused until those gates pass.
