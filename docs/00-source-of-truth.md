# Source of truth and authority

## Authority order

1. Explicit owner decisions.
2. Exact imported source research.
3. Corrective ADR-0016.
4. Machine-readable invariants and append-only contracts.
5. Derived documentation, code and tests.

The canonical imported research is
[`source-material/idea-hub/idea-20260809-content-platform-current-design.md`](source-material/idea-hub/idea-20260809-content-platform-current-design.md),
SHA-256 `c7efb28231223caa6fd02fcc001a38e0f16bcc3fa4c4cd53e744721b2eac0852`.
It must remain byte-identical to its verified provenance. `content-platform` is the former
name of `my-data-hub`, not another project.

## Corrective interpretation

The source specifies one writable PostgreSQL-primary in a Kaggle Notebook, a stable
MCP/control plane on the devstand and direct internal data-plane connections after
service resolution. The owner reset binds current/previous verified checkpoints to
private Kaggle Datasets. Derived documents that replaced this with local persistent
PostgreSQL conflicted with the source; they were not a refinement.

ADR-0016 preserves the correction. `architecture/invariants.yaml` makes it executable.
ADR-0009 remains as historical evidence with superseded status.

## Regional monitoring after CHANGES_REQUIRED (2026-11-04)

The [owner decisions](implementation/regional-novelty/owner-decisions.md) retain
significant regional novelty first, fresh Kaggle execution after eligible free-model
exhaustion, current OpenCode, private Dataset persistence, an active DB target below
100 GB and no external backup platform in this delivery. SQL and migrations are
engineering responsibilities.

The [implementation plan](implementation/regional-novelty/implementation-plan.md)
uses one real PostgreSQL/Dataset/MCP lifecycle and one useful card. The mandatory
Dev journal, independent executors, release framework and two-consumer gate were
author choices and are withdrawn. ADR-0020 does not authorize a Dev data journal.

The owner has accepted [physical base + native WAL](implementation/regional-novelty/STORAGE-BASE-WAL.md):
one main private Dataset version at planned stop, a temporary Dataset for the complete
required WAL tail, physical restore before PG start, and one publisher for both.
Snapshot-only, logical pg_restore and the unresolved tail-loss question are superseded.
Use long Kaggle sessions and design for the ~100 GB target without a separate pilot
storage path. The current base reference and its ready WAL bound recoverability;
an online candidate becomes current only after its base and needed tail are ready. Upload and
processing delays prevent treating 15–20 minutes as an unconditional guarantee.

The [preflight results](implementation/regional-novelty/STORAGE-PREFLIGHT-RESULT.md)
distinguish measured Dataset replacement/deletion and a fresh CPU runtime from
the remaining capacity/quota checks and product recovery. Large-transfer results are
recorded separately in that protocol. Existing PostgreSQL 18.4 /
pgvector 0.8.6 is retained. Neither documentation integration nor disposable probes
are proof that the MDH master or a regional card works.

The [audit route](implementation/regional-novelty/OPUS-AUDIT.md) links current decisions,
implementation and preserved history. The full uncommitted audit was not obtained;
only its forwarded summary was preserved. The shared package is not a prerequisite
for the first product; actual repeated provider/transport code remains a reuse target.

## Canonical boundaries

- PostgreSQL is the canonical database engine; the ACTIVE runtime is the Kaggle master
  Notebook.
- Devstand state is control metadata only: operations, callbacks, leases, epochs,
  registry and checkpoint locators.
- Files/datasets/checkpoints are durable artifacts, not a live database by themselves.
- Joplin internal storage, YDB and other providers are not canonical state.
- Region Talk source inventory and migration remain frozen until master lifecycle proof.

Any future topology change needs an owner-approved ADR with an explicit consequences
diff. It cannot be introduced through deployment convenience or tests.

## IdeaHub Showcase constructor decision

The owner-approved constructor contract is maintained in
[`ideahub-showcase.md`](ideahub-showcase.md). It records product and MCP decisions;
live OAuth, discovery, deployment, and publication are accepted only through the
evidence checklist linked there, not through repository source or deployment scaffold.
