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

The [revised plan](implementation/regional-novelty/implementation-plan.md) starts with
one real PostgreSQL/Dataset/MCP lifecycle and a useful card. The earlier mandatory
Dev journal, independent executor topology, release framework and two-consumer gate
were author design choices and have been withdrawn from the pilot. ADR-0020 is a
withdrawn proposal, not authority to add replay payload storage on Dev.

Snapshot-only storage is recommended but changes the crash guarantee: work after
the latest verified Dataset may require refetch/reanalysis and may not be reproducible.
The owner has not accepted that loss boundary yet. Neither the forwarded audit nor
this documentation silently changes the guarantee. Independent source/lifecycle work
can proceed while this product decision remains explicit.

The [audit route](implementation/regional-novelty/OPUS-AUDIT.md) contains the received
summary, response and current proposal. The full uncommitted worktree audit was not
read in this preparation. The shared repository exists; its initial primitives are
not live provider/WSS adoption and are not prerequisites for the first product.

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
