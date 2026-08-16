# Blogger discovery MCP contract (Phase B)

Status: **implemented and proven as an internal contract; not yet exposed by the shared MCP catalog/broker**.

This phase provides the bounded PostgreSQL-side and control-ledger primitives for the
owner workflow “find a batch of bloggers/accounts, preview it, apply approved canonical
changes, then read the sanitized result.” It does not make the devstand a data plane and
does not create a second database runtime.

## Write flow

1. `SubmitDiscoveryBatch` validates one closed `submit-discovery-batch.v1` request.
   It accepts either 1–500 typed actor/account rows or one immutable private Kaggle
   artifact claim with an exact numeric version, path, byte count, SHA-256 and claim
   SHA-256. It builds a fixed connector envelope; callers cannot choose a connector,
   table or SQL statement.
2. The connector boundary accepts the exact envelope into the ACTIVE master's
   `integration.batch`/`integration.batch_payload` landing tables. Artifact records are
   materialized only through `integration.materialize_blogger_discovery_artifact` after
   provider verification.
3. `integration.preview_blogger_discovery` constructs one immutable plan bound to batch,
   request hash, owner principal/client, ACTIVE master instance/epoch and expected
   canonical revision. Invalid or ambiguous rows are quarantined. Replays return the
   same plan; conflicting identities fail.
4. The owner applies the exact preview plan through
   `integration.apply_blogger_discovery`. The fixed `mdh_canonical_committer` procedure
   creates or links actors/accounts and project membership, records provenance, advances
   exactly one revision, writes the semantic outbox, audit and immutable receipts, and
   changes the batch durability state in one PostgreSQL transaction.
5. If the apply acknowledgement is lost, `integration.reconcile_blogger_discovery`
   reads the canonical receipt. It never executes the canonical DML a second time.
6. The control ledger stores only operation identities, hashes, bounded preview counts,
   receipt/revision metadata and checkpoint lifecycle. Blogger rows remain in the ACTIVE
   master only. A successful write is not durable-complete until the post-change verified
   checkpoint transition completes.

## Read flow

`hub.bloggers_v1` is a sanitized projection of canonical actors, included blogger project
memberships, public account coordinates and nullable historical profile fields.
`BloggerDiscoveryReader.search` supplies a fixed, project-scoped, keyset-paginated search
with a maximum of 100 rows. It exposes no evidence payload, arbitrary relation/column,
generic SQL, DDL, owner role or unrestricted database role.

## Cold-master continuation and shared wiring boundary

The eventual MCP broker must persist the typed operation before calling `ensure_master`.
If no master is ACTIVE, the operation stays pending while the existing Kaggle lifecycle
starts/restores PostgreSQL. After an ACTIVE epoch and role-bound short-lived credential
exist, the broker resumes the same operation at landing/preview/apply/reconcile; it must
not return a terminal business failure merely because the master was initially absent.

That catalog/server/service/control-gateway wiring is intentionally **not part of this
Phase B lane**, because those shared files are owned by the provider integration lane.
Until the shared wiring is merged and deployed, these contracts are not a claim that the
public `bloggers.import` or reader tools are available in OpenCode/ChatGPT.

## Security properties

- PostgreSQL remains the only canonical database and the ACTIVE Kaggle master remains
  the only writable primary.
- No canonical blogger rows or submitted payloads are persisted on the devstand.
- Intake, reader and canonical-committer roles receive only the minimum fixed grants.
- Generic SQL is absent; direct `mdh_canonical_committer` inserts into canonical tables
  are denied.
- Every write function verifies the short-lived login's ACTIVE epoch; expired epochs are
  fenced.
- Dataset artifacts are transport/checkpoint inputs, never the live database.
