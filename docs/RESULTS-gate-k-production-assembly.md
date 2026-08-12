# Gate K central worker production assembly result

Base: `7a1cad5da8aad7c99f4da6f24160f90d5d6dd775`.

## Implemented

- Added a concrete single-adapter central Kaggle launcher. It consumes only the
  hash-bound `EmbeddingLaunchMetadata`, asks an injected ACTIVE-master access
  factory for one epoch-bound worker capability, creates one disposable private
  status Dataset, and launches the exact E5/BGE worker against the direct data
  plane. Job documents and vectors are never accepted by the launcher.
- The private status Dataset contains only metadata, one task callback token,
  exact runtime pins, TLS CA, and the short-lived `mdh_embedding_worker`
  connection/tunnel capability. Provider credentials are never supplied to the
  Notebook.
- `create_app()` advertises Gate K readiness only when a concrete launcher with
  a callable access factory is injected. A `job.claimed` RuntimeClient callback
  is durably accepted before the deterministic central launch is reconciled.
- Wrong-epoch or expired direct access fails before any provider mutation.

## Validation

- `ruff check` passed for changed Python files.
- Focused tests passed: 17 tests covering the launcher and control runtime
  wiring.

## Exact remaining live dependency

No live mutation was performed. The repository/deployment still has no
worker-reachable TLS endpoint or production factory that can mint the
`EmbeddingWorkerDirectAccess` value. The devstand does not host PostgreSQL or
canonical data; its existing broker deliberately exposes the remote-forward
ingress only on loopback (`127.0.0.1:25432`).
Therefore production `create_app()` correctly remains not-ready unless deploy
assembly explicitly injects that factory. This change does not weaken the
loopback broker or pretend a public tunnel exists.

Credential rotation/revocation across a multi-hour Kaggle computation likewise
cannot be honestly completed with the current rolling master lease: epoch
credentials are bounded by the short control lease and the worker has no
worker-reachable refresh endpoint. A deploy-owned direct tunnel/session
authority must supply refresh and revoke semantics behind the injected factory;
until then the capability remains unavailable.
