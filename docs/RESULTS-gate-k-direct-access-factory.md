# Gate K direct-access factory result

Base: `5b72768`.

The new `ExistingEpochEmbeddingAccessFactory` is the smallest safe composition
of the existing ACTIVE-master JIT credential design with a deploy-owned direct
TLS TCP forward. It validates task, epoch, role, expiry, loopback-origin URL,
TLS policy, and CA material; rewrites only the network endpoint and worker CA
path; and revokes credentials that fail validation. It never accepts Kaggle
credentials, job documents, vectors, or a callback/control data proxy.

Readiness now has stable, typed failure reasons rather than a generic blocker:

1. `EMBEDDING_JIT_CREDENTIAL_AUTHORITY_UNAVAILABLE`: the master registrar does
   not yet publish a task-token-bound `embedding_worker` credential. Its current
   exact allow-list is reader/operator and its stored envelope has no
   `task_run_id` or `credential_id` needed for targeted refresh/revocation.
2. `EMBEDDING_WORKER_TLS_FORWARD_UNAVAILABLE`: the root tunnel broker's exact
   contract binds the remote-forward ingress to `127.0.0.1`. A deploy-owned
   externally reachable TLS TCP forward to that ingress is required. It must
   remain a byte-blind transport, not an HTTP/control-plane PostgreSQL proxy.

Those are the two minimum missing production components. The typed
`EmbeddingCredentialAuthority.issue/revoke` seam and `WorkerReachableTunnel`
contract define their exact integration surface. No app/installer, broker,
provider adapter, database migration, or live resource was changed.
