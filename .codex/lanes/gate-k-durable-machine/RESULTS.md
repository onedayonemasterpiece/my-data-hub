# Gate K durable launcher closure

Base: `3820c65`.

Implemented the persisted launcher lifecycle
`REQUESTED → ACCESS_READY → STATUS_CREATED → LAUNCHED → CLEANUP_REQUESTED → COMPLETE`.
Every transition is atomically written to an absolute private journal before the
following provider or cleanup effect. The journal is schema/version/key/task
validated, bounded to 1 MiB, written through a unique 0600 temporary file, and
fsyncs both file and 0700 parent. It contains hashes, claims, identifiers, and
timestamps only—never task tokens, database URLs, private keys, certificates,
or business bytes.

Provider mutations and deletes retain deterministic effect IDs/idempotency keys;
the central adapter journal reconciles ambiguous responses. Cleanup is invoked
by authenticated terminal worker events and by the bounded periodic expiry
reconciler. Notebook and status Dataset are both disposable, and cleanup revokes
the PostgreSQL credential and task-bound SSH certificate before exact deletes.

Production wiring is explicitly opt-in with
`MY_DATA_HUB_EMBEDDING_WORKERS_ENABLED=true`. Compose mounts the private
credential/journal directory and reviewed `tunnel-known-hosts`; gateway/runtime
identity values continue to come from the verified master asset/provider env.
The default remains fail closed.

Validation: focused launcher, assembly, master credential, tunnel broker, and
control runtime tests pass; Ruff, compileall, shell syntax, and diff checks pass.
No provider or deployment mutation was performed.
