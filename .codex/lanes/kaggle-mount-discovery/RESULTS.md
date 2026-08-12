# KAGGLE-MOUNT-DISCOVERY results

- Replaced generated master and embedding-worker assumptions about `/kaggle/input/{provider-slug}` with bounded recursive exact-file discovery.
- Master discovery binds hashes for runtime archive/manifest, known_hosts, verifier, wheel, and status files; verified checkpoint roots are selected by the exact self-hashed checkpoint manifest.
- Worker discovery binds the task-owned status metadata, exact numeric input pins, wheel hash, and generated runtime asset under the discovered Dataset roots.
- Provider mount-name normalization is covered by executable bootstrap tests; duplicate exact assets/task status fail closed.
- No provider mutation and no credential handling were performed.
