# Checkpoint verifier live closure

The production verifier is a separate private, internet-disabled Kaggle Notebook launched by the single injected central `KaggleProviderAdapter`. It receives exactly two numeric Dataset attachments: the durable master runtime asset claim (`owner/slug/N`) and the exact checkpoint candidate (`owner/slug/N`). The launch pins the reviewed immutable Kaggle image digest with pinning type `original`; no Kaggle credentials enter the Notebook.

Before importing the project wheel, the rendered bootstrap recursively inventories `/kaggle/input` under fixed file-count, per-file, and total-byte limits. It rejects symlinks, duplicate hash matches, mixed runtime mounts, and an ambiguous checkpoint manifest. Provider mount slug normalization is therefore not trusted. The exact wheel, PostgreSQL 18.4 runtime archive, runtime manifest, source commit, Python series, image digest, execution pins, and both numeric Dataset refs are hash-bound.

The isolated runtime starts only the PostgreSQL 18 process restored below `/kaggle/working`. Its read-only bounded probe proves:

- PostgreSQL major 18;
- `vector`, `pgcrypto`, `citext`, and `pg_trgm` are installed, with the exact checkpoint pgvector version;
- the append-only `hub_meta.schema_migration` history is contiguous from version 1 and hash-bound;
- singleton and validated-constraint invariants;
- a three-dimensional cosine vector query;
- allowlisted relation counts under 30-second statement and 3-second lock timeouts.

The Notebook emits only canonical metadata in `checkpoint-restore-receipt.json` (maximum 64 KiB). Central validation produces the strict `checkpoint-restore-verified-receipt.v2` envelope, binding numeric provider run ref, canonical source hash, selected output receipt hash, and selected output-tree hash. Missing, extra, noncanonical, mismatched, or wrong-version evidence fails closed. Checkpoint bytes and restored rows never cross the control plane.

Production assembly resolves runtime assets only from the deterministic master `ensure_dataset` effect, its persisted arguments hash, exact resource claim, and matching applied receipt. No latest-slug or caller-provided numeric version is accepted.
