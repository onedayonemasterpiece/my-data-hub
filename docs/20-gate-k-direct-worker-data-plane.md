# Gate K central workers and direct ACTIVE-master exchange

Gate K has one provider authority: the central control process owns the repository's
single `KaggleProviderAdapter`. The PostgreSQL master Notebook never receives a Kaggle
credential and never constructs or reaches through to a provider client.

Embedding job documents and returned vectors are canonical/derived business data. They
are stored only in `search.embedding_dispatch` and
`search.embedding_result_landing` inside the ACTIVE master. Runtime callbacks carry only
`embedding-central-launch-metadata.v1` hashes, counts, identities and source attestations.
They reuse the durable CherryFlash-compatible `event_uid` delivery behavior for
`job.claimed` and terminal job events.

Workers must claim/submit through the two bounded SQL functions as a short-lived LOGIN
member of `mdh_embedding_worker`. The role is epoch-bound, has no canonical table grants,
and every write is rechecked by the deferred master epoch guard at commit.

## Operational blocker

The production control plane does not yet implement a worker-reachable direct tunnel plus
JIT `mdh_embedding_worker` credential handoff. Therefore both control admission and MCP
observation fail closed with `EMBEDDING_DIRECT_DATA_PLANE_UNAVAILABLE`; adapter presence
or an ACTIVE master alone cannot advertise readiness. No fallback may proxy job/result
bytes through devstand, callback bodies, the control ledger, or a status Dataset. A later
central launcher may put only callback/auth/direct-data-plane bootstrap material in its
private per-run status Dataset.
