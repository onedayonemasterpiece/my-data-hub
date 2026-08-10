# Data ownership

| Component | May own |
|---|---|
| Kaggle master Notebook PostgreSQL | canonical catalog, pipeline state, landing, queues, FTS/vector indexes, outbox |
| Devstand control plane | operation IDs, provider intents, callbacks, leases/epochs, service/checkpoint locators, audit metadata |
| Private Kaggle Datasets | immutable verified checkpoint generations and artifacts |
| Ordinary worker notebook | immutable result artifact for an exact input/run |
| Connector | durable producer spool until an exact receipt |
| Joplin | its own local note profile, accessed only through supported interfaces |

Only the latest leased/fenced master epoch accepts canonical writes. Devstand metadata never
becomes a second business database. Dataset HEAD changes only after exact readback/hash and
restore proof.
