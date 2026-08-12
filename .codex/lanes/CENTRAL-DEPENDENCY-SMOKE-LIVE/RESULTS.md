# CENTRAL-DEPENDENCY-SMOKE-LIVE results

- Source commit: `89a8a2c45096705db6e897f47c2905e3f9d19de1`.
- Exact private CPU image: `gcr.io/kaggle-images/python@sha256:c1fa4de30bc268e601e6dcddb6ceb2519b9adde3527dbbfb05e6bdfbbbdcd1a2`; source commit `fc61d5cda7da39530055bae9bd0e92865f995cd9`; CPython `3.12.13`.
- Central adapter instances: `1`. The disposable Notebook had no Kaggle credential and internet was disabled.
- Root-cause sequence was evidence-led: fixed Dataset mount discovery; captured exact failed-run receipts before cleanup; replaced `ir-datasets 0.5.11` (which made evaluation dependencies unconditional) with `0.6.2`; added exact CPython 3.12 manylinux `lz4 4.4.5` after provider evidence proved it absent.
- Final live run: PASS. `FlagEmbedding.BGEM3FlagModel`, `psycopg` binary, Torch, and Transformers imports passed with a recursively checked installed dependency closure. The exact private input Dataset version and disposable smoke Notebook were both claim-bound deleted; bounded provider inventory confirmed both refs absent.
- Tracked receipts:
  - `docs/operations/evidence/2026-08-12-operational-mvp/kaggle-embedding-dependency-smoke-live.json`
  - `docs/operations/evidence/2026-08-12-operational-mvp/kaggle-embedding-dependency-runtime-receipt.json`
- Protected mode-0600 evidence root: `/home/dev/.local/share/my-data-hub/protected/embedding-dependency-smoke-89a8a2c45096`.
- Credential/signed-capability literal scan over the protected evidence root: PASS.
- This closes only the Gate K dependency-image smoke. It is not a master/checkpoint matrix PASS and does not make the final operational receipt COMPLETE.
