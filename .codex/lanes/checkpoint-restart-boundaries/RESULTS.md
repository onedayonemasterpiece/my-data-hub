# CHECKPOINT-RESTART-BOUNDARIES Results

## Status

Complete. No live provider mutation was performed.

## Base and ownership

- Base: `1e699908`
- Branch: `agent/operational-mvp/checkpoint-restart-boundaries`
- Owned implementation: brokered upload restart reconciliation and durable checkpoint
  registry idempotency.
- Owned validation: focused broker/registry restart tests, operations documentation and
  this result record.

## Boundary map

1. A completed blob claim is durable `UPLOADED`; its public publication projection
   contains only exact file name/length/SHA-256. The runtime reads that projection before
   requesting a grant and skips the exact completed identity. The protected ledger clears
   the sealed URL on completion and never projects encrypted token material.
2. Dataset finalize first persists `FINALIZING` plus the expected numeric version. A
   fresh service reconciles that exact version/file inventory rather than issuing another
   finalize mutation.
3. Independent verifier success persists `VERIFIED` before HEAD CAS. The typed receipt
   must be retained with its SHA-256 so a fresh service skips verification and promotes
   the exact candidate once.
4. HEAD CAS and broker journal promotion are distinct transactions. A process loss after
   the CAS leaves durable HEAD authoritative and requires exact idempotent reconciliation
   of the broker journal without incrementing generation again.

## Implementation

- Normal broker publications now retain the same bounded, secret-scanned typed verifier
  body already retained by acceptance publications. The durable receipt SHA-256 and
  provider run reference remain independently projected.
- Durable registry promotion recognizes only the exact committed-response-loss replay:
  current equals the candidate, generation equals source generation plus one, candidate
  status is `VERIFIED`, and candidate source identity equals previous. Any other replay
  remains a conflict.
- Focused tests use fresh control-ledger/service/provider objects over the same SQLite
  path. They cover response loss after one and three completion acknowledgements, after
  Dataset finalize, before HEAD CAS, and after HEAD CAS but before the broker promotion
  journal update. They assert one provider finalize, one verifier, one HEAD generation,
  typed evidence retention, exact numeric version, no duplicate blob start/PUT, and no
  plaintext URL/token leakage.
- A second-candidate verifier failure test proves an existing current/previous HEAD pair
  remains byte-for-byte unchanged.

## Gates

- Focused: `uv run --extra dev pytest -q tests/control/test_brokered_checkpoint_upload.py`
  — 23 passed.
- Lint: `uv run --extra dev ruff check ...` — passed.
- Repository validation: `.venv/bin/python scripts/validate_repository.py` — 4,033
  checks, zero errors.
- Compile: `.venv/bin/python -m compileall -q src tests` — passed.
- Full suite: `.venv/bin/pytest -q` — 1,246 collected; 1,243 passed, 3 skipped.
- `git diff --check` — passed.

## Files

- `src/my_data_hub/checkpoints/brokered_upload.py`
- `src/my_data_hub/checkpoints/registry.py`
- `tests/control/test_brokered_checkpoint_upload.py`
- `docs/operations/brokered-checkpoint-upload.md`
- `.codex/lanes/checkpoint-restart-boundaries/RESULTS.md`

No verifier launcher/probe, embedding, master mount, YDB or deployment file changed.
