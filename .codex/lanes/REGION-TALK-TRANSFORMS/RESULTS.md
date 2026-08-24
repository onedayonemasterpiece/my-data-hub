# REGION-TALK-TRANSFORMS results

- Lane: `REGION-TALK-TRANSFORMS`
- Requirements: `R02`, `R03`
- Base SHA: `1068d103ec261a37dd31e1f6d11265e1e238c168`
- Implementation head SHA: `e6ce6d9f4712825c458c61165ca115fe8defd575`
- Outcome: **Done for the assigned pure-transformation lane**

## Delivered

- Frozen Pydantic inputs/outputs for master-side apply and separate heavy-worker evidence.
- Conservative external-article URL/DOI/title-author identity, grounded evidence policy,
  source policy and media-rights validation.
- Canonical Telegram/VK/web source and post normalization.
- Exact-current E5+BGE-M3 evidence fusion. Both expected evidence fingerprints, both
  model contracts, the text hash and pinned semantic-bank fingerprint must match.
- Fail-closed `region_talk_publication_eligibility_v5` with explicit image,
  final-verifier and writer gates; no model result is fabricated.
- Idempotent candidate memory and immutable current revision formation. Stale worker
  inputs cannot advance the lifecycle.
- Monotonic source/publisher merge with conflict results for identity/locality changes.
- Deterministic MMR review ranking with compatible vectors or disclosed heuristic
  fallback.
- Publication-plan formation with current operator-review binding and hard-disabled
  dispatch effects. Conflicting `11:30`/`12:00` article policy yields no slots.
- Pinned donor commit/blob/SHA provenance and a sanitized structural golden fixture.

## Evidence and commands

All commands used the existing project virtual environment at
`/home/dev/.codex/worktrees/my-data-hub/operational-mvp/.venv` with this worktree's
`PYTHONPATH=src`.

| Command | Result |
| --- | --- |
| `python -m compileall -q src tests` | passed |
| `pytest -q tests/region_talk` | 9 passed |
| `pytest -q tests/region_talk tests/test_architecture_invariants.py tests/test_config_pipeline_and_db.py tests/test_region_talk_migration.py` | 40 passed |
| `pytest -q tests/test_secret_scan.py tests/test_bootstrap_contracts.py tests/test_db_migrations.py` | 11 passed |
| `ruff check src/my_data_hub/workloads/region_talk/transforms tests/region_talk` | passed |
| `python scripts/validate_repository.py` | 4,398 checks, 0 errors, `ok=true` |
| `git diff --check` | passed |

The complete repository pytest run reached 100%, but 15 pre-existing provider-upload
tests failed because the runtime disk-reserve gate observed only 847 MiB free on the 99%
full root filesystem. Every failure was
`ProviderUploadError: provider upload staging disk reserve would be violated` in shared
provider-upload code outside this lane. The assigned and architecture/security/schema
test sets above pass; this lane did not edit or bypass the reserve.

## Risks and remaining integration

- This slice has no database/provider/network code and does not prove Notebook deployment,
  scheduling, ACTIVE-epoch access or transactional application.
- The broader authenticated review of `onedayonemasterpiece/region-talk` is still pending;
  the adaptation manifest is correctly `in_progress`, not `verified`.
- Publication effects remain intentionally disabled. Enabling them requires a separate
  exact-revision canary, receipt and owner approval.
- Worker contracts validate current fingerprints, but actual image/final-verifier/writer
  results must come from their separately registered workers.

## Changed files

- `.codex/lanes/REGION-TALK-TRANSFORMS/RESULTS.md`
- `docs/migrations/region-talk/adaptation-manifest.json`
- `docs/migrations/region-talk/source-provenance.md`
- `docs/migrations/region-talk/transformation-slice.md`
- `src/my_data_hub/workloads/region_talk/transforms/{__init__,_canonical,candidates,eligibility,evidence,merge,models,normalization,planning,ranking}.py`
- `tests/region_talk/fixtures/external_article_golden.v1.json`
- `tests/region_talk/test_normalization_and_evidence.py`
- `tests/region_talk/test_pipeline_transforms.py`
