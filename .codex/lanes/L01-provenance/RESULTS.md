# Lane L01-provenance Results

## Status

committed

## Requirement IDs

- R10

## Branch

`agent/r1-infrastructure-workflow/l01-provenance`

## Worktree

`/home/dev/.codex/worktrees/my-data-hub/l01-provenance`

## Base SHA

`0b6b7311081bdfecdd4f3004e5d6842a42f64253`

## Head SHA

Implementation commit before this results receipt:
`7eeda495950d39c2c6020e7adcef00e097e9d5c4`.

## Files changed

- `docs/source-material/idea-hub/idea-20260809-content-platform-current-design.md`
- `docs/source-material/source-manifest.yaml`
- `docs/source-material/README.md`
- `docs/source-material/idea-hub/README.md`
- `docs/source-material/region-talk/README.md`
- `docs/migrations/region-talk/source-provenance.md`
- `tests/test_source_import.py`
- `.codex/lanes/L01-provenance/RESULTS.md`

## Commands run

- `git clone --filter=blob:none --no-checkout https://github.com/onedayonemasterpiece/idea-hub.git <temporary-source-repo>`
- `python3 scripts/import_source_material.py --source-repo <temporary-source-repo>`
- `git -C <temporary-source-repo> show '<full-commit>:<source-path>' | cmp - <destination>`
- `sha256sum docs/source-material/idea-hub/idea-20260809-content-platform-current-design.md`
- `uv run --no-project --with 'pytest>=8.3,<10' --with 'PyYAML>=6,<7' pytest tests/test_source_import.py -q`
- `uv run --no-project --with 'ruff>=0.9,<1' ruff check tests/test_source_import.py`
- `python3 -m compileall -q scripts/import_source_material.py tests/test_source_import.py`
- `git diff --check`
- `.venv/bin/python scripts/validate_repository.py` (known integration issue below)

## Tests / verification

- Exact source commit resolved to `0c3fcf71b2ee8ba8afa49624bef4b779873802f7`.
- Source Git object and committed destination compare byte-for-byte with `cmp`.
- Destination length is 65,507 bytes.
- Source and destination SHA-256 are both
  `c7efb28231223caa6fd02fcc001a38e0f16bcc3fa4c4cd53e744721b2eac0852`.
- `tests/test_source_import.py`: 3 passed.
- Ruff: all checks passed.
- Targeted compileall: passed.
- Diff whitespace check: passed.

## Risks

- `scripts/validate_repository.py` still requires the obsolete abbreviated commit
  `0c3fcf7`, so repository validation reports `wrong target-vision source commit` after
  this required full-SHA update. The script is outside this lane's writable scope and must
  be updated by the integrator or its owning lane to require the full commit.
- Region Talk donor manifest entries deliberately remain pending with null commits and
  hashes. This lane proves no donor access or curated donor import.
- Region Talk remains paused and production publication remains disabled; this source
  import does not close migration, cutover, canary or owner-approval gates.

## Merge notes

- Cherry-pick the implementation commit, followed by the results-receipt commit returned
  in the lane handoff.
- Preserve the imported Markdown file byte-for-byte; do not reformat or normalize it.
- Update the hard-coded source commit check in `scripts/validate_repository.py` during
  integration, then rerun repository-wide validation.
