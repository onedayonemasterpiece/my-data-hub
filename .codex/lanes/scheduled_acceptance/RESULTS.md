# M1 Stage N scheduled acceptance — RESULTS

## Scope

- Lane ID: `scheduled_acceptance` (M1 Stage N)
- Base SHA: `a0622426a335f74808c6c1ccd7d7581cc880333a`
- Validated implementation SHA: `ea237f77385ae496d51b40d719dc93773d47497a`
- Branch: `agent/operational-mvp/scheduled-acceptance`

## Outcome

Implemented an executable, fail-closed scheduled acceptance runner and wired it into the existing classified nightly and provider-real jobs.

Observed live interfaces are exercised rather than represented by comments:

- bounded owned Kaggle Dataset/Notebook inventory through the repository's single `KaggleProviderAdapter`;
- bounded MCP reader catalog plus `platform.status`, `master.status`, `checkpoint.status`, `embedding.coverage`, and `provider.resources.status`;
- unauthenticated and deliberately invalid-token HTTP authorization negatives;
- public/unproven-private, orphan, registry freshness, active master epoch/lease, current/previous checkpoint, exact version reference, checkpoint freshness, and E5/BGE coverage evaluation;
- weekly/manual validation of real disposable Dataset and Notebook lifecycle/cleanup receipts emitted by `real_kaggle_matrix.py`.

Receipts are bounded JSON containing only counts, booleans, statuses, interface names, timestamps, and commit/workflow identity. Provider refs, MCP result rows, blogger/business rows, credentials, connection strings, and exception messages are not emitted. FAIL outranks BLOCKED, and missing required interfaces exit with code 78 rather than being reported as successful.

## Honest missing-interface blockers

The current runtime has no safe callable contract for these required probes, so the runner emits explicit blockers:

- `CONNECTOR_COVERAGE_API_MISSING`: bounded connector coverage/status without business rows;
- `COLD_RESTORE_REQUEST_API_MISSING`: isolated current-checkpoint cold restore/restore-smoke request;
- `STALE_EPOCH_PROBE_API_MISSING`: safe synthetic stale-epoch request;
- `FORCED_ROTATION_API_MISSING`: checkpoint-bound forced master rotation;
- `PREVIOUS_CHECKPOINT_RESTORE_API_MISSING`: isolated previous-checkpoint restore;
- `PROTECTED_RESOURCE_DENIAL_PROBE_API_MISSING`: safe exact protected-resource mutation denial probe;
- current MCP `checkpoint.status` also blocks with exact-ref or verified-at interface codes until it exposes exact numeric current/previous refs and current verification time.

No success is claimed for these scenarios.

## Workflow behavior

- `.github/workflows/nightly.yml` retains its classified job inventory and runs live nightly acceptance after deterministic validation, uploading `scheduled-nightly.json` even when the acceptance step fails/blocks.
- `.github/workflows/provider-real.yml` adds weekly/manual selection, runs scheduled acceptance after the existing real Dataset/Notebook canaries, consumes their cleanup receipts, and uploads `scheduled-provider-real.json`.
- Missing credentials or interfaces make the scheduled job red while preserving a sanitized blocker receipt.

## Validation evidence

- `ruff check .` — passed.
- `python -m compileall -q src tests scripts` — passed.
- `python scripts/create_notebooks.py --check` — passed with no drift.
- `python scripts/validate_repository.py` — passed, 2,886 checks, zero errors.
- `pytest -q tests/provider/test_scheduled_acceptance.py` — 8 passed.
- Full `pytest -q -rs` — 549 collected: 548 passed, 1 intentional opt-in live PostgreSQL skip.
- Credential-free direct CLI smoke — wrote a sanitized BLOCKED receipt with 14 explicit blockers and exited 78.
- `git diff --check` — passed before commit.

## Changed files

- `.github/workflows/nightly.yml`
- `.github/workflows/provider-real.yml`
- `scripts/provider/scheduled_acceptance.py`
- `tests/provider/test_scheduled_acceptance.py`
- `.codex/lanes/scheduled_acceptance/RESULTS.md`

## Risks / operational notes

- Until the blocker interfaces above are implemented, scheduled acceptance is intentionally non-green; this is evidence of missing operability, not a code-contract false positive.
- A reader token with broader/operator scopes fails the exact reader-catalog gate.
- A provider registry response at the 100-row bound blocks orphan/freshness claims because the current MCP response has no completeness cursor/`has_more` contract.
- The live PostgreSQL proof remains opt-in and was not run; this lane creates no PostgreSQL or canonical data locally.

## Integration correction

The integration owner narrowed public/orphan evaluation to exact registered
my-data-hub refs. Unknown account resources remain `external_read_only`; they are
reported as a bounded count and are never mislabeled as task orphans or used to
fail the system-private-resource gate. A focused regression test covers an
unrelated public account resource alongside one private registered resource.

A second integration correction treats `master=ABSENT`/`STOPPED` as healthy
cold states rather than stale epochs. ACTIVE still requires a positive epoch and
future lease; transitional states fail closed as BLOCKED until `master.status`
exposes transition-age/deadline evidence.
