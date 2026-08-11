# Real Kaggle platform-smoke matrix (diagnostic only)

> **Not operational acceptance.** The provider workflow no longer invokes this
> smoke matrix. The mandatory 24-scenario contract is documented in
> [`operational-kaggle-matrix.md`](operational-kaggle-matrix.md). Nothing in
> this document or the smoke receipts can satisfy an operational gate.

This command is an **opt-in provider platform-smoke run**, not the full operational
acceptance matrix. It is not part of the fake/unit suite and no checked-in
example is evidence that Kaggle was contacted.

## Safety boundary

`scripts/provider/real_kaggle_matrix.py matrix` checks for the modern Kaggle API token (`KAGGLE_API_TOKEN` or a regular, non-symlinked `access_token` in `KAGGLE_CONFIG_DIR`) before it constructs the ledger, adapter, plan, wheel, or starts a provider mutation. A missing token produces a bounded blocker receipt, records `mutations_started: 0`, and exits 78. Legacy `kaggle.json` credentials do not satisfy this gate.

The driver uses the repository's single `KaggleProviderAdapter`. It does not implement another HTTP/CLI transport. It creates one disposable private input Dataset containing the exact source wheel and one typed input manifest per scenario. Each scenario renders the existing generated `notebooks/00-platform-smoke/worker.ipynb` contract, launches a distinct private Notebook with a distinct task run ID, downloads only the exact `matrix-result.json`, validates typed per-item accounting, and deletes the Notebook using its exact task-created claim. The shared input Dataset is also claim-cleaned in `finally`.

## Matrix and evidence

The current plan has 16 physical Notebook scenarios (the required minimum is 15):

- private baseline, exact source, exact numeric run output, and typed accounting;
- repeated status/output observations and bounded polling policy;
- exact-run reconciliation, restart receipt resume, and cleanup replay;
- stale source and stale numeric-version denial probes;
- two manifest-level current/resume checkpoint bindings;
- three sequential short-soak runs.

A successful set of smoke runs writes:

- one stable plan, created before provider mutation;
- one receipt per scenario, binding task run ID, provider ref/run/kernel ID, source version/hash, private status, exact input Dataset version/package hash, manifest/result/output hashes, optional checkpoint identity, accounting, fault probe, and claim cleanup;
- one `SMOKE_PASS` summary requiring at least 15 distinct task run IDs and 15
  distinct exact provider run refs. It always retains
  `MANDATORY_OPERATIONAL_SCENARIOS_NOT_EXECUTED` and exits 78 because these
  platform-smoke variants do not prove master lifecycle, physical checkpoint,
  YDB import, embeddings, remote MCP, write fencing, or the long soak.

Only the uninjected CLI path marks receipts `live_evidence: true`. Fake-adapter tests exercise planning, accounting, cleanup, fault probes, and restart receipt consumption with `live_evidence: false`. Files under `examples/contracts/` are synthetic schema illustrations, not observed provider evidence.

## Invocation

This diagnostic is local/manual only. A local opt-in invocation is:

```bash
python scripts/provider/real_kaggle_matrix.py preflight
python scripts/provider/real_kaggle_matrix.py matrix \
  --ledger artifacts/provider-matrix-effects.sqlite3 \
  --plan artifacts/kaggle-matrix-plan.json \
  --scenario-receipts artifacts/kaggle-matrix-scenarios \
  --receipt artifacts/kaggle-matrix.json
```

Keep the plan, ledger, scenario launch fences, and completed scenario receipts together to resume after a process interruption. A completed exact receipt prevents relaunch of that scenario; an incomplete scenario is reconciled against the exact planned task run/source. A durable launch fence with no exact physical run fails closed and requires a new matrix identity rather than launching a second physical run under the same task run ID. Stale or mismatched state fails closed. Validation against `schemas/kaggle-real-matrix-receipt.v1.schema.json` proves only the explicitly named platform-smoke scope; it can never close full operational acceptance.
