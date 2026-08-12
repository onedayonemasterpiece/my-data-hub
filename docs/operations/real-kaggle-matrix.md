# Real Kaggle platform-smoke matrix (diagnostic only)

> **Not operational acceptance.** The provider workflow no longer invokes this
> smoke matrix. The mandatory 24-scenario contract is documented in
> [`operational-kaggle-matrix.md`](operational-kaggle-matrix.md). Nothing in
> this document or the smoke receipts can satisfy an operational gate.

This command is an **opt-in provider platform-smoke run**, not the full operational
acceptance matrix. It is not part of the fake/unit suite and no checked-in
example is evidence that Kaggle was contacted.

## Safety boundary

The historical CLI is permanently non-mutating in production. `preflight`,
`dataset-canary`, `notebook-canary`, and the uninjected `matrix` command return
`SUPERSEDED_BY_CENTRAL_OPERATIONAL_MATRIX`, record `mutations_started: 0`, and
exit 78 before creating a ledger, plan, adapter, or provider resource. This
prevents the diagnostic module from constructing a second account-authenticated
Kaggle client.

The production acceptance path is
`scripts/provider/operational_kaggle_matrix.py`; all provider effects and live
inventory go through the deployed MCP/control gateway and its one central
`KaggleProviderAdapter`. The fake-adapter unit seam below remains only for
deterministic contract tests and can never mark evidence live.

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

The only supported manual action is the non-mutating supersession proof:

```bash
python scripts/provider/real_kaggle_matrix.py preflight
```

It exits 78 and names `scripts/provider/operational_kaggle_matrix.py` as the
production entrypoint. Do not attach Kaggle credentials to this historical
module. Schema examples and injected fake-adapter tests remain useful for
contract regression only; they cannot close operational acceptance.
