# R21.3 E5 frozen producer results

## Scope and revisions

- Lane: `R21.3-E5-FROZEN-PRODUCER`
- Base SHA: `add47c64072bd982a033a83bb4fe201554672863`
- Producer implementation SHA: `258e166723a09da90d1d37cfc946d5f2d81476e3`
- Carrier/runtime implementation head SHA: `a23cd63cdfc7ec82f787508e8ad40b074df3b35f`
- No migrations, `notebook_stages`, 0032/0033 heavy-submit paths, production business data, or deploy state were changed.

## Requirement disposition

| Requirement | Result | Evidence |
|---|---|---|
| One protected frozen E5 producer at v1 | Done | Private `zigomaro/mdh-region-talk-e5-assets-v1/1`, source version 1, kernel ID 131338450, `COMPLETE`; it remains protected/non-disposable. |
| Exact upstream E5 revision and semantic bank | Done | Provider receipt `47427d3086012093eac5718355a69c53e2583291e7a3e46e2b465d08ea2a2573`; 23 files / 5,322,810,412 bytes; official-tree receipt `a9bf9a773342bb1593801f34bdd8d230b44c4a934842deea0b444ad5371aae70`; semantic-bank SHA `4ec81e6ede79f3dae1bb366a06366e7197d960e1c04e124f77b3db12f2f1981f`. |
| Central source/version/readback fencing | Done | Committed authority `c874531f044d31ef6953387f103dd34c6f4674ed569cafbaaf7f97856881d931`; launcher verifies exact source SHA, version, kernel ID, run ref and `COMPLETE` before E5 dispatch. Drift negative prevents worker Notebook push. |
| Single-KPA `kernel_sources` attachment | Done | KPA push intent and GetKernel readback bind exact `zigomaro/mdh-region-talk-e5-assets-v1`; lost-response reconciliation and drift rejection are tested. |
| Complete worker verification before import | Done | Discovery selects one exact weight root, verifies all manifest files and semantic bank before encoder construction. Live consumer verified the complete 5.32 GB tree before offline Transformers import. |
| BGE exact `model_sources` launch seam | Done | Launcher preflights exact `yethukmutt/bge-m3/Transformers/m3/1`, attaches it through the same KPA, reads it back on reconciliation, and rejects source/version drift. |
| No secrets/model bytes on devstand | Done | Builder and consumer reject provider/HF credential environment; builder uses public HF exact revision; consumer is offline. Only bounded canonical receipts were downloaded. |
| Exact disposable cleanup | Done | Consumer cleanup effect `f94ef064-599d-57dc-b77f-8ccb735c8448`, receipt `adc3256079857308b83980cf3dbd292fb209478ba547d9e53132229a0bff1395`; readback found consumer current version `null`. |
| Publication/notification disabled | Done | Both producer and consumer receipts and runtime manifests bind both flags `false`. |

## Live provider evidence

- Frozen producer source SHA: `345cbeba4f1deb143a3af571594e92d19c536458d157078a071b62b7804861fa`.
- Producer inventory SHA: `b27d94353f1b60ac9817b4d4aa10fd9a38129f4d2404be835a000202f64026f4`.
- Disposable consumer: `zigomaro/mdh-region-talk-e5-consumer-smoke/1` (deleted).
- Consumer task: `eae782ca-12d1-5942-bfc8-b6fc3240a2fc`.
- Consumer receipt: `d8248782d7c007e472a552e5a226d5c48b69eba46800494ae286619895a01d4f`.
- Fixed output: 768 dimensions; SHA-256 `054317752ee2e2343dda3051120aa82290cc6144bf5c21e8c64fb332d8c720bb`.
- Post-cleanup readback: producer exact v1/kernel 131338450/`COMPLETE`; consumer absent.
- Live metadata preflight returned exact BGE source `yethukmutt/bge-m3/Transformers/m3/1`.
- Canonical small evidence is under `docs/operations/evidence/2026-08-20-r21-e5-frozen-producer/`.

## Gates run

```text
PYTHONPATH=src python -m pytest -q \
  tests/provider/test_kaggle_adapter.py \
  tests/region_talk/test_text_runtimes.py \
  tests/region_talk/test_stage_dispatch.py \
  tests/region_talk/test_e5_frozen_producer.py
66 passed

python -m ruff check <all R21.3 Python paths>
All checks passed

PYTHONPATH=src python -m compileall -q src tests <R21.3 scripts>
PASS

PYTHONPATH=src python scripts/validate_repository.py
PASS (exit 0)

git diff --check
PASS
```

The parent explicitly reserved the final full repository test run for integration because of limited disk and concurrent lanes; this lane did not run full `pytest`.

## Executable matrix and residual integration risks

| Stage | Asset carrier | Real execution evidence | Remaining integration boundary |
|---|---|---|---|
| E5 | fenced frozen `kernel_sources` producer | complete provider tree verification plus real offline 768-dim model inference | R20.2 master/runtime-pin and DB 0028/0032 lifecycle must be integrated and exercised by root; this lane does not claim full pipeline `COMPLETE`. |
| BGE-M3 | exact numeric `model_sources` version | prior R21 provider fixed-output proof; this lane adds concrete launch/preflight/readback | Same master/runtime-pin integration boundary; no new BGE model execution was needed in R21.3. |

Kaggle consumer metadata cannot name a numeric kernel-source version. Safety therefore depends on the committed central current-source/version/kernel fence immediately before launch plus complete worker content verification before import. Any producer mutation must remain a retryable failure; the protected producer must not be edited or versioned.

## Changed files

See `git diff --name-only add47c64072bd982a033a83bb4fe201554672863..HEAD`. The lane changes only KPA carrier/readback logic, Region Talk production assembly/text runtime assets, frozen producer/consumer scripts, focused tests, operational docs/evidence, and this RESULTS receipt.
