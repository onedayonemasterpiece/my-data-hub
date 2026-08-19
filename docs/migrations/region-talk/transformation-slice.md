# Region Talk pure transformation slice

This slice is the deterministic middle of the autonomous Region Talk pipeline. It is
deliberately separate from both the lightweight devstand orchestrator and the master-side
PostgreSQL unit of work:

1. the orchestrator registers/schedules a separate Region Talk Notebook and issues a
   fenced task contract;
2. the Notebook obtains current source and worker evidence through bounded contracts;
3. `my_data_hub.workloads.region_talk.transforms` performs pure transformations;
4. the ACTIVE master validates the typed output and applies it in one PostgreSQL
   transaction;
5. publication dispatch remains a separate disabled effect.

The package never connects to YDB, PostgreSQL, Telegram, Google, Kaggle or another
provider. It therefore cannot make the devstand a business-data runtime.

## Contracts and fail-closed gates

| Module | Output |
| --- | --- |
| `normalization` | conservative external-article identities/rights plus canonical source/post rows |
| `evidence` | E5+BGE-M3 fusion only when text, semantic-bank, model-contract and both caller-expected fingerprints are current |
| `eligibility` | `region_talk_publication_eligibility_v5` decision and explicit image/final-verifier/writer worker gates |
| `candidates` | stable candidate memory and one current immutable revision; exact replay does not increment the revision |
| `merge` | monotonic source/publisher profiles; identity/locality contradictions are conflicts, never last-write-wins |
| `ranking` | deterministic greedy MMR with compatible vectors or an explicitly disclosed heuristic fallback |
| `planning` | current operator-review-bound article/social slots with `dispatch_allowed=false` |

No image, final-verifier or writer result is synthesized. Missing, stale or unbound
evidence remains pending/review/reject according to the typed gate. In particular, a
model result for an old input fingerprint cannot advance candidate memory.

## Schedule policy ambiguity

The historical donor uses `12:00` for the article lane. A competing `11:30` value has
not been accepted by an owner decision. The planner therefore accepts exactly one unique
time per lane. Supplying both `11:30` and `12:00` returns
`blocked_policy_ambiguity`, produces no slots and never chooses one implicitly.

## Remaining integration work

This module proves transformation behaviour only. It does **not** prove that the Region
Talk Notebook is deployed, scheduled, connected to the ACTIVE epoch, or that migrated
rows have been transactionally applied. Those are separate pipeline/data-plane lanes and
the existing Region Talk `paused` gate remains authoritative until their acceptance
evidence is complete.
