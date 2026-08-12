# Operational MVP final acceptance receipt

The final receipt is validated by
`schemas/operational-mvp-acceptance-receipt.v1.schema.json` and the cross-artifact
checks in `scripts/validate_repository.py`. Schema validity alone is not proof of
operational completion.

## `COMPLETE` contract

`MY_DATA_HUB_OPERATIONAL_MVP_COMPLETE` is permitted only for an
`OBSERVED_OPERATIONAL` receipt validated in the exact evaluated checkout. The
following conditions are conjunctive:

- `evaluated_source_commit` is the exact checkout commit; the implementation
  merge, deployed commit, and post-deploy verified commit equal it, and the
  deployed tree is observed clean;
- Gates A through N appear exactly once, are `PASS`, and cite content-addressed
  evidence;
- the operational matrix receipt is live, commit-bound, schema-valid, and
  consistent with its 24 individually schema-valid scenario receipts;
- all 24 scenarios pass with at least 15 distinct numeric provider run refs and
  15 distinct provider kernel IDs, three master boots, two clean rotations, an
  abrupt termination, a control restart during an operation, a host reboot, and
  a measured 3,600–5,400 second soak with positive heartbeat, read, checkpoint,
  and recovery counts;
- reviewed implementation, deployment, post-deploy, security-audit,
  data-integrity-audit, and real-matrix evidence sets are non-empty, locally
  readable, match their declared SHA-256 values and source commit, and validate
  against the authoritative semantic schema for their evidence class;
- imported blogger count equals the observed YDB count, undispositioned and
  quarantined counts are zero, and E5/BGE-M3 coverage is 100%; and
- `completion_criteria_met` is true and both internal and external blocker sets
  are empty.

Required live evidence must be assembled as local sanitized files in the exact
checkout being evaluated. URLs may be recorded as supplementary evidence, but
an external locator cannot satisfy a required `COMPLETE` evidence set because
repository validation cannot verify its bytes offline. Every artifact directly
cited to close a Gate must likewise be locally content-verifiable. Files below
`examples/` and evidence marked synthetic or non-live can never support
`COMPLETE`.

The exact-head rule deliberately prevents a previously generated receipt from
being copied into a later checkout and presented as current acceptance. A final
live receipt is an output of validation/deployment evidence collection for the
commit it evaluates; changing that commit requires a new evaluation.

### Semantic evidence records

An evidence index entry is not proof merely because its file exists and its
digest matches. Each entry declares `schema_path`, `gate_ids`, and
`requirement_ids`; the validator reads the referenced JSON, validates it against
the one authoritative schema for its `artifact_kind`, and requires its content
to repeat the same commit, gate, and requirement scope. Typed assertions must
cover that exact scope.

| Gate(s) | Required evidence class |
| --- | --- |
| A-H | `REAL_KAGGLE_MATRIX` or typed `GATE_EVIDENCE` |
| I | `SECURITY_AUDIT` |
| J-K | `DATA_INTEGRITY_AUDIT` |
| L | `CONNECTOR_DURABILITY` |
| M | `DEPLOYMENT` or `POST_DEPLOY` |
| N | `IMPLEMENTATION_REVIEW` or `POST_DEPLOY` |

Gate L therefore cannot be closed by a generic dummy/review artifact. Its
evidence must validate against
`schemas/connector-durability-receipt.v1.schema.json`, report
`DURABLE_COMPLETE`, and be scoped only to Gate L. Likewise, the Kaggle matrix
must declare exact FM01-FM24 coverage. Every Gate A-N reference must be within
the artifact's declared gate scope and carry non-empty requirement scope.

The common semantic record schema is
`schemas/operational-mvp-evidence.v1.schema.json`. It covers implementation
review, deployment, post-deploy, security, data-integrity, and narrowly scoped
gate evidence. The corresponding example is content-bearing rather than a
generic outcome placeholder:
`examples/contracts/operational-mvp-evidence.v1.example.json`.

### Review and deployment provenance

For `COMPLETE`, `reviewed_head_commit` must be a real parent or ancestor of the
declared merge commit in the evaluated Git object graph. The merge commit must
actually have at least two parents; a label on an ordinary commit is not
accepted as a merge. Implementation-review evidence repeats both commits and
the observed relationship, records a GitHub pull request, and includes
successful hosted `contracts` and `postgres-integration` checks. Every hosted
check is bound to the exact reviewed head, not merely to the repository or PR.
Those disposable CI checks use `ubuntu-latest`. A recorded `provider-real`
check instead uses the exact owner-controlled runner labels
`[self-hosted, linux, my-data-hub-devstand]`; using a GitHub-hosted runner for
the private rotating OAuth file is rejected.

Deployment and post-deploy evidence repeat the exact deployed/verified commits
from the final receipt and record a clean deployment tree. Post-deploy hosted
checks are bound to the exact deployed commit. A stale deployment artifact or a
check from another head fails validation even when its file hash is valid.

## `BLOCKED` contract

`MY_DATA_HUB_OPERATIONAL_MVP_BLOCKED` requires
`completion_criteria_met: false` and at least one precise blocker. Every blocker
has an `INTERNAL` or `EXTERNAL` class, affected Gate A–N and/or FM01–FM24
requirements, a concrete missing condition, and the exact closure proof that
would remove it. Partial PASS results remain partial evidence and do not imply
completion.

The checked-in receipt at
`evidence/2026-08-11-operational-mvp/operational-mvp-acceptance-blocked.json`
remains an observed historical `BLOCKED` receipt. Its qualifying operational
scenario/run counts remain zero; the real diagnostics already recorded
elsewhere are not relabelled as operational-matrix passes.

The committed contract example is explicitly `SYNTHETIC_EXAMPLE`, is `BLOCKED`,
and is separately rejected by semantic validation if changed to `COMPLETE`.

## Validation

Run the focused checks and then the repository gate:

```bash
pytest -q tests/test_operational_mvp_acceptance_receipt.py
python scripts/validate_repository.py
```

The validator checks evidence identifiers, semantic evidence schemas and
requirement scope, required Gate-specific evidence kinds, local file hashes,
matrix and scenario schemas, run/kernel identity unions, lifecycle/count
agreement, Git review/merge provenance, hosted-check head binding, exact deploy
identity, and the distinction between synthetic, blocked, and observed-live
receipts. It does not convert missing live evidence into a pass.
