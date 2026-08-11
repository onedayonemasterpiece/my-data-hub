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
  readable, and match their declared SHA-256 values and source commit;
- imported blogger count equals the observed YDB count, undispositioned and
  quarantined counts are zero, and E5/BGE-M3 coverage is 100%; and
- `completion_criteria_met` is true and both internal and external blocker sets
  are empty.

Required live evidence must be assembled as local sanitized files in the exact
checkout being evaluated. URLs may be recorded as supplementary evidence, but
an external locator cannot satisfy a required `COMPLETE` evidence set because
repository validation cannot verify its bytes offline. Files below `examples/`
and evidence marked synthetic or non-live can never support `COMPLETE`.

The exact-head rule deliberately prevents a previously generated receipt from
being copied into a later checkout and presented as current acceptance. A final
live receipt is an output of validation/deployment evidence collection for the
commit it evaluates; changing that commit requires a new evaluation.

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

The validator checks evidence identifiers, required evidence kinds, local file
hashes, matrix and scenario schemas, run/kernel identity unions, lifecycle/count
agreement, exact commit identity, and the distinction between synthetic,
blocked, and observed-live receipts. It does not convert missing live evidence
into a pass.
