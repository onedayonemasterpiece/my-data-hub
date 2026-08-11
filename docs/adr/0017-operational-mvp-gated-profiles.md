# ADR-0017: Operational MVP gated profiles and bounded blogger import

Status: ACCEPTED OWNER DECISION / LIVE GATES BLOCKED

## Decision

The default remote MCP profile remains read-only. The owner/operator profile may be enabled
only after the write-scope, SQL-policy, preview/apply, fencing, pre-change checkpoint,
post-change checkpoint, restore and audit gates all pass against the deployed merge commit.
This does not change the current global safety switch: remote MCP writes remain disabled.

Region Talk production execution remains paused and publication remains disabled. The bounded
blogger-list workload is complete only after exact read-only YDB accounting, transactional
import into the ACTIVE Kaggle PostgreSQL primary, a verified checkpoint, independent cold
restore and MCP accounting/read proof. Until then it must expose an exact blocker rather than
claim completion.

## Current evidence state

The source inventory observed 266 distinct blogger records with a database-scoped read-only
principal, but no canonical import, checkpoint, restore or embedding run has completed. The
modern Kaggle OAuth token and required real Notebook matrix are absent. Therefore the bounded
import and owner/operator activation gates are blocked and the read-only profile is the only
permitted remote profile.

## Machine-readable binding

`architecture/invariants.yaml` records the exact profile and bounded-import gate values. The
repository validator treats them as owner-approved constants; changing them requires another
owner decision and corresponding evidence.
