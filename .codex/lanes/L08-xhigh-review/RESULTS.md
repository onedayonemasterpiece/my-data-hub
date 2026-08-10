# L08 separate security/data-integrity review

Status: **second review complete; final remediation requires third review**

The requested `XHigh` label was not available to the read-only reviewer role. The audit
therefore ran at the maximum available `checklist_reviewer` **High** effort and did not
misrepresent that limitation.

The initial review of PR-head `0a4e0cae00976711c0b2821e43f135175af7b759`
returned **not merge-ready**. It found critical operator transaction/idempotency gaps,
nominal rather than runtime-bound PostgreSQL roles, direct canonical-state mutation,
an events-bot normalizer mismatch, and high findings in recovery, workflows, OAuth,
connector transport and role probes.

Integration-owner remediation includes:

- PostgreSQL-backed operator journal committed in the DML transaction, durable replay,
  rollback-on-journal-failure test and impact/checkpoint binding;
- owner-scoped migrations, a dedicated canonical committer function/role, direct-state
  denials, 66 strict-SQLSTATE role/ownership probes and production service URL splits;
- events-bot deployed payload normalizer contract test and correction same-stream checks;
- durable recovery evidence recording/provider, post-restore object/outbox/bounded MCP
  verification, adapter environment minimization and age-key ownership/mode checks;
- bounded async OAuth/JWKS/revocation work, exact Host-port behavior, separate reader and
  authenticator URLs, workflow fixes and HTTPS/no-redirect/response-capped connector
  transport with jitter.

External deployment/provider findings remain blockers rather than fabricated passes.
The second review of `b214f24` found no remaining Critical issue, but correctly kept the
PR not merge-ready because per-service secret isolation was not runtime-usable, the
application role lacked its orchestration access, the events-bot payload still differed
from its target normalizer, no supervised canonical committer existed, workflows lacked
live post-deploy probes, and the recovery-to-operator checkpoint path was disconnected.

The current integration remediation adds per-service Unix users/environment files,
process-specific configuration validation, application/orchestrator role positives and
90 strict role/ownership probes, an exact events-bot producer-shape normalizer, a
supervised bounded committer, live connector/OAuth/MCP/revocation/process-kill/reboot
workflow probes, fail-closed nightly cadence/queue/recovery/provider checks, and a
verified recovery receipt → `sync.checkpoint` → operator gate path. The branch is not
merge-ready until these changes are committed, CI is green, and the same independent
reviewer returns a final no-Critical/no-High verdict.

Pre-final-review inspection found and remediated three additional High issues: recovery
evidence/checkpoints now bind the exact canonical revision, artifact/manifest hashes,
off-host locator, PostgreSQL major and extension versions and reject conflicting reuse;
the installer enables API/orchestrator/MCP as well as the two timers before any reboot
proof; and the connector canary's final accounting now uses a read-only monitoring
login rather than receiving a role-admin credential.

The final-review pass then identified and remediated three more privilege/liveness/
freshness gaps: service identity verification now rejects every unexpected direct or
transitive membership instead of checking only one expected group; deterministic poison
connector batches receive an idempotent terminal semantic-quarantine receipt without
starving later batches; and operator backup age now comes from the original backup
manifest rather than restore time, while high/bulk gates require the checkpoint's
canonical revision to equal the write's expected revision.
