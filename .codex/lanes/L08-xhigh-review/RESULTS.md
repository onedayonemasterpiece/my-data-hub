# L08 separate security/data-integrity review

Status: **initial review complete; remediation requires second review**

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
This lane is not closed until the remediation commit is pushed, CI is green and the
same independent reviewer completes a second audit.
