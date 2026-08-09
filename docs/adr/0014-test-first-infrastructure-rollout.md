# ADR-0014: Infrastructure and workflow evidence precede Region Talk migration

- Status: Accepted
- Date: 2026-08-09

## Context

A full Region Talk YDB migration is high-effort and hard to diagnose if database,
backup, remote MCP, connector delivery, Kaggle control and CI workflows are not already
proven independently.

## Decision

The next release sequence is test-first:

1. verify and harden the deployed devstand with all dangerous gates off;
2. prove clean migrations, role separation, backup and isolated restore;
3. establish pull-request, devstand, nightly and provider-canary tests;
4. publish remote MCP in read-only mode;
5. prove one synthetic push connector end to end;
6. prove Kaggle inventory and one disposable MCP-managed private resource lifecycle;
7. prove operator reads and bounded writes only in a disposable schema;
8. enable approved application-schema operator writes;
9. only then inventory and migrate Region Talk.

Every phase has negative authorization tests and an explicit receipt. Passing a later
happy-path test cannot waive an earlier restore, scope or protected-resource gate.

## Consequences

- Failures are attributed to one layer instead of being mixed with migration semantics.
- Data connectors and Kaggle management become reusable platform capabilities rather
  than Region Talk-specific side effects.
- Region Talk remains the first migrated workload, but not the first destructive test.
