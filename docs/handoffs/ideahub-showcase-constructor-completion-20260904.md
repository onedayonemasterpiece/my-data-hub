# Handoff: IdeaHub Showcase constructor completion — 2026-09-04

## Objective

Implement the owner-approved constructor contract in
[`../ideahub-showcase.md`](../ideahub-showcase.md), deploy the exact main successor,
repair OAuth/discovery, and close the live content-only acceptance checklist.

## Ordered execution

1. **Luna / low:** perform a fresh read-only inspection of exact `origin/main`,
   live OAuth/discovery, tool schemas, deployment identity, and current receipts. Do
   not infer live state from source code.
2. **Terra / medium:** one Codex implementation task, after Luna evidence, limited to
   create-aware `showcase.apply`, focused tests, deployment of the exact successor,
   OAuth/discovery repair, and live content-only acceptance.
3. Update partner `main` only through the deployed Showcase MCP after acceptance.

Do not use Sol/high unless a specific, evidenced blocker requires it.

## Boundaries

- Do not generate Showcase YAML through Codex as a workaround for the missing
  create-aware contract.
- Do not treat a source commit, CI result, or deployment scaffold as live proof.
- Do not change or rotate an existing partner link on an ordinary update.
- Do not create a new control plane, general IdeaHub search API, fanout, or subagents.
- Do not work in a dirty main worktree; use a fresh isolated worktree from current
  `origin/main`.

## Completion evidence

Use the A–H checklist in
[`../operations/showcase-runtime-v2-verification.md`](../operations/showcase-runtime-v2-verification.md).
The completion record must show all normal source/publish actions after owner approval
occurred through MCP without Codex. Preserve exact source revision, deployment identity,
masked-link policy, receipts, and the partial-failure/rebuild result where exercised.
