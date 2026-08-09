# Orchestration architecture

## Goals

The orchestrator converts product pressure into bounded work. It is not a fixed cron chain
that launches every stage regardless of backlog.

For Region Talk the preserved high-level order is:

1. reconcile completed worker results;
2. finish exact/known pending work before broad discovery;
3. fuse E5 and BGE evidence;
4. apply a single versioned text-eligibility contract;
5. hand eligible candidates to image diagnostics;
6. run final verifier and create exact review revisions;
7. synchronize operator decisions and publication outbox;
8. use remaining budget for source/post discovery and new embeddings;
9. produce funnel and zero-result evidence.

## Work-item lifecycle

```text
pending -> leased -> running -> succeeded
                    |        -> failed_retryable -> pending
                    |        -> failed_terminal
                    |        -> quarantined
                    -> lease_expired -> pending
```

Every state transition produces `orchestration.work_item_event`. Lease acquisition uses a
bounded transaction and `FOR UPDATE SKIP LOCKED`. Attempts, deadlines and retry policy are
explicit; a process crash does not imply success.

## Idempotency

A work item has a stable dedupe key derived from workload, stage, subject identity, input
fingerprint and policy/model version. A worker result has a stable `result_id` and artifact
hash. Re-delivery is a no-op only when both identity and hash match; same identity with a
different payload is quarantined.

## Backpressure

The planning policy supports:

- exact pending SLA before discovery tail;
- stopping similar/discovery lanes when actionable backlog grows;
- model-specific capacity and cooldowns;
- maximum in-flight work per stage/provider;
- suppressing upstream generation when downstream is blocked;
- bounded run duration and a required zero-result reason.

## External side effects

Telegram review dispatch and final publication are not performed in the same transaction as
a domain decision. The decision and outbox record commit atomically; a dispatcher performs
the effect and records an exact receipt. Publication requires the exact text, URL and ordered
media fingerprint that the operator approved.
