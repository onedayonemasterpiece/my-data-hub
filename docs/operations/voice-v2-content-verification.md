# Voice Intake v2 content verification and deletion safety

This note defines the fail-closed boundary introduced for issue #31. It is
additive to API `2.0`: v1 is unchanged, the v2 state strings are unchanged, and
old Android clients retain audio until the legacy terminal response is safe.

## Deletion proof chain

No single receipt authorizes deletion. Every precondition below must be durable
and mutually bound before the next transition.

| Precondition | Durable evidence | What it does **not** prove |
|---|---|---|
| Transport complete | immutable close manifest plus source chunk receipts | transcript completeness |
| Each segment accepted | immutable segment receipt bound to source/input hashes, source and coverage ranges, exact `STOP`, schema and bounded plausibility/usage evidence | coverage of any other chunk |
| Content verified | content-verification receipt bound to the close-manifest hash, ordered segment-receipt hash, deterministic transcript hash and contiguous full range | publication durability or deletion authority |
| Publication verified | exact-commit and current-main readback recorded separately | semantic/content completeness or deletion authority |
| Purge authorized | immutable authorization receipt binding the content receipt and publication commit under a versioned policy | physical deletion |
| Audio purged | immutable purge receipt confirming both source and normalized paths are absent | permission to infer facts missing from earlier receipts |

`server_audio_purged=true`, `audio_purged=true`, and
`client_audio_purge_allowed=true` are truthful only after the final row. The
frozen old-client triplet—`published_verified`, `github_verified=true`,
`server_audio_purged=true`—is withheld until the same point. GitHub readback
alone cannot advance content verification, authorization, or purge.

The publication renderer accepts only a projection containing a valid content-
verification receipt and a consistent ordered per-source-chunk descriptor.
Therefore it cannot emit `Полная расшифровка` for legacy aggregate content,
short schema-valid content, incomplete coverage, or a readback-only row.

## Failure and restart matrix

| Failure/boundary | Durable result | Resume behavior | Audio |
|---|---|---|---|
| `MAX_TOKENS`, malformed/parseable truncation, missing/unknown finish reason, ambiguous provider outcome | no accepted segment receipt | explicit safe retry or reconciliation; never hidden replay | retained |
| Short schema-valid segment | failed plausibility/coverage evidence; no accepted receipt | correct/retry that segment only | retained |
| Missing segment, source mismatch, gap or overlap | no content-verification receipt | repair/reconcile evidence; summary and full publication blocked | retained |
| Crash after an accepted segment receipt | accepted receipt remains | reuse it; process next missing segment | retained |
| Crash after content verification | content receipt remains | do not repeat segment inference; continue summary | retained |
| Crash after summary | summary receipt remains | do not repeat inference; continue publication | retained |
| Exact GitHub readback with incomplete/unverified content | publication fact only | content remains blocked; no authorization | retained |
| Crash after publication readback | publication and content receipts remain | create/reuse purge authorization | retained |
| Filesystem purge failure | authorization remains, no terminal purge receipt | retry deletion only; no inference/publication replay | retained wherever deletion failed |
| Crash after filesystem deletion | authorization and path absence remain | verify absence, write/reuse purge receipt, then terminalize | already absent; never claim terminal before receipt |

Safe false negatives retain audio. No timeout or retention age substitutes for
the evidence chain.

## Existing-ledger migration

Migration is idempotent and additive. It creates independent verification and
receipt storage without promoting historical `github_verified` rows into
`content_verified` or `purge_authorized`. Historical already-purged rows remain
truthful as `legacy_unverified_purge`; they cannot authorize any new deletion
or client purge. The audit is metadata-only and bounded: it reports aggregate
counts, never session IDs or content.

Run migration/schema validation twice against a disposable copy and require a
clean second run plus SQLite integrity success before rollout. Preserve the
original ledger and spool snapshot through the rollback window.

## Rollout, canary and rollback

Roll out only after targeted/full tests, repository/schema validation and a
secret scan. A disposable synthetic canary must demonstrate both paths:

1. a long multi-chunk success reaches ordered full coverage, publication,
   separate authorization and verified physical deletion without replaying a
   successful Gemini call;
2. a deliberately short schema-valid segment never reaches full-transcript
   publication or purge, and every real source file remains.

Rollback disables only v2 processing/routes and restores the prior attested
image. It must preserve the migrated ledger and entire unfinished v2 spool
read-only/in place. Never downgrade the ledger, delete receipts/audio, mark
legacy rows verified, or infer purge permission from a historical GitHub
readback. Forward recovery resumes from the immutable receipts after the fixed
runtime is restored.
