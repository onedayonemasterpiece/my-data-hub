# ADR-0018: Automated Kaggle launch and runtime source attestation

Status: accepted owner decision, 2026-08-11

## Context

The production `events-bot` Telegram monitoring and CherryFlash paths already
launch Kaggle kernels without an interactive browser session. They use a
protected `KAGGLE_USERNAME`/`KAGGLE_KEY`, task-specific callback input,
domain-specific progress events, heartbeat, a local JSONL fallback and terminal
output recovery.

An observed `my-data-hub` canary proved that the same credential can push a
private Notebook, observe it reach `COMPLETE`, read output and delete it. The
pinned `kaggle==2.2.4` push response also carries exact `ref`,
`versionNumber` and `kernelId`. A separate provider-side `GetKernel` source
pull returned HTTP 403 with the legacy credential. Interactive OAuth was used
only to diagnose that endpoint and is not an acceptable production dependency.

## Decision

1. The orchestrator keeps the proven fully automated Kaggle launch model. A
   protected long-lived provider credential may be either the legacy
   username/key pair or a non-interactive API access token. It is mounted only
   into the control process which owns the single Kaggle adapter.
2. No production launch depends on browser OAuth, cookies, a human session, or
   a refresh token stored in a Notebook or devstand state directory.
3. Before push, control durably records the exact task/run/attempt, canonical
   source hash, callback authority and effect intent. It then persists the
   exact `ref`, numeric source version and numeric kernel ID returned by the
   official push call before projecting the launch.
4. The Notebook computes the hash of the source it actually executes and emits
   it in the authenticated `service.ready` event. Control must compare it with
   the persisted push intent before issuing database/tunnel credentials or
   projecting the service `ACTIVE`.
5. The terminal typed output repeats the same task/run/attempt, provider
   identity and runtime source hash. Missing, stale or differing attestation
   fences the attempt and cannot produce operational PASS.
6. A lost/ambiguous push response is never retried blindly. Reconciliation may
   use only the persisted exact effect plus the single control-owned provider
   authority; otherwise the attempt is terminally fenced for owner cleanup.
7. `RuntimeClient` remains the hardened successor to the events-bot status
   client: body tokens are moved to the Authorization header, while custom
   domain states, progress counters, resource lease events, event IDs, durable
   JSONL replay and terminal output recovery are retained.

## Consequences

- Legacy credentials remain sufficient for unattended launch/status/output
  and are not misreported as absent credentials.
- Provider-side source pull is optional corroboration, not the only source
  identity proof. Runtime attestation is mandatory before canonical authority.
- Acceptance tooling and workflows must not instantiate a second Kaggle client
  or accept self-authored Notebook assertions as live evidence; they consume
  the bounded, typed control projection instead.
- The exact private source/status/output canary must prove push-response
  identity, runtime source attestation, terminal binding and claim-bound cleanup
  before it is counted as a successful real run.

