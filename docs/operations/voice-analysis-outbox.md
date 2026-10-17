# Optional voice-analysis delivery

Voice Intake v2 can create an analysis candidate after exact GitHub publication
readback. This is additive: Android, v1, aggregate transcription, summary and
server audio purge keep their existing contract. No analytics request is sent
from the ASR worker. No existing session is scanned or backfilled on startup.

## Configuration and security

Default is off. Deployment does not create credentials, enroll a subscription,
choose a Slack destination or authorize disclosure of future notes.

- `MY_DATA_HUB_VOICE_ANALYSIS_ENABLED=true` opts in for newly verified notes.
- `MY_DATA_HUB_VOICE_ANALYSIS_OWNER_ID` is a trusted server binding for the
  existing single authenticated recording principal. Never derive it from
  `device_label`, summary text or the dictated note.
- `MY_DATA_HUB_VOICE_ANALYSIS_ENDPOINT` is the HTTPS Projects Hub endpoint
  `/api/integrations/voice-analysis/intents`.
- `MY_DATA_HUB_VOICE_ANALYSIS_TOKEN` must be an explicitly authorized internal
  credential bound on the receiving server to the same owner and allowed source.

No owner binding means no capture. A configured owner with missing transport
keeps new captured intents pending without affecting successful recordings.
Disabling the flag stops capture and delivery, retaining existing pending state.
Changed owner/repository bindings cannot dispatch old queued input as a new owner.

## Protocol

Only a phrase match in the **final raw transcript**, never a summary or partial
ASR, creates a candidate. Case and whitespace are normalized by matching;
original spelling and Unicode codepoint offsets are retained. The consumer/dot
must still reject quoted commands, feature discussion and negated intent before
creating questions or applying changes. This event is not unlimited authority.

The immutable `source` contains repository, path, original publication commit SHA
and SHA256 of UTF-8 stripped raw transcript. Its hash identifies `intent_id`.
POST also carries server-bound owner ID, session ID, one-element initial input
manifest and trigger location. It does not send transcript content or credentials
in JSON. The existing SQLite ledger stores only the operational outbox and its
receipts; it is not a canonical project/business database.

The verified source receipt and intent are committed atomically before audio
purge. A restart after that transaction skips publication and inference, finishes
purge and delivers the same immutable intent. POST timeout/503/202 retries the
same ID after 60 seconds; 400/401/403/404/409/422 and redirects are blocked for
operator reconciliation. A 200 must echo the intent and exact source, accepted
state, request ID and event ID. Remote idempotency is mandatory.

A separate bounded worker GETs accepted intents and mirrors the returned
`projection.markdown` only when its SHA256 equals `projection.revision`. It uses
the saved original source commit, not recent Git history, verifies original and
current transcript hashes, and edits only its explicitly marked analysis block.
Other source content remains intact. A manually edited managed block is a
conflict, not an invitation to overwrite. Git writes use optimistic branch CAS;
a missing response is reconciled by readback. Receipts record the source-note
commit and projection revision only after exact/current readback. A later retry
compares the block rather than appending a second copy.

Projection conflicts and transport failure never downgrade a published note or
repeat ASR. The existing publication-history fallback is only for unresolved
initial publication; normal verified notes and their later analytics always use
the saved original commit. No automatic historical migration is enabled.

## Verification boundaries

Synthetic local tests cover trigger/source identity, disabled/unbound behavior,
atomic restart before purge, lost-response replay, terminal/transient HTTP,
original preservation, duplicate projection, changed-source rejection and manual
edit protection. Real device recording, real internal service binding, dot event
subscription and live question/answer delivery require separate rollout readback;
local tests do not establish those results. Broader project document changes and
Slack delivery remain owned by the authorized analysis workflow, not this sender.

### Corrected transport configuration

A trusted local operator may explicitly redrive exactly one blocked HTTP
401/403/404 intent after correcting authorization or deploying the receiving
route, using the same configured source repository and owner:

`python -m my_data_hub.voice_intake_v2.analysis --spool /voice-intake-v2 --intent-id voice:…`

This requires access to the existing private spool and configured transport;
it is not exposed to recording devices. It cannot redrive an immutable-source
conflict, receipt mismatch or changed owner. It preserves the original payload
and reports whether a transition occurred. No historical note scan is performed.

Each projection also compares the expected previous managed revision. An expired
sender cannot overwrite a newer valid projection with an older fetched one. A
crash after the Git write but before its local receipt reconciles if the same
projection is returned. If the backend has already advanced to another revision,
the unknown intermediate becomes `projection_reconciliation_required`, with its
precise conflict code retained. It is not blindly retried or overwritten. The
original intent, PH request and source remain intact. A local operator can inspect
one intent with the above command plus `--status`; reconcile the observed Git
block, PH result and receipt before choosing a recovery. Transport redrive does
not clear this source/projection conflict.

### Persistent private configuration

`MY_DATA_HUB_VOICE_ANALYSIS_CONFIG_FILE` optionally points to an already approved,
read-only mounted JSON file containing exactly `enabled` (boolean), `owner_id`,
`endpoint`, and `token` (strings). This file takes precedence over individual
analysis environment variables. It must be a regular, non-symlink file with no
group/other permissions, at most 32 KiB. Invalid or unavailable configuration
fails only the analytics add-on closed, never baseline recording/ASR. Tokens are
excluded from configuration repr and errors. Provisioning or granting the file
is a separate approved activation step, not part of image creation. Preserve its
mount/reference in the persistent deployment composition; never bake it into
images, build contexts, repository commits or logs.

A removed or malformed original transcript heading is explicitly
`analysis_source_structure_changed` / `projection_reconciliation_required`.
The sender does not retry parsing indefinitely or rewrite the changed source.

### Bounded final projection polling

Projects Hub GET supplies `projection_terminal`, `case_id`, monotonically increasing
`input_revision` within that case, and `member_intent_ids`. Only after the final
projection's same-note Git write/readback is verified does the local row become
`completed`, excluded from periodic polling. Older servers without lifecycle
metadata retain polling rather than prematurely declaring completion.

An active or newly accepted linked intent can refresh existing completed member
rows once when its case changes or its input revision advances. The fenced receipt
transaction checks the existing member's trusted owner and repository, retains its
original source and projection receipt, and records the observed case revision.
Unknown intents, untriggered source notes, other owners and repositories are never
enqueued by this mechanism. Linking remains an explicit Projects Hub operation;
intake does not infer semantic groups or accept arbitrary destination paths.
