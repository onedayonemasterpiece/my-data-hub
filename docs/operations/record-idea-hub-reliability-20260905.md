# Voice Intake v2: recovery source changes, 2026-09-05

Status: source implementation; deployment and physical acceptance are separate gates.
Base: b816f9a5c6f3d166c17b5c2c31110896e40241e2.
Companion Android PR: `onedayonemasterpiece/record-idea-hub#5`.

## Changed contract

A successfully accepted complete manifest transfers ownership of processing to the
server. The phone may disconnect. Pre-send transient failures, publication
retries and audio-purge retries have durable server deadlines (30 seconds by
default, or the specified provider/limiter delay). Repeating complete
is idempotent transport, NOT permission to repeat a paid inference. Unknown
or unusable generation outcomes remain fenced and require explicitly reviewed
recovery. There is no new automatic paid-retry endpoint.

A received and successfully accounted HTTP 429 is a known quota rejection, not
an unknown generation outcome. It waits at least the default 60 seconds when
no other delay was provided, then passes the shared limiter again. There is no
immediate in-adapter retry. If accounting itself failed or the outcome is
ambiguous, even a quota-related error remains fenced. Tests prove both cases.
Every physical request still gets its own accounting; two successful requests
is the normal no-error path, not a claim that rejected attempts never occur.

The `client_version` field is telemetry. Reopening a legacy session after an APK
upgrade may change this field only; stored original metadata and hash are kept.
All other capture metadata, chunk and completion checks remain strict.

`transcribing` / `summarizing` now mark the possible send boundary, not
local preparation or a completed previous stage. An expired in-flight lease
without a complete receipt still fails closed. Expired owners cannot update
session progress or renew a lease. A heartbeat protects long valid work.

## Durable receipts

Private `transcript.receipt.json` / `summary.receipt.json` contain a versioned,
checksummed, session/stage/manifest-bound receipt including request UID. The
adapter saves a valid provider result before limiter finalization. While
accounting is pending, the same lease/UID and usage are retained for an
idempotent accounting retry; no new generateContent call is made. Secret key
values and audio bytes are not in these receipts. Spool permissions are 0700,
receipt files are created 0600 and atomically fsynced/replaced.

A complete valid receipt can be reused after a crash or a failed SQLite write.
Corrupt, foreign and unreceipted results cannot authorize replay or publication.
Once GitHub verification is durable, purge retries skip both inference and
publication. Original value-only transcript/summary files remain compatible.

A pre-inference free-space check requires 64 MiB plus four maximum response
buffers. This is a preflight guard, NOT a reserved allocation or guarantee against
a disk filling concurrently, hardware failure, or a process dying before any
durable response exists. Unexpected worker errors are logged by type only,
without dictated content, credentials or tracebacks. Failed writes preserve
source audio.

## Checks

The regression suite exercises APK metadata compatibility, autonomous purge,
idempotent complete without paid consent, the transcript/summary crash gap,
response recovery after SQLite failure, accounting recovery without a second
provider POST, corrupt/foreign receipts, expired owners, low disk, quota
preflight and explicit versus ambiguous quota rejection. Existing synthetic
M4A/FFmpeg, two-request aggregate, publication and retention tests remain part
of acceptance. Tests use fakes; no real Gemini call or production mutation is
required.

## Rollout gates (not performed by this source change)

1. Deploy this backend before the companion Android client, preserving the
   existing private spool and ledger. Do not reset reconciliation rows or change
   session IDs. Back up the ledger using SQLite backup, not a live WAL-file copy.
2. Verify old Android v1 compatibility and v2 create/chunk/complete idempotency,
   current release identity, writable private spool and sufficient disk/inodes.
3. Test accepted complete with the phone disconnected, then restart between
   inference preparation, receipt persistence, publication and purge. A successful
   normal session still makes exactly two generateContent calls.
4. Test the APK upgrade against the existing installed signing certificate and
   queued recordings. Never uninstall a populated app just to bypass a signature
   mismatch. A CI debug APK is not evidence of install-over compatibility.
5. On the physical Samsung, test Finish -> screen off, network loss/return,
   process death, multiple queued sessions and server-side repair while the app
   is closed. Android/system force-stop and thermal restrictions cannot be
   promised away by source tests.
6. In a separately owner-approved maintenance window, prove the real public
   Nginx/Docker boot path, HTTPS Voice Intake contract and processing worker after
   reboot. `vpn-server` already has `restart: always`; localhost health alone is
   not this acceptance. No reboot or ingress reconfiguration is performed here.

Do not claim PRODUCT DELIVERED until source/CI, deployed backend, installable
upgrade and physical screen-off/reboot gates have independently passed.
