# Record Idea Hub voice intake on devstand

## Product boundary

The Samsung Android application is the durable owner of recording state, WAV
chunks, transcripts and retries. The existing `my-data-hub` control-plane
container is the only server boundary:

```text
Android AudioRecord + SQLite + WorkManager
  -> HTTPS /voice-intake/v1
       -> shared Google AI limiter
       -> Gemini Flash-Lite
       -> atomic GitHub transaction
  -> idea-hub/main readback
  -> Android deletes local audio
```

No additional Fly application, server database, queue or audio store is
introduced. Request bytes live only for the duration of the HTTP handler. The
only new server-side session metadata is the bounded terminology snapshot pin,
stored as a mode-0600 JSON envelope in the existing control-ledger volume so a
container restart cannot re-pin the same session to another card revision.

## HTTP contract

All routes require `Authorization: Bearer <device token>`.

- `GET /voice-intake/v1/health` — authenticated readiness and resolved model.
- `POST /voice-intake/v1/sessions` — validate/open one client-owned session,
  resolve the current `idea-hub/main` revision, and pin exactly one terminology
  snapshot to that session.
- `PUT /voice-intake/v1/sessions/{session_id}/chunks/{chunk_index}` — validate
  RIFF/WAVE and SHA-256, require the pinned IdeaHub terminology snapshot, perform
  one accounted Gemini transcription request and return structured transcript
  JSON.
- `POST /voice-intake/v1/sessions/{session_id}/complete` — summarize ordered
  client transcripts with the same terminology card and publish one IdeaHub
  intake transaction.
- `GET /voice-intake/v1/sessions/{session_id}` — reconcile deterministic GitHub
  publication after a timeout or process restart.

A quota denial returns HTTP 429 with `retryable=true` and
`retry_after_seconds`. Android retains audio, records `WAITING_QUOTA`, releases
the worker and retries later.

## Google accounting

The feature reuses `my_data_hub.google_ai.SupabaseGoogleAILimiter` from the
reviewed Gemini branch. Every physical request follows:

```text
preflight
-> reserve
-> resolve the limiter-selected key
-> mark_sent
-> exactly one provider POST
-> finalize
```

Provider 429 is reported to the canonical quota scope before finalization. The
feature has no process-local limiter, direct-key fallback or post-send retry.
Only models explicitly listed in `MY_DATA_HUB_VOICE_ALLOWED_MODELS` are
accepted, and every listed model must contain `flash-lite`.

## GitHub publication

The server accepts structured session data rather than arbitrary paths or
Markdown. It is hard-bound to `onedayonemasterpiece/idea-hub` and `main`.
One completed voice session creates one non-force Git transaction:

```text
inbox/voice/YYYY/MM/<session_id>.md
registry/sessions/YYYY/MM/<session_id>.md
registry/intake-sessions.yaml
inbox/voice/README.md
```

`config/voice-terminology.yaml` in `idea-hub/main` is the canonical bounded
project/domain vocabulary. At the start of every new session the runtime reads
the current branch head and then reads the card at that exact commit. It does
not use an image-embedded copy, a startup-only copy, a process-global TTL cache,
or a stale fallback. The resulting snapshot is held in bounded process memory,
durably mirrored to the existing `/ledger` volume, and included unchanged in
every transcription and the synthesis request for that session. Repeating
session creation with the same `session_id`, including after a process restart,
returns the original pin and does not resolve a new card. The pin is removed
only after verified publication reconciliation. `terminology_card_version` is
the card schema version and is not used as a freshness key; exact freshness and
identity come from the Git commit and blob SHA.

If GitHub or the current card cannot be read at session start, session creation
fails with a typed retryable error. An older snapshot is never relabelled as
current. If the control-plane restarts during an active recording, it reloads
the exact pin before accepting chunks or completion. If the durable pin is
missing or invalid, chunk/complete fails closed with
`voice_session_terminology_not_initialized` (or a typed state error) rather
than mixing snapshots.

The generated packet records `terminology_card_path`,
`terminology_card_version`, `terminology_card_commit`,
`terminology_card_blob_sha`, and `terminology_card_status: current`. The
user-facing URL is branch-stable
`/blob/main/...`; the exact publication SHA remains a separate receipt field.

Before updating `main`, the current registry is validated against the current
`schemas/intake-session.schema.json`. The operation uses the current tree as a
base, retries normal branch movement, and reconciles an unknown HTTP outcome by
deterministic `session_id`. Success is returned only after exact-commit and
current-main readback.

The neutral commit message is:

```text
intake(voice): register <session_id>
```

## Devstand configuration

The existing control-plane already receives the private provider environment
file selected by `MY_DATA_HUB_CONTROL_PROVIDER_ENV_FILE`. Add the voice settings
to that same private file; no Compose overlay or additional service is needed.
The file must remain mode `0600` and outside Git.

Generate a device credential without printing it into shell history:

```bash
python - <<'PY' > .state/control-plane/.voice-device-token
import secrets
print(secrets.token_urlsafe(48))
PY
chmod 600 .state/control-plane/.voice-device-token
```

Add these values to the existing private provider env. Preserve the already
configured Google limiter URL, service key, key pool and Google key variables:

```dotenv
MY_DATA_HUB_VOICE_INTAKE_ENABLED=true
MY_DATA_HUB_VOICE_DEVICE_TOKEN=<value from .voice-device-token>
MY_DATA_HUB_VOICE_MODEL=gemini-3.1-flash-lite
MY_DATA_HUB_VOICE_ALLOWED_MODELS=gemini-3.1-flash-lite
MY_DATA_HUB_VOICE_PROVIDER_TIMEOUT_SECONDS=180
MY_DATA_HUB_VOICE_MAX_AUDIO_BYTES=8388608
MY_DATA_HUB_VOICE_MAX_JSON_BYTES=2097152
MY_DATA_HUB_VOICE_GITHUB_TOKEN=<private token from gh auth token>
MY_DATA_HUB_VOICE_GITHUB_REPOSITORY=onedayonemasterpiece/idea-hub
MY_DATA_HUB_VOICE_GITHUB_BRANCH=main
MY_DATA_HUB_VOICE_TERMINOLOGY_STATE_PATH=/ledger/voice-terminology-snapshots.json
```

The following existing provider variables are mandatory and must remain in the
same effective container environment:

```dotenv
GOOGLE_AI_LIMITER_SUPABASE_URL=...
GOOGLE_AI_LIMITER_SUPABASE_SERVICE_KEY=...
GOOGLE_AI_NORMAL_KEY_ENVS=GOOGLE_API_KEY,...
GOOGLE_API_KEY=...
```

Use the host's existing GitHub CLI authorization only to bootstrap the
container credential:

```bash
gh auth status
TOKEN="$(gh auth token)"
test -n "$TOKEN"
# Write TOKEN into MY_DATA_HUB_VOICE_GITHUB_TOKEN in the private provider env
# without echoing it to logs or chat.
unset TOKEN
```

The control-plane container cannot rely on the host's interactive `gh` config
unless it is deliberately mounted. A private environment value is simpler and
keeps the container read-only.

Rebuild and restart the existing service through its current deployment
mechanism; the normal Compose equivalent remains unchanged:

```bash
docker compose -f compose.control-plane.yaml build control-plane
docker compose -f compose.control-plane.yaml up -d control-plane
```

The public reverse proxy should route only `/voice-intake/v1/` to the existing
loopback control-plane listener. Keep the control port itself loopback-only.
Apply the same request-size and timeout bounds at the reverse proxy:

- request body: at least 8 MiB plus small header overhead, but no more than
  9 MiB;
- upstream timeout: 240 seconds;
- TLS required;
- no proxy/body logging;
- do not expose `/internal/` or the control-plane catch-all through this rule.

## Smoke sequence

1. Verify ordinary control-plane health before and after deployment.
2. Call authenticated `GET /voice-intake/v1/health` locally.
3. Verify an unauthenticated request returns 401.
4. Verify an invalid WAV and wrong SHA are rejected before the limiter/provider.
5. Run shared-limiter preflight for the configured Lite model.
6. Send one short synthetic Russian WAV and verify one reserve/sent/finalize
   attempt in the limiter ledger.
7. Complete one disposable session and verify the four-file commit (source,
   detail, registry and voice index) and current-main readback in `idea-hub`.
8. Mark the disposable IdeaHub intake as excluded or remove it only through an
   explicit follow-up commit; never rewrite Git history.
9. Confirm application logs contain no token, audio bytes, transcript text or
   GitHub credential.

## Rollback

Set `MY_DATA_HUB_VOICE_INTAKE_ENABLED=false` or remove the voice variables,
rebuild the existing control-plane container and verify
`/voice-intake/v1/health` returns 503. Existing control-plane, MCP and provider
state remains unchanged.
