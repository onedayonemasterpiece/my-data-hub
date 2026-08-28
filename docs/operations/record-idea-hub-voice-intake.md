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
introduced. Request bytes live only for the duration of the HTTP handler.

## HTTP contract

All routes require `Authorization: Bearer <device token>`.

- `GET /voice-intake/v1/health` — authenticated readiness and resolved model.
- `POST /voice-intake/v1/sessions` — validate/open one client-owned session.
- `PUT /voice-intake/v1/sessions/{session_id}/chunks/{chunk_index}` — validate
  RIFF/WAVE and SHA-256, perform one accounted Gemini transcription request and
  return structured transcript JSON.
- `POST /voice-intake/v1/sessions/{session_id}/complete` — summarize ordered
  client transcripts and publish one IdeaHub intake transaction.
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
```

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

Create the private file from the template:

```bash
install -m 600 deploy/control-plane/voice-intake.env.example \
  .state/control-plane/voice-intake.env
```

Generate a device credential without printing it into shell history:

```bash
python - <<'PY' > .state/control-plane/.voice-device-token
import secrets
print(secrets.token_urlsafe(48))
PY
chmod 600 .state/control-plane/.voice-device-token
```

Populate `MY_DATA_HUB_VOICE_DEVICE_TOKEN` from that file. Use the host's
existing GitHub CLI authorization only to bootstrap the container credential:

```bash
gh auth status
TOKEN="$(gh auth token)"
test -n "$TOKEN"
# Write TOKEN into MY_DATA_HUB_VOICE_GITHUB_TOKEN in the private env file
# without echoing it to logs or chat.
unset TOKEN
```

The control-plane container cannot rely on the host's interactive `gh` config
unless it is deliberately mounted. A private environment value is simpler and
keeps the container read-only.

Deploy the existing service with the opt-in overlay:

```bash
docker compose \
  -f compose.control-plane.yaml \
  -f compose.voice-intake.yaml \
  build control-plane

docker compose \
  -f compose.control-plane.yaml \
  -f compose.voice-intake.yaml \
  up -d control-plane
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
7. Complete one disposable session and verify the three-file commit and
   current-main readback in `idea-hub`.
8. Mark the disposable IdeaHub intake as excluded or remove it only through an
   explicit follow-up commit; never rewrite Git history.
9. Confirm application logs contain no token, audio bytes, transcript text or
   GitHub credential.

## Rollback

Remove `compose.voice-intake.yaml` from the compose command or set
`MY_DATA_HUB_VOICE_INTAKE_ENABLED=false`, rebuild the existing control-plane
container and verify `/voice-intake/v1/health` returns 503. Existing
control-plane, MCP and provider state remains unchanged.
