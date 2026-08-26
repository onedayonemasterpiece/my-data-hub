# Direct Gemini analysis of public YouTube videos

## Purpose and boundaries

`youtube.video.analyze` is an additional, explicit route for analyzing one public YouTube URL with the Google Gemini Interactions API. It does not replace, remove, or silently fall back to the existing Kaggle/provider workflow. Every response identifies `provider=google_gemini_interactions` and `source_type=public_youtube_url`.

The feature supports four stateless modes:

- `summary`: summary, timeline, key points, claims requiring verification, visual observations, and uncertainty warnings;
- `transcript`: model-generated speech/video transcription with timestamped segments and separately observed on-screen text;
- `question`: one bounded question answered only from the supplied video, with supporting timestamps and confidence;
- `custom`: one bounded user prompt subordinated to the server-side evidence and safety instruction.

This is **model analysis of the video stream**, not an official YouTube caption track. Transcript responses use `transcript_source=gemini_media_transcription`. The service neither downloads nor returns the audio file and does not claim `youtube_captions`.

## MCP contract

Tool: `youtube.video.analyze`

Scope: `youtube:analyze`

Annotations:

- `readOnlyHint=true`, because analysis does not mutate provider or canonical data;
- quota consumption remains explicit through `idempotentHint=false`, the owner-only
  `youtube:analyze` scope, and the shared Supabase limiter;
- `destructiveHint=false`;
- `openWorldHint=true`;
- `idempotentHint=false` in the first release. `idempotency_key` is a correlation key only; there is no durable provider-side deduplication proof, so callers must not assume replay safety.

The tool is registered only when all of these are true:

1. `MY_DATA_HUB_GOOGLE_YOUTUBE_ENABLED=true`;
2. the remote MCP owner/operator profile or bounded unified-bootstrap profile is selected;
3. `MY_DATA_HUB_MCP_WRITE_ENABLED=true`;
4. `youtube:analyze` is present in `MY_DATA_HUB_MCP_SCOPES`;
5. the dedicated shared-limiter configuration is complete;
6. a fail-closed `YouTubeVideoAnalyzer` dependency is injected.

Reader and provider-only profiles never expose this tool. The bounded unified-bootstrap profile may expose it only under the explicit shared-quota acknowledgement and only to an OAuth identity that receives `youtube:analyze`; this path does not enable the operator write profile or require its checkpoint-protected security gate. The input schema is closed (`additionalProperties=false`). Prompt, question, URL, output-token count, response bytes, and result bytes are bounded. The tool has an isolated injected service and never enters the canonical `HubService._write` path; a YouTube-only owner scope therefore does not require a canonical data write permit or provider gateway.

## Input

Required:

- `youtube_url`: canonicalizable HTTPS URL on `youtube.com`, `www.youtube.com`, `m.youtube.com`, or `youtu.be`;
- `idempotency_key`: bounded correlation value.

Optional:

- `mode`: `summary | transcript | question | custom`;
- `question`: required only for `question`;
- `prompt`: required and allowed only for `custom`;
- `language`: default `ru`;
- `include_timestamps`: default `true`;
- `include_visual_observations`: default `true`;
- `model`: server allowlist only;
- `media_resolution`: `low | medium | high`;
- `max_output_tokens`: bounded by the server cap;
- `thinking_level`: `minimal | low | medium | high`; `minimal` is rejected for `gemini-3.7-flash`.

URL processing does not fetch the supplied URL. The service extracts and validates the 11-character video ID, removes only known tracking/start parameters, builds `https://www.youtube.com/watch?v=<id>`, and passes that URI to Google as a video data reference. HTTP, credentials, non-standard ports, fragments, IP addresses, localhost, arbitrary domains, redirects, playlists, and unknown query parameters are rejected.

## Provider request and streaming

The transport is direct asynchronous REST over `aiohttp`, not `google-genai`. One call to the transport performs exactly one physical `POST https://generativelanguage.googleapis.com/v1beta/interactions?alt=sse`; redirects and automatic retries are disabled. The request uses `Api-Revision: 2026-05-20`, places the video block before the text block, places `resolution` on the video block when supplied, requests bounded structured JSON, and always sends `stream=true`, `background=false`, and `store=false`.

The incremental parser accepts arbitrary network chunk boundaries, LF/CRLF, comments, named `event` fields, multiline `data`, and `[DONE]`. It recognizes interaction lifecycle and step events. On `interaction.created`, the bounded interaction ID is written immediately to the shared ledger before later stream events are consumed. Only text deltas belonging to a `model_output` step are assembled; thought deltas are never exposed. Structured JSON and terminal usage are interpreted only after a terminal interaction event.

The local service budgets are connect 30 seconds, first complete SSE event 120 seconds, idle between events 300 seconds, and total operation deadline 1800 seconds. They are operational bounds, not a Google SLA. Raw SSE bytes, assembled model-output bytes, and the final MCP response have separate configurable caps.

There is no stateful continuation in this release. `previous_interaction_id`, `store=true`, key/project binding state, and provider GET reconciliation are intentionally absent until a separate durable privacy/idempotency design exists.

## Shared quota accounting

The canonical quota ledger and model registry remain owned by `events-bot-new`. `my-data-hub` contains only a narrow adapter to the dedicated ledger. It never imports `events-bot-new` as a runtime dependency and never falls back to generic `SUPABASE_*` variables.

Required dedicated configuration:

- `GOOGLE_AI_LIMITER_SUPABASE_URL`;
- `GOOGLE_AI_LIMITER_SUPABASE_SERVICE_KEY`;
- `GOOGLE_AI_NORMAL_KEY_ENVS`.

Required capabilities:

- `limiter_contract=google_ai_project_model_atomic_v1`;
- `bucket_strategy=rolling_60s_pacific_day_v2`;
- `quota_dimension=quota_scope/model`;
- `lock_dimension=quota_scope/model`;
- `quota_scope_enforced=true`;
- `interaction_accounting=google_ai_interaction_usage_v3`;
- `unsent_release_supported=true`;
- `interaction_started_supported=true`;
- `interaction_started_rpc=google_ai_mark_interaction_started_v1`.

Production fails closed before any provider send when the ledger, capabilities, model row, candidate-key metadata, selected key environment, or exact accounting RPCs are unavailable.

### One physical POST, one lease

The enforced order is:

1. run capability/model/key metadata preflight;
2. reserve one atomic attempt using one stable `request_uid` and `attempt_no`;
3. read only the secret named by the limiter-selected `env_var_name`;
4. mark the exact attempt sent;
5. perform exactly one provider POST;
6. on `interaction.created`, call `google_ai_mark_interaction_started_v1` for the exact attempt without changing usage, reserve, or terminal accounting;
7. shield terminal finalization through `google_ai_finalize_interaction_v2` on success, provider rejection, timeout, network failure, oversized/malformed response, or cancellation;
8. record provider terminal status, duration, input/output/thought/total tokens, input modality breakdown, and typed error;
9. report provider 429 through the shared `quota_scope/model` cooldown.

A disconnect before `interaction.created` is ambiguous and is finalized as incomplete with reconciliation required. A disconnect after it preserves the known interaction ID, is also reconciliation-required, and never repeats the provider POST. Replaying the same ID to the start-marker RPC is idempotent; a different ID for the same attempt fails closed for reconciliation.

A limiter-selected key is metadata for one lease, not an independent quota claim. Multiple ENV keys may share one `quota_scope`; the ledger locks and accounts by `quota_scope/model`, so adding keys does not multiply project quota.

### Conservative video TPM reserve

Before the main POST, the service has no trusted duration/token count for a YouTube URL. It therefore reserves the full current model TPM from `google_ai_model_limits`—not a client-supplied duration and not a small prompt estimate. Finalization corrects the counter and the attempt's effective rolling-window TPM to provider `usage.total_tokens`, while preserving the original full reserve separately in the ledger. This can serialize video calls within a shared scope/model, but prevents parallel over-admission.

No extra token-count probe is issued. Any future pre-count request would itself require a separately accounted lease.

### Preview video-hours quota

The RPM/TPM/RPD ledger does not claim to enforce Google's separate preview quota measured in aggregate YouTube video hours per project/day. No trusted duration is available before the provider call. The response therefore carries `youtube_preview_video_hours_quota_not_preflighted`; provider quota rejection is typed and accounted, but the hours quota is not represented as RPM/TPM/RPD.

## Output and privacy

Success output is bounded and contains:

- request/provider/source/video/model/mode identity;
- `structured_output` and transcript source where applicable;
- provider terminal status and incomplete/truncated flags;
- actual input/output/thought/total usage and input modality usage when returned;
- reserved and actual TPM;
- redacted key and quota-scope aliases;
- limiter contract and bucket strategy;
- retry/reconciliation flags and warnings.

It never returns API keys, service keys, selected `env_var_name`, raw sensitive headers, or tracebacks. Raw video text/results are returned to the caller only; they are not persisted as business data in devstand PostgreSQL/SQLite. The shared ledger stores only quota and technical audit data required for accounting/reconciliation.

## Typed failures

The public contract distinguishes feature/configuration, URL/model, limiter/capability, key metadata/secret, RPM/TPM/RPD, provider 429/timeout/network/video/publicity, incomplete interaction, oversized/schema/usage, finalization, and reconciliation failures. A sent attempt whose finalization cannot be confirmed never returns an ordinary success; it returns `finalization_failed` with `reconciliation_required=true`.

No long quota sleep occurs inside an MCP call. Quota failures return `retryable` and bounded `retry_after_ms` when available. Ambiguous network timeout is not retried automatically because Google may already have accepted and charged the first POST.

## Rollout

1. Merge neither Draft PR until review is complete.
2. Apply the additive canonical limiter migration from the exact `events-bot-new` commit to the dedicated shared ledger, after dry-run/readback and scope-ownership verification.
3. Confirm `gemini-3.6-flash` and `gemini-3.7-flash` are each `RPM=5, TPM=250000, RPD=20` for every candidate quota scope to which the owner matrix applies. If scopes have different tiers, stop: the current global model table needs scoped overrides before traffic.
4. Run compileall, full pytest, Ruff, repository validation, and targeted Google/YouTube tests.
5. Run `scripts/operations/google_youtube_smoke.py` through the production adapter only. Direct curl/SDK probes with a key are forbidden.
6. Verify reserve → sent → interaction_started → finalize and actual usage in the ledger.
7. Enable `MY_DATA_HUB_GOOGLE_YOUTUBE_ENABLED=true` and add `youtube:analyze` only to the intended owner OAuth profile. Use the bounded unified-bootstrap install when no canonical write tools are needed.
8. Restart the existing remote MCP service, verify discovery, call the MCP tool, verify bounded output, MCP audit, and ledger evidence.

## Rollback

Set `MY_DATA_HUB_GOOGLE_YOUTUBE_ENABLED=false`, remove `youtube:analyze` from the owner profile, and restart the remote MCP service. This leaves the Kaggle/provider pipeline untouched. The canonical migration is additive; use the migration document in `events-bot-new` to restore model-limit rows from the mandatory pre-apply snapshot. Do not delete shared-ledger audit rows for attempts already sent.

Immediate rollback/block conditions are capability mismatch, uncertain quota-scope ownership, missing model limit, unleased provider send, unconfirmed finalization, secret exposure, or broader-than-intended MCP scope.

## Diagnosis

- `limiter_contract_mismatch`: deployed ledger lacks the exact atomic project/model contract, interaction-accounting v3 marker, or interaction-start RPC capability.
- `limiter_bucket_strategy_mismatch`: deployed ledger is not the rolling-60-second/Pacific-day contract.
- `model_limit_not_found`: requested stable model ID is absent or has non-positive limits.
- `key_metadata_missing`: a candidate ENV name is not registered with key alias and quota scope, or reserve selected an unconfigured ENV name.
- `key_secret_missing`: metadata selected a key but its secret is absent in the process environment; the unsent lease is released, or reconciliation is required if release cannot prove it was unsent.
- `finalization_failed`: provider send occurred but accounting finalization is unconfirmed; disable further calls and reconcile the request UID.
