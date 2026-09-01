# DevCoveer MCP (local devstand installation)

## Source and runtime

This installation is currently **not a Git checkout**. The executable source is
`/home/dev/.local/libexec/openai-codex-mcp/` and the public entrypoint is
`/home/dev/.local/bin/codex-mcp-server`.

`codex-devserver-tunnel.service` publishes one MCP gateway through the secure tunnel.
The gateway talks to:

- a persistent native `codex app-server --stdio` child;
- `opencode-devcoveer.service` on authenticated `http://127.0.0.1:4097`.

OpenCode 1.18.15 is localhost-only. NVIDIA credentials are visible only to the
OpenCode service, never to the tunnel or Codex child. MCP catalog responses whitelist
model metadata and never return provider options, headers, environment, or keys.

## Routing — NO AUTOMATIC FALLBACK

| Input | Backend |
|---|---|
| model/provider omitted | Codex |
| live Codex model | Codex |
| exact OpenCode-only model | OpenCode |
| explicit connected OpenCode provider | OpenCode |
| `council_run` | OpenCode multi-model council |

A selected backend failure is returned as that backend's failure. It is never silently
replaced by another model or provider.

The default model for explicit `provider=opencode` with no model is
`opencode/nemotron-3-ultra-free`. Exact provider/model IDs come from `list_models`.
Codex `reasoning_effort` is not emulated for OpenCode.

## Task identity and lifecycle

New tasks receive a `dvt_<uuid>` DevCoveer ID stored as mode-0600 JSON under
`~/.local/share/openai-codex-mcp/tasks/`. New Codex responses also retain `threadId`
for backward compatibility, and old native Codex IDs remain readable.

- `read_task` polls Codex or OpenCode without changing sessions.
- `continue_task` reuses the same backend session.
- Active compatible Codex corrections use `turn/steer` on the same turn.
- `cancel_task` uses Codex `turn/interrupt` (including descendants) or OpenCode abort;
  thread/session history remains intact.
- An OpenCode project permits at most one active writer. OpenCode never receives shell,
  external-directory, sub-agent, or dotenv access through DevCoveer. Read-only
  `websearch` and `webfetch` are exposed for research; retrieved pages are explicitly
  treated as untrusted data rather than instructions.

## Council

`council_run` is read-only and has three explicit cost tiers. `tier=free` is the
default; omitting `participants` selects the preset for that tier.

| Tier | Preset execution | Planned NVIDIA calls (`rounds=2`) |
|---|---|---:|
| `free` | All six inference-verified free Zen models perform initial position, cross-critique, and revision | 0 |
| `extended` | The same free debate plus Kimi K3 as a final reviewer only | 1 target success |
| `pro` | One Zen anchor plus Kimi K3, Nemotron 3 Super, and Nemotron 3 Ultra in the full debate | 9 target successes |

The free set is Nemotron 3 Ultra, Nemotron 3.5 Lightning, MiMo V2.5, Muse Spark 1.2,
Big Pickle, and Ling 3.0 Flash. Exact 2–8 participant overrides remain available, but
the gateway enforces the tier:
`free` rejects NVIDIA; `extended` allows at most one NVIDIA model and invokes it only
as the final reviewer; `pro` permits NVIDIA full-debate participants. A paid tier never
starts on its first call. It returns `confirmation_required`, the exact participant/attempt
budget, and a one-time ten-minute token. ChatGPT must show the plan, wait for a new explicit
user confirmation, and only then replay the identical request with
`paid_confirmation_token`. The token is request-bound and single-use. There is no silent
fallback or automatic tier escalation.

Each target inference may be retried only for provider-declared transient failures or
HTTP 408/425/429/5xx capacity errors, with bounded backoff (maximum five accepted attempts
per stage). HTTP 410 and other terminal errors are not retried. The normal response wait is
15 minutes so a slow free model is not discarded just before it completes. Final results
expose retry counts, actionable provider failure details, target calls, maximum transient
attempts, and actual accepted attempts. Known retired hosted models are excluded from both
discovery and routing. DeepSeek V4 Pro was tombstoned after NVIDIA returned HTTP 410 EOL;
the dated replacement visible in the catalog remains catalog-only until separately
inference-verified.

An unqualified council request is always `free`. It must not be supplemented with a
separate `start_task`, Codex/Sol review, NVIDIA call, or any outside model unless the user
explicitly requests that additional work.

The `read_task` result is a `devcoveer-council-result.v2` structured document. It keeps
each initial position, critique, revision, key contribution, and expert review attributed
to its exact provider/model. It also includes agreements, disagreements, novel findings,
unresolved questions, failures, planned/actual provider usage, and an
`integration_guide`. ChatGPT should pass its pre-council answer in
`baseline_conclusion`; the document then preserves that baseline for the before/after
comparison. The guide tells ChatGPT to name the strongest model contribution, explain
what the council added, issue a revised conclusion, and preserve remaining uncertainty.
The bridge does not pretend
that a majority vote is truth and never requests hidden chain-of-thought.

Poll the returned task ID with `read_task`.

## Discovery

`list_models` aggregates live Codex and authenticated localhost OpenCode catalogs.
The default `verified_only=false` returns the complete connected live catalog;
`verified_only=true` intersects providers that have a verification receipt. Listing
never performs inference.
Third-party uptime cannot be guaranteed; the result carries evidence status/time.

## Deployment and rollback

```bash
python -m py_compile /home/dev/.local/libexec/openai-codex-mcp/*.py
cd /home/dev/.local/share/openai-codex-mcp/verification
python -m unittest -v
systemctl --user restart opencode-devcoveer.service
systemctl --user restart codex-devserver-tunnel.service
curl -fsS http://127.0.0.1:18080/healthz
curl -fsS http://127.0.0.1:18080/readyz
```

Rollback bridge source from the timestamped `.bak.*` file, then restart only after
checking that no DevCoveer task is active. Never attach a project `.env` to the tunnel
service.

## Troubleshooting

- 502 plus failing `list_tasks`: check both user units and the tunnel journal.
- OpenCode unavailable: check `opencode-devcoveer.service` and authenticated health.
- Model absent: call `list_models(provider=...)`; do not guess or use a fuzzy alias.
- Active correction with new access/model: cancel, wait terminal, continue same task.
- The source is not presently version-controlled; this is an operational limitation,
  so every change must retain a timestamped rollback copy.
