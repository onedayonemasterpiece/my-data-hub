# Voice Intake API v2 execution matrix

Base: `491b2ba55b8c7ec30fbcc97a9839ad874fbdeba0`

| ID | Requirement | Area | Dependencies | Conflict risk | Primary lane | Done when |
|---|---|---|---|---|---|---|
| R00 | Reconcile deployed drift and preserve live v1 fixes | runtime/Git | none | high | AUDIT-DRIFT | exact deployed source/image ancestry and authoritative Git base are proven |
| R01 | Keep `/voice-intake/v1` request/response/state/error semantics intact | v1 regression | R00 | high | TEST-V2 | unit/integration plus live WAV regression pass |
| R02 | Add authenticated `/voice-intake/v2` capabilities, durable create/upload/complete/status/retry contract | API/contracts | R00 | high | CORE-V2 | frozen schemas, errors, idempotency and states pass tests |
| R03 | Add minimal durable SQLite/spool runtime inside the existing control plane | persistence/worker | R02 | high | CORE-V2 | restart-safe ledger, bounded worker, lease, TTL and purge policy work |
| R04 | Aggregate session audio and perform one transcription request on the priority path | media/Gemini | R02,R03,R06 | high | CORE-V2 | N uploads make zero calls and completion makes one durable transcription call |
| R05 | Perform one durable text-only summary request without repeating transcription | Gemini/stages | R04,R06 | high | CORE-V2 | happy path summary is one call and stage retries are isolated |
| R06 | Reuse the shared limiter with recorded-audio reservation and no hidden post-send retry | quota/provider | R04,R05 | high | CORE-V2 | physical-call accounting and ambiguity tests pass |
| R07 | Publish v2 metadata atomically to IdeaHub and purge audio only after readback | GitHub/Markdown | R02-R05 | high | PUBLISH-V2 | exact/current-main verification precedes purge and Markdown provenance is complete |
| R08 | Implement the complete mandatory regression/security/failure test matrix | tests | R01-R07 | medium | TEST-V2 | all listed tests and repository gates pass |
| R09 | Freeze authoritative Android 1.1 handoff and v2 operations contract | docs/handoff | R02-R07 | low | DOCS-V2 | both requested documents cover routes, states, errors, battery/VAD constraints and deployed SHA |
| R10 | Deploy through the existing control plane and run v1/v2 live acceptance | deployment | R01-R09 | high | DEPLOY-LIVE | exact image/source, route, two-call proof, IdeaHub readback and purge are observed |
| R11 | Provide v2-only rollback preserving v1 and unfinished spool | operations | R03,R10 | low | DOCS-V2 | tested rollback procedure is documented |
| R12 | Produce closure matrix, evidence, and concise Android implementation prompt | reporting | R00-R11 | low | REPORT | every requirement is Done or explicitly qualified |

Dependency path: `R00 -> R02/R03 -> R04/R05/R06 -> R07 -> R08/R09 -> R10 -> R12`.
`R01` remains a continuous regression gate and `R11` is finalized before deployment.
