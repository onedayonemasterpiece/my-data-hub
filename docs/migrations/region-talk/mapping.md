# Initial YDB-to-PostgreSQL mapping

## Strategy

Every source row is inserted first into raw staging. Normalization then runs as an
idempotent mapping release identified by `mapping_version`. A source row may produce several
normalized records. The staging row stores its disposition and links to target identities.

Shared identities are preferred over Region Talk copies. Region-specific status/evidence
references those shared identities.

## Initial mapping registry

| YDB row kind | Primary target(s) | Notes |
|---|---|---|
| `online_source_item` | `hub.actor`, `hub.external_account`, `region_talk.source` | actor/outlet and platform account deduped by typed identity |
| `source_candidate_item` | `region_talk.source_candidate`, `hub.provenance_event` | retain proposed identity and discovery evidence |
| `source_edge_item` | `region_talk.source_edge`, `hub.provenance_event` | typed source-to-source relation and discovery method |
| `comment_link_item` | `region_talk.discovery_observation` | comment/link evidence; attach content/source when resolved |
| `telegram_entity_cache_item` | `region_talk.telegram_entity_cache` | operational cache with expiry and external identity |
| `source_queue_item` | `orchestration.work_item`, `region_talk.source_work_projection` | map state/retry/priority; source PK retained as legacy key |
| `source_status_item` | `region_talk.source_status`, `orchestration.work_item_event` | current status plus reconstructed event where evidence permits |
| `source_onboarding_profile_item` | `region_talk.source_profile` | versioned current profile and policy identity |
| `source_onboarding_evidence_item` | `region_talk.source_profile_evidence` | immutable evidence; never flatten into profile only |
| `post_link_queue_item` | `hub.content_identity`, `orchestration.work_item`, `region_talk.post_intake` | exact/manual URL lane |
| `post_live_item` | `hub.content_item`, `hub.content_identity`, `hub.project_content` | current compact post record |
| `processed_post_item` | `region_talk.post_evaluation`, `hub.provenance_event` | processing/gate state and reasons |
| `text_vector_enrichment_item` | `analysis.result`, `analysis.embedding`, `region_talk.text_evidence` | split metadata/result/vector; dedupe by input/model identity |
| `image_queue_item` | `hub.content_asset`, `orchestration.work_item`, `region_talk.image_evaluation` | preserve ordered media and all prior evidence |
| `candidate_memory_item` | `region_talk.candidate_memory`, `hub.content_identity` | durable duplicate/editorial memory |
| `publication_candidate_item` | `region_talk.publication_candidate`, `region_talk.candidate_revision`, review/publication tables | exact revision is immutable; current projection points to it |
| `external_publication_source_item` | `hub.actor`, `hub.external_account`, `region_talk.external_publication_source` | outlet scope and affiliation remain separate attributes |
| `external_publication_intake_item` | `hub.content_item`, `region_talk.external_publication_intake`, `hub.provenance_event` | preserve request/import identity and candidate/excluded/unresolved disposition |
| unknown | `migration.raw_record`, `migration.row_disposition` | retained as `retained_raw` or `quarantined`; blocks retirement until an explicit accepted disposition |

## Legacy keys and aliases

Each normalized object may carry several source identities through `hub.external_account`,
`hub.content_identity`, `hub.entity_alias` and `migration.legacy_identity_map`. The original YDB PK is never repurposed as the new primary
key. This permits deduplication without breaking traceability.

## Embeddings

Embedding rows are split into metadata and dimension-specific vector tables. Region Talk
currently needs at least 768-dimensional E5 and 1024-dimensional BGE-M3/Qwen-class vectors.
A unique result identity includes content hash, model ID, encoder contract and dimensions.
If a payload is malformed or model dimensions do not match, metadata remains staged and the
vector enters quarantine; it is not truncated/padded.

## Current-state versus history

A YDB row that contains only current state cannot manufacture a full event history. Import
creates a `migration_snapshot` event with source timestamps and raw-row link. Existing true
history/receipts are imported as events where available. New PostgreSQL operation is fully
append-audited from cutover onward.
