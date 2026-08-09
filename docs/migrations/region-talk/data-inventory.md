# Region Talk YDB data inventory

## Source shape observed in the donor implementation

The current orchestrator reads a row store keyed by `kind:` prefixes. Rows expose a source
primary key, JSON payload and update time. The following kinds are explicitly read by the
current metric/orchestration layer and therefore form the minimum mandatory inventory:

| YDB row kind | Intent |
|---|---|
| `candidate_memory_item` | durable candidate/duplicate and editorial memory |
| `image_queue_item` | image-analysis work and evidence state |
| `publication_candidate_item` | final/review candidate projection |
| `post_link_queue_item` | exact/manual post URL intake |
| `text_vector_enrichment_item` | E5/BGE embeddings and semantic evidence |
| `processed_post_item` | processed post state/history projection |
| `post_live_item` | current/live post projection |
| `source_queue_item` | source discovery work queue |
| `source_status_item` | source processing/classification status |
| `online_source_item` | discovered/known source record |
| `source_candidate_item` | proposed source identity/evidence |
| `source_edge_item` | source discovery/recommendation relationship |
| `comment_link_item` | comment-derived link evidence |
| `telegram_entity_cache_item` | resolved Telegram entity cache |
| `source_onboarding_evidence_item` | source profile evidence |
| `source_onboarding_profile_item` | canonical source onboarding/profile state |
| `external_publication_source_item` | external publication outlet/source record |
| `external_publication_intake_item` | manual/deep-research intake and status |

This list is **not accepted as proof that no other kinds exist**. The export must first scan
all keys or source metadata and produce a complete kind histogram. Any newly discovered kind
is exported and retained even before a normalized mapping exists.

## Required inventory outputs

- source endpoint/database/table identity (redacted where public);
- source schema and key ordering;
- exact export start/end timestamps and consistency mode;
- total row count and count by kind;
- min/max key and update timestamp per kind;
- duplicate primary keys (must be zero);
- invalid JSON count and byte size;
- large-row distribution;
- null/missing update timestamp count;
- all row kinds not in the initial mapping registry;
- source code commit used for extraction.

## Semantic fields to preserve

Mapping must not discard evidence fields merely because the first PostgreSQL projection does
not query them. Especially preserve:

- canonical/exact URL and external platform IDs;
- source fingerprints and alias history;
- discovery method/query/edge and timestamps;
- text/full-text excerpt/summary fields allowed by current retention policy;
- model IDs, encoder contracts, text/content hashes and semantic-bank versions;
- source scope/geo/topic/externality verdict and evidence;
- all gate versions, reasons, status transitions and retry metadata;
- image URL/order/hash/verdict/evidence and terminal/unsupported status;
- candidate revision, text, CTA, ordered media and approval fingerprint;
- operator review and publication receipts;
- import request ID and external-publication dedupe identity.

## Export format

One or more UTF-8 JSONL files. Each line conforms to
`schemas/region-talk-ydb-export-row.v1.schema.json` and wraps the original row without rewriting:

```json
{
  "schema_version": "region-talk-ydb-export-row.v1",
  "export_batch_id": "...",
  "source_table": "...",
  "source_pk": "source_queue_item:...",
  "row_kind": "source_queue_item",
  "source_updated_at": "2026-08-09T12:00:00Z",
  "payload": {},
  "payload_sha256": "..."
}
```
