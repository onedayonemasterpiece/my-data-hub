from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class DirectSourceTable:
    """One source table in the lossless Region Talk v2 snapshot."""

    name: str
    primary_key: str
    fixed_kind: str | None


# This tuple is deliberately closed.  The direct reader must not silently widen
# the production snapshot when another YDB table appears.
DIRECT_SOURCE_TABLES: tuple[DirectSourceTable, ...] = (
    DirectSourceTable(
        "acq_discovery_opportunities", "dedupe_key", "acq_discovery_opportunity_item"
    ),
    DirectSourceTable("acq_discovery_runs", "run_uid", "acq_discovery_run_item"),
    DirectSourceTable(
        "acq_discovery_surfaces", "external_id", "acq_discovery_surface_item"
    ),
    # Compact-state kind is a real column.  It must never be reconstructed from
    # the legacy pk prefix: the two values have diverged in historical data.
    DirectSourceTable("region_talk_compact_state_kv", "pk", None),
    DirectSourceTable(
        "region_talk_external_blogger_evidence",
        "record_id",
        "external_blogger_evidence_item",
    ),
)
DIRECT_SOURCE_TABLE_BY_NAME = {item.name: item for item in DIRECT_SOURCE_TABLES}

DIRECT_TYPED_CONTENT_KINDS: dict[str, str] = {
    "external_publication_intake_item": "article",
    "publication_candidate_item": "publication_candidate",
    "processed_post_item": "post",
    "post_live_item": "post",
    "acq_discovery_opportunity_item": "discovery_opportunity",
}

DIRECT_TYPED_QUEUE_KINDS: dict[str, str] = {
    "source_queue_item": "source_frontier",
    "source_candidate_item": "source_candidate",
    "source_status_item": "source_status",
    "source_edge_item": "source_edge",
    "post_link_queue_item": "post_intake",
    "candidate_memory_item": "candidate_memory",
    "image_queue_item": "image_processing",
    "publication_schedule_item": "publication_schedule",
    "publication_schedule_snapshot": "publication_schedule",
    "external_publication_review_item": "review",
    "external_publication_review_state_item": "review_state",
    "external_publication_review_event_item": "review_event",
    "publication_review_state_item": "review_state",
    "publication_review_event_item": "review_event",
    "publication_delivery_item": "delivery_history",
    "operator_feedback_item": "operator_feedback",
    "operator_feedback_latest_item": "operator_feedback_latest",
    "external_publication_intake_observation_item": "article_observation",
    "external_publication_seen_item": "article_seen",
    "queue_cursor": "cursor",
    "queue_metrics": "cursor_metrics",
    "state_snapshot": "state_snapshot",
    "run_state_snapshot": "run_state_snapshot",
    "acq_discovery_surface_item": "discovery_surface",
}

DIRECT_LLM_KINDS = frozenset(
    {
        "region_talk_llm_request_item",
        "region_talk_llm_budget_item",
    }
)

KNOWN_YDB_ROW_KINDS: tuple[str, ...] = (
    "candidate_memory_item",
    "image_queue_item",
    "publication_candidate_item",
    "post_link_queue_item",
    "text_vector_enrichment_item",
    "processed_post_item",
    "post_live_item",
    "source_queue_item",
    "source_status_item",
    "online_source_item",
    "source_candidate_item",
    "source_edge_item",
    "comment_link_item",
    "telegram_entity_cache_item",
    "source_onboarding_evidence_item",
    "source_onboarding_profile_item",
    "external_publication_source_item",
    "external_publication_intake_item",
)

MAPPING_TARGETS: dict[str, tuple[str, ...]] = {
    "candidate_memory_item": ("region_talk.candidate_memory", "hub.content_identity"),
    "image_queue_item": (
        "hub.content_asset",
        "orchestration.work_item",
        "region_talk.image_evaluation",
    ),
    "publication_candidate_item": (
        "region_talk.publication_candidate",
        "region_talk.candidate_revision",
    ),
    "post_link_queue_item": (
        "hub.content_identity",
        "region_talk.post_intake",
        "orchestration.work_item",
    ),
    "text_vector_enrichment_item": (
        "analysis.result",
        "analysis.embedding",
        "region_talk.text_evidence",
    ),
    "processed_post_item": ("region_talk.post_evaluation", "hub.provenance_event"),
    "post_live_item": ("hub.content_item", "hub.content_identity", "hub.project_content"),
    "source_queue_item": (
        "orchestration.work_item",
        "region_talk.source_work_projection",
    ),
    "source_status_item": (
        "region_talk.source_status",
        "orchestration.work_item_event",
    ),
    "online_source_item": ("hub.actor", "hub.external_account", "region_talk.source"),
    "source_candidate_item": ("region_talk.source_candidate", "hub.provenance_event"),
    "source_edge_item": ("region_talk.source_edge", "hub.provenance_event"),
    "comment_link_item": ("region_talk.discovery_observation",),
    "telegram_entity_cache_item": ("region_talk.telegram_entity_cache",),
    "source_onboarding_evidence_item": ("region_talk.source_profile_evidence",),
    "source_onboarding_profile_item": ("region_talk.source_profile",),
    "external_publication_source_item": (
        "hub.actor",
        "hub.external_account",
        "region_talk.external_publication_source",
    ),
    "external_publication_intake_item": (
        "hub.content_item",
        "region_talk.external_publication_intake",
        "hub.provenance_event",
    ),
}
