from __future__ import annotations

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
