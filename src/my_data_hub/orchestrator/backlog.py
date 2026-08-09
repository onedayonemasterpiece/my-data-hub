from __future__ import annotations

from collections import Counter

from my_data_hub.orchestrator.models import RegionTalkBacklog

STAGE_TO_FIELD = {
    "exact_url_intake": "exact_url_pending",
    "bge_m3_embedding": "bge_missing_for_e5",
    "vector_fusion": "fusion_ready",
    "text_eligibility": "text_gate_ready",
    "image_scoring": "image_ready",
    "source_profile": "source_profile_ready",
    "final_verifier": "final_verifier_ready",
    "writer": "writer_ready",
    "review_dispatch": "review_dispatch_ready",
    "review_sync": "review_sync_pending",
    "publication_plan": "publication_plan_ready",
    "publication_dispatch": "publication_dispatch_ready",
    "source_discovery": "source_discovery_due",
    "post_discovery": "post_discovery_due",
    "e5_embedding": "e5_due",
}


def load_region_talk_backlog(database_url: str) -> RegionTalkBacklog:
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("psycopg is required") from exc
    counts: Counter[str] = Counter()
    with psycopg.connect(database_url) as connection, connection.cursor() as cursor:
        cursor.execute(
            """
                SELECT ps.stage_key, count(*)
                FROM orchestration.work_item wi
                JOIN orchestration.pipeline p ON p.pipeline_id = wi.pipeline_id
                JOIN orchestration.pipeline_stage ps ON ps.stage_id = wi.stage_id
                WHERE p.workload = 'region-talk'
                  AND wi.status IN ('pending', 'failed_retryable')
                  AND wi.available_at <= now()
                GROUP BY ps.stage_key
                """
        )
        for stage_key, count in cursor.fetchall():
            counts[str(stage_key)] = int(count)
        cursor.execute(
            """
                SELECT count(*)
                FROM orchestration.worker_result_inbox
                WHERE workload = 'region-talk'
                  AND intake_status IN ('received', 'validated')
                """
        )
        completed_worker_results = int(cursor.fetchone()[0])
    kwargs = {field: counts.get(stage, 0) for stage, field in STAGE_TO_FIELD.items()}
    kwargs["completed_worker_results"] = completed_worker_results
    return RegionTalkBacklog(**kwargs)
