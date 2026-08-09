from __future__ import annotations

import json
from dataclasses import asdict
from typing import Any
from uuid import UUID

from my_data_hub.orchestrator.backlog import load_region_talk_backlog
from my_data_hub.orchestrator.policy import plan_region_talk


def record_region_talk_plan(
    database_url: str,
    *,
    trigger: dict[str, Any],
    max_actions: int,
) -> dict[str, Any]:
    """Create one durable plan without launching providers or external side effects."""
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("psycopg is required") from exc

    backlog = load_region_talk_backlog(database_url)
    actions = plan_region_talk(backlog, max_actions=max_actions)
    plan = {
        "policy": "region-talk-pressure-aware.v1",
        "backlog": asdict(backlog),
        "actions": [asdict(action) for action in actions],
        "dispatch_status": "not_dispatched",
    }
    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT p.pipeline_id, hp.project_id, cs.canonical_revision, p.status
                FROM orchestration.pipeline p
                JOIN hub.project hp ON hp.slug = 'region-talk'
                CROSS JOIN hub.canonical_state cs
                WHERE p.workload = 'region-talk'
                ORDER BY p.created_at DESC
                LIMIT 1
                """
            )
            row = cursor.fetchone()
            if row is None:
                raise RuntimeError("Region Talk pipeline is not registered")
            if str(row[3]) != "active":
                return {
                    "created": False,
                    "pipeline_status": str(row[3]),
                    "plan": plan,
                }
            cursor.execute(
                """
                INSERT INTO orchestration.run (
                    pipeline_id, project_id, run_kind, status, canonical_revision,
                    trigger, plan
                ) VALUES (%s, %s, 'scheduled', 'planned', %s, %s::jsonb, %s::jsonb)
                RETURNING run_id
                """,
                (row[0], row[1], row[2], json.dumps(trigger), json.dumps(plan)),
            )
            run_id = UUID(str(cursor.fetchone()[0]))
        connection.commit()
    return {"created": True, "run_id": str(run_id), "plan": plan}
