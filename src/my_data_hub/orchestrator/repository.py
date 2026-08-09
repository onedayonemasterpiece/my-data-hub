from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

from my_data_hub.orchestrator.models import PipelineDefinition


@dataclass(frozen=True, slots=True)
class PipelineRegistration:
    pipeline_id: UUID
    status: str


def register_pipeline(
    database_url: str,
    definition: PipelineDefinition,
    raw_path: Path,
) -> PipelineRegistration:
    """Register or refresh a versioned definition without changing runtime state.

    The status in a pipeline file is an initial state for a newly inserted version. A
    later migration/restart must never turn an operator-activated pipeline back to
    ``paused`` (or reactivate a deliberately paused pipeline) merely because the same
    definition is registered again.
    """
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("psycopg is required") from exc
    raw = json.loads(raw_path.read_text(encoding="utf-8"))
    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO orchestration.pipeline (workload, name, version, status, definition)
                VALUES (%s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (workload, name, version)
                DO UPDATE SET definition = EXCLUDED.definition, updated_at = now()
                RETURNING pipeline_id, status
                """,
                (
                    definition.workload,
                    definition.name,
                    definition.version,
                    definition.status,
                    json.dumps(raw),
                ),
            )
            row = cursor.fetchone()
            if row is None:  # pragma: no cover - defensive driver contract check
                raise RuntimeError("pipeline registration returned no row")
            registration = PipelineRegistration(
                pipeline_id=UUID(str(row[0])),
                status=str(row[1]),
            )
            for stage in definition.stages:
                cursor.execute(
                    """
                    INSERT INTO orchestration.pipeline_stage (
                        pipeline_id, stage_key, stage_version, compute_lane, priority,
                        max_attempts, timeout_seconds, contract, enabled
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                    ON CONFLICT (pipeline_id, stage_key, stage_version)
                    DO UPDATE SET
                        compute_lane = EXCLUDED.compute_lane,
                        priority = EXCLUDED.priority,
                        max_attempts = EXCLUDED.max_attempts,
                        timeout_seconds = EXCLUDED.timeout_seconds,
                        contract = EXCLUDED.contract,
                        enabled = EXCLUDED.enabled
                    """,
                    (
                        registration.pipeline_id,
                        stage.key,
                        stage.version,
                        stage.compute_lane,
                        stage.priority,
                        stage.max_attempts,
                        stage.timeout_seconds,
                        json.dumps({"name": stage.contract}),
                        stage.enabled_by_default,
                    ),
                )
        connection.commit()
    return registration
