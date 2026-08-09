from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from my_data_hub.artifact_store import LocalArtifactStore, StoredArtifact
from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.notebooks.contracts import NotebookInputManifest


@dataclass(frozen=True, slots=True)
class ClaimedWorkItem:
    work_item_id: UUID
    subject_type: str
    subject_id: UUID
    input_fingerprint: str
    payload: dict[str, Any]


@dataclass(frozen=True, slots=True)
class PreparedDispatch:
    run_id: UUID
    stage_key: str
    compute_lane: str
    lease_token: UUID | None
    work_items: tuple[ClaimedWorkItem, ...]
    artifact: StoredArtifact


def build_notebook_manifest(
    *,
    run_id: UUID,
    workload: str,
    stage_key: str,
    stage_contract_version: str,
    canonical_revision: int,
    work_items: list[ClaimedWorkItem],
    model: dict[str, Any],
    limits: dict[str, Any],
    artifacts: list[dict[str, Any]] | None = None,
) -> NotebookInputManifest:
    return NotebookInputManifest.model_validate(
        {
            "schema_version": "my-data-hub-notebook-input.v1",
            "run_id": run_id,
            "workload": workload,
            "stage": stage_key,
            "stage_contract_version": stage_contract_version,
            "canonical_revision": canonical_revision,
            "work_items": [
                {
                    "work_item_id": item.work_item_id,
                    "subject_type": item.subject_type,
                    "subject_id": item.subject_id,
                    "input_fingerprint": item.input_fingerprint,
                    "payload": item.payload,
                }
                for item in work_items
            ],
            "artifacts": artifacts or [],
            "model": model,
            "limits": limits,
            "created_at": datetime.now(UTC),
        }
    )


def persist_notebook_manifest(
    store: LocalArtifactStore,
    manifest: NotebookInputManifest,
) -> StoredArtifact:
    relative = (
        f"dispatch/{manifest.workload}/{manifest.run_id}/"
        f"{manifest.stage}/input-manifest.json"
    )
    return store.write_bytes(
        relative,
        canonical_json_bytes(manifest.model_dump(mode="json")),
    )


def claim_stage_work(
    database_url: str,
    *,
    workload: str,
    stage_key: str,
    lease_owner: str,
    limit: int,
    lease_seconds: int,
) -> tuple[str, list[ClaimedWorkItem]]:
    """Lease a bounded FIFO/priority batch. Provider launch remains a separate adapter."""
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("psycopg is required") from exc

    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT ps.stage_id, ps.compute_lane
                FROM orchestration.pipeline p
                JOIN orchestration.pipeline_stage ps ON ps.pipeline_id = p.pipeline_id
                WHERE p.workload = %s AND p.status = 'active'
                  AND ps.stage_key = %s AND ps.enabled = true
                ORDER BY p.created_at DESC
                LIMIT 1
                """,
                (workload, stage_key),
            )
            stage = cursor.fetchone()
            if stage is None:
                raise RuntimeError(f"active stage not found: {workload}/{stage_key}")
            cursor.execute(
                "SELECT * FROM orchestration.claim_work_items(%s, %s, %s, %s)",
                (stage[0], lease_owner, limit, lease_seconds),
            )
            rows = cursor.fetchall()
        connection.commit()
    items = [
        ClaimedWorkItem(
            work_item_id=UUID(str(row[0])),
            # Column order follows orchestration.work_item. Keeping conversion here
            # makes the provider adapter independent from psycopg row factories.
            subject_type=str(row[5]),
            subject_id=UUID(str(row[6])),
            input_fingerprint=str(row[8]),
            payload=dict(row[11]),
        )
        for row in rows
    ]
    return str(stage[1]), items
