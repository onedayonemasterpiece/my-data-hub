from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from my_data_hub.artifact_store import LocalArtifactStore
from my_data_hub.hashing import canonical_json_bytes, sha256_value
from my_data_hub.notebooks.contracts import NotebookResult


class WorkerResultConflict(RuntimeError):
    pass


@dataclass(slots=True)
class WorkerResultRepository:
    """Persist an immutable result artifact and register it for reconciliation."""

    database_url: str
    artifact_store: LocalArtifactStore

    def store(self, envelope: NotebookResult) -> dict[str, Any]:
        try:
            import psycopg
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("psycopg is required for worker-result intake") from exc

        payload = envelope.model_dump(mode="json")
        payload_bytes = canonical_json_bytes(payload)
        result_hash = sha256_value(payload)
        stored = self.artifact_store.write_bytes(
            (
                f"worker-results/{envelope.workload}/{envelope.run_id}/"
                f"{envelope.stage}/{envelope.result_id}/{result_hash}.json"
            ),
            payload_bytes,
        )
        if stored.sha256 != result_hash:
            raise RuntimeError("worker result artifact failed SHA-256 readback")

        with psycopg.connect(self.database_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
                cursor.execute(
                    """
                    SELECT result_id, result_sha256, intake_status, artifact_locator
                    FROM orchestration.worker_result_inbox
                    WHERE result_id = %s OR result_sha256 = %s
                    FOR UPDATE
                    """,
                    (envelope.result_id, result_hash),
                )
                existing_rows = cursor.fetchall()
                if existing_rows:
                    exact = next(
                        (
                            row
                            for row in existing_rows
                            if str(row[0]) == str(envelope.result_id)
                            and str(row[1]) == result_hash
                        ),
                        None,
                    )
                    if exact is None or len(existing_rows) != 1:
                        raise WorkerResultConflict(
                            "result identity/hash is already bound to different content"
                        )
                    return {
                        "result_id": str(exact[0]),
                        "result_sha256": result_hash,
                        "status": str(exact[2]),
                        "artifact_locator": str(exact[3]),
                        "duplicate": True,
                    }

                cursor.execute(
                    """
                    SELECT sr.stage_run_id, sr.input_manifest_sha256,
                           ps.contract ->> 'name' AS contract_name
                    FROM orchestration.stage_run sr
                    JOIN orchestration.pipeline_stage ps ON ps.stage_id = sr.stage_id
                    WHERE sr.run_id = %s AND ps.stage_key = %s
                    ORDER BY sr.created_at DESC
                    LIMIT 1
                    FOR UPDATE OF sr
                    """,
                    (envelope.run_id, envelope.stage),
                )
                stage = cursor.fetchone()
                if stage is None:
                    raise WorkerResultConflict(
                        "no dispatched stage_run matches result run_id/stage"
                    )
                if str(stage[1] or "") != envelope.input_manifest_sha256:
                    raise WorkerResultConflict(
                        "input manifest hash does not match the dispatched stage_run"
                    )
                if str(stage[2] or "") != envelope.stage_contract_version:
                    raise WorkerResultConflict(
                        "stage contract version does not match the registered pipeline"
                    )

                cursor.execute(
                    """
                    INSERT INTO orchestration.worker_result_inbox (
                        result_id, run_id, stage_run_id, workload, stage_key,
                        stage_contract_version, input_manifest_sha256, result_sha256,
                        artifact_locator, byte_size, producer, result_status, envelope
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s::jsonb, %s, %s::jsonb
                    )
                    """,
                    (
                        envelope.result_id,
                        envelope.run_id,
                        stage[0],
                        envelope.workload,
                        envelope.stage,
                        envelope.stage_contract_version,
                        envelope.input_manifest_sha256,
                        result_hash,
                        stored.locator,
                        stored.byte_size,
                        json.dumps(
                            envelope.producer.model_dump(mode="json"),
                            ensure_ascii=False,
                        ),
                        envelope.status,
                        json.dumps(payload, ensure_ascii=False),
                    ),
                )
                cursor.execute(
                    """
                    INSERT INTO sync.audit_event (
                        actor_id, client_id, action, outcome, subject_type,
                        subject_id, details
                    ) VALUES (
                        'notebook-worker', 'worker-result-api', 'worker_result.receive',
                        'received', 'orchestration.worker_result', %s, %s::jsonb
                    )
                    """,
                    (
                        envelope.result_id,
                        json.dumps(
                            {
                                "run_id": str(envelope.run_id),
                                "stage": envelope.stage,
                                "result_sha256": result_hash,
                                "byte_size": stored.byte_size,
                                "artifact_locator": stored.locator,
                            },
                            ensure_ascii=False,
                        ),
                    ),
                )
            connection.commit()
        return {
            "result_id": str(envelope.result_id),
            "result_sha256": result_hash,
            "status": "received",
            "artifact_locator": stored.locator,
            "duplicate": False,
        }
