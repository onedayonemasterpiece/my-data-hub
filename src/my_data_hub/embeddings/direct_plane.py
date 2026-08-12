"""Direct ACTIVE-master exchange for embedding workers.

The control plane sees only :class:`EmbeddingLaunchMetadata`.  Canonical job
documents and vector manifests remain in PostgreSQL inside the ACTIVE Kaggle
master and are exchanged through an epoch-bound ``mdh_embedding_worker``
session.  This module deliberately contains no Kaggle client.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from my_data_hub.embeddings.contracts import EmbeddingArtifactManifest, EmbeddingJob
from my_data_hub.hashing import canonical_json_bytes


class DirectEmbeddingPlaneError(RuntimeError):
    """The direct worker/master exchange failed closed."""


class EmbeddingLaunchMetadata(BaseModel):
    """Secret-free, business-data-free launch notification for central control."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: str = Field(pattern=r"^embedding-central-launch-metadata\.v1$")
    request_id: UUID
    request_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    task_run_id: UUID
    model_exact_id: str = Field(min_length=3, max_length=500)
    input_jobs_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    job_count: int = Field(ge=1, le=10_000)
    worker_source_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    worker_primary_source_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    epoch: int = Field(ge=1)


@dataclass(frozen=True, slots=True)
class StagedEmbeddingBatch:
    metadata: EmbeddingLaunchMetadata
    jobs: tuple[EmbeddingJob, ...]


class PostgresEmbeddingWorkerExchange:
    """Store jobs/results only on the direct ACTIVE-master data plane."""

    def stage(self, connection: Any, batch: StagedEmbeddingBatch) -> None:
        payload = {
            "schema_version": "embedding-jobs-batch.v1",
            "jobs": [item.model_dump(mode="json") for item in batch.jobs],
        }
        encoded = canonical_json_bytes(payload)
        if hashlib.sha256(encoded).hexdigest() != batch.metadata.input_jobs_sha256:
            raise DirectEmbeddingPlaneError("embedding launch hash differs from the direct job payload")
        if len(encoded) > 4 * 1024 * 1024:
            raise DirectEmbeddingPlaneError("embedding direct job payload exceeds 4 MiB")
        with connection.transaction(), connection.cursor() as cursor:
            cursor.execute(
                "SELECT search.stage_embedding_dispatch(%s,%s,%s,%s,%s::jsonb)",
                (
                    batch.metadata.request_id,
                    batch.metadata.task_run_id,
                    batch.metadata.model_exact_id,
                    batch.metadata.input_jobs_sha256,
                    encoded.decode(),
                ),
            )
            cursor.fetchone()

    def wait_result(
        self,
        connection: Any,
        *,
        request_id: UUID,
        task_run_id: UUID,
        expected_sha256: str,
        deadline: float,
        lease_guard: Any,
        poll_seconds: float = 5.0,
        poll_hook: Any | None = None,
    ) -> EmbeddingArtifactManifest:
        while time.monotonic() < deadline:
            lease_guard()
            if poll_hook is not None:
                poll_hook()
            with connection.cursor() as cursor:
                row = cursor.execute(
                    "SELECT manifest_sha256,manifest FROM search.embedding_result_landing "
                    "WHERE request_id=%s AND task_run_id=%s",
                    (request_id, task_run_id),
                ).fetchone()
            if row is not None:
                manifest = EmbeddingArtifactManifest.model_validate(row[1])
                observed = hashlib.sha256(
                    canonical_json_bytes(manifest.model_dump(mode="json"))
                ).hexdigest()
                if str(row[0]) != observed:
                    raise DirectEmbeddingPlaneError("landed embedding manifest hash differs")
                if manifest.input_jobs_sha256 != expected_sha256:
                    raise DirectEmbeddingPlaneError("landed embedding manifest belongs to different jobs")
                return manifest
            time.sleep(min(poll_seconds, max(0.0, deadline - time.monotonic())))
        raise DirectEmbeddingPlaneError("direct embedding worker result exceeded its bounded deadline")


def claim_direct_embedding_jobs(
    connection: Any, *, request_id: UUID, task_run_id: UUID, input_jobs_sha256: str
) -> tuple[EmbeddingJob, ...]:
    """Worker-side claim through the only granted direct-plane function."""

    with connection.transaction(), connection.cursor() as cursor:
        cursor.execute("SET LOCAL ROLE mdh_embedding_worker")
        row = cursor.execute(
            "SELECT search.claim_embedding_dispatch(%s,%s,%s)",
            (request_id, task_run_id, input_jobs_sha256),
        ).fetchone()
    if row is None or not isinstance(row[0], (dict, str)):
        raise DirectEmbeddingPlaneError("embedding dispatch claim returned no exact payload")
    payload = json.loads(row[0]) if isinstance(row[0], str) else row[0]
    encoded = canonical_json_bytes(payload)
    if hashlib.sha256(encoded).hexdigest() != input_jobs_sha256:
        raise DirectEmbeddingPlaneError("claimed embedding jobs differ from launch metadata")
    if not isinstance(payload, dict) or payload.get("schema_version") != "embedding-jobs-batch.v1":
        raise DirectEmbeddingPlaneError("claimed embedding jobs use an unknown contract")
    return tuple(EmbeddingJob.model_validate(item) for item in payload.get("jobs", ()))


def submit_direct_embedding_result(
    connection: Any,
    *,
    request_id: UUID,
    task_run_id: UUID,
    input_jobs_sha256: str,
    manifest: EmbeddingArtifactManifest,
) -> str:
    """Write vectors directly to the ACTIVE master landing contract."""

    encoded = canonical_json_bytes(manifest.model_dump(mode="json"))
    digest = hashlib.sha256(encoded).hexdigest()
    with connection.transaction(), connection.cursor() as cursor:
        cursor.execute("SET LOCAL ROLE mdh_embedding_worker")
        cursor.execute(
            "SELECT search.submit_embedding_result(%s,%s,%s,%s,%s::jsonb)",
            (request_id, task_run_id, input_jobs_sha256, digest, encoded.decode()),
        )
        cursor.fetchone()
    return digest
