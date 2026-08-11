from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID, uuid5

from pydantic import ValidationError

from my_data_hub.embeddings.contracts import (
    EmbeddingArtifactManifest,
    EmbeddingFailure,
    EmbeddingJob,
    EmbeddingVectorResult,
    EmbeddingWorkerPolicy,
    FailureCode,
)
from my_data_hub.embeddings.models import EmbeddingModelContract
from my_data_hub.hashing import sha256_value

ARTIFACT_NAMESPACE = UUID("142a1f6d-b4f4-4a13-a1a8-16a236dc01c4")


class DenseEncoder(Protocol):
    """Injected model runtime; production dependencies stay outside the core."""

    def encode(
        self,
        texts: Sequence[str],
        *,
        model: EmbeddingModelContract,
        max_tokens: int,
        pooling: str,
        normalize: bool,
        dense_only: bool,
    ) -> Sequence[Sequence[float]]: ...


class EmbeddingWorker:
    """Deterministic typed producer with no database or provider side effects."""

    def __init__(
        self,
        *,
        model: EmbeddingModelContract,
        encoder: DenseEncoder,
        policy: EmbeddingWorkerPolicy | None = None,
    ) -> None:
        self.model = model
        self.encoder = encoder
        self.policy = policy or EmbeddingWorkerPolicy()

    def run(
        self,
        *,
        run_id: UUID,
        jobs: Sequence[EmbeddingJob],
        started_at: datetime,
        completed_at: datetime,
    ) -> EmbeddingArtifactManifest:
        if not jobs:
            raise ValueError("embedding worker requires at least one job")
        if started_at.tzinfo is None or completed_at.tzinfo is None:
            raise ValueError("worker timestamps must be timezone-aware")

        successes: list[EmbeddingVectorResult] = []
        failures: list[EmbeddingFailure] = []
        compatible: list[EmbeddingJob] = []
        for job in jobs:
            if job.model != self.model:
                failures.append(
                    EmbeddingFailure(
                        job_key=job.job_key,
                        code=FailureCode.MODEL_CONTRACT_MISMATCH,
                        message="job model contract differs from worker model contract",
                        retryable=False,
                        terminal=True,
                    )
                )
            else:
                compatible.append(job)

        if compatible:
            texts = [
                self.model.prepare_text(
                    (
                        job.document.display_name
                        if job.document.representation_kind == "blogger_search_query_v1"
                        else job.document.compact_text()
                    ),
                    query=job.document.representation_kind == "blogger_search_query_v1",
                )
                for job in compatible
            ]
            try:
                raw_vectors = self.encoder.encode(
                    texts,
                    model=self.model,
                    max_tokens=self.model.max_tokens,
                    pooling=self.model.pooling,
                    normalize=True,
                    dense_only=True,
                )
                if len(raw_vectors) != len(compatible):
                    raise RuntimeError(
                        f"encoder returned {len(raw_vectors)} vectors for {len(compatible)} jobs"
                    )
            except Exception as exc:
                failures.extend(
                    EmbeddingFailure(
                        job_key=job.job_key,
                        code=FailureCode.ENCODER_UNAVAILABLE,
                        message=f"{type(exc).__name__}: {exc}",
                        retryable=True,
                        terminal=False,
                    )
                    for job in compatible
                )
            else:
                for job, raw_vector in zip(compatible, raw_vectors, strict=True):
                    try:
                        successes.append(
                            EmbeddingVectorResult.from_job(
                                job,
                                tuple(float(value) for value in raw_vector),
                            )
                        )
                    except (TypeError, ValueError, ValidationError) as exc:
                        failures.append(
                            EmbeddingFailure(
                                job_key=job.job_key,
                                code=FailureCode.INVALID_VECTOR,
                                message=f"{type(exc).__name__}: {exc}",
                                retryable=False,
                                terminal=True,
                            )
                        )

        successes.sort(key=lambda item: item.job_key)
        failures.sort(key=lambda item: item.job_key)
        ordered_jobs = sorted(job.job_key for job in jobs)
        input_jobs_sha256 = sha256_value(ordered_jobs)
        payload_sha256 = sha256_value([item.model_dump(mode="json") for item in successes])
        artifact_id = uuid5(
            ARTIFACT_NAMESPACE,
            f"{run_id}:{self.model.exact_id}:{input_jobs_sha256}:{payload_sha256}",
        )
        return EmbeddingArtifactManifest(
            artifact_id=artifact_id,
            run_id=run_id,
            model=self.model,
            execution_policy=self.policy,
            input_jobs_sha256=input_jobs_sha256,
            payload_sha256=payload_sha256,
            successful_results=tuple(successes),
            failures=tuple(failures),
            total_jobs=len(jobs),
            started_at=started_at.astimezone(UTC),
            completed_at=completed_at.astimezone(UTC),
        )
