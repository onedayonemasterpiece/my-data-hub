"""Primary source for the isolated BGE-M3 dense corpus worker notebook."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from my_data_hub.embeddings.contracts import EmbeddingJob
from my_data_hub.embeddings.models import BGE_M3
from my_data_hub.embeddings.worker import DenseEncoder, EmbeddingWorker

MODEL = BGE_M3


def run(
    *,
    run_id: UUID,
    jobs: tuple[EmbeddingJob, ...],
    encoder: DenseEncoder,
    started_at: datetime,
    completed_at: datetime,
) -> dict[str, object]:
    result = EmbeddingWorker(model=MODEL, encoder=encoder).run(
        run_id=run_id,
        jobs=jobs,
        started_at=started_at,
        completed_at=completed_at,
    )
    return result.model_dump(mode="json")
