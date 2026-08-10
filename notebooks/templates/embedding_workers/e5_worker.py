"""Primary source for the E5 corpus worker notebook.

The shared notebook generator will wrap this source.  Runtime model loading is
injected by the generated notebook so local contract tests never download model
weights.  This source emits an immutable artifact and has no database/YDB code.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from my_data_hub.embeddings.contracts import EmbeddingJob
from my_data_hub.embeddings.models import E5_MULTILINGUAL_BASE
from my_data_hub.embeddings.worker import DenseEncoder, EmbeddingWorker

MODEL = E5_MULTILINGUAL_BASE


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
