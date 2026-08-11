"""Primary runtime source for exact-revision BGE-M3 dense-only encoding."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

from FlagEmbedding import BGEM3FlagModel

from my_data_hub.embeddings.contracts import EmbeddingJob
from my_data_hub.embeddings.models import BGE_M3
from my_data_hub.embeddings.worker import EmbeddingWorker
from my_data_hub.hashing import canonical_json_bytes


class BgeM3Encoder:
    def __init__(self) -> None:
        self.model = BGEM3FlagModel(BGE_M3.model_key, normalize_embeddings=True, use_fp16=False)

    def encode(self, texts, *, model, max_tokens, pooling, normalize, dense_only):  # type: ignore[no-untyped-def]
        if model != BGE_M3 or pooling != "model_native_dense" or not normalize or not dense_only:
            raise ValueError("BGE-M3 runtime contract mismatch")
        result = self.model.encode(list(texts), batch_size=4, max_length=max_tokens, return_dense=True, return_sparse=False, return_colbert_vecs=False)
        return result["dense_vecs"].tolist()


def main() -> int:
    payload = json.loads(Path(os.environ["MY_DATA_HUB_EMBEDDING_JOBS"]).read_text())
    jobs = tuple(EmbeddingJob.model_validate(item) for item in payload["jobs"])
    now = datetime.now(UTC)
    result = EmbeddingWorker(model=BGE_M3, encoder=BgeM3Encoder()).run(
        run_id=UUID(os.environ["MY_DATA_HUB_RUN_ID"]), jobs=jobs, started_at=now, completed_at=datetime.now(UTC)
    )
    Path("/kaggle/working/embedding-result.json").write_bytes(canonical_json_bytes(result.model_dump(mode="json")))
    return 0
