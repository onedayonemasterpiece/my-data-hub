"""Primary runtime source for exact-revision BGE-M3 dense-only encoding."""

from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

from FlagEmbedding import BGEM3FlagModel
from huggingface_hub import snapshot_download

from my_data_hub.embeddings.direct_plane import (
    claim_direct_embedding_jobs,
    submit_direct_embedding_result,
)
from my_data_hub.embeddings.models import BGE_M3
from my_data_hub.embeddings.worker import EmbeddingWorker
from my_data_hub.hashing import canonical_json_bytes


class BgeM3Encoder:
    def __init__(self) -> None:
        snapshot_path = Path(
            snapshot_download(repo_id=BGE_M3.model_key, revision=BGE_M3.revision)
        ).resolve()
        if snapshot_path.name != BGE_M3.revision:
            raise RuntimeError(
                "resolved BGE-M3 snapshot does not match the receipt-bound exact revision"
            )
        self.snapshot_revision = snapshot_path.name
        self.model = BGEM3FlagModel(
            str(snapshot_path), normalize_embeddings=True, use_fp16=False
        )

    def encode(self, texts, *, model, max_tokens, pooling, normalize, dense_only):  # type: ignore[no-untyped-def]
        if model != BGE_M3 or pooling != "model_native_dense" or not normalize or not dense_only:
            raise ValueError("BGE-M3 runtime contract mismatch")
        result = self.model.encode(list(texts), batch_size=4, max_length=max_tokens, return_dense=True, return_sparse=False, return_colbert_vecs=False)
        return result["dense_vecs"].tolist()


def main() -> int:
    import psycopg

    request_id = UUID(os.environ["MY_DATA_HUB_EMBEDDING_REQUEST_ID"])
    task_run_id = UUID(os.environ["MY_DATA_HUB_RUN_ID"])
    input_jobs_sha256 = os.environ["MY_DATA_HUB_EMBEDDING_INPUT_JOBS_SHA256"]
    # The short-lived URL is injected only through the private per-run status
    # Dataset and is never written to output or callbacks.
    with psycopg.connect(os.environ["MY_DATA_HUB_EMBEDDING_DIRECT_DATABASE_URL"]) as connection:
        jobs = claim_direct_embedding_jobs(
            connection, request_id=request_id, task_run_id=task_run_id,
            input_jobs_sha256=input_jobs_sha256,
        )
        now = datetime.now(UTC)
        result = EmbeddingWorker(model=BGE_M3, encoder=BgeM3Encoder()).run(
            run_id=task_run_id, jobs=jobs, started_at=now, completed_at=datetime.now(UTC)
        )
        artifact_sha256 = submit_direct_embedding_result(
            connection, request_id=request_id, task_run_id=task_run_id,
            input_jobs_sha256=input_jobs_sha256, manifest=result,
        )
    Path("/kaggle/working/embedding-result-metadata.json").write_bytes(
        canonical_json_bytes({
            "schema_version": "embedding-direct-result-metadata.v1",
            "request_id": str(request_id), "task_run_id": str(task_run_id),
            "input_jobs_sha256": input_jobs_sha256, "artifact_sha256": artifact_sha256,
        })
    )
    return 0
