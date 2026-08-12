"""Primary runtime source for exact-revision multilingual E5 encoding."""

from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import torch
from transformers import AutoModel, AutoTokenizer

from my_data_hub.embeddings.direct_plane import (
    claim_direct_embedding_jobs, submit_direct_embedding_result,
)
from my_data_hub.embeddings.models import E5_MULTILINGUAL_BASE
from my_data_hub.embeddings.worker import EmbeddingWorker
from my_data_hub.hashing import canonical_json_bytes


class E5Encoder:
    def __init__(self) -> None:
        model = E5_MULTILINGUAL_BASE
        self.tokenizer = AutoTokenizer.from_pretrained(model.model_key, revision=model.revision)
        self.model = AutoModel.from_pretrained(model.model_key, revision=model.revision).eval()

    def encode(self, texts, *, model, max_tokens, pooling, normalize, dense_only):  # type: ignore[no-untyped-def]
        if model != E5_MULTILINGUAL_BASE or pooling != "attention_mask_mean" or not normalize or not dense_only:
            raise ValueError("E5 runtime contract mismatch")
        encoded = self.tokenizer(list(texts), max_length=max_tokens, padding=True, truncation=True, return_tensors="pt")
        with torch.inference_mode():
            hidden = self.model(**encoded).last_hidden_state
        mask = encoded["attention_mask"].unsqueeze(-1).to(hidden.dtype)
        vectors = (hidden * mask).sum(1) / mask.sum(1).clamp(min=1)
        return torch.nn.functional.normalize(vectors, p=2, dim=1).cpu().tolist()


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
        result = EmbeddingWorker(model=E5_MULTILINGUAL_BASE, encoder=E5Encoder()).run(
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
