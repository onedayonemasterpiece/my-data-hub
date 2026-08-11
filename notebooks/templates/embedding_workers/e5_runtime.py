"""Primary runtime source for exact-revision multilingual E5 encoding."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import torch
from transformers import AutoModel, AutoTokenizer

from my_data_hub.embeddings.contracts import EmbeddingJob
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
    payload = json.loads(Path(os.environ["MY_DATA_HUB_EMBEDDING_JOBS"]).read_text())
    jobs = tuple(EmbeddingJob.model_validate(item) for item in payload["jobs"])
    now = datetime.now(UTC)
    result = EmbeddingWorker(model=E5_MULTILINGUAL_BASE, encoder=E5Encoder()).run(
        run_id=UUID(os.environ["MY_DATA_HUB_RUN_ID"]), jobs=jobs, started_at=now, completed_at=datetime.now(UTC)
    )
    Path("/kaggle/working/embedding-result.json").write_bytes(canonical_json_bytes(result.model_dump(mode="json")))
    return 0
