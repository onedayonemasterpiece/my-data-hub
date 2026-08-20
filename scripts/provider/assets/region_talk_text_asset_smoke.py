#!/usr/bin/env python3
"""Credential-free Kaggle observation of exact-version attached text models."""

from __future__ import annotations

import gc
import hashlib
import importlib.metadata
import json
import os
import platform
import re
from pathlib import Path
from typing import Any

TASK_RUN_ID = "R21_TASK_RUN_ID_REPLACED_BY_CENTRAL_RUNNER"
OUTPUT = Path("/kaggle/working/region-talk-text-asset-observation.json")
FAILURE = Path("/kaggle/working/region-talk-text-asset-failure.json")
FIXED_TEXTS = (
    "query: Калининградская область: маршрут по музеям и побережью",
    "passage: Автор делится впечатлениями от поездки в Зеленоградск и на Куршскую косу.",
)
CANDIDATES = (
    {
        "stage": "e5_embedding",
        "model_source": "tanviranjumapurbo/multilingual-e5-base/Transformers/default/1",
        "model_slug": "multilingual-e5-base",
        "variation": "default",
        "weight_name": "model.safetensors",
        "weight_size": 1_112_197_064,
    },
    {
        "stage": "bge_m3_embedding",
        "model_source": "yethukmutt/bge-m3/Transformers/m3/1",
        "model_slug": "bge-m3",
        "variation": "m3",
        "weight_name": "pytorch_model.bin",
        "weight_size": 2_271_145_830,
    },
)
_STAGE = "startup"


def _canonical(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def _sha_file(path: Path) -> tuple[str, str, int]:
    size = path.stat().st_size
    sha256 = hashlib.sha256()
    git_blob = hashlib.sha1(f"blob {size}\0".encode(), usedforsecurity=False)
    with path.open("rb") as stream:
        while chunk := stream.read(8 * 1024 * 1024):
            sha256.update(chunk)
            git_blob.update(chunk)
    return sha256.hexdigest(), git_blob.hexdigest(), size


def _model_root(candidate: dict[str, Any]) -> Path:
    matches: list[Path] = []
    for weight in Path("/kaggle/input").rglob(candidate["weight_name"]):
        if (
            weight.is_file()
            and not weight.is_symlink()
            and weight.stat().st_size == candidate["weight_size"]
            and (weight.parent / "config.json").is_file()
            and candidate["model_slug"] in weight.as_posix().casefold()
        ):
            matches.append(weight.parent.resolve())
    if len(matches) != 1:
        raise RuntimeError(f"exact {candidate['stage']} model root is absent or ambiguous")
    root = matches[0]
    root.relative_to(Path("/kaggle/input").resolve())
    return root


def _inventory(root: Path) -> list[dict[str, Any]]:
    files: list[dict[str, Any]] = []
    for path in sorted(root.rglob("*")):
        if path.is_symlink() or (not path.is_file() and not path.is_dir()):
            raise RuntimeError("attached model contains an unsafe path")
        if not path.is_file():
            continue
        sha256, git_oid, size = _sha_file(path)
        files.append(
            {
                "path": path.relative_to(root).as_posix(),
                "byte_size": size,
                "sha256": sha256,
                "git_blob_oid": git_oid,
            }
        )
    if not files:
        raise RuntimeError("attached model inventory is empty")
    return files


def _provenance(root: Path) -> dict[str, Any] | None:
    matches = [
        path
        for path in root.parent.rglob("__huggingface_repos__.json")
        if path.is_file() and not path.is_symlink()
    ]
    if len(matches) > 1:
        raise RuntimeError("attached Hugging Face provenance is ambiguous")
    if not matches:
        return None
    raw = matches[0].read_bytes()
    value = json.loads(raw)
    return {
        "relative_path": matches[0].relative_to(root.parent).as_posix(),
        "sha256": hashlib.sha256(raw).hexdigest(),
        "value": value,
    }


def _rounded_vector_hash(vectors: Any) -> tuple[str, int, list[float]]:
    rounded = [[round(float(value), 8) for value in row] for row in vectors]
    norms = [round(sum(value * value for value in row) ** 0.5, 8) for row in rounded]
    return hashlib.sha256(_canonical(rounded)).hexdigest(), len(rounded[0]), norms


def _e5_output(root: Path) -> dict[str, Any]:
    import torch
    from transformers import AutoModel, AutoTokenizer

    os.environ["HF_HUB_OFFLINE"] = "1"
    os.environ["TRANSFORMERS_OFFLINE"] = "1"
    tokenizer = AutoTokenizer.from_pretrained(root, local_files_only=True)
    model = AutoModel.from_pretrained(root, local_files_only=True).eval()
    encoded = tokenizer(list(FIXED_TEXTS), padding=True, truncation=True, max_length=512, return_tensors="pt")
    token_body = {key: value.tolist() for key, value in sorted(encoded.items())}
    with torch.inference_mode():
        hidden = model(**encoded).last_hidden_state
    mask = encoded["attention_mask"].unsqueeze(-1).to(hidden.dtype)
    vectors = (hidden * mask).sum(1) / mask.sum(1).clamp(min=1)
    vectors = torch.nn.functional.normalize(vectors, p=2, dim=1).cpu().tolist()
    output_sha, dimensions, norms = _rounded_vector_hash(vectors)
    del model, tokenizer, encoded, hidden, vectors
    gc.collect()
    return {
        "tokenizer_output_sha256": hashlib.sha256(_canonical(token_body)).hexdigest(),
        "dense_output_sha256": output_sha,
        "dimensions": dimensions,
        "norms": norms,
    }


def _bge_output(root: Path) -> dict[str, Any]:
    import torch
    from transformers import AutoModel, AutoTokenizer

    os.environ["HF_HUB_OFFLINE"] = "1"
    os.environ["TRANSFORMERS_OFFLINE"] = "1"
    tokenizer = AutoTokenizer.from_pretrained(root, local_files_only=True)
    model = AutoModel.from_pretrained(root, local_files_only=True).eval()
    encoded = tokenizer(
        list(FIXED_TEXTS), padding=True, truncation=True, max_length=512, return_tensors="pt"
    )
    token_body = {key: value.tolist() for key, value in sorted(encoded.items())}
    with torch.inference_mode():
        # BGE-M3's primary implementation specifies normalized [CLS] as the
        # dense representation.  Using the image-pinned Transformers runtime
        # here avoids silently relying on an unavailable FlagEmbedding wheel
        # while exercising the exact attached model bytes offline.
        hidden = model(**encoded).last_hidden_state
    vectors = torch.nn.functional.normalize(hidden[:, 0], p=2, dim=1).cpu().tolist()
    output_sha, dimensions, norms = _rounded_vector_hash(vectors)
    del model, tokenizer, encoded, hidden, vectors
    gc.collect()
    return {
        "tokenizer_output_sha256": hashlib.sha256(_canonical(token_body)).hexdigest(),
        "dense_output_sha256": output_sha,
        "dimensions": dimensions,
        "norms": norms,
    }


def main() -> int:
    global _STAGE
    if TASK_RUN_ID == "R21_TASK_RUN_ID_REPLACED_BY_CENTRAL_RUNNER":
        raise RuntimeError("central task identity was not embedded")
    if any(os.environ.get(key) for key in ("KAGGLE_USERNAME", "KAGGLE_KEY", "KAGGLE_API_TOKEN")):
        raise RuntimeError("Kaggle credential env is forbidden inside smoke")
    if any(
        path.exists()
        for path in (Path.home() / ".kaggle" / "kaggle.json", Path.home() / ".kaggle" / "access_token")
    ):
        raise RuntimeError("Kaggle credential file is forbidden inside smoke")
    if OUTPUT.exists() or OUTPUT.is_symlink() or FAILURE.is_symlink():
        raise RuntimeError("smoke outputs must be fresh regular paths")
    observations = []
    for candidate in CANDIDATES:
        _STAGE = f"inventory:{candidate['stage']}"
        root = _model_root(candidate)
        inventory = _inventory(root)
        _STAGE = f"fixed_output:{candidate['stage']}"
        fixed = _e5_output(root) if candidate["stage"] == "e5_embedding" else _bge_output(root)
        observations.append(
            {
                "stage": candidate["stage"],
                "model_source": candidate["model_source"],
                "model_root_name": root.name,
                "files": inventory,
                "inventory_sha256": hashlib.sha256(_canonical(inventory)).hexdigest(),
                "huggingface_provenance": _provenance(root),
                "fixed_output": fixed,
            }
        )
    value = {
        "schema_version": "region-talk-text-asset-smoke-observation.v1",
        "task_run_id": TASK_RUN_ID,
        "status": "observed",
        "python_version": platform.python_version(),
        "internet_enabled": False,
        "notebook_kaggle_credentials": False,
        "distributions": {
            name: importlib.metadata.version(name)
            for name in ("safetensors", "tokenizers", "torch", "transformers")
        },
        "candidates": observations,
    }
    OUTPUT.write_bytes(_canonical(value))
    return 0


if __name__ == "__main__":
    try:
        exit_code = main()
    except Exception as exc:
        raw = str(exc)
        message = re.sub(r"https?://\S+", "<redacted-url>", raw)[:512]
        value = {
            "schema_version": "region-talk-text-asset-smoke-failure.v1",
            "stage": _STAGE,
            "exception_type": type(exc).__name__,
            "message_sha256": hashlib.sha256(raw.encode()).hexdigest(),
            "message": message,
        }
        if not FAILURE.exists() and not FAILURE.is_symlink():
            FAILURE.write_bytes(_canonical(value))
        raise
    raise SystemExit(exit_code)
