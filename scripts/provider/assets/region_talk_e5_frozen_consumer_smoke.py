"""Provider-only smoke for the frozen E5 kernel-source output.

The central runner replaces the three R21 markers before dispatch.  Model
bytes never pass through the control host.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
from pathlib import Path

TASK_RUN_ID = "R21_E5_CONSUMER_TASK"
MANIFEST = json.loads("R21_E5_CONSUMER_MANIFEST")
PRODUCER_AUTHORITY_SHA256 = "R21_E5_CONSUMER_AUTHORITY"
OUTPUT = Path("/kaggle/working/region-talk-e5-frozen-consumer-smoke.v1.json")
INPUT = Path("/kaggle/input")


def _canonical(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def main() -> int:
    credential_prefixes = ("KAGGLE_KEY", "KAGGLE_API_TOKEN", "HF_TOKEN", "HUGGING_FACE_HUB_TOKEN")
    if any(name.startswith(credential_prefixes) for name in os.environ):
        raise RuntimeError("consumer smoke credentials are forbidden")
    os.environ.update(HF_HUB_OFFLINE="1", TRANSFORMERS_OFFLINE="1", HF_HUB_DISABLE_TELEMETRY="1")
    weight = next(item for item in MANIFEST["model_files"] if item["relative_path"] == "model/model.safetensors")
    candidates = [
        path
        for path in INPUT.rglob("model.safetensors")
        if path.is_file() and not path.is_symlink() and path.stat().st_size == weight["byte_size"]
    ]
    if len(candidates) != 1 or _sha(candidates[0]) != weight["sha256"]:
        raise RuntimeError("frozen E5 weight mount is absent or ambiguous")
    model_root = candidates[0].parent
    expected = {
        item["relative_path"].removeprefix("model/"): (item["byte_size"], item["sha256"])
        for item in MANIFEST["model_files"]
    }
    observed_paths = sorted(
        path.relative_to(model_root).as_posix() for path in model_root.rglob("*") if path.is_file()
    )
    if observed_paths != sorted(expected) or any(path.is_symlink() for path in model_root.rglob("*")):
        raise RuntimeError("frozen E5 complete mounted path set differs")
    observed = []
    for relative in observed_paths:
        path = model_root / relative
        size, wanted = expected[relative]
        digest = _sha(path)
        if path.stat().st_size != size or digest != wanted:
            raise RuntimeError(f"frozen E5 mounted file differs: {relative}")
        observed.append({"path": relative, "byte_size": size, "sha256": digest})
    bank = MANIFEST["semantic_bank_file"]
    bank_candidates = [
        path
        for path in INPUT.rglob(Path(bank["relative_path"]).name)
        if path.is_file()
        and not path.is_symlink()
        and path.stat().st_size == bank["byte_size"]
        and _sha(path) == bank["sha256"]
    ]
    if len(bank_candidates) != 1:
        raise RuntimeError("frozen E5 semantic bank mount is absent or ambiguous")

    # Import and construct only after every mounted runtime byte is verified.
    import torch
    from transformers import AutoModel, AutoTokenizer

    tokenizer = AutoTokenizer.from_pretrained(str(model_root), local_files_only=True)
    model = AutoModel.from_pretrained(str(model_root), local_files_only=True).eval()
    inputs = tokenizer(
        ["query: музейный маршрут Калининграда"],
        padding=True,
        truncation=True,
        max_length=512,
        return_tensors="pt",
    )
    with torch.inference_mode():
        hidden = model(**inputs).last_hidden_state
        mask = inputs["attention_mask"].unsqueeze(-1).to(hidden.dtype)
        vector = ((hidden * mask).sum(dim=1) / mask.sum(dim=1).clamp(min=1)).cpu()[0].tolist()
    norm = math.sqrt(sum(float(item) ** 2 for item in vector))
    normalized = [round(float(item) / norm, 8) for item in vector]
    if len(normalized) != 768 or not all(math.isfinite(item) for item in normalized):
        raise RuntimeError("frozen E5 fixed output contract differs")
    unsigned = {
        "schema_version": "region-talk-e5-frozen-consumer-smoke.v1",
        "task_run_id": TASK_RUN_ID,
        "producer_authority_sha256": PRODUCER_AUTHORITY_SHA256,
        "asset_manifest_receipt_sha256": MANIFEST["receipt_sha256"],
        "verified_files": len(observed),
        "verified_bytes": sum(item["byte_size"] for item in observed),
        "inventory_sha256": hashlib.sha256(_canonical(observed)).hexdigest(),
        "semantic_bank_file_sha256": bank["sha256"],
        "fixed_output_dimensions": len(normalized),
        "fixed_output_sha256": hashlib.sha256(_canonical(normalized)).hexdigest(),
        "notebook_credentials": False,
        "publication_dispatch": False,
        "notification_dispatch": False,
    }
    receipt = {**unsigned, "receipt_sha256": hashlib.sha256(_canonical(unsigned)).hexdigest()}
    OUTPUT.write_bytes(_canonical(receipt))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
