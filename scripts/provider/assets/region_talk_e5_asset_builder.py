#!/usr/bin/env python3
"""No-secret frozen E5 v1 asset producer; values are injected centrally."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import re
from pathlib import Path
from typing import Any

TASK_RUN_ID = R21_E5_TASK_RUN_ID  # noqa: F821
SOURCE_COMMIT = R21_E5_SOURCE_COMMIT  # noqa: F821
OFFICIAL_TREE: dict[str, Any] = json.loads(R21_E5_OFFICIAL_TREE_JSON)  # noqa: F821
SEMANTIC_BANK: dict[str, Any] = json.loads(R21_E5_SEMANTIC_BANK_JSON)  # noqa: F821
OUTPUT_ROOT = Path("/kaggle/working/region-talk-e5-assets-v1")
MODEL_ROOT = OUTPUT_ROOT / "model"
RECEIPT = Path("/kaggle/working/region-talk-e5-frozen-producer-receipt.v1.json")
FAILURE = Path("/kaggle/working/region-talk-e5-frozen-producer-failure.v1.json")
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


def main() -> int:
    global _STAGE
    if not re.fullmatch(r"[a-f0-9-]{36}", TASK_RUN_ID) or len(SOURCE_COMMIT) != 40:
        raise RuntimeError("central frozen-producer identity was not embedded")
    forbidden = ("KAGGLE_USERNAME", "KAGGLE_KEY", "KAGGLE_API_TOKEN", "HF_TOKEN", "HUGGING_FACE_HUB_TOKEN")
    if any(os.environ.get(key) for key in forbidden):
        raise RuntimeError("asset producer credential environment is forbidden")
    if any(
        path.exists() for path in (Path.home() / ".kaggle" / "kaggle.json", Path.home() / ".kaggle" / "access_token")
    ):
        raise RuntimeError("asset producer credential file is forbidden")
    if OUTPUT_ROOT.exists() or OUTPUT_ROOT.is_symlink() or FAILURE.is_symlink():
        raise RuntimeError("frozen producer output must be fresh")
    expected_paths = [item["path"] for item in OFFICIAL_TREE["files"]]
    if len(expected_paths) != 23 or len(set(expected_paths)) != 23:
        raise RuntimeError("embedded official E5 tree differs")
    _STAGE = "exact_hf_download"
    os.environ["HF_HUB_DISABLE_TELEMETRY"] = "1"
    from huggingface_hub import snapshot_download

    snapshot_download(
        repo_id=OFFICIAL_TREE["model_id"],
        revision=OFFICIAL_TREE["revision"],
        local_dir=MODEL_ROOT,
        allow_patterns=expected_paths,
        token=False,
    )
    cache = MODEL_ROOT / ".cache"
    if cache.exists():
        import shutil

        shutil.rmtree(cache)
    _STAGE = "complete_tree_verification"
    observed_paths = sorted(path.relative_to(MODEL_ROOT).as_posix() for path in MODEL_ROOT.rglob("*") if path.is_file())
    if observed_paths != sorted(expected_paths) or any(path.is_symlink() for path in MODEL_ROOT.rglob("*")):
        raise RuntimeError("downloaded E5 tree differs from official paths")
    files = []
    expected = {item["path"]: item for item in OFFICIAL_TREE["files"]}
    for relative in observed_paths:
        sha256, git_oid, size = _sha_file(MODEL_ROOT / relative)
        item = expected[relative]
        if (
            size != item["byte_size"]
            or (item["lfs_sha256"] and sha256 != item["lfs_sha256"])
            or (not item["lfs_sha256"] and git_oid != item["git_oid"])
        ):
            raise RuntimeError(f"downloaded E5 file differs: {relative}")
        files.append({"path": relative, "byte_size": size, "sha256": sha256, "git_blob_oid": git_oid})
    _STAGE = "semantic_bank"
    bank = _canonical(SEMANTIC_BANK) + b"\n"
    bank_path = OUTPUT_ROOT / "semantic-bank.v1.json"
    bank_path.write_bytes(bank)
    logical = {entry["label"]: entry["examples"] for entry in SEMANTIC_BANK["entries"]}
    logical_sha = hashlib.sha256(_canonical(logical)).hexdigest()
    if logical_sha != SEMANTIC_BANK["semantic_bank_sha256"]:
        raise RuntimeError("embedded semantic bank differs")
    _STAGE = "receipt"
    unsigned = {
        "schema_version": "region-talk-e5-frozen-producer-receipt.v1",
        "task_run_id": TASK_RUN_ID,
        "source_commit": SOURCE_COMMIT,
        "model_id": OFFICIAL_TREE["model_id"],
        "model_revision": OFFICIAL_TREE["revision"],
        "official_tree_receipt_sha256": OFFICIAL_TREE["official_tree_receipt_sha256"],
        "files": files,
        "inventory_sha256": hashlib.sha256(_canonical(files)).hexdigest(),
        "semantic_bank_sha256": logical_sha,
        "semantic_bank_file_sha256": hashlib.sha256(bank).hexdigest(),
        "python_version": platform.python_version(),
        "notebook_kaggle_credentials": False,
        "publication_dispatch": False,
        "notification_dispatch": False,
    }
    value = {**unsigned, "receipt_sha256": hashlib.sha256(_canonical(unsigned)).hexdigest()}
    RECEIPT.write_bytes(_canonical(value))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        raw = str(exc)
        value = {
            "schema_version": "region-talk-e5-frozen-producer-failure.v1",
            "stage": _STAGE,
            "exception_type": type(exc).__name__,
            "message": re.sub(r"https?://\\S+", "<redacted-url>", raw)[:512],
            "message_sha256": hashlib.sha256(raw.encode()).hexdigest(),
        }
        if not FAILURE.exists() and not FAILURE.is_symlink():
            FAILURE.write_bytes(_canonical(value))
        raise
