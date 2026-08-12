"""Canonical hash of the exact Kaggle source that a runtime executes."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


def canonical_executable_source(source: bytes, *, kernel_type: str) -> bytes:
    """Discard mutable Notebook outputs/metadata but retain every executable cell."""

    if kernel_type == "script":
        return source
    if kernel_type != "notebook":
        raise ValueError("Kaggle source kernel type must be notebook or script")
    body = json.loads(source)
    cells = body.get("cells") if isinstance(body, dict) else None
    if not isinstance(cells, list):
        raise ValueError("Kaggle Notebook source lacks a cells array")
    executable: list[dict[str, Any]] = []
    for cell in cells:
        if not isinstance(cell, dict):
            raise ValueError("Kaggle Notebook source contains a non-object cell")
        source_value = cell.get("source", "")
        if isinstance(source_value, list):
            source_value = "".join(str(item) for item in source_value)
        if not isinstance(source_value, str):
            raise ValueError("Kaggle Notebook cell source must be text")
        executable.append({"cell_type": str(cell.get("cell_type", "")), "source": source_value})
    return json.dumps(executable, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def executable_source_sha256(source: bytes, *, kernel_type: str) -> str:
    return hashlib.sha256(canonical_executable_source(source, kernel_type=kernel_type)).hexdigest()


def observed_kaggle_source_sha256(working_directory: Path) -> str:
    """Hash the one provider materialized source file, never a caller-selected path."""

    candidates = (
        (working_directory / "__notebook__.ipynb", "notebook"),
        (working_directory / "__script__.py", "script"),
    )
    present = [(path, kind) for path, kind in candidates if path.is_file() and not path.is_symlink()]
    if len(present) != 1:
        raise RuntimeError("exactly one Kaggle executed source file is required for attestation")
    path, kernel_type = present[0]
    if path.stat().st_size > 8 * 1024 * 1024:
        raise RuntimeError("Kaggle executed source exceeds the 8 MiB attestation bound")
    return executable_source_sha256(path.read_bytes(), kernel_type=kernel_type)
