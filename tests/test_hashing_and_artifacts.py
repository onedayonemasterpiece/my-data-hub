from __future__ import annotations

import json
from pathlib import Path

import pytest

from my_data_hub.artifact_store import ArtifactStoreError, LocalArtifactStore
from my_data_hub.hashing import canonical_json_bytes, sha256_file, sha256_value


def test_canonical_json_is_deterministic_and_utf8() -> None:
    left = canonical_json_bytes({"я": 1, "a": [2, 3]})
    right = canonical_json_bytes({"a": [2, 3], "я": 1})
    assert left == right
    assert b"\\u" not in left
    assert sha256_value({"b": 2, "a": 1}) == sha256_value({"a": 1, "b": 2})


def test_canonical_json_rejects_nan() -> None:
    with pytest.raises(ValueError):
        canonical_json_bytes({"value": float("nan")})


def test_artifact_store_writes_and_overwrites_atomically(tmp_path: Path) -> None:
    store = LocalArtifactStore(tmp_path / "artifacts")
    first = store.write_bytes("run/one.json", b"one")
    second = store.write_bytes("run/one.json", b"two")
    assert first.path == second.path
    assert second.path.read_bytes() == b"two"
    assert second.sha256 == sha256_file(second.path)
    assert second.byte_size == 3
    assert second.locator.startswith("file://")
    assert not list(second.path.parent.glob("*.tmp"))


def test_artifact_store_rejects_parent_escape(tmp_path: Path) -> None:
    store = LocalArtifactStore(tmp_path / "artifacts")
    with pytest.raises(ArtifactStoreError, match="escapes"):
        store.write_bytes("../outside.txt", b"bad")
