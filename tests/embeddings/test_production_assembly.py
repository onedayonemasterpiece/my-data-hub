from __future__ import annotations

from pathlib import Path

import pytest

from my_data_hub.embeddings.production_assembly import build_embedding_production_assembly


def test_embedding_production_assembly_absent_is_fail_closed(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    for name in tuple(__import__("os").environ):
        if name.startswith("MY_DATA_HUB_EMBEDDING_"):
            monkeypatch.delenv(name, raising=False)
    assert build_embedding_production_assembly(object()) is None


def test_embedding_production_assembly_rejects_partial_environment(monkeypatch, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("MY_DATA_HUB_EMBEDDING_CREDENTIAL_DIR", str(tmp_path))
    with pytest.raises(ValueError, match="incomplete"):
        build_embedding_production_assembly(object())
