from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from my_data_hub.embeddings.dependency_smoke import EmbeddingDependencySmokeReceipt
from my_data_hub.embeddings.production_assembly import build_embedding_production_assembly
from my_data_hub.hashing import canonical_json_bytes


def test_embedding_production_assembly_absent_is_fail_closed(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    for name in tuple(__import__("os").environ):
        if name.startswith("MY_DATA_HUB_EMBEDDING_"):
            monkeypatch.delenv(name, raising=False)
    assert build_embedding_production_assembly(object()) is None


def test_embedding_production_assembly_rejects_partial_environment(monkeypatch, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("MY_DATA_HUB_EMBEDDING_WORKERS_ENABLED", "true")
    monkeypatch.setenv("MY_DATA_HUB_EMBEDDING_CREDENTIAL_DIR", str(tmp_path))
    with pytest.raises(ValueError, match="incomplete"):
        build_embedding_production_assembly(object())


def test_opt_in_assembles_only_with_exact_numeric_asset_claim(monkeypatch, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    ca = tmp_path / "ca.pem"
    ca.write_text("certificate")
    ca.chmod(0o600)
    known = tmp_path / "known_hosts"
    known.write_text("|1|a|b ssh-ed25519 AAAA")
    values = {
        "MY_DATA_HUB_EMBEDDING_WORKERS_ENABLED": "true",
        "MY_DATA_HUB_EMBEDDING_CREDENTIAL_DIR": str(tmp_path / "credentials"),
        "MY_DATA_HUB_MASTER_TUNNEL_GATEWAY_HOST": "gateway.example.org",
        "MY_DATA_HUB_MASTER_TUNNEL_GATEWAY_PORT": "22",
        "MY_DATA_HUB_MASTER_TLS_CA_PATH": str(ca),
        "MY_DATA_HUB_EMBEDDING_RUNTIME_IMAGE_IDENTITY": "kaggle/python@sha256:" + "a" * 64,
        "MY_DATA_HUB_EMBEDDING_WHEEL_RELATIVE_PATH": "my_data_hub.whl",
        "MY_DATA_HUB_EMBEDDING_WHEEL_SHA256": "b" * 64,
        "MY_DATA_HUB_CALLBACK_URL": "https://mcp-datahub.kenigevents.ru/internal/runtime/events",
        "MY_DATA_HUB_KAGGLE_OWNER": "owner",
        "MY_DATA_HUB_MASTER_TUNNEL_KNOWN_HOSTS_PATH": str(known),
        "MY_DATA_HUB_EMBEDDING_RUNTIME_PYTHON_SERIES": "3.12",
        "MY_DATA_HUB_EMBEDDING_RUNTIME_SOURCE_COMMIT": "f" * 40,
        "MY_DATA_HUB_EMBEDDING_DEPENDENCY_MANIFEST_SHA256": "c" * 64,
    }
    for key, value in values.items():
        monkeypatch.setenv(key, value)
    receipt_path = tmp_path / "smoke.json"
    receipt_path.write_bytes(canonical_json_bytes(EmbeddingDependencySmokeReceipt(
        observed_at=datetime.now(UTC), provider_run_ref="owner/smoke/1",
        observation_sha256="d" * 64, image_identity=values["MY_DATA_HUB_EMBEDDING_RUNTIME_IMAGE_IDENTITY"],
        image_source_commit="f" * 40, python_version="3.12.3",
        dependency_manifest_sha256="c" * 64, project_wheel_sha256="b" * 64,
        wheel_sha256s={"dependency.whl": "e" * 64}, imports=["psycopg"],
        psycopg_implementation="binary", distributions={"psycopg": "3.2"},
    ).model_dump(mode="json")))
    receipt_path.chmod(0o600)
    assembled = build_embedding_production_assembly(
        object(), broker=object(), master_instance=lambda: "instance",
        runtime_dataset_exact_ref="owner/master-assets/7",
        dependency_smoke_receipt_path=receipt_path,
    )
    assert assembled is not None
    with pytest.raises(ValueError, match="exact numeric"):
        build_embedding_production_assembly(
            object(), broker=object(), master_instance=lambda: "instance",
            runtime_dataset_exact_ref="owner/master-assets",
            dependency_smoke_receipt_path=receipt_path,
        )
