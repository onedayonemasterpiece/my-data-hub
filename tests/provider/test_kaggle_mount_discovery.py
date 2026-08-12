from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from my_data_hub.providers.kaggle.master_runtime import _runtime_bootstrap


def _put(path: Path, body: bytes) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(body)
    return hashlib.sha256(body).hexdigest()


def _bootstrap(tmp_path: Path) -> tuple[str, dict[str, str]]:
    status = tmp_path / "provider-normalized-status-v9" / "nested"
    assets = tmp_path / "provider-normalized-assets-v17" / "payload"
    bodies = {
        "kaggle_run.json": b"status",
        "kaggle_status_client.py": b"helper",
        "master-config.json": b"{}",
        "postgresql-18-runtime.bundle": b"archive",
        "postgresql-18-runtime.json": b"manifest",
        "tunnel-known-hosts": b"known",
        "verifier.py": b"verifier",
        "project.whl": b"wheel",
    }
    hashes = {
        name: _put(
            (
                status
                if name in bodies and name in {"kaggle_run.json", "kaggle_status_client.py", "master-config.json"}
                else assets
            )
            / name,
            body,
        )
        for name, body in bodies.items()
    }
    values = {
        "MY_DATA_HUB_POSTGRES_RUNTIME_ARCHIVE": "postgresql-18-runtime.bundle",
        "MY_DATA_HUB_POSTGRES_RUNTIME_ARCHIVE_SHA256": hashes["postgresql-18-runtime.bundle"],
        "MY_DATA_HUB_POSTGRES_RUNTIME_MANIFEST": "postgresql-18-runtime.json",
        "MY_DATA_HUB_POSTGRES_RUNTIME_MANIFEST_SHA256": hashes["postgresql-18-runtime.json"],
        "MY_DATA_HUB_TUNNEL_KNOWN_HOSTS": "tunnel-known-hosts",
        "MY_DATA_HUB_TUNNEL_KNOWN_HOSTS_SHA256": hashes["tunnel-known-hosts"],
        "MY_DATA_HUB_CHECKPOINT_VERIFIER_SOURCE_PATH": "verifier.py",
        "MY_DATA_HUB_CHECKPOINT_VERIFIER_SOURCE_SHA256": hashes["verifier.py"],
        "MY_DATA_HUB_WHEEL_PATH": "project.whl",
        "MY_DATA_HUB_WHEEL_SHA256": hashes["project.whl"],
    }
    source = _runtime_bootstrap(
        values,
        status_dataset_ref="owner/original-status-slug",
        status_config_sha256=hashes["kaggle_run.json"],
        status_helper_sha256=hashes["kaggle_status_client.py"],
        master_config_sha256=hashes["master-config.json"],
        secret_bindings={},
    ).replace("/kaggle/input", str(tmp_path))
    prefix = source.split("_mdh_os.environ.update(_mdh_values)", 1)[0] + "_mdh_os.environ.update(_mdh_values)\n"
    return prefix, values


def test_master_mount_discovery_survives_provider_slug_normalization(tmp_path: Path) -> None:
    source, _ = _bootstrap(tmp_path)
    scope: dict[str, object] = {}
    exec(compile(source, "master-bootstrap", "exec"), scope)
    values = scope["_mdh_values"]
    assert isinstance(values, dict)
    assert str(values["MY_DATA_HUB_WHEEL_PATH"]).startswith(str(tmp_path / "provider-normalized-assets-v17"))
    assert scope["_mdh_status_root"] == tmp_path / "provider-normalized-status-v9" / "nested"


def test_master_mount_discovery_rejects_ambiguous_exact_assets(tmp_path: Path) -> None:
    source, _values = _bootstrap(tmp_path)
    duplicate = tmp_path / "second-provider-name" / "postgresql-18-runtime.bundle"
    _put(duplicate, b"archive")
    with pytest.raises(RuntimeError, match="absent or ambiguous"):
        exec(compile(source, "master-bootstrap", "exec"), {})


def test_master_mount_discovery_rejects_required_files_split_across_mounts(tmp_path: Path) -> None:
    source, _values = _bootstrap(tmp_path)
    verifier = tmp_path / "provider-normalized-assets-v17" / "payload" / "verifier.py"
    separated = tmp_path / "wrong-attached-dataset" / "verifier.py"
    separated.parent.mkdir()
    verifier.replace(separated)
    with pytest.raises(RuntimeError, match="file set differs"):
        exec(compile(source, "master-bootstrap", "exec"), {})
