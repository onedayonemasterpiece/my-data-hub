from __future__ import annotations

import hashlib
import json
import os
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
    psycopg = b"psycopg-wheel"
    binary = b"psycopg-binary-wheel"
    dependencies = [
        {
            "distribution": "psycopg",
            "filename": "psycopg-3.3.4-py3-none-any.whl",
            "sha256": hashlib.sha256(psycopg).hexdigest(),
            "version": "3.3.4",
        },
        {
            "distribution": "psycopg-binary",
            "filename": "psycopg_binary-3.3.4-cp312.whl",
            "sha256": hashlib.sha256(binary).hexdigest(),
            "version": "3.3.4",
        },
    ]
    manifest = {
        "schema_version": "my-data-hub-embedding-worker-dependencies.v1",
        "runtime": {
            "image_identity": "image@example@sha256:" + "1" * 64,
            "source_commit": "2" * 40,
            "python_abi": "cp312",
            "platform": "manylinux2014_x86_64",
        },
        "wheels": dependencies,
    }
    bodies = {
        "kaggle_run.json": b"status",
        "kaggle_status_client.py": b"helper",
        "master-config.json": b"{}",
        "postgresql-18-runtime.bundle": b"archive",
        "postgresql-18-runtime.json": b"manifest",
        "tunnel-known-hosts": b"known",
        "verifier.py": b"verifier",
        "project.whl": b"wheel",
        "embedding-worker-dependencies.json": json.dumps(
            manifest, sort_keys=True, separators=(",", ":")
        ).encode(),
        "embedding-worker-wheelhouse/psycopg-3.3.4-py3-none-any.whl": psycopg,
        "embedding-worker-wheelhouse/psycopg_binary-3.3.4-cp312.whl": binary,
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
        "MY_DATA_HUB_PYTHON_DEPENDENCY_MANIFEST": "embedding-worker-dependencies.json",
        "MY_DATA_HUB_PYTHON_DEPENDENCY_MANIFEST_SHA256": hashes[
            "embedding-worker-dependencies.json"
        ],
        "MY_DATA_HUB_MASTER_PYTHON_DEPENDENCIES_JSON": json.dumps(
            dependencies, sort_keys=True, separators=(",", ":")
        ),
        "MY_DATA_HUB_KAGGLE_RUNTIME_IMAGE_IDENTITY": manifest["runtime"]["image_identity"],
        "MY_DATA_HUB_KAGGLE_RUNTIME_SOURCE_COMMIT": manifest["runtime"]["source_commit"],
        "MY_DATA_HUB_RUNTIME_PYTHON_ABI": "cp312",
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
    assert len(scope["_mdh_dependency_paths"]) == 2
    assert scope["_mdh_status_root"] == tmp_path / "provider-normalized-status-v9" / "nested"


def test_master_bootstrap_scrubs_platform_kaggle_lifecycle_credentials_before_assets(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    secret_names = (
        "KAGGLE_KEY",
        "KAGGLE_API_TOKEN",
        "KAGGLE_API_V1_TOKEN",
        "KAGGLE_ACCESS_TOKEN",
    )
    for name in secret_names:
        monkeypatch.setenv(name, f"platform-injected-{name}")
    monkeypatch.setenv("KAGGLE_USERNAME", "non-secret-platform-account")

    source, _ = _bootstrap(tmp_path)
    scope: dict[str, object] = {}
    exec(compile(source, "master-bootstrap", "exec"), scope)

    assert all(name not in os.environ for name in secret_names)
    assert os.environ["KAGGLE_USERNAME"] == "non-secret-platform-account"
    assert "platform-injected" not in repr(scope)
    scrub = source.index("_mdh_os.environ.pop(_mdh_credential_name, None)")
    first_asset_read = source.index("_mdh_exact_file('kaggle_run.json'")
    assert scrub < first_asset_read


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


def test_master_bootstrap_accepts_the_pinned_archive_root_directory() -> None:
    source = _runtime_bootstrap(
        {},
        status_dataset_ref="owner/status",
        status_config_sha256="a" * 64,
        status_helper_sha256="b" * 64,
        master_config_sha256="c" * 64,
        secret_bindings={},
    )

    assert "(m.name != 'pgsql' and not m.name.startswith('pgsql/'))" in source
    assert "_mdh_pathlib.Path('/opt/mdh-postgresql-runtime')" in source
    assert "/tmp/mdh-postgresql-runtime" not in source
    assert "/kaggle/working/mdh-postgresql-runtime" not in source
    extracted = source.index("_mdh_tar.extractall(_mdh_pg_root")
    traversable = source.index("_mdh_pg_root.chmod(0o755)")
    library_binding = source.index("_mdh_os.environ['LD_LIBRARY_PATH']")
    assert extracted < traversable < library_binding
