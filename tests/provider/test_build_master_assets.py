from __future__ import annotations

import hashlib
import json
import stat
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from scripts.provider.build_master_assets import AssetBundleError, build_bundle
from scripts.provider.verify_master_assets import AssetVerificationError, verify_bundle

COMMIT = "1" * 40


def _root(tmp_path: Path) -> Path:
    root = tmp_path / "source"
    master = root / "notebooks/02-postgres-master/worker.ipynb"
    verifier = root / "notebooks/03-checkpoint-verifier-restore-smoke/worker.ipynb"
    master.parent.mkdir(parents=True)
    verifier.parent.mkdir(parents=True)
    master.write_bytes(b'{"master":true}')
    verifier.write_bytes(b'{"verifier":true}')
    return root


def _wheel_builder(_root: Path, destination: Path) -> Path:
    wheel = destination / "my_data_hub-0.1.0-py3-none-any.whl"
    wheel.write_bytes(b"exact-test-wheel")
    return wheel


def _build(tmp_path: Path) -> tuple[Path, dict[str, object]]:
    root = _root(tmp_path)
    output = tmp_path / "bundle"
    manifest = build_bundle(
        root=root,
        output=output,
        source_commit=COMMIT,
        launch_dataset_ref="owner/master-assets",
        master_notebook_ref="owner/postgres-master",
        checkpoint_dataset_ref="owner/checkpoints",
        checkpoint_verifier_ref="owner/checkpoint-verifier",
        probe_relations=["hub.canonical_state"],
        wheel_builder=_wheel_builder,
    )
    return output, manifest


def test_build_bundle_is_exact_secret_free_and_schema_valid(tmp_path: Path) -> None:
    output, manifest = _build(tmp_path)
    schema = json.loads(
        (Path(__file__).parents[2] / "schemas/master-asset-bundle.v1.schema.json").read_text()
    )
    Draft202012Validator(schema).validate(manifest)

    persisted = json.loads((output / "master-asset-bundle.json").read_text())
    assert persisted == manifest
    assert manifest["source_identity"] == f"git:{COMMIT}"
    assets = manifest["assets"]
    assert isinstance(assets, dict)
    for asset in assets.values():
        assert isinstance(asset, dict)
        body = (output / str(asset["path"])).read_bytes()
        assert asset["sha256"] == hashlib.sha256(body).hexdigest()
        assert asset["byte_size"] == len(body)

    env = (output / "master-assets.env").read_text()
    assert "MY_DATA_HUB_KAGGLE_MASTER_SOURCE_VERSION=" + COMMIT in env
    assert "MY_DATA_HUB_KAGGLE_MASTER_DATASET_DIR=/master-assets/dataset" in env
    assert "MY_DATA_HUB_KAGGLE_MASTER_NOTEBOOK_SOURCE=/master-assets/postgres-master.ipynb" in env
    assert not any(word in env.upper() for word in ("TOKEN=", "PASSWORD=", "SECRET="))
    assert stat.S_IMODE(output.stat().st_mode) == 0o700
    assert stat.S_IMODE((output / "dataset").stat().st_mode) == 0o700
    assert all(
        stat.S_IMODE(path.stat().st_mode) == 0o600
        for path in output.rglob("*")
        if path.is_file()
    )
    assert verify_bundle(bundle=output, expected_commit=COMMIT) == {
        "schema_version": "my-data-hub-master-asset-bundle.v1",
        "source_commit": COMMIT,
        "manifest_sha256": hashlib.sha256(
            (output / "master-asset-bundle.json").read_bytes()
        ).hexdigest(),
        "asset_count": 3,
        "verified": True,
    }


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("source_commit", "not-a-commit"),
        ("launch_dataset_ref", "latest"),
        ("probe_relations", ["hub.canonical_state", "hub.canonical_state"]),
    ],
)
def test_build_bundle_rejects_ambiguous_identity(
    tmp_path: Path, field: str, value: object
) -> None:
    root = _root(tmp_path)
    arguments: dict[str, object] = {
        "root": root,
        "output": tmp_path / "bundle",
        "source_commit": COMMIT,
        "launch_dataset_ref": "owner/master-assets",
        "master_notebook_ref": "owner/postgres-master",
        "checkpoint_dataset_ref": "owner/checkpoints",
        "checkpoint_verifier_ref": "owner/checkpoint-verifier",
        "probe_relations": ["hub.canonical_state"],
        "wheel_builder": _wheel_builder,
    }
    arguments[field] = value
    with pytest.raises(AssetBundleError):
        build_bundle(**arguments)  # type: ignore[arg-type]


def test_build_bundle_refuses_source_checkout_output_and_nonempty_output(tmp_path: Path) -> None:
    root = _root(tmp_path)
    common = {
        "root": root,
        "source_commit": COMMIT,
        "launch_dataset_ref": "owner/master-assets",
        "master_notebook_ref": "owner/postgres-master",
        "checkpoint_dataset_ref": "owner/checkpoints",
        "checkpoint_verifier_ref": "owner/checkpoint-verifier",
        "probe_relations": ["hub.canonical_state"],
        "wheel_builder": _wheel_builder,
    }
    with pytest.raises(AssetBundleError, match="outside"):
        build_bundle(output=root / "generated", **common)

    output = tmp_path / "bundle"
    output.mkdir()
    (output / "unexpected").write_text("x")
    with pytest.raises(AssetBundleError, match="empty"):
        build_bundle(output=output, **common)


def test_verify_bundle_rejects_tampering_and_unexpected_files(tmp_path: Path) -> None:
    output, _ = _build(tmp_path)
    wheel = next((output / "dataset").glob("*.whl"))
    wheel.write_bytes(b"tampered")
    wheel.chmod(0o600)
    with pytest.raises(AssetVerificationError, match="bytes"):
        verify_bundle(bundle=output, expected_commit=COMMIT)

    output, _ = _build(tmp_path / "second")
    extra = output / "unexpected.txt"
    extra.write_text("not allowed")
    extra.chmod(0o600)
    with pytest.raises(AssetVerificationError, match="unexpected"):
        verify_bundle(bundle=output, expected_commit=COMMIT)


def test_verify_bundle_rejects_wrong_commit_and_unsafe_mode(tmp_path: Path) -> None:
    output, _ = _build(tmp_path)
    with pytest.raises(AssetVerificationError, match="approved release"):
        verify_bundle(bundle=output, expected_commit="2" * 40)

    (output / "master-assets.env").chmod(0o644)
    with pytest.raises(AssetVerificationError, match="0600"):
        verify_bundle(bundle=output, expected_commit=COMMIT)
