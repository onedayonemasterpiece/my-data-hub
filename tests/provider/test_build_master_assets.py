from __future__ import annotations

import hashlib
import json
import stat
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from scripts.provider import build_master_assets as build_module
from scripts.provider import verify_master_assets as verify_module
from scripts.provider.build_master_assets import AssetBundleError, build_bundle, release_asset_dataset_ref
from scripts.provider.verify_master_assets import AssetVerificationError, verify_bundle

COMMIT = "1" * 40


def test_release_asset_dataset_ref_is_commit_scoped_and_kaggle_bounded() -> None:
    ref = release_asset_dataset_ref("owner", "0123456789abcdef" * 2 + "01234567")

    assert ref == "owner/mdh-master-assets-0123456789abcdef0123456789abcdef"
    assert len(ref.split("/", 1)[1]) == 50


@pytest.fixture(autouse=True)
def _approved_test_runtime(monkeypatch: pytest.MonkeyPatch) -> None:
    digest = hashlib.sha256(b"exact-postgresql-runtime").hexdigest()
    monkeypatch.setattr(build_module, "APPROVED_POSTGRES_RUNTIME_SHA256", digest)
    monkeypatch.setattr(verify_module, "APPROVED_POSTGRES_RUNTIME_SHA256", digest)


def _root(tmp_path: Path) -> Path:
    root = tmp_path / "source"
    master = root / "notebooks/02-postgres-master/worker.ipynb"
    verifier = root / "notebooks/03-checkpoint-verifier-restore-smoke/worker.ipynb"
    master.parent.mkdir(parents=True)
    verifier.parent.mkdir(parents=True)
    master.write_bytes(b'{"master":true}')
    verifier.write_bytes(b'{"verifier":true}')
    embedding_assets = root / "src/my_data_hub/embeddings/assets"
    embedding_assets.mkdir(parents=True)
    for name in ("e5-worker.json", "bge-worker.json"):
        (embedding_assets / name).write_bytes(
            (Path(__file__).parents[2] / "src/my_data_hub/embeddings/assets" / name).read_bytes()
        )
    recipe = root / "scripts/provider/assets/postgresql-18.4-pgvector-0.8.6.Dockerfile"
    recipe.parent.mkdir(parents=True)
    recipe.write_bytes(
        (Path(__file__).parents[2] / "scripts/provider/assets/postgresql-18.4-pgvector-0.8.6.Dockerfile").read_bytes()
    )
    smoke_runner = root / "scripts/provider/assets/embedding_dependency_smoke.py"
    smoke_runner.write_bytes(
        (Path(__file__).parents[2] / "scripts/provider/assets/embedding_dependency_smoke.py").read_bytes()
    )
    return root


def _wheel_builder(_root: Path, destination: Path) -> Path:
    wheel = destination / "my_data_hub-0.1.0-py3-none-any.whl"
    wheel.write_bytes(b"exact-test-wheel")
    return wheel


def _dependency_inputs(tmp_path: Path) -> tuple[Path, Path]:
    wheelhouse = tmp_path / "embedding-wheelhouse"
    wheelhouse.mkdir(parents=True)
    specs = (
        ("flagembedding", "1.4.0", "flagembedding-1.4.0-py3-none-any.whl"),
        ("ir-datasets", "0.6.2", "ir_datasets-0.6.2-py3-none-any.whl"),
        (
            "lz4",
            "4.4.5",
            "lz4-4.4.5-cp312-cp312-manylinux2014_x86_64.manylinux_2_17_x86_64.manylinux_2_28_x86_64.whl",
        ),
        ("psycopg", "3.3.4", "psycopg-3.3.4-py3-none-any.whl"),
        (
            "psycopg-binary",
            "3.3.4",
            "psycopg_binary-3.3.4-cp312-cp312-manylinux2014_x86_64.manylinux_2_17_x86_64.whl",
        ),
    )
    wheels = []
    for distribution, version, filename in specs:
        body = f"exact-test-wheel:{distribution}:{version}".encode()
        (wheelhouse / filename).write_bytes(body)
        wheels.append(
            {
                "distribution": distribution,
                "version": version,
                "filename": filename,
                "sha256": hashlib.sha256(body).hexdigest(),
                "source_url": f"https://files.pythonhosted.org/packages/test/{filename}",
            }
        )
    lock = {
        "schema_version": "my-data-hub-embedding-worker-wheel-lock.v1",
        "index_url": "https://pypi.org/simple",
        "runtime": {
            "image_identity": build_module.KAGGLE_CPU_IMAGE_IDENTITY,
            "source_commit": build_module.KAGGLE_CPU_IMAGE_SOURCE_COMMIT,
            "python_abi": "cp312",
            "platform": "manylinux2014_x86_64",
        },
        "wheels": wheels,
    }
    lock_path = tmp_path / "embedding-worker-wheel-lock.v1.json"
    lock_path.write_text(json.dumps(lock, sort_keys=True, separators=(",", ":")))
    return wheelhouse, lock_path


def _build(tmp_path: Path) -> tuple[Path, dict[str, object], Path]:
    root = _root(tmp_path)
    output = tmp_path / "bundle"
    runtime = tmp_path / "postgresql-runtime.tar.gz"
    runtime.write_bytes(b"exact-postgresql-runtime")
    known_hosts = tmp_path / "known_hosts"
    known_hosts.write_bytes(b"|1|aaaa|bbbb ssh-ed25519 AAAA\n")
    wheelhouse, dependency_lock = _dependency_inputs(tmp_path)
    manifest = build_bundle(
        root=root,
        output=output,
        source_commit=COMMIT,
        launch_dataset_ref="owner/master-assets",
        master_notebook_ref="owner/postgres-master",
        checkpoint_dataset_ref="owner/checkpoints",
        checkpoint_verifier_ref="owner/checkpoint-verifier",
        probe_relations=["hub.canonical_state"],
        postgres_runtime_archive=runtime,
        postgres_runtime_sha256=hashlib.sha256(runtime.read_bytes()).hexdigest(),
        tunnel_known_hosts=known_hosts,
        embedding_wheelhouse=wheelhouse,
        embedding_dependency_lock=dependency_lock,
        wheel_builder=_wheel_builder,
    )
    return output, manifest, dependency_lock


def test_build_bundle_is_exact_secret_free_and_schema_valid(tmp_path: Path) -> None:
    output, manifest, dependency_lock = _build(tmp_path)
    schema = json.loads((Path(__file__).parents[2] / "schemas/master-asset-bundle.v1.schema.json").read_text())
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
    assert all(stat.S_IMODE(path.stat().st_mode) == 0o600 for path in output.rglob("*") if path.is_file())
    assert verify_bundle(bundle=output, expected_commit=COMMIT, dependency_lock=dependency_lock) == {
        "schema_version": "my-data-hub-master-asset-bundle.v1",
        "source_commit": COMMIT,
        "manifest_sha256": hashlib.sha256((output / "master-asset-bundle.json").read_bytes()).hexdigest(),
        "asset_count": 15,
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
def test_build_bundle_rejects_ambiguous_identity(tmp_path: Path, field: str, value: object) -> None:
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
        "postgres_runtime_archive": tmp_path / "postgresql-runtime.tar.gz",
        "postgres_runtime_sha256": hashlib.sha256(b"exact-postgresql-runtime").hexdigest(),
        "tunnel_known_hosts": tmp_path / "known_hosts",
        "wheel_builder": _wheel_builder,
    }
    arguments["embedding_wheelhouse"], arguments["embedding_dependency_lock"] = _dependency_inputs(tmp_path)
    Path(arguments["postgres_runtime_archive"]).write_bytes(b"exact-postgresql-runtime")
    Path(arguments["tunnel_known_hosts"]).write_bytes(b"|1|aaaa|bbbb ssh-ed25519 AAAA\n")
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
        "postgres_runtime_archive": tmp_path / "postgresql-runtime.tar.gz",
        "postgres_runtime_sha256": hashlib.sha256(b"exact-postgresql-runtime").hexdigest(),
        "tunnel_known_hosts": tmp_path / "known_hosts",
        "wheel_builder": _wheel_builder,
    }
    common["embedding_wheelhouse"], common["embedding_dependency_lock"] = _dependency_inputs(tmp_path)
    Path(common["postgres_runtime_archive"]).write_bytes(b"exact-postgresql-runtime")
    Path(common["tunnel_known_hosts"]).write_bytes(b"|1|aaaa|bbbb ssh-ed25519 AAAA\n")
    with pytest.raises(AssetBundleError, match="outside"):
        build_bundle(output=root / "generated", **common)

    output = tmp_path / "bundle"
    output.mkdir()
    (output / "unexpected").write_text("x")
    with pytest.raises(AssetBundleError, match="empty"):
        build_bundle(output=output, **common)


def test_verify_bundle_rejects_tampering_and_unexpected_files(tmp_path: Path) -> None:
    output, _, dependency_lock = _build(tmp_path)
    wheel = next((output / "dataset").glob("*.whl"))
    wheel.write_bytes(b"tampered")
    wheel.chmod(0o600)
    with pytest.raises(AssetVerificationError, match="bytes"):
        verify_bundle(bundle=output, expected_commit=COMMIT, dependency_lock=dependency_lock)

    output, _, dependency_lock = _build(tmp_path / "second")
    extra = output / "unexpected.txt"
    extra.write_text("not allowed")
    extra.chmod(0o600)
    with pytest.raises(AssetVerificationError, match="unexpected"):
        verify_bundle(bundle=output, expected_commit=COMMIT, dependency_lock=dependency_lock)


def test_verify_bundle_rejects_wrong_commit_and_unsafe_mode(tmp_path: Path) -> None:
    output, _, dependency_lock = _build(tmp_path)
    with pytest.raises(AssetVerificationError, match="approved release"):
        verify_bundle(bundle=output, expected_commit="2" * 40, dependency_lock=dependency_lock)

    (output / "master-assets.env").chmod(0o644)
    with pytest.raises(AssetVerificationError, match="0600"):
        verify_bundle(bundle=output, expected_commit=COMMIT, dependency_lock=dependency_lock)


def test_build_fails_closed_on_incomplete_or_tampered_embedding_wheelhouse(tmp_path: Path) -> None:
    wheelhouse, dependency_lock = _dependency_inputs(tmp_path)
    missing = next(wheelhouse.iterdir())
    missing.unlink()
    lock, _ = build_module._load_dependency_lock(dependency_lock)
    with pytest.raises(AssetBundleError, match="inventory"):
        build_module._verify_wheelhouse(lock, wheelhouse)

    wheelhouse, dependency_lock = _dependency_inputs(tmp_path / "tampered")
    tampered = next(wheelhouse.iterdir())
    tampered.write_bytes(b"tampered")
    lock, _ = build_module._load_dependency_lock(dependency_lock)
    with pytest.raises(AssetBundleError, match="differs from lock"):
        build_module._verify_wheelhouse(lock, wheelhouse)


def test_smoke_contracts_distinguish_observation_from_central_verified_receipt() -> None:
    root = Path(__file__).parents[2]
    observation = json.loads(
        (root / "schemas/embeddings/embedding-dependency-smoke-observation.v1.schema.json").read_text()
    )
    receipt = json.loads(
        (root / "schemas/embeddings/embedding-dependency-smoke-receipt.v1.schema.json").read_text()
    )
    Draft202012Validator.check_schema(observation)
    Draft202012Validator.check_schema(receipt)
    assert "verified_by_central_adapter" not in observation["properties"]
    assert "observation_sha256" not in observation["properties"]
    assert receipt["properties"]["verified_by_central_adapter"]["const"] is True
    assert "observation_sha256" in receipt["required"]
    assert receipt["properties"]["observation_sha256"]["pattern"] == "^[0-9a-f]{64}$"
    assert receipt["properties"]["internet_enabled"]["const"] is False
