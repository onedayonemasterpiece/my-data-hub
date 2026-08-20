#!/usr/bin/env python3
"""Verify the exact master asset bundle before a control-plane install."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import stat
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "my-data-hub-master-asset-bundle.v1"
SHA40 = re.compile(r"^[0-9a-f]{40}$")
SHA256 = re.compile(r"^[0-9a-f]{64}$")
PROVIDER_REF = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
RELATION = re.compile(r"^[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*$")
WHEEL_PATH = re.compile(r"^dataset/my_data_hub-[A-Za-z0-9_.+-]+\.whl$")
DEPENDENCY_WHEEL_PATH = re.compile(
    r"^dataset/embedding-worker-wheelhouse/[A-Za-z0-9_.+-]+\.whl$"
)
MASTER_YDB_DEPENDENCY_WHEEL_PATH = re.compile(
    r"^dataset/master-python-wheelhouse/[A-Za-z0-9_.+-]+\.whl$"
)
MAX_MANIFEST_BYTES = 64 * 1024
MAX_ASSET_BYTES = 64 * 1024 * 1024
POSTGRES_BUILDER_IMAGE = "ubuntu:22.04@sha256:3b06811b2afd352be909dd088a004166d665dc76d38b13eada33522a9d915c6f"
POSTGRESQL_SOURCE_URL = "https://ftp.postgresql.org/pub/source/v18.4/postgresql-18.4.tar.bz2"
POSTGRESQL_SOURCE_SHA256 = "81a81ec695fb0c7901407defaa1d2f7973617154cf27ba74e3a7ab8e64436094"
PGVECTOR_SOURCE_URL = "https://github.com/pgvector/pgvector/archive/refs/tags/v0.8.6.tar.gz"
PGVECTOR_SOURCE_SHA256 = "10bf9938906e5d643bbc4a7eea104b6f57ba4898e5b76b20e60484ea1d5a7f8f"
APPROVED_POSTGRES_RUNTIME_SHA256 = "40bf34fb4a97a248537d0221127e38deb98c9b35208d474dd1b93f773c2558b5"
KAGGLE_CPU_IMAGE_IDENTITY = (
    "gcr.io/kaggle-images/python@sha256:"
    "c1fa4de30bc268e601e6dcddb6ceb2519b9adde3527dbbfb05e6bdfbbbdcd1a2"
)
KAGGLE_CPU_IMAGE_RELEASE = "https://github.com/Kaggle/docker-python/releases/tag/v170-CPU-c1fa4de30bc268e601e6dcddb6ceb2519b9adde3527dbbfb05e6bdfbbbdcd1a2"
KAGGLE_CPU_IMAGE_SOURCE_COMMIT = "fc61d5cda7da39530055bae9bd0e92865f995cd9"
KAGGLE_CPU_IMAGE_PYTHON_SERIES = "3.12"
EMBEDDING_DEPENDENCY_MANIFEST_NAME = "embedding-worker-dependencies.json"
EMBEDDING_WHEELHOUSE_NAME = "embedding-worker-wheelhouse"
EMBEDDING_WHEEL_LOCK_PATH = "assets/embedding-worker-wheel-lock.v1.json"
DEPENDENCY_SMOKE_IMPORTS = [
    "FlagEmbedding.BGEM3FlagModel",
    "psycopg",
    "torch",
    "transformers.AutoModel",
    "transformers.AutoTokenizer",
]
DEPENDENCY_IMAGE_DISTRIBUTIONS = [
    "accelerate",
    "datasets",
    "packaging",
    "peft",
    "protobuf",
    "sentence-transformers",
    "sentencepiece",
    "torch",
    "transformers",
    "typing-extensions",
]
EMBEDDING_DEPENDENCY_SMOKE_RUNNER_NAME = "embedding-dependency-smoke.py"
MASTER_YDB_DEPENDENCY_MANIFEST_NAME = "master-ydb-dependency.json"
MASTER_YDB_WHEELHOUSE_NAME = "master-python-wheelhouse"
MASTER_YDB_WHEEL_NAME = "ydb-3.31.2-py3-none-any.whl"
MASTER_YDB_WHEEL_SHA256 = "043b91af7dab122e9ee24cb1948576f324dc9b6dbb45952d2e7c58d99e2c5ddb"
MASTER_YDB_WHEEL_SOURCE_URL = (
    "https://files.pythonhosted.org/packages/f4/2c/"
    "0822896487b379b3dfce9011428728c3e22dcf311a29eacf5e47d203e182/"
    "ydb-3.31.2-py3-none-any.whl"
)
MASTER_YDB_WHEEL_LOCK_PATH = "assets/master-ydb-wheel-lock.v2.json"
MASTER_YDB_DISTRIBUTIONS = {
    "aiohappyeyeballs",
    "aiohttp",
    "aiosignal",
    "attrs",
    "frozenlist",
    "grpcio",
    "idna",
    "multidict",
    "packaging",
    "propcache",
    "protobuf",
    "typing-extensions",
    "yarl",
    "ydb",
}


class AssetVerificationError(RuntimeError):
    pass


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _private_file(path: Path, *, maximum: int) -> bytes:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise AssetVerificationError(f"missing bundle file: {path.name}") from exc
    if path.is_symlink() or not stat.S_ISREG(metadata.st_mode):
        raise AssetVerificationError(f"bundle file is not regular: {path.name}")
    if stat.S_IMODE(metadata.st_mode) != 0o600:
        raise AssetVerificationError(f"bundle file mode is not 0600: {path.name}")
    if not 1 <= metadata.st_size <= maximum:
        raise AssetVerificationError(f"bundle file size is outside its bound: {path.name}")
    return path.read_bytes()


def _private_directory(path: Path) -> None:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise AssetVerificationError(f"missing bundle directory: {path.name}") from exc
    if path.is_symlink() or not stat.S_ISDIR(metadata.st_mode):
        raise AssetVerificationError(f"bundle path is not a directory: {path.name}")
    if stat.S_IMODE(metadata.st_mode) != 0o700:
        raise AssetVerificationError(f"bundle directory mode is not 0700: {path.name}")


def _require_string(payload: dict[str, Any], key: str, pattern: re.Pattern[str]) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not pattern.fullmatch(value):
        raise AssetVerificationError(f"manifest field is invalid: {key}")
    return value


def _expected_environment(manifest: dict[str, Any]) -> dict[str, str]:
    relations = manifest["probe_relations"]
    return {
        "MY_DATA_HUB_KAGGLE_MASTER_SOURCE_IDENTITY": manifest["source_identity"],
        "MY_DATA_HUB_KAGGLE_MASTER_SOURCE_VERSION": manifest["source_version"],
        "MY_DATA_HUB_KAGGLE_MASTER_CHECKPOINT_REF": manifest["checkpoint_dataset_ref"],
        "MY_DATA_HUB_KAGGLE_MASTER_DATASET_REF": manifest["launch_dataset_ref"],
        "MY_DATA_HUB_KAGGLE_MASTER_NOTEBOOK_REF": manifest["master_notebook_ref"],
        "MY_DATA_HUB_KAGGLE_MASTER_DATASET_DIR": "/master-assets/dataset",
        "MY_DATA_HUB_KAGGLE_MASTER_NOTEBOOK_SOURCE": "/master-assets/postgres-master.ipynb",
        "MY_DATA_HUB_KAGGLE_CHECKPOINT_VERIFIER_REF": manifest["checkpoint_verifier_ref"],
        "MY_DATA_HUB_KAGGLE_CHECKPOINT_VERIFIER_SOURCE_FILE": "checkpoint-verifier.ipynb",
        "MY_DATA_HUB_KAGGLE_CHECKPOINT_PROBE_RELATIONS_JSON": json.dumps(relations, separators=(",", ":")),
        "MY_DATA_HUB_EMBEDDING_RUNTIME_IMAGE_IDENTITY": manifest["worker_runtime"]["image_identity"],
        "MY_DATA_HUB_EMBEDDING_RUNTIME_IMAGE_PINNING_TYPE": "original",
        "MY_DATA_HUB_EMBEDDING_RUNTIME_SOURCE_COMMIT": manifest["worker_runtime"]["source_commit"],
        "MY_DATA_HUB_EMBEDDING_WHEEL_RELATIVE_PATH": Path(manifest["assets"]["wheel"]["path"]).name,
        "MY_DATA_HUB_EMBEDDING_WHEEL_SHA256": manifest["assets"]["wheel"]["sha256"],
        "MY_DATA_HUB_EMBEDDING_DEPENDENCY_MANIFEST_RELATIVE_PATH": EMBEDDING_DEPENDENCY_MANIFEST_NAME,
        "MY_DATA_HUB_EMBEDDING_DEPENDENCY_MANIFEST_SHA256": manifest["assets"][
            "embedding_dependency_manifest"
        ]["sha256"],
        "MY_DATA_HUB_EMBEDDING_WHEELHOUSE_RELATIVE_PATH": EMBEDDING_WHEELHOUSE_NAME,
        "MY_DATA_HUB_MASTER_TUNNEL_KNOWN_HOSTS_PATH": "/master-assets/dataset/tunnel-known-hosts",
        "MY_DATA_HUB_MASTER_YDB_DEPENDENCY_MANIFEST_SHA256": manifest["assets"][
            "master_ydb_dependency_manifest"
        ]["sha256"],
        "MY_DATA_HUB_EMBEDDING_RUNTIME_PYTHON_SERIES": manifest["worker_runtime"]["python_series"],
    }


def _parse_environment(body: bytes) -> dict[str, str]:
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise AssetVerificationError("master-assets.env is not UTF-8") from exc
    values: dict[str, str] = {}
    for line in text.splitlines():
        if not line or "=" not in line:
            raise AssetVerificationError("master-assets.env contains an invalid line")
        key, value = line.split("=", 1)
        if key in values or not re.fullmatch(r"[A-Z][A-Z0-9_]*", key):
            raise AssetVerificationError("master-assets.env contains an invalid key")
        values[key] = value
    return values


def _verify_embedding_worker_asset(body: bytes) -> None:
    try:
        notebook = json.loads(body)
        source = "\n".join(
            "".join(cell.get("source", []))
            if isinstance(cell.get("source"), list)
            else str(cell.get("source", ""))
            for cell in notebook["cells"]
        )
    except (UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError) as exc:
        raise AssetVerificationError("generated embedding worker asset is invalid") from exc
    required = (
        "MY_DATA_HUB_EMBEDDING_DEPENDENCY_MANIFEST_SHA256",
        "MY_DATA_HUB_EMBEDDING_DEPENDENCY_SMOKE_RECEIPT_SHA256",
        "my-data-hub-embedding-dependency-smoke-receipt.v1",
        "'pip', 'install', '--no-index', '--no-deps'",
        "for item in wheels:",
    )
    if any(marker not in source for marker in required):
        raise AssetVerificationError("generated embedding worker omits offline dependency admission")


def verify_bundle(
    *,
    bundle: Path,
    expected_commit: str,
    dependency_lock: Path | None = None,
    ydb_dependency_lock: Path | None = None,
) -> dict[str, object]:
    if not SHA40.fullmatch(expected_commit):
        raise AssetVerificationError("expected commit is not an exact lowercase SHA")
    bundle = bundle.resolve()
    _private_directory(bundle)
    dataset = bundle / "dataset"
    _private_directory(dataset)

    manifest_body = _private_file(bundle / "master-asset-bundle.json", maximum=MAX_MANIFEST_BYTES)
    try:
        manifest: Any = json.loads(manifest_body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AssetVerificationError("master asset manifest is invalid JSON") from exc
    if not isinstance(manifest, dict):
        raise AssetVerificationError("master asset manifest must be an object")
    expected_keys = {
        "schema_version",
        "source_commit",
        "source_identity",
        "source_version",
        "launch_dataset_ref",
        "master_notebook_ref",
        "checkpoint_dataset_ref",
        "checkpoint_verifier_ref",
        "probe_relations",
        "worker_runtime",
        "assets",
        "embedding_dependency_wheels",
        "master_ydb_dependency_wheels",
    }
    if set(manifest) != expected_keys or manifest.get("schema_version") != SCHEMA_VERSION:
        raise AssetVerificationError("master asset manifest shape is not exact")
    if manifest["worker_runtime"] != {
        "image_identity": KAGGLE_CPU_IMAGE_IDENTITY,
        "docker_image_pinning_type": "original",
        "release_url": KAGGLE_CPU_IMAGE_RELEASE,
        "source_commit": KAGGLE_CPU_IMAGE_SOURCE_COMMIT,
        "python_series": KAGGLE_CPU_IMAGE_PYTHON_SERIES,
    }:
        raise AssetVerificationError("worker runtime provenance differs from the reviewed official release")
    source_commit = _require_string(manifest, "source_commit", SHA40)
    if source_commit != expected_commit:
        raise AssetVerificationError("master asset commit does not match the approved release")
    if manifest.get("source_identity") != f"git:{expected_commit}":
        raise AssetVerificationError("master asset source identity is inconsistent")
    if manifest.get("source_version") != expected_commit:
        raise AssetVerificationError("master asset source version is inconsistent")
    for key in (
        "launch_dataset_ref",
        "master_notebook_ref",
        "checkpoint_dataset_ref",
        "checkpoint_verifier_ref",
    ):
        _require_string(manifest, key, PROVIDER_REF)
    relations = manifest.get("probe_relations")
    if (
        not isinstance(relations, list)
        or not 1 <= len(relations) <= 100
        or len(relations) != len(set(relations))
        or any(not isinstance(item, str) or not RELATION.fullmatch(item) for item in relations)
    ):
        raise AssetVerificationError("master asset probe relations are invalid")

    assets = manifest.get("assets")
    if not isinstance(assets, dict) or set(assets) != {
        "master_notebook",
        "checkpoint_verifier",
        "wheel",
        "postgres_runtime",
        "postgres_runtime_manifest",
        "tunnel_known_hosts",
        "embedding_e5_worker",
        "embedding_bge_worker",
        "embedding_dependency_manifest",
        "embedding_dependency_smoke_runner",
        "master_ydb_dependency_manifest",
        "master_ydb_wheel",
    }:
        raise AssetVerificationError("master asset inventory is invalid")
    expected_paths = {
        "master_notebook": "postgres-master.ipynb",
        "checkpoint_verifier": "dataset/checkpoint-verifier.ipynb",
        "postgres_runtime": "dataset/postgresql-18-runtime.bundle",
        "postgres_runtime_manifest": "dataset/postgresql-18-runtime.json",
        "tunnel_known_hosts": "dataset/tunnel-known-hosts",
        "embedding_e5_worker": "dataset/e5-worker.json",
        "embedding_bge_worker": "dataset/bge-worker.json",
        "embedding_dependency_manifest": f"dataset/{EMBEDDING_DEPENDENCY_MANIFEST_NAME}",
        "embedding_dependency_smoke_runner": f"dataset/{EMBEDDING_DEPENDENCY_SMOKE_RUNNER_NAME}",
        "master_ydb_dependency_manifest": f"dataset/{MASTER_YDB_DEPENDENCY_MANIFEST_NAME}",
        "master_ydb_wheel": f"dataset/{MASTER_YDB_WHEELHOUSE_NAME}/{MASTER_YDB_WHEEL_NAME}",
    }
    verified_assets: dict[str, dict[str, object]] = {}
    for name, raw in assets.items():
        if not isinstance(raw, dict) or set(raw) != {"path", "sha256", "byte_size"}:
            raise AssetVerificationError(f"asset entry is invalid: {name}")
        relative = raw.get("path")
        if not isinstance(relative, str):
            raise AssetVerificationError(f"asset path is invalid: {name}")
        if name == "wheel":
            if not WHEEL_PATH.fullmatch(relative):
                raise AssetVerificationError("wheel path is invalid")
        elif relative != expected_paths[name]:
            raise AssetVerificationError(f"asset path is not fixed: {name}")
        digest = raw.get("sha256")
        byte_size = raw.get("byte_size")
        if not isinstance(digest, str) or not SHA256.fullmatch(digest):
            raise AssetVerificationError(f"asset hash is invalid: {name}")
        if not isinstance(byte_size, int) or not 1 <= byte_size <= MAX_ASSET_BYTES:
            raise AssetVerificationError(f"asset size is invalid: {name}")
        body = _private_file(bundle / relative, maximum=MAX_ASSET_BYTES)
        if len(body) != byte_size or _sha256(body) != digest:
            raise AssetVerificationError(f"asset bytes do not match the manifest: {name}")
        verified_assets[name] = {
            "path": relative,
            "sha256": digest,
            "byte_size": byte_size,
        }
    dependency_wheel_assets = manifest.get("embedding_dependency_wheels")
    if not isinstance(dependency_wheel_assets, list) or not dependency_wheel_assets:
        raise AssetVerificationError("embedding dependency wheel inventory is absent")
    verified_dependency_wheels: list[dict[str, object]] = []
    dependency_paths: set[str] = set()
    for raw in dependency_wheel_assets:
        if not isinstance(raw, dict) or set(raw) != {"path", "sha256", "byte_size"}:
            raise AssetVerificationError("embedding dependency wheel asset is invalid")
        relative = raw.get("path")
        digest = raw.get("sha256")
        byte_size = raw.get("byte_size")
        if (
            not isinstance(relative, str)
            or not DEPENDENCY_WHEEL_PATH.fullmatch(relative)
            or relative in dependency_paths
            or not isinstance(digest, str)
            or not SHA256.fullmatch(digest)
            or not isinstance(byte_size, int)
            or not 1 <= byte_size <= MAX_ASSET_BYTES
        ):
            raise AssetVerificationError("embedding dependency wheel asset identity is invalid")
        body = _private_file(bundle / relative, maximum=MAX_ASSET_BYTES)
        if len(body) != byte_size or _sha256(body) != digest:
            raise AssetVerificationError("embedding dependency wheel bytes do not match the manifest")
        dependency_paths.add(relative)
        verified_dependency_wheels.append(raw)

    dependency_manifest_body = (bundle / str(assets["embedding_dependency_manifest"]["path"])).read_bytes()
    try:
        dependency_manifest: Any = json.loads(dependency_manifest_body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AssetVerificationError("embedding dependency manifest is invalid JSON") from exc
    lock_path = dependency_lock or (Path(__file__).resolve().parent / EMBEDDING_WHEEL_LOCK_PATH)
    lock_body = lock_path.read_bytes()
    lock = json.loads(lock_body)
    if lock_body != json.dumps(
        lock, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode():
        raise AssetVerificationError("embedding dependency source lock is not canonical JSON")
    expected_dependency_keys = {
        "schema_version", "source_lock_sha256", "index_url", "runtime",
        "install_order", "required_image_distributions", "wheels", "smoke_requirement",
    }
    smoke = dependency_manifest.get("smoke_requirement") if isinstance(dependency_manifest, dict) else None
    dependency_wheels = dependency_manifest.get("wheels") if isinstance(dependency_manifest, dict) else None
    if (
        not isinstance(dependency_manifest, dict)
        or set(dependency_manifest) != expected_dependency_keys
        or dependency_manifest.get("schema_version")
        != "my-data-hub-embedding-worker-dependencies.v1"
        or dependency_manifest.get("source_lock_sha256") != _sha256(lock_body)
        or dependency_manifest.get("index_url") != "https://pypi.org/simple"
        or dependency_manifest.get("runtime") != lock.get("runtime")
        or dependency_manifest.get("required_image_distributions")
        != DEPENDENCY_IMAGE_DISTRIBUTIONS
        or not isinstance(dependency_wheels, list)
        or not isinstance(smoke, dict)
        or smoke != {
            "schema_version": "my-data-hub-embedding-dependency-smoke-receipt.v1",
            "observation_schema_version": (
                "my-data-hub-embedding-dependency-smoke-observation.v1"
            ),
            "required": True,
            "receipt_source": "central-provider-exact-private-kaggle-run",
            "worker_admission": "deny-without-verified-receipt",
            "imports": DEPENDENCY_SMOKE_IMPORTS,
        }
    ):
        raise AssetVerificationError("embedding dependency provenance is invalid")
    lock_wheels = lock.get("wheels")
    if not isinstance(lock_wheels, list) or len(lock_wheels) != len(dependency_wheels):
        raise AssetVerificationError("embedding dependency lock is inconsistent")
    expected_install_order: list[str] = []
    expected_wheel_assets: list[dict[str, object]] = []
    for locked, generated in zip(lock_wheels, dependency_wheels, strict=True):
        if not isinstance(locked, dict) or not isinstance(generated, dict):
            raise AssetVerificationError("embedding dependency wheel manifest is invalid")
        expected_generated = {
            "distribution": locked.get("distribution"),
            "version": locked.get("version"),
            "filename": locked.get("filename"),
            "sha256": locked.get("sha256"),
            "byte_size": generated.get("byte_size"),
        }
        if (
            generated != expected_generated
            or not isinstance(generated.get("byte_size"), int)
            or not 1 <= generated["byte_size"] <= MAX_ASSET_BYTES
        ):
            raise AssetVerificationError("embedding dependency wheel differs from the source lock")
        filename = str(locked["filename"])
        expected_install_order.append(filename)
        expected_wheel_assets.append(
            {
                "path": f"dataset/{EMBEDDING_WHEELHOUSE_NAME}/{filename}",
                "sha256": locked["sha256"],
                "byte_size": generated["byte_size"],
            }
        )
    if (
        dependency_manifest.get("install_order") != expected_install_order
        or verified_dependency_wheels != expected_wheel_assets
        or dependency_manifest_body
        != json.dumps(dependency_manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    ):
        raise AssetVerificationError("embedding dependency inventory is not exact")
    ydb_dependency_wheel_assets = manifest.get("master_ydb_dependency_wheels")
    if (
        not isinstance(ydb_dependency_wheel_assets, list)
        or len(ydb_dependency_wheel_assets) != len(MASTER_YDB_DISTRIBUTIONS)
    ):
        raise AssetVerificationError("master YDB dependency wheel inventory is absent")
    verified_ydb_dependency_wheels: list[dict[str, object]] = []
    ydb_dependency_paths: set[str] = set()
    for raw in ydb_dependency_wheel_assets:
        if not isinstance(raw, dict) or set(raw) != {"path", "sha256", "byte_size"}:
            raise AssetVerificationError("master YDB dependency wheel asset is invalid")
        relative = raw.get("path")
        digest = raw.get("sha256")
        byte_size = raw.get("byte_size")
        if (
            not isinstance(relative, str)
            or not MASTER_YDB_DEPENDENCY_WHEEL_PATH.fullmatch(relative)
            or relative in ydb_dependency_paths
            or not isinstance(digest, str)
            or not SHA256.fullmatch(digest)
            or not isinstance(byte_size, int)
            or isinstance(byte_size, bool)
            or not 1 <= byte_size <= MAX_ASSET_BYTES
        ):
            raise AssetVerificationError("master YDB dependency wheel identity is invalid")
        body = _private_file(bundle / relative, maximum=MAX_ASSET_BYTES)
        if len(body) != byte_size or _sha256(body) != digest:
            raise AssetVerificationError(
                "master YDB dependency wheel bytes do not match the manifest"
            )
        ydb_dependency_paths.add(relative)
        verified_ydb_dependency_wheels.append(raw)
    smoke_runner_source = Path(__file__).resolve().parent / "assets/embedding_dependency_smoke.py"
    if (
        smoke_runner_source.is_symlink()
        or not smoke_runner_source.is_file()
        or _sha256(smoke_runner_source.read_bytes())
        != assets["embedding_dependency_smoke_runner"]["sha256"]
    ):
        raise AssetVerificationError("embedding dependency smoke runner differs from the release")
    ydb_manifest_body = (bundle / str(assets["master_ydb_dependency_manifest"]["path"])).read_bytes()
    try:
        ydb_manifest: Any = json.loads(ydb_manifest_body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AssetVerificationError("master YDB dependency manifest is invalid JSON") from exc
    ydb_wheel = assets["master_ydb_wheel"]
    ydb_lock_path = ydb_dependency_lock or (
        Path(__file__).resolve().parent / MASTER_YDB_WHEEL_LOCK_PATH
    )
    ydb_lock_body = ydb_lock_path.read_bytes()
    ydb_lock = json.loads(ydb_lock_body)
    ydb_wheels = ydb_manifest.get("wheels") if isinstance(ydb_manifest, dict) else None
    ydb_distributions = {
        item.get("distribution")
        for item in ydb_wheels or []
        if isinstance(item, dict)
    }
    if (
        ydb_manifest_body
        != json.dumps(ydb_manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
        or ydb_lock_body != ydb_manifest_body
        or ydb_lock != ydb_manifest
        or not isinstance(ydb_manifest, dict)
        or set(ydb_manifest)
        != {
            "schema_version",
            "index_url",
            "runtime",
            "root_requirement",
            "install_order",
            "wheels",
        }
        or ydb_manifest.get("schema_version") != "my-data-hub-master-ydb-wheel-lock.v2"
        or ydb_manifest.get("index_url") != "https://pypi.org/simple"
        or ydb_manifest.get("runtime")
        != {
            "python_abi": "cp312",
            "platform": "manylinux2014_x86_64",
            "source_commit": KAGGLE_CPU_IMAGE_SOURCE_COMMIT,
        }
        or ydb_manifest.get("root_requirement") != "ydb==3.31.2"
        or not isinstance(ydb_wheels, list)
        or len(ydb_wheels) != len(MASTER_YDB_DISTRIBUTIONS)
        or ydb_distributions != MASTER_YDB_DISTRIBUTIONS
        or ydb_manifest.get("install_order")
        != [item.get("filename") for item in ydb_wheels if isinstance(item, dict)]
        or ydb_wheel.get("sha256") != MASTER_YDB_WHEEL_SHA256
    ):
        raise AssetVerificationError("master YDB dependency differs from the exact lock")
    expected_ydb_wheel_assets: list[dict[str, object]] = []
    root_wheel = None
    for item in ydb_wheels:
        if not isinstance(item, dict) or set(item) != {
            "distribution",
            "version",
            "filename",
            "sha256",
            "source_url",
        }:
            raise AssetVerificationError("master YDB dependency lock wheel is invalid")
        filename = item.get("filename")
        digest = item.get("sha256")
        source_url = item.get("source_url")
        relative = f"dataset/{MASTER_YDB_WHEELHOUSE_NAME}/{filename}"
        match = next(
            (raw for raw in verified_ydb_dependency_wheels if raw["path"] == relative),
            None,
        )
        if (
            not isinstance(filename, str)
            or not isinstance(digest, str)
            or not isinstance(source_url, str)
            or not source_url.startswith("https://files.pythonhosted.org/packages/")
            or not source_url.endswith("/" + filename)
            or match is None
            or match["sha256"] != digest
        ):
            raise AssetVerificationError("master YDB dependency wheel differs from lock")
        expected_ydb_wheel_assets.append(match)
        if item.get("distribution") == "ydb":
            root_wheel = item
    if (
        verified_ydb_dependency_wheels != expected_ydb_wheel_assets
        or root_wheel
        != {
            "distribution": "ydb",
            "version": "3.31.2",
            "filename": MASTER_YDB_WHEEL_NAME,
            "sha256": MASTER_YDB_WHEEL_SHA256,
            "source_url": MASTER_YDB_WHEEL_SOURCE_URL,
        }
        or ydb_wheel
        != next(
            raw
            for raw in verified_ydb_dependency_wheels
            if raw["path"].endswith("/" + MASTER_YDB_WHEEL_NAME)
        )
    ):
        raise AssetVerificationError("master YDB dependency inventory is not exact")
    for name in ("embedding_e5_worker", "embedding_bge_worker"):
        _verify_embedding_worker_asset(
            (bundle / str(assets[name]["path"])).read_bytes()
        )
    runtime_manifest = json.loads((bundle / str(assets["postgres_runtime_manifest"]["path"])).read_bytes())
    runtime_archive = assets["postgres_runtime"]
    expected_runtime_keys = {
        "schema_version",
        "postgresql_version",
        "pgvector_version",
        "platform",
        "archive_sha256",
        "postgresql_source_url",
        "postgresql_source_sha256",
        "pgvector_source_url",
        "pgvector_source_sha256",
        "builder_image",
        "build_recipe_sha256",
    }
    if (
        not isinstance(runtime_manifest, dict)
        or set(runtime_manifest) != expected_runtime_keys
        or runtime_manifest.get("schema_version") != "my-data-hub-postgresql-runtime.v1"
        or runtime_manifest.get("postgresql_version") != "18.4"
        or runtime_manifest.get("pgvector_version") != "0.8.6"
        or runtime_manifest.get("platform") != "linux-x86_64"
        or runtime_manifest.get("archive_sha256") != runtime_archive["sha256"]
        or runtime_archive["sha256"] != APPROVED_POSTGRES_RUNTIME_SHA256
        or runtime_manifest.get("postgresql_source_url") != POSTGRESQL_SOURCE_URL
        or runtime_manifest.get("postgresql_source_sha256") != POSTGRESQL_SOURCE_SHA256
        or runtime_manifest.get("pgvector_source_url") != PGVECTOR_SOURCE_URL
        or runtime_manifest.get("pgvector_source_sha256") != PGVECTOR_SOURCE_SHA256
        or runtime_manifest.get("builder_image") != POSTGRES_BUILDER_IMAGE
        or not SHA256.fullmatch(str(runtime_manifest.get("build_recipe_sha256", "")))
    ):
        raise AssetVerificationError("PostgreSQL runtime provenance is invalid")
    recipe = Path(__file__).resolve().parent / "assets/postgresql-18.4-pgvector-0.8.6.Dockerfile"
    if (
        recipe.is_symlink()
        or not recipe.is_file()
        or _sha256(recipe.read_bytes()) != runtime_manifest["build_recipe_sha256"]
    ):
        raise AssetVerificationError("PostgreSQL runtime recipe differs from the release")
    known_hosts_body = (bundle / str(assets["tunnel_known_hosts"]["path"])).read_bytes()
    try:
        known_host_lines = known_hosts_body.decode("ascii").splitlines()
    except UnicodeDecodeError as exc:
        raise AssetVerificationError("tunnel known_hosts is not ASCII") from exc
    if not known_host_lines or any(
        not line.startswith("|") or " ssh-ed25519 " not in line or len(line) > 4096 for line in known_host_lines
    ):
        raise AssetVerificationError("tunnel known_hosts is not hashed ed25519 metadata")

    env_body = _private_file(bundle / "master-assets.env", maximum=MAX_MANIFEST_BYTES)
    if _parse_environment(env_body) != _expected_environment(manifest):
        raise AssetVerificationError("master-assets.env does not exactly match the manifest")
    expected_files = {
        "master-asset-bundle.json",
        "master-assets.env",
        *(str(item["path"]) for item in verified_assets.values()),
        *(str(item["path"]) for item in verified_dependency_wheels),
        *(str(item["path"]) for item in verified_ydb_dependency_wheels),
    }
    observed_files = {path.relative_to(bundle).as_posix() for path in bundle.rglob("*") if path.is_file()}
    observed_directories = {path.relative_to(bundle).as_posix() for path in bundle.rglob("*") if path.is_dir()}
    if observed_files != expected_files or observed_directories != {
        "dataset", f"dataset/{EMBEDDING_WHEELHOUSE_NAME}", f"dataset/{MASTER_YDB_WHEELHOUSE_NAME}"
    }:
        raise AssetVerificationError("master asset bundle contains unexpected paths")
    canonical_manifest = json.dumps(manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    if manifest_body != canonical_manifest:
        raise AssetVerificationError("master asset manifest is not canonical JSON")
    return {
        "schema_version": SCHEMA_VERSION,
        "source_commit": expected_commit,
        "manifest_sha256": _sha256(manifest_body),
        "asset_count": len(expected_files) - 2,
        "verified": True,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--expected-commit", required=True)
    args = parser.parse_args()
    print(
        json.dumps(
            verify_bundle(bundle=args.bundle, expected_commit=args.expected_commit),
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
