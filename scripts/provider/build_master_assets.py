#!/usr/bin/env python3
"""Build the exact, secret-free asset bundle consumed by the control plane."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import tempfile
from collections.abc import Callable, Sequence
from pathlib import Path

from my_data_hub.hashing import canonical_json_bytes

SCHEMA_VERSION = "my-data-hub-master-asset-bundle.v1"
SHA40 = re.compile(r"^[0-9a-f]{40}$")
REF = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
MAX_ASSET_BYTES = 64 * 1024 * 1024
POSTGRES_RUNTIME_NAME = "postgresql-18-runtime.tar.gz"
POSTGRES_RUNTIME_MANIFEST_NAME = "postgresql-18-runtime.json"
TUNNEL_KNOWN_HOSTS_NAME = "tunnel-known-hosts"
SHA256 = re.compile(r"^[0-9a-f]{64}$")
REJECTED_POSTGRES_RUNTIME_SHA256 = {
    # Built with pgvector's host-native OPTFLAGS and therefore not portable.
    "9be7324987fa81656e6b54888b9ec707851481254cdf839517a6a0f9732671f6",
}
POSTGRES_RECIPE_PATH = "scripts/provider/assets/postgresql-18.4-pgvector-0.8.6.Dockerfile"
POSTGRES_BUILDER_IMAGE = "ubuntu:22.04@sha256:3b06811b2afd352be909dd088a004166d665dc76d38b13eada33522a9d915c6f"
APPROVED_POSTGRES_RUNTIME_SHA256 = "40bf34fb4a97a248537d0221127e38deb98c9b35208d474dd1b93f773c2558b5"


class AssetBundleError(RuntimeError):
    pass


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _read_bounded(path: Path, *, maximum: int) -> bytes:
    if path.is_symlink() or not path.is_file():
        raise AssetBundleError(f"asset is not a regular file: {path}")
    size = path.stat().st_size
    if not 1 <= size <= maximum:
        raise AssetBundleError(f"asset size is outside its bound: {path}")
    return path.read_bytes()


def _clean_commit(root: Path) -> str:
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    if not SHA40.fullmatch(commit):
        raise AssetBundleError("repository HEAD is not an exact commit")
    status = subprocess.check_output(["git", "status", "--porcelain", "--untracked-files=all"], cwd=root, text=True)
    if status.strip():
        raise AssetBundleError("master assets require a clean exact source tree")
    return commit


def _build_wheel(root: Path, destination: Path) -> Path:
    uv = shutil.which("uv")
    if uv is None:
        raise AssetBundleError("uv is required to build the reviewed wheel")
    subprocess.run(
        [
            uv,
            "build",
            "--wheel",
            "--no-create-gitignore",
            "--out-dir",
            str(destination),
            str(root),
        ],
        cwd=root,
        check=True,
        stdout=subprocess.DEVNULL,
    )
    wheels = tuple(destination.glob("*.whl"))
    if len(wheels) != 1:
        raise AssetBundleError("wheel build did not produce exactly one artifact")
    return wheels[0]


def build_bundle(
    *,
    root: Path,
    output: Path,
    source_commit: str,
    launch_dataset_ref: str,
    master_notebook_ref: str,
    checkpoint_dataset_ref: str,
    checkpoint_verifier_ref: str,
    probe_relations: Sequence[str],
    postgres_runtime_archive: Path,
    postgres_runtime_sha256: str,
    tunnel_known_hosts: Path,
    wheel_builder: Callable[[Path, Path], Path] = _build_wheel,
) -> dict[str, object]:
    root = root.resolve()
    output = output.resolve()
    if output == root or root in output.parents:
        raise AssetBundleError("output must be outside the source checkout")
    if not SHA40.fullmatch(source_commit):
        raise AssetBundleError("source_commit must be an exact lowercase commit SHA")
    refs = {
        "launch_dataset_ref": launch_dataset_ref,
        "master_notebook_ref": master_notebook_ref,
        "checkpoint_dataset_ref": checkpoint_dataset_ref,
        "checkpoint_verifier_ref": checkpoint_verifier_ref,
    }
    if any(not REF.fullmatch(value) for value in refs.values()):
        raise AssetBundleError("all provider identities must be exact owner/slug refs")
    relations = tuple(probe_relations)
    if (
        not relations
        or len(relations) > 100
        or len(set(relations)) != len(relations)
        or any(not re.fullmatch(r"[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*", item) for item in relations)
    ):
        raise AssetBundleError("checkpoint probe relations are invalid")
    if output.exists():
        if output.is_symlink() or not output.is_dir() or any(output.iterdir()):
            raise AssetBundleError("output must be an absent or empty regular directory")
    else:
        output.mkdir(parents=True, mode=0o700)
    os.chmod(output, 0o700)
    dataset_dir = output / "dataset"
    dataset_dir.mkdir(mode=0o700)

    master_path = root / "notebooks/02-postgres-master/worker.ipynb"
    verifier_path = root / "notebooks/03-checkpoint-verifier-restore-smoke/worker.ipynb"
    master = _read_bounded(master_path, maximum=8 * 1024 * 1024)
    verifier = _read_bounded(verifier_path, maximum=8 * 1024 * 1024)
    with tempfile.TemporaryDirectory(prefix="mdh-master-wheel-") as temporary:
        wheel_path = wheel_builder(root, Path(temporary))
        wheel = _read_bounded(wheel_path, maximum=MAX_ASSET_BYTES)
        wheel_name = wheel_path.name
        if not re.fullmatch(r"my_data_hub-[A-Za-z0-9_.+-]+\.whl", wheel_name):
            raise AssetBundleError("wheel name does not identify my-data-hub")
    postgres_runtime = _read_bounded(postgres_runtime_archive, maximum=MAX_ASSET_BYTES)
    if (
        not SHA256.fullmatch(postgres_runtime_sha256)
        or postgres_runtime_sha256 != APPROVED_POSTGRES_RUNTIME_SHA256
        or postgres_runtime_sha256 in REJECTED_POSTGRES_RUNTIME_SHA256
        or _sha256(postgres_runtime) != postgres_runtime_sha256
    ):
        raise AssetBundleError("PostgreSQL runtime differs from the reviewed exact archive")
    recipe = _read_bounded(root / POSTGRES_RECIPE_PATH, maximum=64 * 1024)
    build_receipt = {
        "schema_version": "my-data-hub-postgresql-runtime.v1",
        "postgresql_version": "18.4",
        "pgvector_version": "0.8.6",
        "platform": "linux-x86_64",
        "archive_sha256": postgres_runtime_sha256,
        "postgresql_source_url": "https://ftp.postgresql.org/pub/source/v18.4/postgresql-18.4.tar.bz2",
        "postgresql_source_sha256": "81a81ec695fb0c7901407defaa1d2f7973617154cf27ba74e3a7ab8e64436094",
        "pgvector_source_url": "https://github.com/pgvector/pgvector/archive/refs/tags/v0.8.6.tar.gz",
        "pgvector_source_sha256": "10bf9938906e5d643bbc4a7eea104b6f57ba4898e5b76b20e60484ea1d5a7f8f",
        "builder_image": POSTGRES_BUILDER_IMAGE,
        "build_recipe_sha256": _sha256(recipe),
    }
    known_hosts = _read_bounded(tunnel_known_hosts, maximum=64 * 1024)
    try:
        known_host_lines = known_hosts.decode("ascii").splitlines()
    except UnicodeDecodeError as exc:
        raise AssetBundleError("tunnel known_hosts must be ASCII") from exc
    if not known_host_lines or any(
        not line.startswith("|") or " ssh-ed25519 " not in line or len(line) > 4096 for line in known_host_lines
    ):
        raise AssetBundleError("tunnel known_hosts must contain reviewed hashed ed25519 entries")
    runtime_manifest = canonical_json_bytes(build_receipt)

    files = {
        output / "postgres-master.ipynb": master,
        dataset_dir / "checkpoint-verifier.ipynb": verifier,
        dataset_dir / wheel_name: wheel,
        dataset_dir / POSTGRES_RUNTIME_NAME: postgres_runtime,
        dataset_dir / POSTGRES_RUNTIME_MANIFEST_NAME: runtime_manifest,
        dataset_dir / TUNNEL_KNOWN_HOSTS_NAME: known_hosts,
    }
    for path, body in files.items():
        path.write_bytes(body)
        os.chmod(path, 0o600)

    manifest: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "source_commit": source_commit,
        "source_identity": f"git:{source_commit}",
        "source_version": source_commit,
        **refs,
        "probe_relations": list(relations),
        "assets": {
            "master_notebook": {
                "path": "postgres-master.ipynb",
                "sha256": _sha256(master),
                "byte_size": len(master),
            },
            "checkpoint_verifier": {
                "path": "dataset/checkpoint-verifier.ipynb",
                "sha256": _sha256(verifier),
                "byte_size": len(verifier),
            },
            "wheel": {
                "path": f"dataset/{wheel_name}",
                "sha256": _sha256(wheel),
                "byte_size": len(wheel),
            },
            "postgres_runtime": {
                "path": f"dataset/{POSTGRES_RUNTIME_NAME}",
                "sha256": _sha256(postgres_runtime),
                "byte_size": len(postgres_runtime),
            },
            "postgres_runtime_manifest": {
                "path": f"dataset/{POSTGRES_RUNTIME_MANIFEST_NAME}",
                "sha256": _sha256(runtime_manifest),
                "byte_size": len(runtime_manifest),
            },
            "tunnel_known_hosts": {
                "path": f"dataset/{TUNNEL_KNOWN_HOSTS_NAME}",
                "sha256": _sha256(known_hosts),
                "byte_size": len(known_hosts),
            },
        },
    }
    manifest_body = canonical_json_bytes(manifest)
    manifest_path = output / "master-asset-bundle.json"
    manifest_path.write_bytes(manifest_body)
    os.chmod(manifest_path, 0o600)

    env = {
        "MY_DATA_HUB_KAGGLE_MASTER_SOURCE_IDENTITY": f"git:{source_commit}",
        "MY_DATA_HUB_KAGGLE_MASTER_SOURCE_VERSION": source_commit,
        "MY_DATA_HUB_KAGGLE_MASTER_CHECKPOINT_REF": checkpoint_dataset_ref,
        "MY_DATA_HUB_KAGGLE_MASTER_DATASET_REF": launch_dataset_ref,
        "MY_DATA_HUB_KAGGLE_MASTER_NOTEBOOK_REF": master_notebook_ref,
        "MY_DATA_HUB_KAGGLE_MASTER_DATASET_DIR": "/master-assets/dataset",
        "MY_DATA_HUB_KAGGLE_MASTER_NOTEBOOK_SOURCE": "/master-assets/postgres-master.ipynb",
        "MY_DATA_HUB_KAGGLE_CHECKPOINT_VERIFIER_REF": checkpoint_verifier_ref,
        "MY_DATA_HUB_KAGGLE_CHECKPOINT_VERIFIER_SOURCE_FILE": "checkpoint-verifier.ipynb",
        "MY_DATA_HUB_KAGGLE_CHECKPOINT_PROBE_RELATIONS_JSON": json.dumps(relations, separators=(",", ":")),
    }
    env_path = output / "master-assets.env"
    env_path.write_text(
        "".join(f"{key}={value}\n" for key, value in env.items()),
        encoding="utf-8",
    )
    os.chmod(env_path, 0o600)
    if any(
        path.is_symlink() or stat.S_IMODE(path.stat().st_mode) != 0o600 for path in output.rglob("*") if path.is_file()
    ):
        raise AssetBundleError("bundle contains an unsafe file")
    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--provider-owner", required=True)
    parser.add_argument("--probe-relation", action="append", default=[])
    parser.add_argument("--postgres-runtime-archive", type=Path, required=True)
    parser.add_argument("--postgres-runtime-sha256", required=True)
    parser.add_argument("--tunnel-known-hosts", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[2]
    commit = _clean_commit(root)
    owner = args.provider_owner.strip()
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", owner):
        raise SystemExit("--provider-owner is invalid")
    manifest = build_bundle(
        root=root,
        output=args.output.expanduser().resolve(),
        source_commit=commit,
        launch_dataset_ref=f"{owner}/my-data-hub-master-assets",
        master_notebook_ref=f"{owner}/my-data-hub-postgres-master",
        checkpoint_dataset_ref=f"{owner}/my-data-hub-checkpoints",
        checkpoint_verifier_ref=f"{owner}/my-data-hub-checkpoint-verifier",
        probe_relations=args.probe_relation or ["hub.canonical_state"],
        postgres_runtime_archive=args.postgres_runtime_archive.expanduser().resolve(),
        postgres_runtime_sha256=args.postgres_runtime_sha256,
        tunnel_known_hosts=args.tunnel_known_hosts.expanduser().resolve(),
    )
    print(
        json.dumps(
            {
                "output": str(args.output),
                "manifest_sha256": _sha256(canonical_json_bytes(manifest)),
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
