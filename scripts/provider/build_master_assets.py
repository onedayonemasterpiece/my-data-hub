#!/usr/bin/env python3
"""Build the exact, secret-free asset bundle consumed by the control plane."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import subprocess
import sys
import tempfile
from collections.abc import Callable, Sequence
from pathlib import Path

from my_data_hub.hashing import canonical_json_bytes

SCHEMA_VERSION = "my-data-hub-master-asset-bundle.v1"
SHA40 = re.compile(r"^[0-9a-f]{40}$")
REF = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
MAX_ASSET_BYTES = 64 * 1024 * 1024


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
    commit = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=root, text=True
    ).strip()
    if not SHA40.fullmatch(commit):
        raise AssetBundleError("repository HEAD is not an exact commit")
    status = subprocess.check_output(
        ["git", "status", "--porcelain", "--untracked-files=all"], cwd=root, text=True
    )
    if status.strip():
        raise AssetBundleError("master assets require a clean exact source tree")
    return commit


def _build_wheel(root: Path, destination: Path) -> Path:
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "wheel",
            "--no-deps",
            "--no-build-isolation",
            "--wheel-dir",
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

    files = {
        output / "postgres-master.ipynb": master,
        dataset_dir / "checkpoint-verifier.ipynb": verifier,
        dataset_dir / wheel_name: wheel,
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
        "MY_DATA_HUB_KAGGLE_CHECKPOINT_PROBE_RELATIONS_JSON": json.dumps(
            relations, separators=(",", ":")
        ),
    }
    env_path = output / "master-assets.env"
    env_path.write_text(
        "".join(f"{key}={value}\n" for key, value in env.items()),
        encoding="utf-8",
    )
    os.chmod(env_path, 0o600)
    if any(
        path.is_symlink() or stat.S_IMODE(path.stat().st_mode) != 0o600
        for path in output.rglob("*")
        if path.is_file()
    ):
        raise AssetBundleError("bundle contains an unsafe file")
    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--provider-owner", required=True)
    parser.add_argument("--probe-relation", action="append", default=[])
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
