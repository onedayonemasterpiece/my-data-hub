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
MAX_MANIFEST_BYTES = 64 * 1024
MAX_ASSET_BYTES = 64 * 1024 * 1024


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
        "MY_DATA_HUB_KAGGLE_CHECKPOINT_PROBE_RELATIONS_JSON": json.dumps(
            relations, separators=(",", ":")
        ),
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


def verify_bundle(*, bundle: Path, expected_commit: str) -> dict[str, object]:
    if not SHA40.fullmatch(expected_commit):
        raise AssetVerificationError("expected commit is not an exact lowercase SHA")
    bundle = bundle.resolve()
    _private_directory(bundle)
    dataset = bundle / "dataset"
    _private_directory(dataset)

    manifest_body = _private_file(
        bundle / "master-asset-bundle.json", maximum=MAX_MANIFEST_BYTES
    )
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
        "assets",
    }
    if set(manifest) != expected_keys or manifest.get("schema_version") != SCHEMA_VERSION:
        raise AssetVerificationError("master asset manifest shape is not exact")
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
    }:
        raise AssetVerificationError("master asset inventory is invalid")
    expected_paths = {
        "master_notebook": "postgres-master.ipynb",
        "checkpoint_verifier": "dataset/checkpoint-verifier.ipynb",
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

    env_body = _private_file(bundle / "master-assets.env", maximum=MAX_MANIFEST_BYTES)
    if _parse_environment(env_body) != _expected_environment(manifest):
        raise AssetVerificationError("master-assets.env does not exactly match the manifest")
    expected_files = {
        "master-asset-bundle.json",
        "master-assets.env",
        *(str(item["path"]) for item in verified_assets.values()),
    }
    observed_files = {
        path.relative_to(bundle).as_posix()
        for path in bundle.rglob("*")
        if path.is_file()
    }
    observed_directories = {
        path.relative_to(bundle).as_posix()
        for path in bundle.rglob("*")
        if path.is_dir()
    }
    if observed_files != expected_files or observed_directories != {"dataset"}:
        raise AssetVerificationError("master asset bundle contains unexpected paths")
    canonical_manifest = json.dumps(
        manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    if manifest_body != canonical_manifest:
        raise AssetVerificationError("master asset manifest is not canonical JSON")
    return {
        "schema_version": SCHEMA_VERSION,
        "source_commit": expected_commit,
        "manifest_sha256": _sha256(manifest_body),
        "asset_count": len(verified_assets),
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
