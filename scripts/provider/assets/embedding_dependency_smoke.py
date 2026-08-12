#!/usr/bin/env python3
"""Credential-free exact-image smoke observation for embedding dependencies."""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
import platform
import subprocess
import sys
from pathlib import Path

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _required_path(name: str, *, directory: bool = False) -> Path:
    path = Path(os.environ.get(name, ""))
    valid = path.is_dir() if directory else path.is_file()
    if path.is_symlink() or not valid:
        raise RuntimeError(f"required smoke asset is absent: {name}")
    return path.resolve()


def main() -> int:
    manifest_path = _required_path("MY_DATA_HUB_EMBEDDING_DEPENDENCY_MANIFEST_PATH")
    wheelhouse = _required_path("MY_DATA_HUB_EMBEDDING_WHEELHOUSE_PATH", directory=True)
    project_wheel = _required_path("MY_DATA_HUB_WHEEL_PATH")
    output = Path(os.environ.get("MY_DATA_HUB_DEPENDENCY_SMOKE_OBSERVATION_PATH", ""))
    if not output.name or output.is_symlink() or output.exists():
        raise RuntimeError("fresh smoke observation output path is required")
    manifest_body = manifest_path.read_bytes()
    manifest = json.loads(manifest_body)
    if manifest_body != json.dumps(
        manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode():
        raise RuntimeError("dependency manifest is not canonical JSON")
    expected_image = manifest["runtime"]["image_identity"]
    expected_source_commit = manifest["runtime"]["source_commit"]
    if (
        os.environ.get("MY_DATA_HUB_KAGGLE_RUNTIME_IMAGE_IDENTITY") != expected_image
        or os.environ.get("MY_DATA_HUB_KAGGLE_RUNTIME_SOURCE_COMMIT") != expected_source_commit
        or Path("/etc/git_commit").read_text().strip() != expected_source_commit
        or not platform.python_version().startswith("3.12.")
    ):
        raise RuntimeError("smoke runtime differs from the dependency manifest")
    expected_project_sha = os.environ.get("MY_DATA_HUB_WHEEL_SHA256", "")
    if _sha256(project_wheel) != expected_project_sha:
        raise RuntimeError("project wheel hash mismatch")
    wheels = manifest["wheels"]
    if manifest["install_order"] != [entry["filename"] for entry in wheels]:
        raise RuntimeError("dependency install order differs from manifest")
    if {path.name for path in wheelhouse.iterdir()} != {entry["filename"] for entry in wheels}:
        raise RuntimeError("wheelhouse inventory differs from manifest")
    target = Path("/tmp/my-data-hub-embedding-dependency-smoke")
    target.mkdir(mode=0o700)
    for entry in wheels:
        wheel = wheelhouse / entry["filename"]
        if wheel.is_symlink() or not wheel.is_file() or _sha256(wheel) != entry["sha256"]:
            raise RuntimeError("wheel hash mismatch before smoke install")
        subprocess.run(
            [
                sys.executable,
                "-m",
                "pip",
                "install",
                "--no-index",
                "--no-deps",
                "--disable-pip-version-check",
                "--target",
                str(target),
                str(wheel),
            ],
            check=True,
        )
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--no-index",
            "--no-deps",
            "--disable-pip-version-check",
            "--target",
            str(target),
            str(project_wheel),
        ],
        check=True,
    )
    sys.path.insert(0, str(target))
    import psycopg  # noqa: F401
    import torch  # noqa: F401
    from FlagEmbedding import BGEM3FlagModel  # noqa: F401
    from psycopg import pq
    from transformers import AutoModel, AutoTokenizer  # noqa: F401

    if pq.__impl__ != "binary":
        raise RuntimeError("psycopg binary implementation is not active")
    overlay = {canonicalize_name(entry["distribution"]) for entry in wheels}
    queue = list(overlay)
    seen: set[str] = set()
    versions: dict[str, str] = {}
    while queue:
        name = canonicalize_name(queue.pop())
        if name in seen:
            continue
        seen.add(name)
        distribution = importlib.metadata.distribution(name)
        versions[name] = distribution.version
        for raw_requirement in distribution.requires or ():
            requirement = Requirement(raw_requirement)
            if requirement.marker is not None and not requirement.marker.evaluate({"extra": ""}):
                continue
            dependency = canonicalize_name(requirement.name)
            installed = importlib.metadata.version(dependency)
            if requirement.specifier and installed not in requirement.specifier:
                raise RuntimeError(f"incompatible dependency: {requirement}")
            queue.append(dependency)
    required_image_distributions = set(manifest["required_image_distributions"])
    if not required_image_distributions.issubset(versions):
        raise RuntimeError("required image distribution was not proven by the smoke")
    observation = {
        "schema_version": "my-data-hub-embedding-dependency-smoke-observation.v1",
        "status": "imports_passed",
        "expected_image_identity": expected_image,
        "image_source_commit": expected_source_commit,
        "python_version": platform.python_version(),
        "dependency_manifest_sha256": hashlib.sha256(manifest_body).hexdigest(),
        "project_wheel_sha256": expected_project_sha,
        "wheel_sha256s": {entry["filename"]: entry["sha256"] for entry in wheels},
        "imports": manifest["smoke_requirement"]["imports"],
        "psycopg_implementation": pq.__impl__,
        "distributions": dict(sorted(versions.items())),
    }
    output.write_text(
        json.dumps(observation, sort_keys=True, separators=(",", ":")), encoding="utf-8"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
