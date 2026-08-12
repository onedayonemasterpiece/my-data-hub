"""Primary source for an independent checkpoint restore-smoke notebook."""

from __future__ import annotations

import os
import shutil
from pathlib import Path

from my_data_hub.checkpoints.manifest import load_and_verify
from my_data_hub.checkpoints.verifier import IsolatedPostgresRestoreVerifier
from my_data_hub.hashing import canonical_json_bytes, sha256_file
from my_data_hub.providers.kaggle.adapter import tree_sha256


def _path(name: str) -> Path:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"required exact artifact is absent: {name}")
    path = Path(value)
    if not path.exists() or path.is_symlink():
        raise RuntimeError(f"required exact artifact is absent: {name}")
    return path


def _required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"required exact identity is absent: {name}")
    return value


def main() -> int:
    package = _path("MY_DATA_HUB_CHECKPOINT_DIRECTORY")
    manifest_path = _path("MY_DATA_HUB_CHECKPOINT_MANIFEST")
    if not package.is_dir() or manifest_path.parent.resolve() != package.resolve():
        raise RuntimeError("checkpoint manifest must be inside the exact private dataset input")
    expected_package_sha = _required("MY_DATA_HUB_CHECKPOINT_PACKAGE_SHA256")
    if len(expected_package_sha) != 64 or tree_sha256(package) != expected_package_sha:
        raise RuntimeError("exact private checkpoint dataset tree hash mismatch")
    manifest = load_and_verify(manifest_path, package)
    if str(manifest.checkpoint_id) != _required("MY_DATA_HUB_CHECKPOINT_ID"):
        raise RuntimeError("checkpoint id differs from the verifier launch identity")
    if manifest.manifest_sha256 != _required("MY_DATA_HUB_CHECKPOINT_MANIFEST_SHA256"):
        raise RuntimeError("checkpoint manifest hash differs from the verifier launch identity")

    pg_ctl_value = os.environ.get("MY_DATA_HUB_PG_CTL", "").strip() or shutil.which("pg_ctl") or ""
    pg_ctl = Path(pg_ctl_value)
    if not pg_ctl.is_absolute() or not pg_ctl.is_file() or pg_ctl.is_symlink() or not os.access(pg_ctl, os.X_OK):
        raise RuntimeError("exact executable pg_ctl is required for isolated restore")
    working = Path("/kaggle/working/checkpoint-restore-work")
    working.mkdir(mode=0o700, exist_ok=True)
    port = int(os.environ.get("MY_DATA_HUB_RESTORE_PORT", "55432"))
    verifier = IsolatedPostgresRestoreVerifier(
        pg_ctl=pg_ctl,
        working_directory=working,
        port=port,
        timeout_seconds=180,
    )
    restore = verifier.verify_restore(package, manifest)
    observed = {
        "schema_version": restore["schema_version"],
        "canonical_revision": restore["canonical_revision"],
        "logical_hash_sha256": restore["logical_hash_sha256"],
        "row_counts": restore["row_counts"],
    }
    receipt = {
        "schema_version": "my-data-hub-checkpoint-restore-smoke.v1",
        "task_run_id": _required("MY_DATA_HUB_VERIFIER_TASK_RUN_ID"),
        "checkpoint_id": str(manifest.checkpoint_id),
        "manifest_sha256": manifest.manifest_sha256,
        "manifest_file_sha256": sha256_file(manifest_path),
        "dataset_ref": _required("MY_DATA_HUB_CHECKPOINT_DATASET_REF"),
        "dataset_version": int(_required("MY_DATA_HUB_CHECKPOINT_DATASET_VERSION")),
        "package_sha256": expected_package_sha,
        "restore_mode": restore["mode"],
        "ok": True,
        "observed": observed,
    }
    output = Path("/kaggle/working/checkpoint-restore-receipt.json")
    output.write_bytes(canonical_json_bytes(receipt))
    return 0
