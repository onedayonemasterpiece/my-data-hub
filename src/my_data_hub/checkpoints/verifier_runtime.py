"""Primary source for an independent checkpoint restore-smoke notebook."""

from __future__ import annotations

import json
import os
import tarfile
from pathlib import Path, PurePosixPath

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


def _install_postgres_runtime() -> Path:
    archive = _path("MY_DATA_HUB_POSTGRES_RUNTIME_ARCHIVE")
    manifest_path = _path("MY_DATA_HUB_POSTGRES_RUNTIME_MANIFEST")
    expected_archive_sha = _required("MY_DATA_HUB_POSTGRES_RUNTIME_ARCHIVE_SHA256")
    expected_manifest_sha = _required("MY_DATA_HUB_POSTGRES_RUNTIME_MANIFEST_SHA256")
    if sha256_file(archive) != expected_archive_sha or sha256_file(manifest_path) != expected_manifest_sha:
        raise RuntimeError("exact PostgreSQL runtime asset hash mismatch")
    manifest = json.loads(manifest_path.read_bytes())
    if (
        not isinstance(manifest, dict)
        or manifest.get("schema_version") != "my-data-hub-postgresql-runtime.v1"
        or manifest.get("archive_sha256") != expected_archive_sha
        or manifest.get("postgresql_version") != "18.4"
        or manifest.get("pgvector_version") != "0.8.6"
        or manifest.get("platform") != "linux-x86_64"
    ):
        raise RuntimeError("PostgreSQL runtime provenance differs")
    root = Path("/kaggle/working/checkpoint-postgresql-runtime")
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    with tarfile.open(archive, "r:gz") as bundle:
        members = bundle.getmembers()
        if not 1 <= len(members) <= 4000 or sum(max(0, item.size) for item in members) > 536870912:
            raise RuntimeError("PostgreSQL runtime archive exceeds fixed bounds")
        if any(
            item.islnk()
            or (item.issym() and ("/" in item.linkname or ".." in PurePosixPath(item.linkname).parts))
            or not item.name.startswith("pgsql/")
            or ".." in PurePosixPath(item.name).parts
            for item in members
        ):
            raise RuntimeError("PostgreSQL runtime archive contains an unsafe member")
        bundle.extractall(root, members=members, filter="data")
    library_path = root / "pgsql/lib"
    os.environ["LD_LIBRARY_PATH"] = f"{library_path}:{library_path / 'runtime-deps'}"
    pg_ctl = root / "pgsql/bin/pg_ctl"
    if not pg_ctl.is_file() or pg_ctl.is_symlink() or not os.access(pg_ctl, os.X_OK):
        raise RuntimeError("exact extracted pg_ctl is unavailable")
    return pg_ctl


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

    pg_ctl = _install_postgres_runtime()
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
        "postgres_version": restore["postgres_version"],
        "extensions": restore["extensions"],
        "migration_boundary": restore["migration_boundary"],
        "database_invariants": restore["database_invariants"],
        "vector_query": restore["vector_query"],
        "bounded_read_smoke": restore["bounded_read_smoke"],
    }
    receipt = {
        "schema_version": "my-data-hub-checkpoint-restore-smoke.v2",
        "task_run_id": _required("MY_DATA_HUB_VERIFIER_TASK_RUN_ID"),
        "checkpoint_id": str(manifest.checkpoint_id),
        "manifest_sha256": manifest.manifest_sha256,
        "manifest_file_sha256": sha256_file(manifest_path),
        "dataset_ref": _required("MY_DATA_HUB_CHECKPOINT_DATASET_REF"),
        "dataset_version": int(_required("MY_DATA_HUB_CHECKPOINT_DATASET_VERSION")),
        "package_sha256": expected_package_sha,
        "restore_mode": restore["mode"],
        "execution_pins_sha256": _required("MY_DATA_HUB_EXECUTION_PINS_SHA256"),
        "runtime_image_identity": _required("MY_DATA_HUB_KAGGLE_RUNTIME_IMAGE_IDENTITY"),
        "runtime_image_source_commit": _required("MY_DATA_HUB_KAGGLE_RUNTIME_SOURCE_COMMIT"),
        "input_dataset_versions": json.loads(_required("MY_DATA_HUB_INPUT_DATASET_VERSIONS_JSON")),
        "ok": True,
        "observed": observed,
    }
    output = Path("/kaggle/working/checkpoint-restore-receipt.json")
    output.write_bytes(canonical_json_bytes(receipt))
    return 0
