"""Primary source for an independent checkpoint restore-smoke notebook."""

from __future__ import annotations

import os
from pathlib import Path

import psycopg

from my_data_hub.checkpoints.manifest import load_and_verify
from my_data_hub.checkpoints.publisher import assert_restore_equality
from my_data_hub.checkpoints.restore_probe import collect_restore_probe
from my_data_hub.hashing import canonical_json_bytes, sha256_file


def _path(name: str) -> Path:
    path = Path(os.environ.get(name, ""))
    if not path.exists() or path.is_symlink():
        raise RuntimeError(f"required exact artifact is absent: {name}")
    return path


def main() -> int:
    package = _path("MY_DATA_HUB_CHECKPOINT_DIRECTORY")
    manifest_path = _path("MY_DATA_HUB_CHECKPOINT_MANIFEST")
    manifest = load_and_verify(manifest_path, package)
    database_url = os.environ.get("MY_DATA_HUB_RESTORE_DATABASE_URL", "")
    if not database_url.startswith(("postgresql://", "postgres://")):
        raise RuntimeError("isolated restore database URL is required")
    with psycopg.connect(database_url, connect_timeout=15) as connection:
        observed = collect_restore_probe(connection, tuple(sorted(manifest.restore_probe.row_counts)))
    assert_restore_equality(manifest, observed)
    receipt = {
        "schema_version": "my-data-hub-checkpoint-restore-smoke.v1",
        "checkpoint_id": str(manifest.checkpoint_id),
        "manifest_sha256": manifest.manifest_sha256,
        "manifest_file_sha256": sha256_file(manifest_path),
        "ok": True,
        "observed": observed,
    }
    output = Path("/kaggle/working/checkpoint-restore-receipt.json")
    output.write_bytes(canonical_json_bytes(receipt))
    return 0
