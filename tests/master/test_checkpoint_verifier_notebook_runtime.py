from __future__ import annotations

import importlib.util
import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

from my_data_hub.checkpoints.manifest import RestoreProbe, build_manifest, write_manifest
from my_data_hub.providers.kaggle.adapter import tree_sha256

ROOT = Path(__file__).resolve().parents[2]
CHECKPOINT_ID = UUID("11111111-1111-4111-8111-111111111111")
MASTER_ID = UUID("22222222-2222-4222-8222-222222222222")
RUN_ID = UUID("33333333-3333-4333-8333-333333333333")


def _package(root: Path):  # type: ignore[no-untyped-def]
    for relative, content in {
        "physical/base.tar.gz": b"base",
        "physical/backup_manifest": b"native",
        "physical/pg_wal.tar.gz": b"wal",
        "logical/hub.dump": b"logical",
        "receipts/verification.json": b'{"ok":true}',
    }.items():
        target = root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
    manifest = build_manifest(
        package_directory=root,
        checkpoint_id=CHECKPOINT_ID,
        master_instance_id=MASTER_ID,
        epoch=1,
        parent_checkpoint_id=None,
        postgres_version="18.0",
        pgvector_version="0.8.1",
        schema_version=13,
        canonical_revision=4,
        source_run_id=str(RUN_ID),
        source_identity="owner/postgres-master/1",
        created_at=datetime(2026, 8, 11, tzinfo=UTC),
        checkpoint_lsn="0/16B6C50",
        file_kinds={
            "logical/hub.dump": "logical",
            "physical/backup_manifest": "postgres_backup_manifest",
            "physical/base.tar.gz": "physical",
            "physical/pg_wal.tar.gz": "physical",
            "receipts/verification.json": "verification_receipt",
        },
        restore_probe=RestoreProbe(13, 4, "a" * 64, {"hub.canonical_state": 1}),
    )
    path = root / "checkpoint-manifest.json"
    write_manifest(path, manifest)
    return manifest, path


def test_generated_verifier_actually_starts_an_isolated_restore_and_emits_bound_receipt(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    source_path = ROOT / "src/my_data_hub/checkpoints/verifier_runtime.py"
    source = source_path.read_text(encoding="utf-8")
    assert "IsolatedPostgresRestoreVerifier" in source
    assert "MY_DATA_HUB_RESTORE_DATABASE_URL" not in source
    assert "psycopg.connect" not in source

    spec = importlib.util.spec_from_file_location("checkpoint_verifier_runtime_test", source_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    package = tmp_path / "package"
    package.mkdir()
    manifest, manifest_path = _package(package)
    calls: list[tuple[Path, object]] = []

    class FakeIsolatedVerifier:
        def __init__(self, **kwargs: object) -> None:
            assert kwargs["pg_ctl"] == Path("/bin/true")
            assert str(kwargs["working_directory"]).endswith("/checkpoint-restore-work")

        def verify_restore(self, exact_package: Path, exact_manifest: object) -> dict[str, object]:
            calls.append((exact_package, exact_manifest))
            return {
                "ok": True,
                "mode": "isolated_physical_restore",
                "schema_version": 13,
                "canonical_revision": 4,
                "logical_hash_sha256": "a" * 64,
                "row_counts": {"hub.canonical_state": 1},
                "postgres_version": "18.4",
                "extensions": {
                    "citext": "1.6", "pg_trgm": "1.6", "pgcrypto": "1.3", "vector": "0.8.1"
                },
                "migration_boundary": {
                    "first_version": 1, "last_version": 13, "applied_count": 13,
                    "contiguous": True, "history_sha256": "b" * 64,
                },
                "database_invariants": {
                    "canonical_state_singletons": 1, "epoch_state_singletons": 1,
                    "unvalidated_constraints": 0,
                },
                "vector_query": {"operator": "cosine_distance", "dimensions": 3, "distance": 0.0},
                "bounded_read_smoke": {
                    "relation_count": 1, "total_rows": 1,
                    "statement_timeout_ms": 30000, "lock_timeout_ms": 3000,
                },
            }

    monkeypatch.setattr(module, "IsolatedPostgresRestoreVerifier", FakeIsolatedVerifier)
    monkeypatch.setattr(module, "_install_postgres_runtime", lambda: Path("/bin/true"))
    environment = {
        "MY_DATA_HUB_CHECKPOINT_DIRECTORY": str(package),
        "MY_DATA_HUB_CHECKPOINT_MANIFEST": str(manifest_path),
        "MY_DATA_HUB_CHECKPOINT_PACKAGE_SHA256": tree_sha256(package),
        "MY_DATA_HUB_CHECKPOINT_ID": str(CHECKPOINT_ID),
        "MY_DATA_HUB_CHECKPOINT_MANIFEST_SHA256": manifest.manifest_sha256,
        "MY_DATA_HUB_CHECKPOINT_DATASET_REF": "owner/private-checkpoints",
        "MY_DATA_HUB_CHECKPOINT_DATASET_VERSION": "7",
        "MY_DATA_HUB_VERIFIER_TASK_RUN_ID": str(RUN_ID),
        "MY_DATA_HUB_EXECUTION_PINS_SHA256": "c" * 64,
        "MY_DATA_HUB_KAGGLE_RUNTIME_IMAGE_IDENTITY": (
            "gcr.io/kaggle-images/python@sha256:" + "d" * 64
        ),
        "MY_DATA_HUB_KAGGLE_RUNTIME_SOURCE_COMMIT": "e" * 40,
        "MY_DATA_HUB_INPUT_DATASET_VERSIONS_JSON": json.dumps(
            ["owner/master-assets/3", "owner/private-checkpoints/7"]
        ),
    }
    for key, value in environment.items():
        monkeypatch.setenv(key, value)
    real_path = Path
    fake_working = tmp_path / "kaggle-working"
    fake_working.mkdir()

    def notebook_path(value: object = ".") -> Path:
        rendered = str(value)
        if rendered == "/kaggle/working/checkpoint-restore-work":
            return fake_working / "checkpoint-restore-work"
        if rendered == "/kaggle/working/checkpoint-restore-receipt.json":
            return fake_working / "checkpoint-restore-receipt.json"
        return real_path(value)  # type: ignore[arg-type]

    monkeypatch.setattr(module, "Path", notebook_path)
    output = fake_working / "checkpoint-restore-receipt.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.unlink(missing_ok=True)
    try:
        assert module.main() == 0
        receipt = json.loads(output.read_text())
    finally:
        output.unlink(missing_ok=True)
    assert calls == [(package, manifest)]
    assert receipt["task_run_id"] == str(RUN_ID)
    assert receipt["checkpoint_id"] == str(CHECKPOINT_ID)
    assert receipt["dataset_version"] == 7
    assert receipt["package_sha256"] == environment["MY_DATA_HUB_CHECKPOINT_PACKAGE_SHA256"]
    assert receipt["restore_mode"] == "isolated_physical_restore"
    assert receipt["schema_version"] == "my-data-hub-checkpoint-restore-smoke.v2"
    assert receipt["observed"]["extensions"]["vector"] == "0.8.1"
