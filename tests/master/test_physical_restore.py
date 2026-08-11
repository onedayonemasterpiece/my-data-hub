from __future__ import annotations

import io
import tarfile
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest

from my_data_hub.checkpoints.manifest import RestoreProbe, build_manifest
from my_data_hub.checkpoints.restore import PhysicalRestoreError, restore_physical_archive
from my_data_hub.checkpoints.verifier import IsolatedPostgresRestoreVerifier


class _File:
    kind = "physical"

    def __init__(self, path: str) -> None:
        self.path = path


class _Manifest:
    files = (_File("physical/base.tar.gz"), _File("physical/pg_wal.tar.gz"))


def _archive(path: Path, name: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(path, "w:gz") as stream:
        payload = b"18\n"
        item = tarfile.TarInfo(name)
        item.size = len(payload)
        stream.addfile(item, io.BytesIO(payload))


def test_physical_restore_extracts_regular_members_with_restrictive_modes(tmp_path: Path) -> None:
    package = tmp_path / "package"
    _archive(package / "physical/base.tar.gz", "PG_VERSION")
    _archive(package / "physical/pg_wal.tar.gz", "000000010000000000000001")
    target = tmp_path / "pgdata"
    restore_physical_archive(package, _Manifest(), target)  # type: ignore[arg-type]
    assert (target / "PG_VERSION").read_text() == "18\n"
    assert (target / "pg_wal/000000010000000000000001").read_text() == "18\n"
    assert (target / "PG_VERSION").stat().st_mode & 0o077 == 0


def test_physical_restore_rejects_traversal(tmp_path: Path) -> None:
    package = tmp_path / "package"
    _archive(package / "physical/base.tar.gz", "../escape")
    _archive(package / "physical/pg_wal.tar.gz", "000000010000000000000001")
    with pytest.raises(PhysicalRestoreError, match="unsafe"):
        restore_physical_archive(package, _Manifest(), tmp_path / "pgdata")  # type: ignore[arg-type]


def test_production_verifier_starts_only_the_restored_isolated_pgdata(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    package = tmp_path / "package"
    _archive(package / "physical/base.tar.gz", "PG_VERSION")
    _archive(package / "physical/pg_wal.tar.gz", "000000010000000000000001")
    for relative in ("logical/hub.dump", "physical/backup_manifest", "receipts/verification.json"):
        target = package / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"x")
    manifest = build_manifest(
        package_directory=package,
        checkpoint_id=UUID("11111111-1111-4111-8111-111111111111"),
        master_instance_id=UUID("22222222-2222-4222-8222-222222222222"),
        epoch=1,
        parent_checkpoint_id=None,
        postgres_version="18.0",
        pgvector_version="0.8.1",
        schema_version=13,
        canonical_revision=7,
        source_run_id="run-1",
        source_identity="private/checkpoints/v1",
        created_at=datetime(2026, 8, 11, tzinfo=UTC),
        checkpoint_lsn="0/16B6C50",
        file_kinds={
            "physical/base.tar.gz": "physical",
            "physical/backup_manifest": "postgres_backup_manifest",
            "physical/pg_wal.tar.gz": "physical",
            "logical/hub.dump": "logical",
            "receipts/verification.json": "verification_receipt",
        },
        restore_probe=RestoreProbe(13, 7, "e" * 64, {"hub.canonical_state": 1}),
    )
    actions: list[str] = []

    class Runner:
        def run(self, arguments: list[str], *, timeout_seconds: int) -> None:
            pgdata = Path(arguments[arguments.index("--pgdata") + 1])
            if arguments[-1] == "start":
                assert (pgdata / "PG_VERSION").is_file()
                assert (pgdata / "pg_wal/000000010000000000000001").is_file()
                actions.append("start")
            else:
                actions.append("stop")

    class Connection:
        def __enter__(self):  # type: ignore[no-untyped-def]
            return self

        def __exit__(self, *args):  # type: ignore[no-untyped-def]
            return None

    observed = {
        "schema_version": 13,
        "canonical_revision": 7,
        "logical_hash_sha256": "e" * 64,
        "row_counts": {"hub.canonical_state": 1},
    }
    monkeypatch.setattr(
        "my_data_hub.checkpoints.verifier.collect_restore_probe",
        lambda connection, relations: observed,
    )
    connected_host: list[Path] = []

    def connect(**kwargs):  # type: ignore[no-untyped-def]
        assert actions == ["start"]
        connected_host.append(Path(kwargs["host"]))
        return Connection()

    verifier = IsolatedPostgresRestoreVerifier(
        pg_ctl=tmp_path / "pg_ctl",
        working_directory=tmp_path,
        port=15434,
        runner=Runner(),
        connect=connect,
    )
    receipt = verifier.verify_restore(package, manifest)
    assert receipt["ok"] is True and receipt["mode"] == "isolated_physical_restore"
    assert actions == ["start", "stop"]
    assert connected_host and not connected_host[0].exists()
