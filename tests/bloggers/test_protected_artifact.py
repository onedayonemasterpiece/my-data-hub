from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from my_data_hub.hashing import sha256_file
from my_data_hub.workloads.bloggers.importer import batch_identity
from my_data_hub.workloads.bloggers.protected_artifact import (
    DATA_NAME,
    MANIFEST_NAME,
    RECEIPT_NAME,
    ObservedInventoryBinding,
    ProtectedArtifactError,
    ProtectedExportManifest,
    ProtectedExportReceipt,
    ReaderPrincipalBinding,
    SourceBinding,
    load_protected_artifact,
    scan_evidence,
    scan_rows,
)
from my_data_hub.workloads.bloggers.schema import SOURCE_COLUMNS, BloggerSourceRow

REVISION = "b" * 40
SNAPSHOT = datetime(2026, 8, 11, 23, 27, 5, tzinfo=UTC)


def source_row(record_id: str, *, batch_id: str = "batch-a") -> dict[str, object]:
    value: dict[str, object] = {name: f"{name}-value" for name in SOURCE_COLUMNS}
    value.update(
        {
            "record_id": record_id,
            "batch_id": batch_id,
            "list_order": 1,
            "source_file_sha256": "a" * 64,
            "ingested_at": datetime(2026, 8, 1, tzinfo=UTC),
            "updated_at": datetime(2026, 8, 2, tzinfo=UTC),
            "confirmation_status": "confirmed_external",
            "telegram_url": None,
            "vk_public_url": None,
            "vk_video_url": None,
            "rutube_url": None,
            "external_region_basis": None,
            "external_region_evidence_url": None,
            "submission_batch_ids_json": None,
            "other_primary_url": None,
            "social_links_type": None,
            "evidence_type": None,
        }
    )
    return value


def _write_json(path: Path, value: dict[str, object]) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
    path.chmod(0o600)


def protected_artifact(tmp_path: Path) -> Path:
    directory = tmp_path / "protected"
    directory.mkdir(mode=0o700, parents=True)
    data = directory / DATA_NAME
    with data.open("wb") as handle:
        receipt = scan_rows(
            [source_row("id-a"), source_row("id-b", batch_id="batch-b")], handle
        )
    data.chmod(0o600)
    scan = scan_evidence(
        receipt,
        path=data,
        started_at=SNAPSHOT,
        completed_at=SNAPSHOT + timedelta(seconds=1),
    )
    inventory = ObservedInventoryBinding(
        receipt_schema_version="inventory.v1",
        receipt_sha256="c" * 64,
        observed_at=SNAPSHOT - timedelta(minutes=1),
        row_count=2,
        distinct_record_ids=2,
        batch_count=2,
        source_file_count=1,
        confirmation_status_counts={"confirmed_external": 2},
    )
    principal = ReaderPrincipalBinding(
        service_account_id="ajeri3qs6jbijih0bs5d",
        access_bindings_observed_at=SNAPSHOT - timedelta(seconds=10),
        access_bindings_sha256="d" * 64,
        database_roles=("ydb.viewer",),
        write_denial_verified_at=SNAPSHOT - timedelta(seconds=5),
    )
    batch_id = batch_identity(SNAPSHOT, 2)
    manifest = ProtectedExportManifest(
        export_batch_id=batch_id,
        snapshot_at=SNAPSHOT,
        created_at=SNAPSHOT + timedelta(seconds=3),
        source=SourceBinding(columns=SOURCE_COLUMNS, source_revision=REVISION),
        inventory=inventory,
        principal=principal,
        primary_scan=scan,
        verification_scan=scan.model_copy(
            update={
                "started_at": SNAPSHOT + timedelta(seconds=1),
                "completed_at": SNAPSHOT + timedelta(seconds=2),
            }
        ),
        data_file={
            "row_count": 2,
            "byte_size": data.stat().st_size,
            "sha256": sha256_file(data),
        },
    )
    manifest_path = directory / MANIFEST_NAME
    _write_json(manifest_path, manifest.model_dump(mode="json"))
    detached = ProtectedExportReceipt(
        created_at=SNAPSHOT + timedelta(seconds=4),
        manifest_sha256=sha256_file(manifest_path),
        export_batch_id=batch_id,
        snapshot_at=SNAPSHOT,
        source_revision=REVISION,
        row_count=2,
        logical_sha256=scan.logical_sha256,
        record_id_set_sha256=scan.record_id_set_sha256,
        data_file_sha256=scan.file_sha256,
    )
    _write_json(directory / RECEIPT_NAME, detached.model_dump(mode="json"))
    return manifest_path


def test_load_and_stream_owner_only_artifact(tmp_path: Path) -> None:
    manifest_path = protected_artifact(tmp_path)
    artifact = load_protected_artifact(manifest_path)

    artifact.assert_import_binding(
        snapshot_at=SNAPSHOT, expected_row_count=2, source_revision=REVISION
    )
    assert [row["record_id"] for row in artifact.iter_rows()] == ["id-a", "id-b"]
    assert os.stat(artifact.directory).st_mode & 0o777 == 0o700
    assert os.stat(artifact.data_path).st_mode & 0o777 == 0o600


def test_loader_rejects_mode_and_byte_tamper(tmp_path: Path) -> None:
    manifest_path = protected_artifact(tmp_path)
    data = manifest_path.parent / DATA_NAME
    data.chmod(0o640)
    with pytest.raises(ProtectedArtifactError, match="mode must be 0600"):
        load_protected_artifact(manifest_path)

    data.chmod(0o600)
    data.write_bytes(data.read_bytes() + b"{}\n")
    data.chmod(0o600)
    with pytest.raises(ProtectedArtifactError, match="receipt or protected bytes"):
        load_protected_artifact(manifest_path)


def test_loader_rejects_detached_receipt_and_manifest_tamper(tmp_path: Path) -> None:
    manifest_path = protected_artifact(tmp_path)
    receipt_path = manifest_path.parent / RECEIPT_NAME
    receipt = json.loads(receipt_path.read_bytes())
    receipt["manifest_sha256"] = "0" * 64
    _write_json(receipt_path, receipt)
    with pytest.raises(ProtectedArtifactError, match="receipt or protected bytes"):
        load_protected_artifact(manifest_path)

    manifest_path = protected_artifact(tmp_path / "other")
    manifest = json.loads(manifest_path.read_bytes())
    manifest["source"]["source_revision"] = "f" * 40
    _write_json(manifest_path, manifest)
    with pytest.raises(ProtectedArtifactError, match="receipt or protected bytes"):
        load_protected_artifact(manifest_path)


def test_contract_rejects_zero_inventory_and_nonidentical_scans(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="greater than 0"):
        ObservedInventoryBinding(
            receipt_schema_version="inventory.v1",
            receipt_sha256="a" * 64,
            observed_at=SNAPSHOT,
            row_count=0,
            distinct_record_ids=0,
            batch_count=0,
            source_file_count=0,
            confirmation_status_counts={},
        )

    manifest_path = protected_artifact(tmp_path)
    raw = json.loads(manifest_path.read_bytes())
    raw["verification_scan"]["logical_sha256"] = "0" * 64
    with pytest.raises(ValidationError, match="independent ordered scans differ"):
        ProtectedExportManifest.model_validate(raw)


def test_import_binding_is_deterministic_and_fail_closed(tmp_path: Path) -> None:
    artifact = load_protected_artifact(protected_artifact(tmp_path))
    assert artifact.manifest.export_batch_id == batch_identity(SNAPSHOT, 2)
    with pytest.raises(ProtectedArtifactError, match="snapshot differs"):
        artifact.assert_import_binding(
            snapshot_at=SNAPSHOT + timedelta(seconds=1),
            expected_row_count=2,
            source_revision=REVISION,
        )
    with pytest.raises(ProtectedArtifactError, match="count differs"):
        artifact.assert_import_binding(
            snapshot_at=SNAPSHOT, expected_row_count=3, source_revision=REVISION
        )
    with pytest.raises(ProtectedArtifactError, match="revision differs"):
        artifact.assert_import_binding(
            snapshot_at=SNAPSHOT, expected_row_count=2, source_revision="e" * 40
        )


def test_canonical_row_tamper_is_rejected_even_with_resealed_file_hash(tmp_path: Path) -> None:
    manifest_path = protected_artifact(tmp_path)
    artifact = load_protected_artifact(manifest_path)
    data = artifact.data_path
    first = BloggerSourceRow.from_mapping(source_row("id-a")).payload()
    # Valid JSON but deliberately non-canonical whitespace/key ordering.
    lines = [json.dumps(first).encode() + b"\n", *data.read_bytes().splitlines(keepends=True)[1:]]
    data.write_bytes(b"".join(lines))
    data.chmod(0o600)
    with pytest.raises(ProtectedArtifactError, match=r"canonical|changed"):
        list(artifact.iter_rows())


def test_terminal_cleanup_removes_only_exact_bundle(tmp_path: Path) -> None:
    artifact = load_protected_artifact(protected_artifact(tmp_path))
    directory = artifact.directory
    artifact.destroy_source_bytes()
    assert not directory.exists()

    artifact = load_protected_artifact(protected_artifact(tmp_path / "extra"))
    undeclared = artifact.directory / "unexpected"
    undeclared.write_text("metadata")
    undeclared.chmod(0o600)
    with pytest.raises(ProtectedArtifactError, match="inexact"):
        artifact.destroy_source_bytes()
    assert artifact.data_path.exists()
