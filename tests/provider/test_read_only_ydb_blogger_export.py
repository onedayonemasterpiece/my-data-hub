from __future__ import annotations

import json
import runpy
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from jsonschema import Draft202012Validator
from pydantic import ValidationError

from my_data_hub.workloads.bloggers.importer import batch_identity
from my_data_hub.workloads.bloggers.master_stage import BloggerMigrationRequest
from my_data_hub.workloads.bloggers.schema import SOURCE_COLUMNS
from my_data_hub.workloads.bloggers.ydb_reader import (
    BloggerYdbSourceReadReceipt,
    YdbSnapshotError,
    _set_sha256,
    scan_ydb_rows,
)


def _row(record_id: str, *, batch_id: str = "batch-a") -> dict[str, object]:
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


def test_scan_rows_accounts_identities_without_materializing_source_bytes() -> None:
    started = datetime(2026, 8, 3, tzinfo=UTC)
    receipt = scan_ydb_rows(
        [_row("id-a"), _row("id-b", batch_id="batch-b")],
        started_at=started,
        completed_at=started,
    )

    assert receipt.row_count == receipt.distinct_record_ids == 2
    assert receipt.batch_count == 2
    assert receipt.source_file_count == 1
    assert receipt.confirmation_status_counts == {"confirmed_external": 2}
    assert receipt.record_id_set_sha256 == _set_sha256(["id-a", "id-b"])
    assert len(receipt.logical_sha256) == 64


def test_scan_rows_rejects_reordered_or_duplicate_identity() -> None:
    with pytest.raises(YdbSnapshotError, match="strictly ordered"):
        scan_ydb_rows([_row("id-b"), _row("id-a")], started_at=datetime.now(UTC))
    with pytest.raises(YdbSnapshotError, match=r"strictly ordered|duplicate"):
        scan_ydb_rows([_row("id-a"), _row("id-a")], started_at=datetime.now(UTC))


def _source_receipt() -> BloggerYdbSourceReadReceipt:
    started = datetime(2026, 8, 3, tzinfo=UTC)
    rows = [_row("id-a"), _row("id-b", batch_id="batch-b")]
    first = scan_ydb_rows(rows, started_at=started, completed_at=started + timedelta(seconds=1))
    repeat = scan_ydb_rows(
        rows,
        started_at=started + timedelta(seconds=2),
        completed_at=started + timedelta(seconds=3),
    )
    return BloggerYdbSourceReadReceipt(
        export_batch_id=batch_identity(started, 2),
        snapshot_at=started,
        source_revision="b" * 40,
        reader_service_account_id="a" * 20,
        database_roles=("ydb.viewer",),
        access_bindings_sha256="c" * 64,
        first_scan=first,
        repeat_scan=repeat,
    )


def test_detached_receipt_is_dynamic_metadata_only_and_rejects_scan_drift() -> None:
    receipt = _source_receipt()
    encoded = json.dumps(receipt.model_dump(mode="json"), sort_keys=True)

    assert receipt.row_count == 2
    assert "blogger_name-value" not in encoded
    assert "evidence_url-value" not in encoded
    assert "token" not in encoded.lower()
    with pytest.raises(ValidationError, match="independent ordered YDB scans differ"):
        BloggerYdbSourceReadReceipt.model_validate(
            {
                **receipt.model_dump(mode="json"),
                "repeat_scan": {
                    **receipt.repeat_scan.model_dump(mode="json"),
                    "logical_sha256": "d" * 64,
                },
            }
        )


def test_provider_writer_creates_only_one_private_metadata_receipt(tmp_path: Path) -> None:
    module = runpy.run_path("scripts/provider/read_only_ydb_blogger_export.py")
    destination = tmp_path / "receipt.json"
    module["_write_receipt"](destination, _source_receipt())

    assert [item.name for item in tmp_path.iterdir()] == ["receipt.json"]
    assert destination.stat().st_mode & 0o777 == 0o600
    parsed = BloggerYdbSourceReadReceipt.model_validate_json(destination.read_bytes())
    assert parsed.row_count == 2
    source = Path("scripts/provider/read_only_ydb_blogger_export.py").read_text()
    for forbidden in ("rows.jsonl", "--output-root", "protected_artifact"):
        assert forbidden not in source


def test_migration_request_binds_dynamic_count_revision_and_snapshot_to_receipt() -> None:
    receipt = _source_receipt()
    request = BloggerMigrationRequest(
        request_id=uuid4(),
        operation_id=uuid4(),
        project_id=uuid4(),
        snapshot_at=receipt.snapshot_at,
        expected_rows=2,
        source_revision=receipt.source_revision,
        source_read_receipt=receipt,
    )
    assert request.expected_rows == 2
    for changed in (
        {"expected_rows": 3},
        {"source_revision": "e" * 40},
        {"snapshot_at": receipt.snapshot_at + timedelta(seconds=1)},
    ):
        with pytest.raises(ValidationError, match="detached YDB read receipt differs"):
            BloggerMigrationRequest.model_validate({**request.model_dump(mode="json"), **changed})


def test_nonhistorical_source_receipt_example_validates_as_dynamic_contract() -> None:
    schema = json.loads(Path("schemas/region-talk-ydb-source-read-receipt.v1.schema.json").read_text())
    example = json.loads(Path("examples/bloggers/region-talk-ydb-source-read-receipt.v1.example.json").read_text())
    Draft202012Validator(schema).validate(example)
    assert BloggerYdbSourceReadReceipt.model_validate(example).row_count == 2
