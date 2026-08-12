from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from io import BytesIO

import pytest

from my_data_hub.workloads.bloggers.protected_artifact import _set_sha256, scan_rows
from my_data_hub.workloads.bloggers.schema import SOURCE_COLUMNS


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


def test_scan_rows_writes_ordered_canonical_jsonl_and_accounts_identities() -> None:
    output = BytesIO()
    receipt = scan_rows([_row("id-a"), _row("id-b", batch_id="batch-b")], output)

    assert receipt.row_count == receipt.distinct_record_ids == 2
    assert receipt.batch_count == 2
    assert receipt.source_file_count == 1
    assert receipt.confirmation_status_counts == {"confirmed_external": 2}
    assert receipt.record_id_set_sha256 == _set_sha256(["id-a", "id-b"])
    logical = hashlib.sha256()
    for line in output.getvalue().splitlines():
        logical.update(len(line).to_bytes(8, "big"))
        logical.update(line)
    assert receipt.logical_sha256 == logical.hexdigest()


def test_scan_rows_rejects_reordered_or_duplicate_identity() -> None:
    with pytest.raises(ValueError, match="strictly ordered"):
        scan_rows([_row("id-b"), _row("id-a")])
    with pytest.raises(ValueError, match=r"strictly ordered|duplicate"):
        scan_rows([_row("id-a"), _row("id-a")])
