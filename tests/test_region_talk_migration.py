from __future__ import annotations

import json
from pathlib import Path
from uuid import UUID

import pytest
from pydantic import ValidationError

from my_data_hub.hashing import sha256_value
from my_data_hub.workloads.region_talk.contracts import MigrationReconciliationReport
from my_data_hub.workloads.region_talk.migration import (
    RegionTalkMigrationError,
    build_reconciliation_accounting,
    load_manifest,
    reconciliation_blocking_findings,
    raw_record_id,
    validate_export,
)
from my_data_hub.workloads.region_talk.ydb_export import (
    LegacyYdbRecord,
    YdbExportError,
    export_records,
)


def fixture_records() -> list[LegacyYdbRecord]:
    return [
        LegacyYdbRecord(
            source_table="region_talk_state",
            source_pk="candidate_memory_item:001",
            payload={"url": "https://example.test/1", "score": 0.9},
        ),
        LegacyYdbRecord(
            source_table="region_talk_state",
            source_pk="post_link_queue_item:002",
            payload={"url": "https://example.test/2"},
        ),
        LegacyYdbRecord(
            source_table="region_talk_state",
            source_pk="source_queue_item:003",
            payload={"source": "example"},
        ),
    ]


def test_export_validate_and_identity_are_deterministic(tmp_path: Path) -> None:
    batch_id = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
    bundle = export_records(
        fixture_records(),
        output_root=tmp_path,
        database="/ru-central1/test/db",
        table="region_talk_state",
        export_batch_id=batch_id,
    )
    validated = validate_export(bundle.manifest_path)
    assert validated.row_count == 3
    assert validated.unknown_row_kinds == ()
    assert validated.logical_sha256 == bundle.logical_sha256
    first_line = validated.files[0].read_text(encoding="utf-8").splitlines()[0]
    from my_data_hub.workloads.region_talk.contracts import YdbExportRow

    row = YdbExportRow.model_validate_json(first_line)
    assert raw_record_id(row) == raw_record_id(row)


def test_tampered_export_is_rejected(tmp_path: Path) -> None:
    bundle = export_records(
        fixture_records(),
        output_root=tmp_path,
        database="db",
        table="region_talk_state",
    )
    data = next(bundle.directory.glob("*.jsonl"))
    data.write_text(data.read_text(encoding="utf-8") + "{}\n", encoding="utf-8")
    with pytest.raises(RegionTalkMigrationError, match="byte-size mismatch|SHA-256 mismatch"):
        validate_export(bundle.manifest_path)


def test_out_of_order_source_rows_fail_and_leave_no_bundle(tmp_path: Path) -> None:
    records = list(reversed(fixture_records()))
    with pytest.raises(YdbExportError, match="strictly ordered"):
        export_records(
            records,
            output_root=tmp_path,
            database="db",
            table="region_talk_state",
        )
    assert list(tmp_path.iterdir()) == []


def test_manifest_path_traversal_is_rejected(tmp_path: Path) -> None:
    bundle = export_records(
        fixture_records(),
        output_root=tmp_path,
        database="db",
        table="region_talk_state",
    )
    raw = json.loads(bundle.manifest_path.read_text(encoding="utf-8"))
    raw["files"][0]["path"] = "../outside.jsonl"
    bundle.manifest_path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(RegionTalkMigrationError, match="invalid export manifest"):
        load_manifest(bundle.manifest_path)


def test_unknown_kind_is_retained_and_reported(tmp_path: Path) -> None:
    records = [
        LegacyYdbRecord(
            source_table="region_talk_state",
            source_pk="unknown_future_kind:001",
            payload={"raw": True},
        )
    ]
    bundle = export_records(
        records,
        output_root=tmp_path,
        database="db",
        table="region_talk_state",
    )
    validated = validate_export(bundle.manifest_path)
    assert validated.unknown_row_kinds == ("unknown_future_kind",)


def test_reconciliation_accounting_blocks_undispositioned_rows() -> None:
    report = build_reconciliation_accounting(
        expected_by_kind={"source_queue_item": 3},
        actual_rows=[
            ("source_queue_item", "normalized"),
            ("source_queue_item", "deduplicated"),
            ("source_queue_item", None),
        ],
    )[0]
    assert report["raw_matches_expected"] is True
    assert report["fully_accounted"] is False
    assert report["cutover_ready"] is False
    assert report["undispositioned"] == 1
    findings = reconciliation_blocking_findings([report])
    assert findings[0]["reasons"] == ["undispositioned_rows"]


def test_reconciliation_quarantine_is_accounted_but_blocks_cutover() -> None:
    report = build_reconciliation_accounting(
        expected_by_kind={"candidate_memory_item": 2},
        actual_rows=[
            ("candidate_memory_item", "normalized"),
            ("candidate_memory_item", "quarantined"),
        ],
    )[0]
    assert report["raw_matches_expected"] is True
    assert report["fully_accounted"] is True
    assert report["quarantined"] == 1
    assert report["cutover_ready"] is False
    findings = reconciliation_blocking_findings([report])
    assert findings[0]["reasons"] == ["quarantined_rows"]


def test_reconciliation_preserves_expected_kind_with_zero_raw_rows() -> None:
    report = build_reconciliation_accounting(
        expected_by_kind={"post_link_queue_item": 1},
        actual_rows=[],
    )[0]
    assert report["row_kind"] == "post_link_queue_item"
    assert report["expected"] == 1
    assert report["raw"] == 0
    assert report["raw_matches_expected"] is False
    assert report["fully_accounted"] is False
    assert report["cutover_ready"] is False
    findings = reconciliation_blocking_findings([report])
    assert findings[0]["reasons"] == ["raw_count_mismatch"]


def test_reconciliation_rejects_unknown_disposition() -> None:
    with pytest.raises(RegionTalkMigrationError, match="unknown migration disposition"):
        build_reconciliation_accounting(
            expected_by_kind={"source_queue_item": 1},
            actual_rows=[("source_queue_item", "silently_dropped")],
        )


def test_reconciliation_report_model_cannot_hide_quarantine() -> None:
    with pytest.raises(ValidationError, match="blocking findings"):
        MigrationReconciliationReport.model_validate(
            {
                "schema_version": "migration-reconciliation-report.v1",
                "workload": "region-talk",
                "export_batch_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                "source": {"database": "db", "tables": ["region_talk_state"]},
                "batch_status": "reconciled",
                "expected_row_count": 1,
                "manifest_sha256": "a" * 64,
                "logical_sha256": "b" * 64,
                "completed_at": "2026-08-09T18:00:00Z",
                "accounting": [
                    {
                        "row_kind": "candidate_memory_item",
                        "expected": 1,
                        "raw": 1,
                        "normalized": 0,
                        "deduplicated": 0,
                        "intentionally_excluded": 0,
                        "retained_raw": 0,
                        "quarantined": 1,
                        "undispositioned": 0,
                        "raw_matches_expected": True,
                        "fully_accounted": True,
                        "cutover_ready": False,
                    }
                ],
                "blocking_findings": [],
                "passed": True,
            }
        )


def test_reconciliation_contract_example_is_runtime_valid() -> None:
    root = Path(__file__).resolve().parents[1]
    raw = json.loads(
        (root / "examples/contracts/migration-reconciliation-report.v1.example.json").read_text(
            encoding="utf-8"
        )
    )
    report = MigrationReconciliationReport.model_validate(raw)
    assert report.passed is True
    assert report.accounting[0].cutover_ready is True


def test_reconciliation_contract_rejects_contradictory_pass_flag() -> None:
    root = Path(__file__).resolve().parents[1]
    raw = json.loads(
        (root / "examples/contracts/migration-reconciliation-report.v1.example.json").read_text(
            encoding="utf-8"
        )
    )
    raw["passed"] = False
    with pytest.raises(ValueError, match="passed contradicts blocking findings"):
        MigrationReconciliationReport.model_validate(raw)
