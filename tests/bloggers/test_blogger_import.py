from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

import pytest

from my_data_hub.workloads.bloggers.accounting import BloggerExportAccumulator
from my_data_hub.workloads.bloggers.schema import (
    SOURCE_COLUMNS,
    SOURCE_QUERY,
    SOURCE_QUERY_SHA256,
    BloggerSourceError,
    BloggerSourceRow,
    assert_query_identity,
    source_query_sha256,
)
from my_data_hub.workloads.bloggers.transform import BloggerDisposition, transform_row


def source_row(**changes: object) -> dict[str, object]:
    value: dict[str, object] = {
        "record_id": "record-001",
        "batch_id": "batch-001",
        "list_order": 1,
        "level": "regional",
        "blogger_name": "Тестовый автор",
        "segment": "культура",
        "region_relation_status": "external",
        "visit_period_text": "2025",
        "locations_text": "Россия",
        "confirmation_basis": "public profile",
        "evidence_url": "https://example.test/evidence/1",
        "telegram_url": "http://t.me/TestAuthor/",
        "vk_public_url": None,
        "vk_video_url": None,
        "rutube_url": "https://rutube.ru/channel/123/",
        "source_kind": "manual_external_confirmation",
        "confirmation_status": "confirmed_external",
        "pipeline_status": "stored_only",
        "source_file_sha256": "a" * 64,
        "ingested_at": "2026-08-03T13:30:00Z",
        "updated_at": "2026-08-03T13:31:00Z",
        "external_region_basis": None,
        "external_region_evidence_url": None,
        "submission_batch_ids_json": None,
        "other_primary_url": None,
        "social_links_type": None,
        "evidence_type": None,
    }
    value.update(changes)
    return value


def test_exact_27_column_query_and_executed_byte_hash_are_fixed() -> None:
    assert_query_identity()
    assert len(SOURCE_COLUMNS) == 27
    assert SOURCE_QUERY.endswith("ORDER BY `record_id`;")
    assert "LIMIT" not in SOURCE_QUERY and "SELECT *" not in SOURCE_QUERY
    assert SOURCE_QUERY_SHA256 == "ef94cf114fea0e2f89418c5dacbc289e5c2d21f6935883c2b685ec4f64bd0e50"
    assert source_query_sha256(SOURCE_QUERY) == SOURCE_QUERY_SHA256


def test_query_mutation_cannot_retain_the_claimed_executed_byte_hash() -> None:
    mutated = SOURCE_QUERY + " "
    assert source_query_sha256(mutated) != SOURCE_QUERY_SHA256
    with pytest.raises(AssertionError, match="executed query bytes"):
        assert_query_identity(mutated, SOURCE_QUERY_SHA256)


def test_unknown_or_missing_source_field_is_not_silently_discarded() -> None:
    extra = source_row(extra_secret="must-not-pass")
    with pytest.raises(BloggerSourceError, match="unknown"):
        BloggerSourceRow.from_mapping(extra)
    missing = source_row()
    missing.pop("segment")
    with pytest.raises(BloggerSourceError, match="missing"):
        BloggerSourceRow.from_mapping(missing)


def test_transform_is_deterministic_preserves_unknown_actor_kind_and_normalizes_accounts() -> None:
    row = BloggerSourceRow.from_mapping(source_row())
    first = transform_row(row)
    second = transform_row(row)
    assert first == second
    assert first.actor_kind == "unknown"
    assert first.disposition is BloggerDisposition.NORMALIZED
    assert [account.platform for account in first.accounts] == ["rutube", "telegram"]
    assert first.accounts[1].normalized_url == "https://t.me/TestAuthor"
    assert first.actor_id == UUID("ece4157f-be6f-5c49-9418-d96f4ac668ab")


def test_malformed_url_is_retained_raw_not_dropped_or_misclassified() -> None:
    row = BloggerSourceRow.from_mapping(source_row(telegram_url="javascript:alert(1)"))
    projection = transform_row(row)
    assert projection.disposition is BloggerDisposition.RETAINED_RAW
    assert projection.reason_code == "malformed_public_account_url"
    assert projection.actor_kind == "unknown"


def test_streaming_accounting_exact_replay_and_missing_row_gate() -> None:
    now = datetime(2026, 8, 11, tzinfo=UTC)
    accumulator = BloggerExportAccumulator(UUID("11111111-1111-4111-8111-111111111111"), now)
    row = BloggerSourceRow.from_mapping(source_row())
    accumulator.add(row, transform_row(row))
    receipt = accumulator.finish(expected_row_count=1)
    assert receipt.complete
    assert receipt.row_count == receipt.distinct_record_ids == 1
    assert receipt.undispositioned == 0
    assert receipt.dispositions["quarantined"] == 0

    duplicate = BloggerExportAccumulator(UUID("22222222-2222-4222-8222-222222222222"), now)
    duplicate.add(row, transform_row(row))
    with pytest.raises(ValueError, match="duplicate"):
        duplicate.add(row, transform_row(row))

    missing = BloggerExportAccumulator(UUID("33333333-3333-4333-8333-333333333333"), now)
    missing.add(row, transform_row(row))
    with pytest.raises(ValueError, match="row count mismatch"):
        missing.finish(expected_row_count=2)
