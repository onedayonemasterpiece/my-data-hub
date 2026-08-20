from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import pytest

from my_data_hub.workloads.region_talk.constants import DIRECT_SOURCE_TABLES
from my_data_hub.workloads.region_talk.direct_snapshot import (
    DirectSnapshotError,
    DirectSnapshotRunner,
    source_row,
)


class MemoryReader:
    def __init__(self, rows: Mapping[str, list[dict[str, Any]]]) -> None:
        self.rows = rows
        self.scans: defaultdict[str, int] = defaultdict(int)

    def scan_page(
        self,
        source_table: str,
        *,
        primary_key: str,
        after_primary_key: str | None,
        limit: int,
    ) -> Sequence[Mapping[str, Any]]:
        if after_primary_key is None:
            self.scans[source_table] += 1
        values = self.rows[source_table]
        return [
            row
            for row in values
            if after_primary_key is None or str(row[primary_key]) > after_primary_key
        ][:limit]


class FakeCursor:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection
        self.value: tuple[Any, ...] | None = None

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, statement: str, params: tuple[Any, ...] = ()) -> FakeCursor:
        self.connection.calls.append((statement, params))
        if "begin_region_talk_direct_snapshot" in statement:
            self.value = (self.connection.batch_id,)
        elif "land_region_talk_direct_page" in statement:
            self.value = ({"duplicate": False, "row_count": 1},)
        elif "finalize_region_talk_direct_snapshot" in statement:
            self.value = (
                {
                    "schema_version": "region-talk-direct-snapshot-receipt.v2",
                    "export_batch_id": str(self.connection.batch_id),
                    "task_run_id": str(self.connection.task_id),
                    "status": "complete",
                    "expected_row_count": self.connection.expected,
                    "landed_row_count": self.connection.expected,
                    "dispositioned_row_count": self.connection.expected,
                    "quarantined_row_count": 0,
                    "logical_sha256": self.connection.logical_sha256,
                    "publication_effects_enabled": False,
                    "completed_at": "2026-08-19T22:00:00Z",
                },
            )
        else:
            self.value = (None,)
        return self

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.value


class FakeConnection:
    def __init__(self, batch_id: UUID, task_id: UUID) -> None:
        self.batch_id = batch_id
        self.task_id = task_id
        self.expected = 0
        self.logical_sha256 = "0" * 64
        self.calls: list[tuple[str, tuple[Any, ...]]] = []
        self.commits = 0
        self.rollbacks = 0

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


def fixture_rows() -> dict[str, list[dict[str, Any]]]:
    return {
        "acq_discovery_opportunities": [
            {"dedupe_key": "opp-1", "platform": "web", "payload_json": "{\"status\":\"new\"}"}
        ],
        "acq_discovery_runs": [{"run_uid": "run-1", "stats_json": "{}"}],
        "acq_discovery_surfaces": [
            {"external_id": "surface-1", "platform": "telegram", "status": "active"}
        ],
        "region_talk_compact_state_kv": [
            {
                "pk": "another-unrelated-key",
                "kind": "source_queue_item",
                "payload_json": {"status": "pending"},
            },
            {
                "pk": "does-not-contain-the-kind",
                "kind": "external_publication_intake_item",
                "payload_json": {"title": "Article", "url": "https://example.test/article"},
            },
        ],
        "region_talk_external_blogger_evidence": [
            {"record_id": "blogger-1", "blogger_name": "One", "updated_at": "2026-08-19T20:00:00Z"}
        ],
    }


def test_compact_kind_comes_from_explicit_column_not_pk_prefix() -> None:
    spec = next(item for item in DIRECT_SOURCE_TABLES if item.name == "region_talk_compact_state_kv")
    row = source_row(spec, fixture_rows()[spec.name][1])
    assert row.row_kind == "external_publication_intake_item"
    assert not row.source_pk.startswith(row.row_kind)


def test_inventory_is_dynamic_exact_five_table_and_row_free() -> None:
    batch_id = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
    task_id = UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
    reader = MemoryReader(fixture_rows())
    connection = FakeConnection(batch_id, task_id)
    runner = DirectSnapshotRunner(reader, connection, page_size=1)
    manifest = runner.inventory(
        export_batch_id=batch_id,
        task_run_id=task_id,
        master_instance_id=UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc"),
        master_epoch=47,
        source_database="/ru-central1/example/database",
        request_sha256="1" * 64,
        created_at=datetime(2026, 8, 19, 22, tzinfo=UTC),
    )
    assert [item.source_table for item in manifest.tables] == [
        item.name for item in DIRECT_SOURCE_TABLES
    ]
    assert manifest.expected_row_count == sum(len(rows) for rows in fixture_rows().values())
    assert manifest.row_kind_counts["external_publication_intake_item"] == 1
    assert manifest.publication_effects_enabled is False
    assert connection.calls == []


def test_run_lands_only_bounded_pages_and_returns_typed_receipt() -> None:
    batch_id = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
    task_id = UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
    reader = MemoryReader(fixture_rows())
    connection = FakeConnection(batch_id, task_id)
    runner = DirectSnapshotRunner(reader, connection, page_size=1)
    manifest = runner.inventory(
        export_batch_id=batch_id,
        task_run_id=task_id,
        master_instance_id=UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc"),
        master_epoch=47,
        source_database="db",
        request_sha256="2" * 64,
        created_at=datetime(2026, 8, 19, 22, tzinfo=UTC),
    )
    connection.expected = manifest.expected_row_count
    connection.logical_sha256 = manifest.logical_sha256
    receipt = runner.run(manifest)
    page_calls = [call for call in connection.calls if "land_region_talk_direct_page" in call[0]]
    assert len(page_calls) == manifest.expected_row_count
    assert all('"rows":[' in call[1][2] for call in page_calls)
    assert receipt.status == "complete"
    assert receipt.landed_row_count == manifest.expected_row_count
    assert connection.rollbacks == 0


def test_transport_refreshes_between_pages_and_resumes_exact_cursor() -> None:
    batch_id = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
    task_id = UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
    reader = MemoryReader(fixture_rows())
    first = FakeConnection(batch_id, task_id)
    second = FakeConnection(batch_id, task_id)
    refresh_points: list[tuple[str, str, str | None, int]] = []

    def refresh(connection, *, phase, source_table, after_primary_key, page_number):  # type: ignore[no-untyped-def]
        refresh_points.append((phase, source_table, after_primary_key, page_number))
        if (
            phase == "pass_b"
            and source_table == DIRECT_SOURCE_TABLES[0].name
            and page_number == 2
        ):
            return second
        return connection

    runner = DirectSnapshotRunner(
        reader,
        first,
        page_size=1,
        transport_refresh=refresh,
    )
    manifest = runner.inventory(
        export_batch_id=batch_id,
        task_run_id=task_id,
        master_instance_id=UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc"),
        master_epoch=47,
        source_database="db",
        request_sha256="4" * 64,
        created_at=datetime(2026, 8, 19, 22, tzinfo=UTC),
    )
    first.expected = second.expected = manifest.expected_row_count
    first.logical_sha256 = second.logical_sha256 = manifest.logical_sha256
    receipt = runner.run(manifest)

    assert receipt.status == "complete"
    assert ("pass_b", DIRECT_SOURCE_TABLES[0].name, "opp-1", 2) in refresh_points
    # The page after the rotation resumes strictly after the already-landed PK;
    # begin is on the original connection and finalization on the replacement.
    assert any("begin_region_talk_direct_snapshot" in sql for sql, _ in first.calls)
    assert any("finalize_region_talk_direct_snapshot" in sql for sql, _ in second.calls)


def test_second_pass_drift_fails_closed_and_records_only_error_class() -> None:
    batch_id = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
    task_id = UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
    reader = MemoryReader(fixture_rows())
    connection = FakeConnection(batch_id, task_id)
    runner = DirectSnapshotRunner(reader, connection, page_size=10)
    manifest = runner.inventory(
        export_batch_id=batch_id,
        task_run_id=task_id,
        master_instance_id=UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc"),
        master_epoch=47,
        source_database="db",
        request_sha256="3" * 64,
        created_at=datetime(2026, 8, 19, 22, tzinfo=UTC),
    )
    reader.rows["region_talk_compact_state_kv"][0]["payload_json"] = {"title": "changed"}
    with pytest.raises(DirectSnapshotError, match="source changed between passes"):
        runner.run(manifest)
    fail_call = next(call for call in connection.calls if "fail_region_talk_direct_snapshot" in call[0])
    assert fail_call[1][2] == "DirectSnapshotError"
    assert "changed" not in repr(fail_call)
