from __future__ import annotations

from dataclasses import dataclass

import pytest

from my_data_hub.workloads.region_talk import direct_pipeline


@dataclass
class _ResultSet:
    rows: list[dict[str, object]]


class _Results:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.value = [_ResultSet(rows)]

    def __enter__(self):  # type: ignore[no-untyped-def]
        return self.value

    def __exit__(self, *_args):  # type: ignore[no-untyped-def]
        return False


class _Transaction:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        # Snapshot isolation is represented by a copy at transaction start.
        self.rows = [dict(row) for row in rows]

    def execute(self, query, *, parameters=None):  # type: ignore[no-untyped-def]
        if "IS NULL" in query:
            return _Results([{"null_pk_count": sum(row.get("pk") is None for row in self.rows)}])
        assert parameters is not None
        after = parameters["$after"]
        limit = parameters["$limit"]
        page = [row for row in self.rows if str(row["pk"]) > after][:limit]
        return _Results(page)


class _Pool:
    def __init__(self, source: list[dict[str, object]], calls: list[int]) -> None:
        self.source = source
        self.calls = calls

    def __enter__(self):
        return self

    def __exit__(self, *_args):  # type: ignore[no-untyped-def]
        return False

    def retry_tx_sync(self, callback, *, tx_mode):  # type: ignore[no-untyped-def]
        assert tx_mode == "snapshot-read-only"
        self.calls.append(1)
        return callback(_Transaction(self.source))


def test_ydb_reader_keeps_cross_page_rows_in_one_snapshot(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    source = [
        {"pk": "a", "kind": "source_queue_item", "payload_json": "{}"},
        {"pk": "b", "kind": "source_queue_item", "payload_json": "{}"},
        {"pk": "c", "kind": "source_queue_item", "payload_json": "{}"},
    ]
    transactions: list[int] = []
    monkeypatch.setattr(
        direct_pipeline,
        "_query_pool",
        lambda _driver: _Pool(source, transactions),
    )
    monkeypatch.setattr(direct_pipeline, "_snapshot_mode", lambda: "snapshot-read-only")
    reader = direct_pipeline.YdbDirectReader(object())

    first = reader.scan_page(
        "region_talk_compact_state_kv",
        primary_key="pk",
        after_primary_key=None,
        limit=2,
    )
    # A live-source mutation between consumer pages must not appear in pass A.
    source.insert(
        2,
        {"pk": "bb", "kind": "publication_candidate_item", "payload_json": "{}"}
    )
    second = reader.scan_page(
        "region_talk_compact_state_kv",
        primary_key="pk",
        after_primary_key="b",
        limit=2,
    )
    exhausted = reader.scan_page(
        "region_talk_compact_state_kv",
        primary_key="pk",
        after_primary_key="c",
        limit=2,
    )

    assert [row["pk"] for row in first] == ["a", "b"]
    assert [row["pk"] for row in second] == ["c"]
    assert exhausted == ()
    assert len(transactions) == 1

    # Pass B begins a fresh independent SnapshotReadOnly transaction and sees
    # the change, allowing DirectSnapshotRunner to reject the changed source.
    refreshed = reader.scan_page(
        "region_talk_compact_state_kv",
        primary_key="pk",
        after_primary_key=None,
        limit=10,
    )
    assert [row["pk"] for row in refreshed] == ["a", "b", "bb", "c"]
    assert len(transactions) == 2


def test_ydb_reader_rejects_null_primary_key(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    source = [{"pk": None, "kind": "source_queue_item", "payload_json": "{}"}]
    monkeypatch.setattr(
        direct_pipeline, "_query_pool", lambda _driver: _Pool(source, [])
    )
    monkeypatch.setattr(direct_pipeline, "_snapshot_mode", lambda: "snapshot-read-only")
    with pytest.raises(
        direct_pipeline.DirectPipelineConfigurationError, match="NULL primary key"
    ):
        direct_pipeline.YdbDirectReader(object()).scan_page(
            "region_talk_compact_state_kv",
            primary_key="pk",
            after_primary_key=None,
            limit=2,
        )
