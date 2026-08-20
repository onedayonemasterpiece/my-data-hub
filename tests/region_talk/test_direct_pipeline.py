from __future__ import annotations

import re
from dataclasses import dataclass
from types import SimpleNamespace
from uuid import uuid4

import pytest

from my_data_hub.workloads.region_talk import direct_pipeline
from my_data_hub.workloads.region_talk.pipeline_runtime import (
    RegionTalkCycleDisposition,
    RegionTalkCycleRequest,
)
from my_data_hub.workloads.region_talk.stage_execution import StageRunStatus


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


class _DatabaseTransaction:
    def __init__(self, tables: dict[str, list[dict[str, object]]]) -> None:
        self.tables = {
            name: [dict(row) for row in rows] for name, rows in tables.items()
        }

    def execute(self, query, *, parameters=None):  # type: ignore[no-untyped-def]
        table = re.search(r"FROM `([^`]+)`", query).group(1)
        rows = self.tables[table]
        primary_key = re.search(r"WHERE `([^`]+)`", query).group(1)
        if "IS NULL" in query:
            return _Results(
                [{"null_pk_count": sum(row.get(primary_key) is None for row in rows)}]
            )
        assert parameters is not None
        after = parameters["$after"]
        limit = parameters["$limit"]
        return _Results(
            [row for row in rows if str(row[primary_key]) > after][:limit]
        )


class _DatabasePool:
    def __init__(self, tables: dict[str, list[dict[str, object]]], calls: list[int]) -> None:
        self.tables = tables
        self.calls = calls

    def __enter__(self):
        return self

    def __exit__(self, *_args):  # type: ignore[no-untyped-def]
        return False

    def retry_tx_sync(self, callback, *, tx_mode):  # type: ignore[no-untyped-def]
        assert tx_mode == "snapshot-read-only"
        self.calls.append(1)
        return callback(_DatabaseTransaction(self.tables))


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

    def pass_a():  # type: ignore[no-untyped-def]
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
        return first, second, exhausted

    first, second, exhausted = reader.run_snapshot_pass("pass_a", pass_a)

    assert [row["pk"] for row in first] == ["a", "b"]
    assert [row["pk"] for row in second] == ["c"]
    assert exhausted == ()
    assert len(transactions) == 1

    # Pass B begins a fresh independent SnapshotReadOnly transaction and sees
    # the change, allowing DirectSnapshotRunner to reject the changed source.
    refreshed = reader.run_snapshot_pass(
        "pass_b",
        lambda: (
            tuple(
                reader.scan_page(
                    "region_talk_compact_state_kv",
                    primary_key="pk",
                    after_primary_key=None,
                    limit=10,
                )
            ),
            tuple(
                reader.scan_page(
                    "region_talk_compact_state_kv",
                    primary_key="pk",
                    after_primary_key="c",
                    limit=10,
                )
            ),
        )[0],
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
        reader = direct_pipeline.YdbDirectReader(object())
        reader.run_snapshot_pass(
            "pass_a",
            lambda: reader.scan_page(
                "region_talk_compact_state_kv",
                primary_key="pk",
                after_primary_key=None,
                limit=2,
            ),
        )


def test_ydb_reader_uses_one_database_snapshot_across_tables_per_pass(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tables = {
        "acq_discovery_opportunities": [{"dedupe_key": "opp-1"}],
        "acq_discovery_runs": [{"run_uid": "run-1"}],
    }
    transactions: list[int] = []
    monkeypatch.setattr(
        direct_pipeline,
        "_query_pool",
        lambda _driver: _DatabasePool(tables, transactions),
    )
    monkeypatch.setattr(direct_pipeline, "_snapshot_mode", lambda: "snapshot-read-only")
    reader = direct_pipeline.YdbDirectReader(object())

    def pass_a() -> list[str]:
        opportunities = reader.scan_page(
            "acq_discovery_opportunities",
            primary_key="dedupe_key",
            after_primary_key=None,
            limit=10,
        )
        reader.scan_page(
            "acq_discovery_opportunities",
            primary_key="dedupe_key",
            after_primary_key="opp-1",
            limit=10,
        )
        tables["acq_discovery_runs"].append({"run_uid": "run-2"})
        runs = reader.scan_page(
            "acq_discovery_runs",
            primary_key="run_uid",
            after_primary_key=None,
            limit=10,
        )
        reader.scan_page(
            "acq_discovery_runs",
            primary_key="run_uid",
            after_primary_key="run-1",
            limit=10,
        )
        assert [row["dedupe_key"] for row in opportunities] == ["opp-1"]
        return [str(row["run_uid"]) for row in runs]

    assert reader.run_snapshot_pass("pass_a", pass_a) == ["run-1"]

    def pass_b() -> list[str]:
        runs = reader.scan_page(
            "acq_discovery_runs",
            primary_key="run_uid",
            after_primary_key=None,
            limit=10,
        )
        reader.scan_page(
            "acq_discovery_runs",
            primary_key="run_uid",
            after_primary_key="run-2",
            limit=10,
        )
        return [str(row["run_uid"]) for row in runs]

    assert reader.run_snapshot_pass("pass_b", pass_b) == ["run-1", "run-2"]
    assert len(transactions) == 2


def test_cycle_requires_typed_post_import_receipt_before_success(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    task_run_id = uuid4()
    master_instance_id = uuid4()
    export_batch_id = uuid4()
    connection = object()
    refreshed_connection = object()
    calls: list[object] = []

    class Runner:
        def __init__(self, _reader, supplied_connection, **_kwargs):  # type: ignore[no-untyped-def]
            assert supplied_connection is connection
            self.connection = supplied_connection

        def inventory(self, **_kwargs):  # type: ignore[no-untyped-def]
            calls.append("inventory")
            return object()

        def run(self, _manifest):  # type: ignore[no-untyped-def]
            calls.append("snapshot")
            return SimpleNamespace(
                export_batch_id=export_batch_id,
                expected_row_count=58_554,
                dispositioned_row_count=58_554,
                model_dump=lambda **_kwargs: {
                    "export_batch_id": str(export_batch_id),
                    "expected_row_count": 58_554,
                },
            )

    class Function:
        def __init__(self, supplied_connection):  # type: ignore[no-untyped-def]
            assert supplied_connection is refreshed_connection

    class Supervisor:
        calls = 0

        def __init__(self, _function):  # type: ignore[no-untyped-def]
            pass

        def execute_after_import(self, **identity):  # type: ignore[no-untyped-def]
            type(self).calls += 1
            calls.append(("stages", identity))
            return SimpleNamespace(
                status=(
                    StageRunStatus.WAITING_WORK
                    if type(self).calls == 1
                    else StageRunStatus.COMPLETE
                ),
                receipt_sha256="a" * 64,
                queue_revision=17,
                rows_changed=9,
            )

    monkeypatch.setattr(direct_pipeline, "DirectSnapshotRunner", Runner)
    monkeypatch.setattr(direct_pipeline, "PostgresPostImportStageFunction", Function)
    monkeypatch.setattr(direct_pipeline, "RegionTalkPostImportSupervisor", Supervisor)
    executor = direct_pipeline.DirectRegionTalkCycleExecutor(
        connection=connection,
        reader=object(),
        source_database="/ru-central1/example",
        task_run_id=task_run_id,
        master_instance_id=master_instance_id,
        epoch=47,
        source_revision="snapshot-1",
    )

    def refresh(supplied_connection, **position):  # type: ignore[no-untyped-def]
        assert supplied_connection is connection
        assert position["phase"] == "post_import_stages"
        calls.append("refresh")
        return refreshed_connection

    executor.set_transport_refresher(refresh)
    reconciled: list[str] = []
    executor.set_stage_work_reconciler(
        SimpleNamespace(reconcile_next=lambda: reconciled.append("claim"))
    )
    result = executor.execute_cycle(
        RegionTalkCycleRequest(
            task_run_id=task_run_id,
            master_instance_id=master_instance_id,
            epoch=47,
            cycle_number=1,
        )
    )

    assert result.disposition is RegionTalkCycleDisposition.RETRYABLE
    assert result.rows_observed == 58_554
    assert result.rows_changed == 58_563
    assert result.queue_revision == 17
    assert result.accepted_snapshot_receipt_sha256 is not None
    assert result.stage_receipt_sha256 == "a" * 64
    assert reconciled == ["claim"]
    assert calls == [
        "inventory",
        "snapshot",
        "refresh",
        (
            "stages",
            {"task_run_id": task_run_id, "export_batch_id": export_batch_id},
        ),
    ]
    completed = executor.execute_cycle(
        RegionTalkCycleRequest(
            task_run_id=task_run_id,
            master_instance_id=master_instance_id,
            epoch=47,
            cycle_number=2,
        )
    )
    assert completed.disposition is RegionTalkCycleDisposition.COMPLETE
    assert reconciled == ["claim"]
    assert calls.count("snapshot") == 1
    assert calls[-1] == (
        "stages",
        {"task_run_id": task_run_id, "export_batch_id": export_batch_id},
    )


def test_cycle_does_not_report_success_for_failed_post_import_stages(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    task_run_id = uuid4()
    master_instance_id = uuid4()
    export_batch_id = uuid4()

    class Runner:
        def __init__(self, _reader, supplied_connection, **_kwargs):  # type: ignore[no-untyped-def]
            self.connection = supplied_connection

        def inventory(self, **_kwargs):  # type: ignore[no-untyped-def]
            return object()

        def run(self, _manifest):  # type: ignore[no-untyped-def]
            return SimpleNamespace(
                export_batch_id=export_batch_id,
                expected_row_count=1,
                dispositioned_row_count=1,
                model_dump=lambda **_kwargs: {"export_batch_id": str(export_batch_id)},
            )

    class Supervisor:
        def __init__(self, _function):  # type: ignore[no-untyped-def]
            pass

        def execute_after_import(self, **_identity):  # type: ignore[no-untyped-def]
            return SimpleNamespace(
                status=StageRunStatus.FAILED,
                receipt_sha256="a" * 64,
                queue_revision=0,
                rows_changed=0,
            )

    monkeypatch.setattr(direct_pipeline, "DirectSnapshotRunner", Runner)
    monkeypatch.setattr(direct_pipeline, "PostgresPostImportStageFunction", lambda _value: object())
    monkeypatch.setattr(direct_pipeline, "RegionTalkPostImportSupervisor", Supervisor)
    executor = direct_pipeline.DirectRegionTalkCycleExecutor(
        connection=object(),
        reader=object(),
        source_database="/ru-central1/example",
        task_run_id=task_run_id,
        master_instance_id=master_instance_id,
        epoch=47,
        source_revision=None,
    )

    result = executor.execute_cycle(
        RegionTalkCycleRequest(
            task_run_id=task_run_id,
            master_instance_id=master_instance_id,
            epoch=47,
            cycle_number=1,
        )
    )
    assert result.disposition is RegionTalkCycleDisposition.FAILED
