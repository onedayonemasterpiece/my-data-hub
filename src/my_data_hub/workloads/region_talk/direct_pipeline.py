"""Private Region Talk Notebook executor for the direct ACTIVE-master plane.

This module is imported only inside the disposable Kaggle supervisor.  Source
rows move from a read-only YDB snapshot scan straight into fixed PostgreSQL
functions; neither rows nor database credentials cross a control callback.
"""

from __future__ import annotations

import hashlib
import os
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any, TypeVar
from uuid import NAMESPACE_URL, UUID, uuid5

from my_data_hub.hashing import canonical_json_bytes

from .constants import DIRECT_SOURCE_TABLE_BY_NAME
from .direct_snapshot import DirectSnapshotRunner, DirectYdbReader
from .pipeline_runtime import (
    RegionTalkCycleDisposition,
    RegionTalkCycleRequest,
    RegionTalkCycleResult,
)
from .stage_execution import (
    PostgresPostImportStageFunction,
    RegionTalkPostImportSupervisor,
    StageRunStatus,
)


class DirectPipelineConfigurationError(RuntimeError):
    """The private runtime lacks an exact source or ACTIVE-master binding."""


_PassResult = TypeVar("_PassResult")


class YdbDirectReader(DirectYdbReader):
    """Closed-table, read-only keyset adapter over the official YDB SDK."""

    def __init__(self, driver: Any) -> None:
        self.driver = driver
        self._snapshots: dict[str, tuple[Mapping[str, Any], ...]] = {}
        self._active_transaction: Any | None = None

    def run_snapshot_pass(
        self, phase: str, callback: Callable[[], _PassResult]
    ) -> _PassResult:
        if phase not in {"pass_a", "pass_b"} or self._active_transaction is not None:
            raise DirectPipelineConfigurationError("YDB snapshot pass lifecycle is invalid")

        def read(transaction: Any) -> _PassResult:
            self._active_transaction = transaction
            self._snapshots.clear()
            try:
                result = callback()
                if self._snapshots:
                    raise DirectPipelineConfigurationError(
                        "YDB snapshot pass left a table scan incomplete"
                    )
                return result
            finally:
                self._snapshots.clear()
                self._active_transaction = None

        with _query_pool(self.driver) as pool:
            return pool.retry_tx_sync(read, tx_mode=_snapshot_mode())

    def scan_page(
        self,
        source_table: str,
        *,
        primary_key: str,
        after_primary_key: str | None,
        limit: int,
    ) -> Sequence[Mapping[str, Any]]:
        spec = DIRECT_SOURCE_TABLE_BY_NAME.get(source_table)
        if spec is None or spec.primary_key != primary_key:
            raise DirectPipelineConfigurationError("YDB source table/key is not allowlisted")
        if not 1 <= limit <= 500:
            raise DirectPipelineConfigurationError("YDB page limit is outside 1..500")
        snapshot = self._snapshots.get(source_table)
        if snapshot is None:
            if self._active_transaction is None:
                raise DirectPipelineConfigurationError(
                    "YDB table scan requires a database-wide snapshot pass"
                )
            if after_primary_key is not None:
                raise DirectPipelineConfigurationError("YDB snapshot scan did not start at the first key")
            snapshot = self._read_table_snapshot(spec.name, spec.primary_key, limit)
            self._snapshots[source_table] = snapshot
        start = 0
        if after_primary_key is not None:
            while start < len(snapshot) and str(snapshot[start][primary_key]) <= after_primary_key:
                start += 1
        page = snapshot[start : start + limit]
        if not page:
            # DirectSnapshotRunner issues one final empty read.  Release the
            # pass so the next pass obtains a new independent SnapshotReadOnly
            # transaction and can detect source changes.
            self._snapshots.pop(source_table, None)
        return page

    def _read_table_snapshot(
        self, source_table: str, primary_key: str, page_size: int
    ) -> tuple[Mapping[str, Any], ...]:
        query = (
            "DECLARE $after AS Utf8; DECLARE $limit AS Uint64; "
            f"SELECT * FROM `{source_table}` WHERE `{primary_key}` > $after "
            f"ORDER BY `{primary_key}` LIMIT $limit;"
        )
        null_query = (
            f"SELECT COUNT(*) AS null_pk_count FROM `{source_table}` "
            f"WHERE `{primary_key}` IS NULL;"
        )

        def read(tx: Any) -> tuple[Mapping[str, Any], ...]:
            rows: list[Mapping[str, Any]] = []
            null_count = 0
            with tx.execute(null_query) as results:
                for result_set in results:
                    for row in result_set.rows:
                        null_count += int(row["null_pk_count"])
            if null_count:
                raise DirectPipelineConfigurationError("YDB source contains a NULL primary key")
            after = ""
            while True:
                page: list[Mapping[str, Any]] = []
                with tx.execute(
                    query,
                    parameters={"$after": after, "$limit": page_size},
                ) as results:
                    for result_set in results:
                        page.extend(dict(row) for row in result_set.rows)
                if not page:
                    break
                if any(primary_key not in row for row in page):
                    raise DirectPipelineConfigurationError("YDB row lacks the explicit primary key")
                if rows and str(page[0][primary_key]) <= str(rows[-1][primary_key]):
                    raise DirectPipelineConfigurationError("YDB snapshot page order is not monotonic")
                rows.extend(page)
                if len(rows) > 200_000:
                    raise DirectPipelineConfigurationError(
                        "YDB table snapshot exceeds the 200000-row memory bound"
                    )
                after = str(page[-1][primary_key])
                if len(page) < page_size:
                    break
            return tuple(rows)

        assert self._active_transaction is not None
        return read(self._active_transaction)


def _ydb_module() -> Any:
    try:
        import ydb
    except ImportError as exc:  # pragma: no cover - runtime dependency gate
        raise DirectPipelineConfigurationError("my-data-hub[ydb] is required") from exc
    return ydb


def _query_pool(driver: Any) -> Any:
    return _ydb_module().QuerySessionPool(driver)


def _snapshot_mode() -> Any:
    return _ydb_module().QuerySnapshotReadOnly()


class DirectRegionTalkCycleExecutor:
    """Import once, then poll post-import stages until explicit DB completion."""

    def __init__(
        self,
        *,
        connection: Any,
        reader: DirectYdbReader,
        source_database: str,
        task_run_id: UUID,
        master_instance_id: UUID,
        epoch: int,
        source_revision: str | None,
    ) -> None:
        self.connection = connection
        self.reader = reader
        self.source_database = source_database
        self.task_run_id = task_run_id
        self.master_instance_id = master_instance_id
        self.epoch = epoch
        self.source_revision = source_revision
        self._receipt: RegionTalkCycleResult | None = None
        self._export_batch_id: UUID | None = None
        self._snapshot_receipt_sha256: str | None = None
        self._snapshot_rows_observed = 0
        self._snapshot_rows_changed = 0
        self._stage_poll_count = 0
        self._transport_refresher: Callable[..., Any] | None = None

    def set_transport_refresher(self, refresher: Callable[..., Any]) -> None:
        """Install the Notebook-owned tunnel/DB rotation hook."""

        self._transport_refresher = refresher

    def execute_cycle(self, request: RegionTalkCycleRequest) -> RegionTalkCycleResult:
        if request.publication_dispatch:
            raise DirectPipelineConfigurationError("publication dispatch is disabled")
        if (
            request.task_run_id != self.task_run_id
            or request.master_instance_id != self.master_instance_id
            or request.epoch != self.epoch
        ):
            raise DirectPipelineConfigurationError("cycle differs from the task/epoch binding")
        if (
            self._receipt is not None
            and self._receipt.disposition is RegionTalkCycleDisposition.COMPLETE
        ):
            return self._receipt
        if self._export_batch_id is not None:
            return self._execute_post_import_stages()
        export_batch_id = uuid5(
            NAMESPACE_URL, f"my-data-hub:region-talk:direct:{self.task_run_id}"
        )
        request_body = {
            "task_run_id": str(self.task_run_id),
            "master_instance_id": str(self.master_instance_id),
            "epoch": self.epoch,
            "source_database": self.source_database,
            "source_revision": self.source_revision,
            "publication_dispatch": False,
        }
        request_sha256 = hashlib.sha256(canonical_json_bytes(request_body)).hexdigest()
        runner = DirectSnapshotRunner(
            self.reader,
            self.connection,
            page_size=250,
            transport_refresh=self._transport_refresher,
        )
        manifest = runner.inventory(
            export_batch_id=export_batch_id,
            task_run_id=self.task_run_id,
            master_instance_id=self.master_instance_id,
            master_epoch=self.epoch,
            source_database=self.source_database,
            request_sha256=request_sha256,
            created_at=datetime.now(UTC),
        )
        receipt = runner.run(manifest)
        self.connection = runner.connection
        snapshot_receipt_sha256 = hashlib.sha256(
            canonical_json_bytes(receipt.model_dump(mode="json"))
        ).hexdigest()
        self._export_batch_id = receipt.export_batch_id
        self._snapshot_receipt_sha256 = snapshot_receipt_sha256
        self._snapshot_rows_observed = receipt.expected_row_count
        self._snapshot_rows_changed = receipt.dispositioned_row_count
        if self._transport_refresher is not None:
            replacement = self._transport_refresher(
                self.connection,
                phase="post_import_stages",
                source_table=None,
                after_primary_key=None,
                page_number=0,
            )
            if replacement is None:
                raise DirectPipelineConfigurationError(
                    "transport refresh returned no post-import connection"
                )
            self.connection = replacement
        return self._execute_post_import_stages()

    def _execute_post_import_stages(self) -> RegionTalkCycleResult:
        assert self._export_batch_id is not None
        assert self._snapshot_receipt_sha256 is not None
        stage_receipt = RegionTalkPostImportSupervisor(
            PostgresPostImportStageFunction(self.connection)
        ).execute_after_import(
            task_run_id=self.task_run_id,
            export_batch_id=self._export_batch_id,
        )
        receipt_sha256 = hashlib.sha256(
            canonical_json_bytes(
                {
                    "schema_version": "region-talk-supervised-cycle-receipt.v1",
                    "task_run_id": str(self.task_run_id),
                    "export_batch_id": str(self._export_batch_id),
                    "snapshot_receipt_sha256": self._snapshot_receipt_sha256,
                    "stage_receipt_sha256": stage_receipt.receipt_sha256,
                    "stage_status": stage_receipt.status.value,
                    "queue_revision": stage_receipt.queue_revision,
                    "publication_dispatch": False,
                    "notification_dispatch": False,
                }
            )
        ).hexdigest()
        first_poll = self._stage_poll_count == 0
        self._stage_poll_count += 1
        self._receipt = RegionTalkCycleResult(
            disposition=(
                RegionTalkCycleDisposition.COMPLETE
                if stage_receipt.status is StageRunStatus.COMPLETE
                else RegionTalkCycleDisposition.FAILED
                if stage_receipt.status is StageRunStatus.FAILED
                else RegionTalkCycleDisposition.RETRYABLE
            ),
            rows_observed=self._snapshot_rows_observed if first_poll else 0,
            rows_changed=(self._snapshot_rows_changed if first_poll else 0)
            + stage_receipt.rows_changed,
            receipt_sha256=receipt_sha256,
            queue_revision=stage_receipt.queue_revision,
        )
        return self._receipt


def build_cycle_executor(
    *,
    database_url: str,
    tls_ca_path: str,
    task_run_id: str,
    master_instance_id: str,
    epoch: int,
    source_revision: str | None,
    publication_dispatch: bool,
) -> DirectRegionTalkCycleExecutor:
    """Build the private executor from exact runtime-only environment inputs."""

    if publication_dispatch:
        raise DirectPipelineConfigurationError("publication dispatch is disabled")
    endpoint = os.getenv("MY_DATA_HUB_YDB_ENDPOINT", "").strip()
    database = os.getenv("MY_DATA_HUB_YDB_DATABASE", "").strip()
    if not endpoint or not database:
        raise DirectPipelineConfigurationError("YDB endpoint/database is absent")
    ydb = _ydb_module()
    driver = ydb.Driver(
        endpoint=endpoint,
        database=database,
        credentials=ydb.credentials_from_env_variables(),
    )
    driver.wait(timeout=20, fail_fast=True)
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - pinned wheel gate
        driver.stop()
        raise DirectPipelineConfigurationError("psycopg runtime is absent") from exc
    connection = psycopg.connect(database_url, connect_timeout=5)
    with connection.cursor() as cursor:
        cursor.execute("SET statement_timeout='300s'")
        cursor.execute("SET lock_timeout='5s'")
        cursor.execute("SET idle_in_transaction_session_timeout='30s'")
        cursor.execute("SELECT master_control.assert_session_write_epoch()")
    connection.commit()
    # Keep the driver on the executor for the bounded Notebook process lifetime.
    executor = DirectRegionTalkCycleExecutor(
        connection=connection,
        reader=YdbDirectReader(driver),
        source_database=database,
        task_run_id=UUID(task_run_id),
        master_instance_id=UUID(master_instance_id),
        epoch=epoch,
        source_revision=source_revision,
    )
    executor._ydb_driver = driver  # type: ignore[attr-defined]
    executor._tls_ca_path = tls_ca_path  # type: ignore[attr-defined]
    return executor


__all__ = [
    "DirectPipelineConfigurationError",
    "DirectRegionTalkCycleExecutor",
    "YdbDirectReader",
    "build_cycle_executor",
]
