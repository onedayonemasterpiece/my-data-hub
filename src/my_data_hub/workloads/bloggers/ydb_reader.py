"""Read-only YDB snapshot reader with no local artifact or payload spool."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from .schema import SOURCE_COLUMNS, SOURCE_QUERY, SOURCE_QUERY_SHA256, assert_query_identity

ZERO_ROW_WRITE_DENIAL_PROBE = (
    "UPDATE `region_talk_external_blogger_evidence` SET blogger_name = blogger_name "
    'WHERE record_id = "__my_data_hub_permission_probe_never_matches__";'
)
DENIAL_REQUEST_TIMEOUT_SECONDS = 10
SNAPSHOT_REQUEST_TIMEOUT_SECONDS = 30


class YdbSnapshotError(RuntimeError):
    """The exact read-only snapshot could not be established."""


class YdbBloggerSnapshot:
    """Consumes one QuerySnapshotReadOnly result without writing it to disk.

    The caller supplies an already-authenticated YDB driver whose principal has
    been independently verified to have only database-scoped ``ydb.viewer``.
    """

    def __init__(self, driver: Any, *, acquire_timeout_seconds: float = 20.0) -> None:
        if acquire_timeout_seconds <= 0 or acquire_timeout_seconds > 60:
            raise ValueError("session acquire timeout is outside the bounded contract")
        self.driver = driver
        self.acquire_timeout_seconds = acquire_timeout_seconds

    def assert_write_denied(self) -> None:
        """Prove the live principal cannot execute even a zero-row UPDATE.

        Only the SDK's exact UNAUTHORIZED status is accepted as evidence.
        Connectivity, syntax, timeout, and generic failures fail closed.
        """

        import ydb

        pool = ydb.QuerySessionPool(self.driver, size=1)
        try:
            with pool.checkout(timeout=self.acquire_timeout_seconds) as session:
                try:
                    responses = session.transaction(ydb.QuerySerializableReadWrite()).execute(
                        ZERO_ROW_WRITE_DENIAL_PROBE,
                        commit_tx=True,
                        settings=self._request_settings(ydb, DENIAL_REQUEST_TIMEOUT_SECONDS),
                    )
                    # Query Service responses are streaming.  Consume the
                    # iterator so a deferred UNAUTHORIZED cannot be mistaken
                    # for a successful denial probe.
                    for _response in responses:
                        pass
                except ydb.issues.Unauthorized:
                    return
                raise YdbSnapshotError("YDB viewer write-denial probe unexpectedly succeeded")
        finally:
            pool.stop(timeout=5)

    @contextmanager
    def iter_rows(self) -> Iterator[Iterator[dict[str, object]]]:
        import ydb
        from ydb import convert

        assert_query_identity(SOURCE_QUERY, SOURCE_QUERY_SHA256)
        pool = ydb.QuerySessionPool(self.driver, size=1)
        try:
            with pool.checkout(timeout=self.acquire_timeout_seconds) as session:
                tx = session.transaction(ydb.QuerySnapshotReadOnly())
                responses = tx.execute(
                    SOURCE_QUERY,
                    commit_tx=True,
                    settings=self._request_settings(ydb, SNAPSHOT_REQUEST_TIMEOUT_SECONDS),
                )
                result_sets = convert.aggregate_result_sets_by_index(responses)
                if len(result_sets) != 1:
                    raise YdbSnapshotError("blogger query returned an unexpected result-set count")

                def rows() -> Iterator[dict[str, object]]:
                    for raw in result_sets[0].rows:
                        value = raw if isinstance(raw, dict) else {name: getattr(raw, name) for name in SOURCE_COLUMNS}
                        if set(value) != set(SOURCE_COLUMNS):
                            raise YdbSnapshotError("YDB result shape differs from exact 27-column contract")
                        yield value

                yield rows()
        finally:
            pool.stop(timeout=5)

    @staticmethod
    def _request_settings(ydb: Any, timeout_seconds: int) -> Any:
        return (
            ydb.BaseRequestSettings()
            .with_timeout(timeout_seconds)
            .with_operation_timeout(timeout_seconds)
            .with_cancel_after(timeout_seconds)
        )
