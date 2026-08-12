from __future__ import annotations

from my_data_hub.workloads.bloggers.schema import SOURCE_QUERY
from my_data_hub.workloads.bloggers.ydb_reader import ZERO_ROW_WRITE_DENIAL_PROBE


def test_reader_contract_is_one_ordered_snapshot_and_zero_row_probe() -> None:
    assert "ORDER BY `record_id`" in SOURCE_QUERY
    assert "LIMIT" not in SOURCE_QUERY
    assert "UPDATE `region_talk_external_blogger_evidence`" in ZERO_ROW_WRITE_DENIAL_PROBE
    assert "__my_data_hub_permission_probe_never_matches__" in ZERO_ROW_WRITE_DENIAL_PROBE


def test_write_denial_probe_accepts_only_exact_unauthorized(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    import sys
    from types import SimpleNamespace

    from my_data_hub.workloads.bloggers.ydb_reader import YdbBloggerSnapshot

    class Unauthorized(Exception):
        pass

    class Settings:
        def with_timeout(self, value):
            assert value == 10
            return self

        def with_operation_timeout(self, value):
            assert value == 10
            return self

        def with_cancel_after(self, value):
            assert value == 10
            return self

    class Transaction:
        def execute(self, query: str, *, commit_tx: bool, settings):
            assert query == ZERO_ROW_WRITE_DENIAL_PROBE
            assert commit_tx is True
            assert isinstance(settings, Settings)

            def responses():
                raise Unauthorized("denied")
                yield None

            return responses()

    class Session:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def transaction(self, mode):
            return Transaction()

    class Pool:
        def __init__(self, driver, size):
            assert size == 1

        def checkout(self, timeout):
            return Session()

        def stop(self, timeout):
            assert timeout == 5

    fake = SimpleNamespace(
        QuerySessionPool=Pool,
        BaseRequestSettings=Settings,
        QuerySerializableReadWrite=lambda: object(),
        issues=SimpleNamespace(Unauthorized=Unauthorized),
    )
    monkeypatch.setitem(sys.modules, "ydb", fake)
    YdbBloggerSnapshot(object()).assert_write_denied()


def test_write_denial_probe_rejects_non_authorization_failure(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    import sys
    from types import SimpleNamespace

    import pytest

    from my_data_hub.workloads.bloggers.ydb_reader import YdbBloggerSnapshot

    class Unauthorized(Exception):
        pass

    class Session:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def transaction(self, mode):
            class Transaction:
                def execute(self, query, *, commit_tx, settings):
                    raise TimeoutError("network")

            return Transaction()

    class Pool:
        def __init__(self, driver, size):
            pass

        def checkout(self, timeout):
            return Session()

        def stop(self, timeout):
            pass

    fake = SimpleNamespace(
        QuerySessionPool=Pool,
        BaseRequestSettings=lambda: SimpleNamespace(
            with_timeout=lambda _value: SimpleNamespace(
                with_operation_timeout=lambda _other: SimpleNamespace(with_cancel_after=lambda _last: object())
            )
        ),
        QuerySerializableReadWrite=lambda: object(),
        issues=SimpleNamespace(Unauthorized=Unauthorized),
    )
    monkeypatch.setitem(sys.modules, "ydb", fake)
    with pytest.raises(TimeoutError, match="network"):
        YdbBloggerSnapshot(object()).assert_write_denied()


def test_write_denial_probe_accepts_only_exact_structured_access_denied_wrapper(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    import sys
    from types import SimpleNamespace

    from my_data_hub.workloads.bloggers.schema import SOURCE_DATABASE_PATH, SOURCE_TABLE
    from my_data_hub.workloads.bloggers.ydb_reader import YdbBloggerSnapshot

    class Unauthorized(Exception):
        pass

    class Aborted(Exception):
        status = 400040

        def __init__(self, *, issues):
            super().__init__("structured provider denial")
            self.issues = issues
            self.status = 400040

    class Settings:
        def with_timeout(self, value): return self
        def with_operation_timeout(self, value): return self
        def with_cancel_after(self, value): return self

    exact_issues = [
        SimpleNamespace(
            issue_code=2028,
            severity=1,
            message=f"Failed to resolve table `{SOURCE_DATABASE_PATH}/{SOURCE_TABLE}` status: AccessDenied.",
            issues=[],
        ),
        SimpleNamespace(
            issue_code=2019,
            severity=1,
            message="Query invalidated on scheme/internal error during Data execution",
            issues=[],
        ),
    ]

    class RepeatedIssues:
        """Match the iterable protobuf container returned by ydb 3.31."""

        def __iter__(self):  # type: ignore[no-untyped-def]
            return iter(exact_issues)

    class Transaction:
        def execute(self, query, *, commit_tx, settings):
            def responses():
                raise Aborted(issues=RepeatedIssues())
                yield None
            return responses()

    class Session:
        def __enter__(self): return self
        def __exit__(self, *args): return None
        def transaction(self, mode): return Transaction()

    class Pool:
        def __init__(self, driver, size): pass
        def checkout(self, timeout): return Session()
        def stop(self, timeout): pass

    fake = SimpleNamespace(
        QuerySessionPool=Pool,
        BaseRequestSettings=Settings,
        QuerySerializableReadWrite=lambda: object(),
        issues=SimpleNamespace(Unauthorized=Unauthorized, Aborted=Aborted),
    )
    monkeypatch.setitem(sys.modules, "ydb", fake)
    YdbBloggerSnapshot(object()).assert_write_denied()


def test_write_denial_probe_rejects_lookalike_aborted_wrapper(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    import sys
    from types import SimpleNamespace

    import pytest

    from my_data_hub.workloads.bloggers.ydb_reader import YdbBloggerSnapshot

    class Unauthorized(Exception):
        pass

    class Aborted(Exception):
        status = 400040

        def __init__(self):
            super().__init__("lookalike")
            self.status = 400040
            self.issues = [SimpleNamespace(issue_code=2028, severity=1, message="AccessDenied", issues=[])]

    class Settings:
        def with_timeout(self, value): return self
        def with_operation_timeout(self, value): return self
        def with_cancel_after(self, value): return self

    class Transaction:
        def execute(self, query, *, commit_tx, settings):
            def responses():
                raise Aborted()
                yield None
            return responses()

    class Session:
        def __enter__(self): return self
        def __exit__(self, *args): return None
        def transaction(self, mode): return Transaction()

    class Pool:
        def __init__(self, driver, size): pass
        def checkout(self, timeout): return Session()
        def stop(self, timeout): pass

    fake = SimpleNamespace(
        QuerySessionPool=Pool,
        BaseRequestSettings=Settings,
        QuerySerializableReadWrite=lambda: object(),
        issues=SimpleNamespace(Unauthorized=Unauthorized, Aborted=Aborted),
    )
    monkeypatch.setitem(sys.modules, "ydb", fake)
    with pytest.raises(Aborted, match="lookalike"):
        YdbBloggerSnapshot(object()).assert_write_denied()
