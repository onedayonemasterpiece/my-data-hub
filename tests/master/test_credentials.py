from __future__ import annotations

from typing import Any

from my_data_hub.master_runtime.credentials import CredentialProvisioner


def test_drop_revokes_the_direct_database_connect_grant_before_dropping_login() -> None:
    calls: list[tuple[Any, Any]] = []

    class Result:
        def fetchone(self) -> tuple[str]:
            return ("postgres",)

    class Cursor:
        def __enter__(self) -> Cursor:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def execute(self, query: Any, params: Any = None) -> Result:
            calls.append((query, params))
            return Result()

    class Connection:
        def __init__(self) -> None:
            self.commits = 0

        def cursor(self) -> Cursor:
            return Cursor()

        def commit(self) -> None:
            self.commits += 1

    connection = Connection()
    CredentialProvisioner(connection).drop("mdh_e16_reader_cc9e8156")

    assert calls[0] == (
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = %s",
        ("mdh_e16_reader_cc9e8156",),
    )
    assert calls[1] == ("SELECT current_database()", None)
    assert "REVOKE CONNECT ON DATABASE" in str(calls[2][0])
    assert "Identifier('postgres')" in str(calls[2][0])
    assert "Identifier('mdh_e16_reader_cc9e8156')" in str(calls[2][0])
    assert "DROP ROLE IF EXISTS" in str(calls[3][0])
    assert connection.commits == 1
