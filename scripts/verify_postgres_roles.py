#!/usr/bin/env python3
"""Run positive and adversarial PostgreSQL ACL probes under each R1 remote role."""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class Probe:
    role: str
    name: str
    expected: str
    passed: bool
    sqlstate: str | None = None
    detail: str | None = None


def _set_role(cursor: Any, role: str) -> None:
    from psycopg import sql

    cursor.execute(sql.SQL("SET ROLE {}").format(sql.Identifier(role)))


def _positive(cursor: Any, role: str, name: str, statement: str) -> Probe:
    cursor.execute("RESET ROLE")
    _set_role(cursor, role)
    try:
        cursor.execute(statement)
        cursor.fetchall() if cursor.description else None
        return Probe(role, name, "allow", True)
    except Exception as exc:  # pragma: no cover - exercised by live PostgreSQL job
        return Probe(role, name, "allow", False, getattr(exc, "sqlstate", None), str(exc).splitlines()[0])
    finally:
        cursor.execute("RESET ROLE")


def _negative(cursor: Any, role: str, name: str, statement: str) -> Probe:
    cursor.execute("RESET ROLE")
    _set_role(cursor, role)
    cursor.execute("SAVEPOINT negative_probe")
    try:
        cursor.execute(statement)
    except Exception as exc:  # PostgreSQL permission denial is the expected boundary
        cursor.execute("ROLLBACK TO SAVEPOINT negative_probe")
        cursor.execute("RELEASE SAVEPOINT negative_probe")
        cursor.execute("RESET ROLE")
        return Probe(role, name, "deny", True, getattr(exc, "sqlstate", None), str(exc).splitlines()[0])
    cursor.execute("ROLLBACK TO SAVEPOINT negative_probe")
    cursor.execute("RELEASE SAVEPOINT negative_probe")
    cursor.execute("RESET ROLE")
    return Probe(role, name, "deny", False, detail="statement unexpectedly succeeded")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url", default=os.getenv("MY_DATA_HUB_ROLE_ADMIN_DATABASE_URL", ""))
    args = parser.parse_args()
    if not args.database_url:
        raise SystemExit("MY_DATA_HUB_ROLE_ADMIN_DATABASE_URL or --database-url is required")

    import psycopg

    probes: list[Probe] = []
    with psycopg.connect(args.database_url) as connection, connection.cursor() as cursor:
        cursor.execute("BEGIN")
        cursor.execute("CREATE SCHEMA operator_disposable")
        cursor.execute("CREATE TABLE operator_disposable.probe (id integer PRIMARY KEY, value text NOT NULL)")
        cursor.execute("GRANT USAGE ON SCHEMA operator_disposable TO mdh_mcp_editor")
        cursor.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON operator_disposable.probe TO mdh_mcp_editor")

        probes.extend(
            [
                _positive(
                    cursor,
                    "mdh_mcp_reader",
                    "bounded application read",
                    "SELECT schema_revision FROM hub.canonical_state",
                ),
                _positive(
                    cursor,
                    "mdh_connector_intake",
                    "connector registry read",
                    "SELECT connector_id FROM integration.connector LIMIT 1",
                ),
                _positive(
                    cursor,
                    "mdh_monitoring",
                    "health projection read",
                    "SELECT schema_revision FROM hub.canonical_state",
                ),
                _positive(
                    cursor,
                    "mdh_mcp_editor",
                    "disposable insert",
                    "INSERT INTO operator_disposable.probe VALUES (1, 'ok') RETURNING id",
                ),
                _positive(
                    cursor,
                    "mdh_mcp_editor",
                    "disposable update",
                    "UPDATE operator_disposable.probe SET value = 'updated' WHERE id = 1 RETURNING id",
                ),
                _positive(
                    cursor,
                    "mdh_mcp_editor",
                    "disposable delete",
                    "DELETE FROM operator_disposable.probe WHERE id = 1 RETURNING id",
                ),
            ]
        )

        adversarial = {
            "permanent DDL": "CREATE TABLE hub.mcp_forbidden(id integer)",
            "temporary DDL": "CREATE TEMP TABLE mcp_forbidden(id integer)",
            "role management": "CREATE ROLE mcp_forbidden",
            "extension management": "CREATE EXTENSION hstore",
            "server file": "SELECT pg_read_file('/etc/passwd', 0, 1)",
            "COPY PROGRAM": "COPY (SELECT 1) TO PROGRAM 'true'",
            "migration accounting write": "DELETE FROM hub_meta.schema_migration",
            "cutover write": "DELETE FROM migration.cutover_receipt",
            "provider class write": "UPDATE integration.provider_resource SET control_class = 'mcp_managed'",
            "audit mutation": "DELETE FROM sync.audit_event",
            "operator receipt mutation": "DELETE FROM operator_control.apply_receipt",
        }
        for role in ("mdh_mcp_reader", "mdh_mcp_editor", "mdh_connector_intake"):
            for name, statement in adversarial.items():
                probes.append(_negative(cursor, role, name, statement))

        # The entire disposable probe environment and all successful DML are removed.
        cursor.execute("RESET ROLE")
        connection.rollback()

    failures = [probe for probe in probes if not probe.passed]
    print(
        json.dumps(
            {
                "ok": not failures,
                "probe_count": len(probes),
                "failures": [asdict(probe) for probe in failures],
                "probes": [asdict(probe) for probe in probes],
                "cleanup": "transaction_rolled_back",
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if not failures else 2


if __name__ == "__main__":
    raise SystemExit(main())
