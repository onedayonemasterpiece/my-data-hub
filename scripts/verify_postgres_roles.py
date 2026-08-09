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
    cursor.execute("SELECT current_user")
    current = str(cursor.fetchone()[0])
    if current != role:
        raise RuntimeError(f"SET ROLE identity mismatch: expected={role}, actual={current}")


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
        sqlstate = getattr(exc, "sqlstate", None)
        return Probe(
            role,
            name,
            "deny",
            sqlstate in {"42501", "P0001"},
            sqlstate,
            str(exc).splitlines()[0],
        )
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
                    "mdh_canonical_committer",
                    "bounded canonical compare-and-swap",
                    "SELECT hub.advance_canonical_revision((SELECT canonical_revision "
                    "FROM hub.canonical_state WHERE singleton))",
                ),
                _positive(
                    cursor,
                    "mdh_authenticator",
                    "revocation lookup",
                    "SELECT count(*) FROM auth.oauth_revocation",
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
            "extension management": "CREATE EXTENSION hstore WITH SCHEMA public",
            "server file": "SELECT pg_read_file('/etc/passwd', 0, 1)",
            "COPY PROGRAM": "COPY (SELECT 1) TO PROGRAM 'true'",
            "migration accounting write": "DELETE FROM hub_meta.schema_migration",
            "cutover write": "DELETE FROM migration.cutover_receipt",
            "provider class write": "UPDATE integration.provider_resource SET control_class = 'mcp_managed'",
            "audit mutation": "DELETE FROM sync.audit_event",
            "operator receipt mutation": "DELETE FROM operator_control.apply_receipt",
        }
        for role in (
            "mdh_mcp_reader",
            "mdh_mcp_editor",
            "mdh_connector_intake",
            "mdh_authenticator",
        ):
            for name, statement in adversarial.items():
                probes.append(_negative(cursor, role, name, statement))
        for role in ("mdh_mcp_reader", "mdh_mcp_editor", "mdh_connector_intake"):
            probes.append(
                _negative(
                    cursor,
                    role,
                    "revocation journal read",
                    "SELECT * FROM auth.oauth_revocation",
                )
            )
        for role in (
            "mdh_application",
            "mdh_mcp_reader",
            "mdh_mcp_editor",
            "mdh_connector_intake",
            "mdh_canonical_committer",
        ):
            probes.extend(
                [
                    _negative(
                        cursor,
                        role,
                        "direct canonical revision update",
                        "UPDATE hub.canonical_state SET canonical_revision = canonical_revision + 1",
                    ),
                    _negative(
                        cursor,
                        role,
                        "canonical singleton delete",
                        "DELETE FROM hub.canonical_state",
                    ),
                ]
            )

        cursor.execute("RESET ROLE")
        cursor.execute(
            """
            SELECT n.nspname, c.relname, r.rolname
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_roles r ON r.oid = c.relowner
            WHERE c.relkind IN ('r', 'p', 'S', 'v', 'm')
              AND n.nspname IN (
                'hub_meta', 'hub', 'analysis', 'orchestration', 'sync', 'region_talk',
                'migration', 'joplin', 'integration', 'recovery', 'operator_control', 'auth'
              )
              AND r.rolname <> 'mdh_owner'
            ORDER BY n.nspname, c.relname
            """
        )
        wrong_owners = cursor.fetchall()
        probes.append(
            Probe(
                "mdh_owner",
                "all canonical objects owned by non-login owner",
                "allow",
                not wrong_owners,
                detail=None if not wrong_owners else str(wrong_owners[:10]),
            )
        )

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
