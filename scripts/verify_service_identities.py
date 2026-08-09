#!/usr/bin/env python3
"""Prove each production connection uses one restricted PostgreSQL login."""

from __future__ import annotations

import json
import os
from urllib.parse import urlsplit

IDENTITIES = {
    "MY_DATA_HUB_APPLICATION_DATABASE_URL": "mdh_application",
    "MY_DATA_HUB_CONNECTOR_INTAKE_DATABASE_URL": "mdh_connector_intake",
    "MY_DATA_HUB_ORCHESTRATOR_DATABASE_URL": "mdh_orchestrator",
    "MY_DATA_HUB_MCP_READER_DATABASE_URL": "mdh_mcp_reader",
    "MY_DATA_HUB_MCP_REVOCATION_DATABASE_URL": "mdh_authenticator",
    "MY_DATA_HUB_CANONICAL_COMMITTER_DATABASE_URL": "mdh_canonical_committer",
    "MY_DATA_HUB_BACKUP_DATABASE_URL": "mdh_backup",
    "MY_DATA_HUB_MIGRATOR_DATABASE_URL": "mdh_migrator",
    "MY_DATA_HUB_MONITORING_DATABASE_URL": "mdh_monitoring",
    "MY_DATA_HUB_MIGRATION_OPERATOR_DATABASE_URL": "mdh_migration_operator",
}


def main() -> int:
    import psycopg

    findings: list[str] = []
    observations: list[dict[str, object]] = []
    usernames: list[str] = []
    for environment_name, group_role in IDENTITIES.items():
        database_url = os.getenv(environment_name, "").strip()
        if not database_url:
            findings.append(f"{environment_name} is absent")
            continue
        configured_username = urlsplit(database_url).username or ""
        usernames.append(configured_username)
        try:
            with psycopg.connect(database_url, connect_timeout=3) as connection, connection.cursor() as cursor:
                cursor.execute("SET TRANSACTION READ ONLY")
                cursor.execute("SET LOCAL statement_timeout = '1000ms'")
                cursor.execute(
                    """
                    SELECT current_user, session_user, r.rolsuper, r.rolcreatedb,
                           r.rolcreaterole, r.rolreplication, r.rolbypassrls,
                           pg_has_role(current_user, %s, 'MEMBER')
                    FROM pg_roles r WHERE r.rolname = current_user
                    """,
                    (group_role,),
                )
                row = cursor.fetchone()
        except Exception as exc:
            findings.append(f"{environment_name} connection failed: {type(exc).__name__}")
            continue
        if row is None:
            findings.append(f"{environment_name} current role is absent")
            continue
        safe = (
            str(row[0]) == str(row[1]) == configured_username
            and configured_username != group_role
            and not any(bool(value) for value in row[2:7])
            and bool(row[7])
        )
        if not safe:
            findings.append(f"{environment_name} is not a restricted {group_role} login")
        observations.append(
            {
                "environment": environment_name,
                "login": configured_username,
                "required_group": group_role,
                "restricted": safe,
            }
        )
    if len(usernames) != len(set(usernames)):
        findings.append("service database login principals are not distinct")
    report = {"ok": not findings, "findings": findings, "identities": observations}
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if not findings else 2


if __name__ == "__main__":
    raise SystemExit(main())
