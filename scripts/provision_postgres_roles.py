#!/usr/bin/env python3
"""Apply the checksummed, password-free PostgreSQL R1 group-role contract."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ROLE_SQL = ROOT / "sql/admin/role_contract.sql"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url", default=os.getenv("MY_DATA_HUB_MIGRATOR_DATABASE_URL", ""))
    args = parser.parse_args()
    if not args.database_url:
        raise SystemExit("MY_DATA_HUB_MIGRATOR_DATABASE_URL or --database-url is required")

    import psycopg

    sql_bytes = ROLE_SQL.read_bytes()
    with psycopg.connect(args.database_url) as connection, connection.cursor() as cursor:
        cursor.execute(sql_bytes.decode("utf-8"))
        cursor.execute(
            """
            SELECT rolname, rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls,
                   rolcanlogin, rolinherit
            FROM pg_roles
            WHERE rolname = ANY(%s)
            ORDER BY rolname
            """,
            (
                [
                    "mdh_owner",
                    "mdh_migrator",
                    "mdh_application",
                    "mdh_orchestrator",
                    "mdh_connector_intake",
                    "mdh_mcp_reader",
                    "mdh_mcp_editor",
                    "mdh_migration_operator",
                    "mdh_backup",
                    "mdh_monitoring",
                ],
            ),
        )
        roles = [
            {
                "role": row[0],
                "superuser": row[1],
                "createdb": row[2],
                "createrole": row[3],
                "replication": row[4],
                "bypassrls": row[5],
                "login": row[6],
                "inherit": row[7],
            }
            for row in cursor.fetchall()
        ]
    print(
        json.dumps(
            {
                "ok": len(roles) == 10
                and all(
                    not any(value for key, value in role.items() if key != "role")
                    for role in roles
                ),
                "contract_sha256": hashlib.sha256(sql_bytes).hexdigest(),
                "roles": roles,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
