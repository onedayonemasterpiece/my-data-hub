#!/usr/bin/env python3
"""Prove upgrade from the previously released schema revision in a fresh database."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import tempfile
from pathlib import Path
from uuid import uuid4

import psycopg
from psycopg import sql

from my_data_hub.db.migrations import migrate

ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS = ROOT / "sql/migrations"
PREVIOUS_RELEASE_REVISION = 9
CURRENT_RELEASE_REVISION = max(
    int(path.name.split("_", 1)[0]) for path in MIGRATIONS.glob("*.sql")
)


def _database_url(base_url: str, database: str) -> str:
    from urllib.parse import urlsplit, urlunsplit

    parsed = urlsplit(base_url)
    return urlunsplit((parsed.scheme, parsed.netloc, f"/{database}", parsed.query, ""))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--admin-database-url",
        default=os.getenv("MY_DATA_HUB_ROLE_ADMIN_DATABASE_URL", ""),
    )
    args = parser.parse_args()
    if not args.admin_database_url:
        raise SystemExit("MY_DATA_HUB_ROLE_ADMIN_DATABASE_URL or --admin-database-url is required")

    database = f"mdh_upgrade_{uuid4().hex[:12]}"
    cleanup = "not_started"
    report: dict[str, object] = {"ok": False, "database": database}
    with tempfile.TemporaryDirectory(prefix="mdh-previous-release-") as temp:
        previous = Path(temp)
        for path in sorted(MIGRATIONS.glob("*.sql")):
            version = int(path.name.split("_", 1)[0])
            if version <= PREVIOUS_RELEASE_REVISION:
                shutil.copy2(path, previous / path.name)
        try:
            with psycopg.connect(args.admin_database_url, autocommit=True) as connection:
                connection.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
            target_url = _database_url(args.admin_database_url, database)
            previous_applied = migrate(target_url, previous)
            current_applied = migrate(target_url, MIGRATIONS)
            repeated_applied = migrate(target_url, MIGRATIONS)
            with psycopg.connect(target_url) as connection, connection.cursor() as cursor:
                cursor.execute(
                    "SELECT schema_revision, canonical_revision "
                    "FROM hub.canonical_state WHERE singleton = true"
                )
                schema_revision, canonical_revision = cursor.fetchone()
                cursor.execute("SELECT array_agg(version ORDER BY version) FROM hub_meta.schema_migration")
                versions = list(cursor.fetchone()[0])
            report.update(
                {
                    "ok": (
                        len(previous_applied) == PREVIOUS_RELEASE_REVISION
                        and len(current_applied)
                        == CURRENT_RELEASE_REVISION - PREVIOUS_RELEASE_REVISION
                        and not repeated_applied
                        and int(schema_revision) == CURRENT_RELEASE_REVISION
                        and int(canonical_revision) == 0
                        and versions == list(range(1, CURRENT_RELEASE_REVISION + 1))
                    ),
                    "previous_release_revision": PREVIOUS_RELEASE_REVISION,
                    "current_release_revision": CURRENT_RELEASE_REVISION,
                    "previous_applied": [item.filename for item in previous_applied],
                    "upgrade_applied": [item.filename for item in current_applied],
                    "repeated_applied": [item.filename for item in repeated_applied],
                    "schema_revision": int(schema_revision),
                    "canonical_revision": int(canonical_revision),
                    "migration_versions": versions,
                }
            )
        finally:
            try:
                with psycopg.connect(args.admin_database_url, autocommit=True) as connection:
                    connection.execute(
                        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                        "WHERE datname = %s AND pid <> pg_backend_pid()",
                        (database,),
                    )
                    connection.execute(
                        sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(database))
                    )
                cleanup = "dropped_upgrade_database"
            except Exception:
                cleanup = "failed"

    report["cleanup"] = cleanup
    report["ok"] = bool(report["ok"] and cleanup != "failed")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
