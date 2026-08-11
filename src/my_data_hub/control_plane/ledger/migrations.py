from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from .errors import MigrationError


@dataclass(frozen=True, slots=True)
class ControlMigration:
    version: int
    filename: str
    path: Path
    sha256: str


def default_migration_directory() -> Path:
    return Path(__file__).resolve().parents[4] / "control_migrations"


def discover_control_migrations(directory: Path) -> list[ControlMigration]:
    migrations: list[ControlMigration] = []
    seen: set[int] = set()
    for path in sorted(directory.glob("*.sql")):
        prefix = path.name.split("_", 1)[0]
        if not prefix.isdigit():
            raise MigrationError(f"control migration filename must start with digits: {path.name}")
        version = int(prefix)
        if version in seen:
            raise MigrationError(f"duplicate control migration version: {version}")
        seen.add(version)
        migrations.append(ControlMigration(version, path.name, path, hashlib.sha256(path.read_bytes()).hexdigest()))
    if not migrations:
        raise MigrationError(f"no control migrations found in {directory}")
    actual = [migration.version for migration in migrations]
    expected = list(range(actual[0], actual[-1] + 1))
    if actual != expected or actual[0] != 1:
        raise MigrationError(f"control migrations must be contiguous from 1: {actual}")
    return migrations


def _iter_statements(sql: str):  # type: ignore[no-untyped-def]
    buffer = ""
    for line in sql.splitlines(keepends=True):
        buffer += line
        if sqlite3.complete_statement(buffer):
            statement = buffer.strip()
            if statement:
                yield statement
            buffer = ""
    if buffer.strip():
        raise MigrationError("incomplete SQL statement in control migration")


def apply_control_migrations(connection: sqlite3.Connection, directory: Path | None = None) -> list[int]:
    directory = directory or default_migration_directory()
    migrations = discover_control_migrations(directory)
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS control_schema_migrations (
            version INTEGER PRIMARY KEY,
            filename TEXT NOT NULL UNIQUE,
            sha256 TEXT NOT NULL,
            applied_at TEXT NOT NULL
        )
        """
    )
    applied = {
        int(row[0]): (str(row[1]), str(row[2]))
        for row in connection.execute(
            "SELECT version, filename, sha256 FROM control_schema_migrations ORDER BY version"
        )
    }
    known = {migration.version: migration for migration in migrations}
    for version, (filename, checksum) in applied.items():
        migration = known.get(version)
        if migration is None:
            raise MigrationError(f"applied control migration {version} is missing")
        if (migration.filename, migration.sha256) != (filename, checksum):
            raise MigrationError(f"applied control migration {version} was modified")

    executed: list[int] = []
    for migration in migrations:
        if migration.version in applied:
            continue
        try:
            connection.execute("BEGIN IMMEDIATE")
            for statement in _iter_statements(migration.path.read_text(encoding="utf-8")):
                connection.execute(statement)
            connection.execute(
                "INSERT INTO control_schema_migrations(version, filename, sha256, applied_at) "
                "VALUES (?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))",
                (migration.version, migration.filename, migration.sha256),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        executed.append(migration.version)
    return executed
