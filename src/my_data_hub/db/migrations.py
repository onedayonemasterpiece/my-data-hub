from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


BOOTSTRAP_SQL = """
CREATE SCHEMA IF NOT EXISTS hub_meta;
CREATE TABLE IF NOT EXISTS hub_meta.schema_migration (
    version integer PRIMARY KEY,
    filename text NOT NULL UNIQUE,
    sha256 text NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now()
)
"""


class MigrationError(RuntimeError):
    """Migration contract or application failure."""


@dataclass(frozen=True, slots=True)
class Migration:
    version: int
    filename: str
    path: Path
    sha256: str


def discover_migrations(directory: Path) -> list[Migration]:
    migrations: list[Migration] = []
    seen_versions: set[int] = set()
    for path in sorted(directory.glob("*.sql")):
        prefix = path.name.split("_", 1)[0]
        if not prefix.isdigit():
            raise MigrationError(f"migration filename must start with digits: {path.name}")
        version = int(prefix)
        if version in seen_versions:
            raise MigrationError(f"duplicate migration version {version}")
        seen_versions.add(version)
        migrations.append(
            Migration(
                version=version,
                filename=path.name,
                path=path,
                sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
            )
        )
    if not migrations:
        raise MigrationError(f"no SQL migrations found in {directory}")
    expected = list(range(migrations[0].version, migrations[-1].version + 1))
    actual = [item.version for item in migrations]
    if actual != expected:
        raise MigrationError(f"migration versions must be contiguous: expected={expected}, actual={actual}")
    return migrations


def _connect(database_url: str):  # type: ignore[no-untyped-def]
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - deployment dependency
        raise MigrationError("psycopg is required for database commands") from exc
    return psycopg.connect(database_url)


def applied_migrations(database_url: str) -> dict[int, tuple[str, str]]:
    with _connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(BOOTSTRAP_SQL)
            cursor.execute(
                "SELECT version, filename, sha256 FROM hub_meta.schema_migration ORDER BY version"
            )
            rows = cursor.fetchall()
        connection.commit()
    return {int(row[0]): (str(row[1]), str(row[2])) for row in rows}


def validate_history(
    migrations: Iterable[Migration], applied: dict[int, tuple[str, str]]
) -> None:
    known = {migration.version: migration for migration in migrations}
    for version, (filename, checksum) in applied.items():
        migration = known.get(version)
        if migration is None:
            raise MigrationError(
                f"database contains migration {version} ({filename}) missing from repository"
            )
        if migration.filename != filename or migration.sha256 != checksum:
            raise MigrationError(
                f"applied migration {version} was modified: database={filename}/{checksum}, "
                f"repository={migration.filename}/{migration.sha256}"
            )


def migrate(database_url: str, directory: Path) -> list[Migration]:
    migrations = discover_migrations(directory)
    applied = applied_migrations(database_url)
    validate_history(migrations, applied)
    executed: list[Migration] = []
    for migration in migrations:
        if migration.version in applied:
            continue
        sql = migration.path.read_text(encoding="utf-8")
        with _connect(database_url) as connection:
            try:
                with connection.cursor() as cursor:
                    cursor.execute(BOOTSTRAP_SQL)
                    cursor.execute(sql)
                    cursor.execute(
                        """
                        INSERT INTO hub_meta.schema_migration (version, filename, sha256)
                        VALUES (%s, %s, %s)
                        """,
                        (migration.version, migration.filename, migration.sha256),
                    )
                    cursor.execute(
                        """
                        UPDATE hub.canonical_state
                        SET schema_revision = greatest(schema_revision, %s), updated_at = now()
                        WHERE singleton = true
                        """,
                        (migration.version,),
                    )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        executed.append(migration)
    return executed
