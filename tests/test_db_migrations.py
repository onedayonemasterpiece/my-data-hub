from __future__ import annotations

from pathlib import Path

import pytest

from my_data_hub.db.migrations import MigrationError, discover_migrations, validate_history

ROOT = Path(__file__).resolve().parents[1]


def test_repository_migrations_are_contiguous() -> None:
    migrations = discover_migrations(ROOT / "sql/migrations")
    assert [item.version for item in migrations] == list(range(1, 14))
    assert all(len(item.sha256) == 64 for item in migrations)


def test_migration_gap_is_rejected(tmp_path: Path) -> None:
    (tmp_path / "0001_one.sql").write_text("SELECT 1;", encoding="utf-8")
    (tmp_path / "0003_three.sql").write_text("SELECT 3;", encoding="utf-8")
    with pytest.raises(MigrationError, match="contiguous"):
        discover_migrations(tmp_path)


def test_applied_migration_checksum_drift_is_rejected() -> None:
    migrations = discover_migrations(ROOT / "sql/migrations")
    first = migrations[0]
    with pytest.raises(MigrationError, match="modified"):
        validate_history([first], {first.version: (first.filename, "0" * 64)})
