"""Independent, bounded logical probe for checkpoint creation and restore."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from my_data_hub.hashing import sha256_value

_RELATION = re.compile(r"^[a-z][a-z0-9_]{0,62}\.[a-z][a-z0-9_]{0,62}$")


def logical_probe_hash(*, schema_version: int, canonical_revision: int, row_counts: Mapping[str, int]) -> str:
    if schema_version < 1 or canonical_revision < 0:
        raise ValueError("restore probe revisions are invalid")
    normalized = {name: int(row_counts[name]) for name in sorted(row_counts)}
    if not normalized or any(not _RELATION.fullmatch(name) or count < 0 for name, count in normalized.items()):
        raise ValueError("restore probe row-count contract is invalid")
    return sha256_value(
        {
            "contract": "my-data-hub-logical-restore-probe.v1",
            "schema_version": schema_version,
            "canonical_revision": canonical_revision,
            "row_counts": normalized,
        }
    )


def collect_restore_probe(connection: Any, relations: tuple[str, ...]) -> dict[str, object]:
    """Read only explicit relations and return a deterministic equality receipt.

    Relation identifiers are syntax-validated and quoted with psycopg Identifier;
    callers cannot turn a manifest row-count key into SQL.
    """

    if not relations or len(relations) > 100 or len(relations) != len(set(relations)):
        raise ValueError("restore probe relation set is invalid")
    if any(not _RELATION.fullmatch(name) for name in relations):
        raise ValueError("restore probe relation name is not allowlisted syntax")
    from psycopg import sql

    with connection.transaction(), connection.cursor() as cursor:
        cursor.execute("SET TRANSACTION READ ONLY")
        cursor.execute("SET LOCAL statement_timeout='30s'")
        cursor.execute("SET LOCAL lock_timeout='3s'")
        schema_version = int(cursor.execute("SELECT max(revision) FROM migration.schema_migration").fetchone()[0])
        canonical_revision = int(
            cursor.execute(
                "SELECT canonical_revision FROM hub.canonical_state WHERE singleton=true"
            ).fetchone()[0]
        )
        counts: dict[str, int] = {}
        for name in sorted(relations):
            schema_name, relation_name = name.split(".", 1)
            query = sql.SQL("SELECT count(*) FROM {}.{}").format(
                sql.Identifier(schema_name), sql.Identifier(relation_name)
            )
            counts[name] = int(cursor.execute(query).fetchone()[0])
    return {
        "schema_version": schema_version,
        "canonical_revision": canonical_revision,
        "logical_hash_sha256": logical_probe_hash(
            schema_version=schema_version,
            canonical_revision=canonical_revision,
            row_counts=counts,
        ),
        "row_counts": counts,
    }
