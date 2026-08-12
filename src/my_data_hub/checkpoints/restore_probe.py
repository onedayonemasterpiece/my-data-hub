"""Independent, bounded logical probe for checkpoint creation and restore."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from my_data_hub.hashing import sha256_value

_RELATION = re.compile(r"^[a-z][a-z0-9_]{0,62}\.[a-z][a-z0-9_]{0,62}$")
_SHA256 = re.compile(r"^[a-f0-9]{64}$")
_REQUIRED_EXTENSIONS = ("citext", "pg_trgm", "pgcrypto", "vector")


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
        postgres_version = str(cursor.execute("SHOW server_version").fetchone()[0])
        if not postgres_version.startswith("18."):
            raise ValueError("restore probe requires PostgreSQL 18")
        extension_rows = cursor.execute(
            "SELECT extname,extversion FROM pg_extension WHERE extname = ANY(%s) ORDER BY extname",
            (list(_REQUIRED_EXTENSIONS),),
        ).fetchall()
        extensions = {str(name): str(version) for name, version in extension_rows}
        if set(extensions) != set(_REQUIRED_EXTENSIONS) or any(not value for value in extensions.values()):
            raise ValueError("restore probe extensions are incomplete")
        migration_rows = cursor.execute(
            "SELECT version,filename,sha256 FROM hub_meta.schema_migration ORDER BY version"
        ).fetchall()
        migrations = [(int(version), str(filename), str(sha256)) for version, filename, sha256 in migration_rows]
        if (
            not migrations
            or [item[0] for item in migrations] != list(range(1, migrations[-1][0] + 1))
            or any(not filename or not _SHA256.fullmatch(sha256) for _, filename, sha256 in migrations)
        ):
            raise ValueError("restore probe migration history is not append-only contiguous")
        schema_version = migrations[-1][0]
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
        canonical_singletons = int(
            cursor.execute("SELECT count(*) FROM hub.canonical_state WHERE singleton=true").fetchone()[0]
        )
        epoch_singletons = int(
            cursor.execute("SELECT count(*) FROM master_control.epoch_state WHERE singleton=true").fetchone()[0]
        )
        invalid_constraints = int(
            cursor.execute(
                "SELECT count(*) FROM pg_constraint c JOIN pg_namespace n ON n.oid=c.connamespace "
                "WHERE n.nspname = ANY(%s) AND NOT c.convalidated",
                (["hub", "hub_meta", "master_control", "migration", "region_talk", "search"],),
            ).fetchone()[0]
        )
        if canonical_singletons != 1 or epoch_singletons != 1 or invalid_constraints != 0:
            raise ValueError("restore probe database invariants differ")
        vector_distance = float(
            cursor.execute("SELECT '[1,0,0]'::vector(3) <=> '[1,0,0]'::vector(3)").fetchone()[0]
        )
        if vector_distance != 0.0:
            raise ValueError("restore probe vector query differs")
    migration_history_sha256 = sha256_value(
        {"migrations": [
            {"version": version, "filename": filename, "sha256": sha256}
            for version, filename, sha256 in migrations
        ]}
    )
    return {
        "schema_version": schema_version,
        "canonical_revision": canonical_revision,
        "logical_hash_sha256": logical_probe_hash(
            schema_version=schema_version,
            canonical_revision=canonical_revision,
            row_counts=counts,
        ),
        "row_counts": counts,
        "postgres_version": postgres_version,
        "extensions": extensions,
        "migration_boundary": {
            "first_version": 1,
            "last_version": schema_version,
            "applied_count": len(migrations),
            "contiguous": True,
            "history_sha256": migration_history_sha256,
        },
        "database_invariants": {
            "canonical_state_singletons": canonical_singletons,
            "epoch_state_singletons": epoch_singletons,
            "unvalidated_constraints": invalid_constraints,
        },
        "vector_query": {
            "operator": "cosine_distance",
            "dimensions": 3,
            "distance": vector_distance,
        },
        "bounded_read_smoke": {
            "relation_count": len(counts),
            "total_rows": sum(counts.values()),
            "statement_timeout_ms": 30_000,
            "lock_timeout_ms": 3_000,
        },
    }
