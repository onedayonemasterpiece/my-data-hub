from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class DatabaseHealth:
    ok: bool
    postgres_version: str
    schema_revision: int
    canonical_revision: int
    extensions: tuple[str, ...]
    findings: tuple[str, ...]


def verify_database(database_url: str) -> DatabaseHealth:
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("psycopg is required for database verification") from exc

    findings: list[str] = []
    with psycopg.connect(database_url) as connection, connection.cursor() as cursor:
        cursor.execute("SHOW server_version")
        version = str(cursor.fetchone()[0])
        cursor.execute(
            "SELECT extname FROM pg_extension "
            "WHERE extname IN ('pgcrypto', 'citext', 'vector', 'pg_trgm') "
            "ORDER BY extname"
        )
        extensions = tuple(str(row[0]) for row in cursor.fetchall())
        missing = sorted({"pgcrypto", "citext", "vector", "pg_trgm"} - set(extensions))
        if missing:
            findings.append("missing extensions: " + ", ".join(missing))
        cursor.execute(
            "SELECT schema_revision, canonical_revision FROM hub.canonical_state WHERE singleton = true"
        )
        row = cursor.fetchone()
        if row is None:
            findings.append("hub.canonical_state singleton is missing")
            schema_revision = -1
            canonical_revision = -1
        else:
            schema_revision = int(row[0])
            canonical_revision = int(row[1])
        cursor.execute("SELECT count(*) FROM hub.project WHERE slug = 'region-talk'")
        if int(cursor.fetchone()[0]) != 1:
            findings.append("region-talk project seed is missing or duplicated")
        cursor.execute(
            """
                SELECT count(*) FROM orchestration.pipeline_stage ps
                JOIN orchestration.pipeline p ON p.pipeline_id = ps.pipeline_id
                WHERE p.workload = 'region-talk' AND ps.stage_key = 'publication_dispatch' AND ps.enabled
                """
        )
        if int(cursor.fetchone()[0]) != 0:
            findings.append("publication_dispatch must remain disabled in bootstrap")
    return DatabaseHealth(
        ok=not findings,
        postgres_version=version,
        schema_revision=schema_revision,
        canonical_revision=canonical_revision,
        extensions=extensions,
        findings=tuple(findings),
    )
