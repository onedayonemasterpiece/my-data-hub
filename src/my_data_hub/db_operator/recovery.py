"""Durable recovery evidence provider for database-operator gates."""

from __future__ import annotations

from dataclasses import dataclass

from .policy import BackupState


@dataclass(frozen=True, slots=True)
class PostgresBackupStateProvider:
    database_url: str

    def __call__(self) -> BackupState:
        import psycopg

        with psycopg.connect(
            self.database_url, connect_timeout=3
        ) as connection, connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '1000ms'")
            cursor.execute(
                """
                SELECT evidence_id, manifest->'backup'->>'completed_at', readback_verified,
                       private_offhost, schema_revision, restore_verified,
                       manifest->'restore'->>'completed_at',
                       checkpoint.canonical_revision
                FROM recovery.evidence
                JOIN sync.checkpoint AS checkpoint
                  ON checkpoint.canonical_revision =
                     (manifest->'restore'->>'canonical_revision')::bigint
                 AND checkpoint.verified_readback_at IS NOT NULL
                 AND checkpoint.checkpoint_kind = 'portable_logical'
                 AND checkpoint.encrypted
                 AND checkpoint.locator = manifest->'off_host'->>'object_locator'
                 AND checkpoint.sha256 =
                     manifest->'backup'->>'encrypted_artifact_sha256'
                 AND checkpoint.manifest_sha256 = manifest->'backup'->>'manifest_sha256'
                 AND checkpoint.postgres_major =
                     (manifest->'restore'->>'postgres_major')::integer
                 AND checkpoint.extension_versions =
                     manifest->'restore'->'extension_versions'
                WHERE evidence_type = 'isolated_restore' AND status = 'passed'
                ORDER BY completed_at DESC, recorded_at DESC
                LIMIT 1
                """
            )
            row = cursor.fetchone()
        if row is None:
            raise RuntimeError("no passed isolated recovery evidence is available")
        restore_at = row[6]
        if not restore_at:
            raise RuntimeError("recovery evidence lacks restore completion time")
        from datetime import datetime

        return BackupState(
            evidence_revision=str(row[0]),
            completed_at=datetime.fromisoformat(str(row[1]).replace("Z", "+00:00")),
            readback_verified=bool(row[2]),
            offsite_available=bool(row[3]),
            schema_revision=int(row[4]),
            restore_drill_at=datetime.fromisoformat(str(restore_at).replace("Z", "+00:00")),
            restore_drill_succeeded=bool(row[5]),
            checkpoint_revision=int(row[7]) if row[7] is not None else None,
        )
