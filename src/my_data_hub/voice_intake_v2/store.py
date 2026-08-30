from __future__ import annotations

import json
import os
import shutil
import sqlite3
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from .contracts import SessionCompleteRequest, SessionCreateRequest, StatusResponse


class StoreError(RuntimeError):
    def __init__(self, code: str, *, status_code: int = 409) -> None:
        super().__init__(code)
        self.code = code
        self.status_code = status_code


@dataclass(frozen=True, slots=True)
class ChunkReceipt:
    session_id: str
    chunk_index: int
    sha256: str
    duration_ms: int
    audio_start_ms: int
    audio_end_ms: int
    wall_start_ms: int
    wall_end_ms: int
    size_bytes: int
    path: str
    duplicate: bool = False


@dataclass(frozen=True, slots=True)
class ClaimedSession:
    session_id: str
    state: str
    lease_owner: str
    create: dict[str, Any]
    complete: dict[str, Any]
    terminology: dict[str, Any]
    transcript: dict[str, Any] | None
    summary: dict[str, Any] | None
    chunks: tuple[ChunkReceipt, ...]


@dataclass(frozen=True, slots=True)
class PublicationProjection:
    session_id: str
    create: dict[str, Any]
    complete: dict[str, Any]
    terminology: dict[str, Any]
    transport_chunks: tuple[dict[str, Any], ...]
    transcript: dict[str, Any]
    summary: dict[str, Any]
    transcription_request_uid: str | None
    content_verification_receipt_sha256: str
    summary_request_uid: str
    transcription_limiter: dict[str, Any]
    summary_limiter: dict[str, Any]
    model: str


@dataclass(frozen=True, slots=True)
class StoredSegmentReceipt:
    """Immutable provider-attempt evidence for one source chunk.

    Failed or ambiguous attempts deliberately carry no transcript value.  Only
    receipts marked ``accepted`` can participate in content verification.
    """

    session_id: str
    chunk_index: int
    source_sha256: str
    audio_start_ms: int
    audio_end_ms: int
    coverage_start_ms: int
    coverage_end_ms: int
    provider_request_uid: str
    finish_reason: str
    schema_version: str
    accepted: bool
    transcript: dict[str, Any] | None
    coverage: dict[str, Any]
    limiter: dict[str, Any]
    transcript_receipt_sha256: str | None = None
    inference_receipt_sha256: str | None = None


@dataclass(frozen=True, slots=True)
class VerificationState:
    publication_verified: bool
    content_verified: bool
    purge_authorized: bool
    audio_purged: bool
    legacy_unverified_purge: bool


@dataclass(frozen=True, slots=True)
class LegacyMigrationAudit:
    migration_version: str
    rows_examined: int
    rows_truncated: bool
    publication_verified_observed: int
    audio_purged_observed: int
    legacy_unverified_purge_observed: int
    long_transcript_rows_observed: int
    suspicious_long_transcript_rows_observed: int
    finish_coverage_evidence_rows_observed: int
    transcript_without_finish_coverage_evidence_rows_observed: int


class VoiceIntakeV2Store:
    """Small SQLite control ledger plus a private temporary audio spool."""

    _CONTENT_MIGRATION = "voice_v2_content_verification_v1"
    _AUDIT_LIMIT = 1_000

    def __init__(self, root: Path, *, clock: Callable[[], float] = time.time) -> None:
        self.root = root.resolve()
        self.db_path = self.root / "voice-intake-v2.sqlite3"
        self.sessions_root = self.root / "sessions"
        self._clock = clock
        self._prepare_root()
        self._migrate()

    def _prepare_root(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.sessions_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(self.root, 0o700)
        os.chmod(self.sessions_root, 0o700)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path, timeout=10, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=10000")
        return connection

    def _secure_database_files(self) -> None:
        for suffix in ("", "-wal", "-shm"):
            path = Path(f"{self.db_path}{suffix}")
            if path.exists():
                os.chmod(path, 0o600)

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
            self._secure_database_files()

    def _migrate(self) -> None:
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=FULL")
        with self._transaction() as connection:
            sessions_existed = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='sessions'"
            ).fetchone() is not None
            old_columns = (
                {row["name"] for row in connection.execute("PRAGMA table_info(sessions)")}
                if sessions_existed else set()
            )
            connection.execute(
                """CREATE TABLE IF NOT EXISTS sessions (
                    session_id TEXT PRIMARY KEY,
                    create_json TEXT NOT NULL,
                    create_sha256 TEXT NOT NULL,
                    terminology_json TEXT NOT NULL,
                    model TEXT NOT NULL,
                    state TEXT NOT NULL,
                    complete_json TEXT,
                    complete_sha256 TEXT,
                    transcript_json TEXT,
                    transcript_request_uid TEXT,
                    transcript_limiter_json TEXT,
                    summary_json TEXT,
                    summary_request_uid TEXT,
                    summary_limiter_json TEXT,
                    github_url TEXT,
                    github_commit_sha TEXT,
                    github_verified INTEGER NOT NULL DEFAULT 0,
                    server_audio_purged INTEGER NOT NULL DEFAULT 0,
                    publication_verified INTEGER NOT NULL DEFAULT 0,
                    content_verified INTEGER NOT NULL DEFAULT 0,
                    purge_authorized INTEGER NOT NULL DEFAULT 0,
                    audio_purged INTEGER NOT NULL DEFAULT 0,
                    legacy_unverified_purge INTEGER NOT NULL DEFAULT 0,
                    retryable INTEGER NOT NULL DEFAULT 0,
                    retry_at REAL,
                    error_code TEXT,
                    reconciliation_required INTEGER NOT NULL DEFAULT 0,
                    lease_owner TEXT,
                    lease_until REAL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                )"""
            )
            for column in (
                "publication_verified", "content_verified", "purge_authorized",
                "audio_purged", "legacy_unverified_purge",
            ):
                if column not in old_columns and sessions_existed:
                    connection.execute(
                        f"ALTER TABLE sessions ADD COLUMN {column} INTEGER NOT NULL DEFAULT 0"
                    )
            connection.execute(
                """CREATE TABLE IF NOT EXISTS chunks (
                    session_id TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
                    chunk_index INTEGER NOT NULL,
                    sha256 TEXT NOT NULL,
                    duration_ms INTEGER NOT NULL,
                    audio_start_ms INTEGER NOT NULL,
                    audio_end_ms INTEGER NOT NULL,
                    wall_start_ms INTEGER NOT NULL,
                    wall_end_ms INTEGER NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    path TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    PRIMARY KEY(session_id, chunk_index)
                )"""
            )
            connection.execute(
                """CREATE INDEX IF NOT EXISTS sessions_work_idx
                  ON sessions(state, lease_until, retry_at, updated_at)"""
            )
            connection.execute(
                """CREATE TABLE IF NOT EXISTS voice_v2_schema_migrations (
                    version TEXT PRIMARY KEY,
                    applied_at REAL NOT NULL
                )"""
            )
            connection.execute(
                """CREATE TABLE IF NOT EXISTS segment_inference_receipts (
                    session_id TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE RESTRICT,
                    chunk_index INTEGER NOT NULL,
                    provider_request_uid TEXT NOT NULL,
                    source_sha256 TEXT NOT NULL,
                    audio_start_ms INTEGER NOT NULL,
                    audio_end_ms INTEGER NOT NULL,
                    coverage_start_ms INTEGER NOT NULL,
                    coverage_end_ms INTEGER NOT NULL,
                    finish_reason TEXT NOT NULL,
                    schema_version TEXT NOT NULL,
                    accepted INTEGER NOT NULL,
                    transcript_json TEXT,
                    transcript_sha256 TEXT,
                    coverage_json TEXT NOT NULL,
                    limiter_json TEXT NOT NULL,
                    inference_receipt_sha256 TEXT,
                    receipt_sha256 TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    PRIMARY KEY(session_id, chunk_index, provider_request_uid),
                    CHECK(accepted IN (0,1)),
                    CHECK((accepted=1 AND transcript_json IS NOT NULL AND transcript_sha256 IS NOT NULL)
                       OR (accepted=0 AND transcript_json IS NULL AND transcript_sha256 IS NULL))
                )"""
            )
            connection.execute(
                """CREATE UNIQUE INDEX IF NOT EXISTS accepted_segment_receipt_idx
                   ON segment_inference_receipts(session_id,chunk_index) WHERE accepted=1"""
            )
            segment_columns = {
                row["name"]
                for row in connection.execute("PRAGMA table_info(segment_inference_receipts)")
            }
            if "inference_receipt_sha256" not in segment_columns:
                connection.execute(
                    "ALTER TABLE segment_inference_receipts "
                    "ADD COLUMN inference_receipt_sha256 TEXT"
                )
            connection.execute(
                """CREATE TABLE IF NOT EXISTS content_verification_receipts (
                    session_id TEXT PRIMARY KEY REFERENCES sessions(session_id) ON DELETE RESTRICT,
                    schema_version TEXT NOT NULL,
                    verifier_version TEXT NOT NULL,
                    source_manifest_sha256 TEXT NOT NULL,
                    transcript_sha256 TEXT NOT NULL,
                    segment_count INTEGER NOT NULL,
                    coverage_start_ms INTEGER NOT NULL,
                   coverage_end_ms INTEGER NOT NULL,
                   verification_json TEXT NOT NULL,
                    segment_receipts_sha256 TEXT NOT NULL,
                   receipt_sha256 TEXT NOT NULL UNIQUE,
                    created_at REAL NOT NULL
                )"""
            )
            content_columns = {
                row["name"]
                for row in connection.execute("PRAGMA table_info(content_verification_receipts)")
            }
            if "segment_receipts_sha256" not in content_columns:
                connection.execute(
                    "ALTER TABLE content_verification_receipts "
                    "ADD COLUMN segment_receipts_sha256 TEXT"
                )
            connection.execute(
                """CREATE TABLE IF NOT EXISTS purge_authorization_receipts (
                    session_id TEXT PRIMARY KEY REFERENCES sessions(session_id) ON DELETE RESTRICT,
                    policy_version TEXT NOT NULL,
                    content_receipt_sha256 TEXT NOT NULL,
                    publication_commit_sha TEXT NOT NULL,
                    receipt_sha256 TEXT NOT NULL UNIQUE,
                    created_at REAL NOT NULL
                )"""
            )
            connection.execute(
                """CREATE TABLE IF NOT EXISTS audio_purge_receipts (
                    session_id TEXT PRIMARY KEY REFERENCES sessions(session_id) ON DELETE RESTRICT,
                    authorization_receipt_sha256 TEXT NOT NULL,
                    chunks_absent INTEGER NOT NULL,
                    normalized_absent INTEGER NOT NULL,
                    receipt_sha256 TEXT NOT NULL UNIQUE,
                    created_at REAL NOT NULL,
                    CHECK(chunks_absent=1 AND normalized_absent=1)
                )"""
            )
            connection.execute(
                """CREATE TABLE IF NOT EXISTS legacy_content_migration_audit (
                    migration_version TEXT PRIMARY KEY,
                    rows_examined INTEGER NOT NULL,
                    rows_truncated INTEGER NOT NULL,
                    publication_verified_observed INTEGER NOT NULL,
                    audio_purged_observed INTEGER NOT NULL,
                    legacy_unverified_purge_observed INTEGER NOT NULL,
                    long_transcript_rows_observed INTEGER NOT NULL,
                    suspicious_long_transcript_rows_observed INTEGER NOT NULL,
                    finish_coverage_evidence_rows_observed INTEGER NOT NULL,
                    transcript_without_finish_coverage_evidence_rows_observed INTEGER NOT NULL,
                    created_at REAL NOT NULL
                )"""
            )
            audit_columns = {
                row["name"]
                for row in connection.execute("PRAGMA table_info(legacy_content_migration_audit)")
            }
            for column in (
                "long_transcript_rows_observed",
                "suspicious_long_transcript_rows_observed",
                "finish_coverage_evidence_rows_observed",
                "transcript_without_finish_coverage_evidence_rows_observed",
            ):
                if column not in audit_columns:
                    connection.execute(
                        f"ALTER TABLE legacy_content_migration_audit "
                        f"ADD COLUMN {column} INTEGER NOT NULL DEFAULT 0"
                    )
            for table in (
                "segment_inference_receipts", "content_verification_receipts",
                "purge_authorization_receipts", "audio_purge_receipts",
            ):
                connection.execute(
                    f"""CREATE TRIGGER IF NOT EXISTS {table}_immutable_update
                        BEFORE UPDATE ON {table} BEGIN
                          SELECT RAISE(ABORT, 'immutable_receipt');
                        END"""
                )
                connection.execute(
                    f"""CREATE TRIGGER IF NOT EXISTS {table}_immutable_delete
                        BEFORE DELETE ON {table} BEGIN
                          SELECT RAISE(ABORT, 'immutable_receipt');
                        END"""
                )

            migration = connection.execute(
                "SELECT 1 FROM voice_v2_schema_migrations WHERE version=?",
                (self._CONTENT_MIGRATION,),
            ).fetchone()
            if migration is None:
                # Existing publication and physical-deletion facts remain truthful,
                # but neither is evidence of content completeness or authorization.
                if sessions_existed:
                    sample = connection.execute(
                        """SELECT s.github_verified,s.server_audio_purged,
                                  s.complete_json,s.transcript_json,
                                  CASE WHEN EXISTS(
                                      SELECT 1 FROM chunks c WHERE c.session_id=s.session_id
                                  ) AND NOT EXISTS(
                                      SELECT 1 FROM chunks c
                                      WHERE c.session_id=s.session_id AND NOT EXISTS(
                                          SELECT 1 FROM segment_inference_receipts r
                                          WHERE r.session_id=c.session_id
                                            AND r.chunk_index=c.chunk_index
                                            AND r.accepted=1 AND r.finish_reason='STOP'
                                            AND r.source_sha256=c.sha256
                                            AND r.coverage_start_ms=c.audio_start_ms
                                            AND r.coverage_end_ms=c.audio_end_ms
                                      )
                                  ) THEN 1 ELSE 0 END AS finish_coverage_evidence
                           FROM sessions s ORDER BY s.rowid LIMIT ?""",
                        (self._AUDIT_LIMIT + 1,),
                    ).fetchall()
                    observed = sample[: self._AUDIT_LIMIT]
                    connection.execute(
                        "UPDATE sessions SET publication_verified=github_verified"
                    )
                    connection.execute(
                        """UPDATE sessions SET audio_purged=server_audio_purged,
                           legacy_unverified_purge=CASE WHEN server_audio_purged=1 THEN 1 ELSE 0 END"""
                    )
                    connection.execute(
                        """INSERT INTO legacy_content_migration_audit(
                           migration_version,rows_examined,rows_truncated,
                           publication_verified_observed,audio_purged_observed,
                           legacy_unverified_purge_observed,long_transcript_rows_observed,
                           suspicious_long_transcript_rows_observed,
                           finish_coverage_evidence_rows_observed,
                           transcript_without_finish_coverage_evidence_rows_observed,created_at
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                        (
                            self._CONTENT_MIGRATION, len(observed), int(len(sample) > self._AUDIT_LIMIT),
                            sum(int(row["github_verified"]) for row in observed),
                            sum(int(row["server_audio_purged"]) for row in observed),
                            sum(int(row["server_audio_purged"]) for row in observed),
                            sum(self._legacy_long_transcript_metrics(row)[0] for row in observed),
                            sum(self._legacy_long_transcript_metrics(row)[1] for row in observed),
                            sum(int(row["finish_coverage_evidence"]) for row in observed),
                            sum(
                                int(bool(row["transcript_json"]) and not row["finish_coverage_evidence"])
                                for row in observed
                            ),
                            self._clock(),
                        ),
                    )
                connection.execute(
                    "INSERT INTO voice_v2_schema_migrations(version,applied_at) VALUES(?,?)",
                    (self._CONTENT_MIGRATION, self._clock()),
                )
            connection.execute(
                """CREATE TRIGGER IF NOT EXISTS sessions_content_verified_guard
                   BEFORE UPDATE OF content_verified ON sessions
                   WHEN OLD.content_verified=0 AND NEW.content_verified=1
                     AND NOT EXISTS(
                       SELECT 1 FROM content_verification_receipts c
                       WHERE c.session_id=NEW.session_id
                     )
                   BEGIN SELECT RAISE(ABORT, 'content_verification_receipt_required'); END"""
            )
            connection.execute(
                """CREATE TRIGGER IF NOT EXISTS sessions_publication_verified_guard
                   BEFORE UPDATE OF publication_verified ON sessions
                   WHEN OLD.publication_verified=0 AND NEW.publication_verified=1
                     AND (NEW.github_verified!=1 OR NEW.github_url IS NULL OR NEW.github_commit_sha IS NULL)
                   BEGIN SELECT RAISE(ABORT, 'publication_receipt_required'); END"""
            )
            connection.execute(
                """CREATE TRIGGER IF NOT EXISTS sessions_purge_authorized_guard
                   BEFORE UPDATE OF purge_authorized ON sessions
                   WHEN OLD.purge_authorized=0 AND NEW.purge_authorized=1
                     AND NOT EXISTS(
                       SELECT 1 FROM purge_authorization_receipts a
                       JOIN content_verification_receipts c USING(session_id)
                       WHERE a.session_id=NEW.session_id
                         AND a.content_receipt_sha256=c.receipt_sha256
                         AND a.publication_commit_sha=NEW.github_commit_sha
                     )
                   BEGIN SELECT RAISE(ABORT, 'purge_authorization_receipt_required'); END"""
            )
            connection.execute(
                """CREATE TRIGGER IF NOT EXISTS sessions_audio_purged_guard
                   BEFORE UPDATE OF audio_purged,server_audio_purged ON sessions
                   WHEN (OLD.audio_purged=0 AND NEW.audio_purged=1)
                     OR (OLD.server_audio_purged=0 AND NEW.server_audio_purged=1)
                   BEGIN
                     SELECT CASE WHEN NOT(
                       NEW.content_verified=1 AND NEW.publication_verified=1
                       AND NEW.purge_authorized=1 AND NEW.audio_purged=1
                       AND NEW.server_audio_purged=1 AND EXISTS(
                         SELECT 1 FROM audio_purge_receipts p
                         JOIN purge_authorization_receipts a USING(session_id)
                         JOIN content_verification_receipts c USING(session_id)
                         WHERE p.session_id=NEW.session_id
                           AND p.authorization_receipt_sha256=a.receipt_sha256
                           AND a.content_receipt_sha256=c.receipt_sha256
                           AND a.publication_commit_sha=NEW.github_commit_sha
                           AND p.chunks_absent=1 AND p.normalized_absent=1
                       )
                     ) THEN RAISE(ABORT, 'audio_purge_receipt_required') END;
                   END"""
            )
            connection.execute(
                """CREATE TRIGGER IF NOT EXISTS sessions_published_terminal_guard
                   BEFORE UPDATE OF state ON sessions
                   WHEN OLD.state!='published_verified' AND NEW.state='published_verified'
                     AND (
                       NEW.content_verified!=1 OR NEW.publication_verified!=1
                       OR NEW.purge_authorized!=1 OR NEW.audio_purged!=1
                       OR NOT EXISTS(
                         SELECT 1 FROM audio_purge_receipts p WHERE p.session_id=NEW.session_id
                       )
                     )
                   BEGIN SELECT RAISE(ABORT, 'audio_purge_receipt_required'); END"""
            )

    @staticmethod
    def _canonical(value: dict[str, Any]) -> tuple[str, str]:
        import hashlib

        encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return encoded, hashlib.sha256(encoded.encode()).hexdigest()

    @staticmethod
    def _legacy_long_transcript_metrics(row: sqlite3.Row) -> tuple[int, int]:
        """Return bounded counters only; never expose identifiers or content."""
        if not row["complete_json"] or not row["transcript_json"]:
            return 0, 0
        try:
            duration_ms = int(json.loads(row["complete_json"])["recorded_audio_ms"])
            transcript = json.loads(row["transcript_json"])["transcript"]
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            return 0, 0
        if duration_ms < 10 * 60 * 1000 or not isinstance(transcript, str):
            return 0, 0
        alphanumeric = sum(character.isalnum() for character in transcript)
        suspicious = alphanumeric / max(1.0, duration_ms / 1000) < 0.75
        return 1, int(suspicious)

    @classmethod
    def _bounded_metadata(cls, value: dict[str, Any], *, code: str) -> tuple[str, str]:
        forbidden = {
            "audio", "authorization", "device_token", "raw", "raw_response",
            "summary", "terminology", "transcript",
        }

        def inspect(item: Any) -> None:
            if isinstance(item, dict):
                for key, nested in item.items():
                    if not isinstance(key, str) or len(key) > 64:
                        raise StoreError(code, status_code=422)
                    lowered = key.lower()
                    if lowered in forbidden or lowered.endswith(("_content", "_text")):
                        raise StoreError(code, status_code=422)
                    inspect(nested)
            elif isinstance(item, list):
                if len(item) > 64:
                    raise StoreError(code, status_code=422)
                for nested in item:
                    inspect(nested)
            elif not isinstance(item, (str, int, float, bool, type(None))):
                raise StoreError(code, status_code=422)

        inspect(value)
        encoded, digest = cls._canonical(value)
        if len(encoded.encode()) > 16 * 1024:
            raise StoreError(code, status_code=422)
        return encoded, digest

    def session_directory(self, session_id: str) -> Path:
        # Callers only pass IDs validated by the frozen Pydantic contract. Keep
        # the path assertion as a second boundary against traversal regressions.
        path = (self.sessions_root / session_id).resolve()
        if path.parent != self.sessions_root:
            raise StoreError("session_id_invalid", status_code=422)
        return path

    def create_session(
        self,
        request: SessionCreateRequest,
        *,
        terminology: dict[str, Any],
        model: str = "gemini-3.1-flash-lite",
    ) -> tuple[StatusResponse, bool]:
        payload = request.model_dump(mode="json")
        encoded, digest = self._canonical(payload)
        now = self._clock()
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT create_sha256 FROM sessions WHERE session_id=?", (request.session_id,)
            ).fetchone()
            if row is not None:
                if row["create_sha256"] != digest:
                    raise StoreError("session_metadata_conflict")
                return self.status(request.session_id, connection=connection), True
            required = {
                "status", "source_path", "schema_version", "source_commit_sha",
                "source_blob_sha", "prompt",
            }
            if not required.issubset(terminology) or terminology.get("status") != "current":
                raise StoreError("terminology_not_current", status_code=503)
            terminology_encoded = self._canonical(terminology)[0]
            connection.execute(
                """INSERT INTO sessions(
                    session_id, create_json, create_sha256, terminology_json, model,
                    state, created_at, updated_at
                ) VALUES(?,?,?,?,?,?,?,?)""",
                (request.session_id, encoded, digest, terminology_encoded, model, "receiving", now, now),
            )
        directory = self.session_directory(request.session_id)
        (directory / "chunks").mkdir(parents=True, exist_ok=True, mode=0o700)
        (directory / "normalized").mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(directory, 0o700)
        return self.status(request.session_id), False

    def existing_session(self, request: SessionCreateRequest) -> StatusResponse | None:
        digest = self._canonical(request.model_dump(mode="json"))[1]
        with self._connect() as connection:
            row = connection.execute(
                "SELECT create_sha256 FROM sessions WHERE session_id=?", (request.session_id,)
            ).fetchone()
        if row is None:
            return None
        if row["create_sha256"] != digest:
            raise StoreError("session_metadata_conflict")
        return self.status(request.session_id)

    def get_chunk(self, session_id: str, chunk_index: int) -> ChunkReceipt | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM chunks WHERE session_id=? AND chunk_index=?", (session_id, chunk_index)
            ).fetchone()
        return self._chunk(row) if row else None

    @staticmethod
    def _chunk(row: sqlite3.Row) -> ChunkReceipt:
        return ChunkReceipt(**{key: row[key] for key in ChunkReceipt.__dataclass_fields__ if key != "duplicate"})

    def record_chunk(
        self, receipt: ChunkReceipt, *, max_session_bytes: int | None = None
    ) -> tuple[ChunkReceipt, StatusResponse]:
        now = self._clock()
        with self._transaction() as connection:
            session = connection.execute(
                "SELECT state FROM sessions WHERE session_id=?", (receipt.session_id,)
            ).fetchone()
            if session is None:
                raise StoreError("session_not_created")
            existing = connection.execute(
                "SELECT * FROM chunks WHERE session_id=? AND chunk_index=?",
                (receipt.session_id, receipt.chunk_index),
            ).fetchone()
            if existing is not None:
                current = self._chunk(existing)
                comparable = (
                    "sha256", "duration_ms", "audio_start_ms", "audio_end_ms",
                    "wall_start_ms", "wall_end_ms", "size_bytes",
                )
                if any(getattr(current, key) != getattr(receipt, key) for key in comparable):
                    raise StoreError("chunk_conflict")
                return ChunkReceipt(
                    session_id=current.session_id, chunk_index=current.chunk_index,
                    sha256=current.sha256, duration_ms=current.duration_ms,
                    audio_start_ms=current.audio_start_ms, audio_end_ms=current.audio_end_ms,
                    wall_start_ms=current.wall_start_ms, wall_end_ms=current.wall_end_ms,
                    size_bytes=current.size_bytes, path=current.path, duplicate=True,
                ), self.status(receipt.session_id, connection=connection)
            if session["state"] != "receiving":
                raise StoreError("session_not_receiving")
            if max_session_bytes is not None:
                aggregate = connection.execute(
                    "SELECT COALESCE(SUM(size_bytes),0) AS bytes FROM chunks WHERE session_id=?",
                    (receipt.session_id,),
                ).fetchone()
                if aggregate["bytes"] + receipt.size_bytes > max_session_bytes:
                    raise StoreError("session_size_limit_exceeded", status_code=413)
            connection.execute(
                """INSERT INTO chunks(
                    session_id,chunk_index,sha256,duration_ms,audio_start_ms,audio_end_ms,
                    wall_start_ms,wall_end_ms,size_bytes,path,created_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    receipt.session_id, receipt.chunk_index, receipt.sha256, receipt.duration_ms,
                    receipt.audio_start_ms, receipt.audio_end_ms, receipt.wall_start_ms,
                    receipt.wall_end_ms, receipt.size_bytes, receipt.path, now,
                ),
            )
            connection.execute("UPDATE sessions SET updated_at=? WHERE session_id=?", (now, receipt.session_id))
            status = self.status(receipt.session_id, connection=connection)
        return receipt, status

    def complete(self, session_id: str, request: SessionCompleteRequest) -> tuple[StatusResponse, bool]:
        payload = request.model_dump(mode="json")
        encoded, digest = self._canonical(payload)
        now = self._clock()
        with self._transaction() as connection:
            session = connection.execute("SELECT * FROM sessions WHERE session_id=?", (session_id,)).fetchone()
            if session is None:
                raise StoreError("session_not_created")
            if session["complete_sha256"] is not None:
                if session["complete_sha256"] != digest:
                    raise StoreError("complete_manifest_conflict")
                # Repeating the same authenticated complete request is the
                # explicit retry signal. It never replays a durable prior
                # stage: claim() derives the next stage from its artifacts.
                if session["state"] == "retryable_error" and session["retryable"]:
                    connection.execute(
                        """UPDATE sessions SET state='queued',retryable=0,retry_at=NULL,
                           error_code=NULL,updated_at=? WHERE session_id=?""",
                        (now, session_id),
                    )
                return self.status(session_id, connection=connection), True
            create = json.loads(session["create_json"])
            ended = datetime.fromisoformat(request.ended_at)
            if (
                ended < datetime.fromisoformat(create["started_at"])
                or ended.utcoffset() != ended.astimezone(ZoneInfo(create["timezone"])).utcoffset()
            ):
                raise StoreError("complete_time_invalid", status_code=422)
            rows = connection.execute(
                "SELECT * FROM chunks WHERE session_id=? ORDER BY chunk_index", (session_id,)
            ).fetchall()
            if len(rows) != request.chunk_count or [row["chunk_index"] for row in rows] != list(
                range(request.chunk_count)
            ):
                raise StoreError("chunks_missing")
            for row, manifest in zip(rows, request.chunks, strict=True):
                for key in (
                    "chunk_index", "sha256", "duration_ms", "audio_start_ms", "audio_end_ms",
                    "wall_start_ms", "wall_end_ms",
                ):
                    if row[key] != getattr(manifest, key):
                        raise StoreError("complete_manifest_mismatch")
            connection.execute(
                """UPDATE sessions SET complete_json=?, complete_sha256=?, state='queued',
                    retryable=0,retry_at=NULL,error_code=NULL,reconciliation_required=0,
                    updated_at=? WHERE session_id=?""",
                (encoded, digest, now, session_id),
            )
            return self.status(session_id, connection=connection), False

    def status(self, session_id: str, *, connection: sqlite3.Connection | None = None) -> StatusResponse:
        owns = connection is None
        connection = connection or self._connect()
        try:
            row = connection.execute("SELECT * FROM sessions WHERE session_id=?", (session_id,)).fetchone()
            if row is None:
                raise StoreError("session_not_found", status_code=404)
            aggregate = connection.execute(
                "SELECT COUNT(*) AS count, COALESCE(SUM(size_bytes),0) AS bytes FROM chunks WHERE session_id=?",
                (session_id,),
            ).fetchone()
            segment_progress = connection.execute(
                """SELECT COUNT(*) AS attempts,
                          COALESCE(SUM(CASE WHEN accepted=1 THEN 1 ELSE 0 END),0) AS accepted,
                          COALESCE(SUM(CASE WHEN accepted=0 THEN 1 ELSE 0 END),0) AS failed
                   FROM segment_inference_receipts WHERE session_id=?""",
                (session_id,),
            ).fetchone()
            content_receipt = connection.execute(
                "SELECT 1 FROM content_verification_receipts WHERE session_id=?", (session_id,)
            ).fetchone()
            complete = json.loads(row["complete_json"]) if row["complete_json"] else None
            inference_completed = int(row["transcript_json"] is not None) + int(row["summary_json"] is not None)
            inference_total = 2
            transcription_complete = row["transcript_json"] is not None
            extended_status: dict[str, Any] = {}
            if "content_verified" in StatusResponse.model_fields:
                segment_total = complete["chunk_count"] if complete else 0
                content_verified = bool(row["content_verified"] and content_receipt is not None)
                audio_purged = bool(row["audio_purged"])
                inference_total = segment_total + 1 if complete else 0
                inference_completed = segment_progress["accepted"] + int(row["summary_json"] is not None)
                transcription_complete = content_verified
                extended_status = {
                    "transcription_segments_total": segment_total,
                    "transcription_segments_completed": segment_progress["accepted"],
                    "transcription_coverage_complete": content_verified,
                    "content_verification_status": (
                        "passed" if content_verified else "failed" if segment_progress["failed"] else "pending"
                    ),
                    "content_verified": content_verified,
                    "publication_verified": bool(row["publication_verified"]),
                    "purge_authorized": bool(row["purge_authorized"]),
                    "audio_purged": audio_purged,
                    "legacy_unverified_purge": bool(row["legacy_unverified_purge"]),
                    "client_audio_purge_allowed": bool(
                        content_verified and row["publication_verified"] and row["purge_authorized"]
                        and audio_purged
                    ),
                }
            return StatusResponse(
                session_id=session_id,
                state=row["state"],
                recording_finished=complete is not None,
                chunks_expected=complete["chunk_count"] if complete else None,
                chunks_received=aggregate["count"],
                bytes_received=aggregate["bytes"],
                recorded_audio_ms=complete["recorded_audio_ms"] if complete else None,
                auto_silence_skipped_ms=complete["auto_silence_skipped_ms"] if complete else None,
                inference_batches_total=inference_total,
                inference_batches_completed=inference_completed,
                gemini_requests_total=inference_total,
                gemini_requests_completed=inference_completed,
                transcription_complete=transcription_complete,
                summary_complete=row["summary_json"] is not None,
                # Frozen-client aliases intentionally expose only the new,
                # independently durable facts.
                github_verified=bool(row["publication_verified"]),
                server_audio_purged=bool(row["audio_purged"]),
                github_url=row["github_url"],
                github_commit_sha=row["github_commit_sha"],
                retryable=bool(row["retryable"]),
                retry_at=(
                    time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(row["retry_at"]))
                    if row["retry_at"] is not None else None
                ),
                error_code=row["error_code"],
                reconciliation_required=bool(row["reconciliation_required"]),
                transcription_request_uid=row["transcript_request_uid"],
                summary_request_uid=row["summary_request_uid"],
                transcription_limiter=(
                    json.loads(row["transcript_limiter_json"]) if row["transcript_limiter_json"] else None
                ),
                summary_limiter=(
                    json.loads(row["summary_limiter_json"]) if row["summary_limiter_json"] else None
                ),
                **extended_status,
            )
        finally:
            if owns:
                connection.close()

    def claim(self, owner: str, lease_seconds: int) -> ClaimedSession | None:
        now = self._clock()
        with self._transaction() as connection:
            row = connection.execute(
                """SELECT * FROM sessions
                   WHERE (state IN ('queued','normalizing','publishing','verifying')
                          OR (state='waiting_quota' AND retry_at<=?))
                     AND (lease_until IS NULL OR lease_until<?)
                   ORDER BY updated_at LIMIT 1""",
                (now, now),
            ).fetchone()
            if row is None:
                return None
            state = (
                "verifying" if row["publication_verified"] else
                "normalizing" if row["transcript_json"] is None else
                "summarizing" if row["summary_json"] is None else "publishing"
            )
            changed = connection.execute(
                """UPDATE sessions SET state=?,lease_owner=?,lease_until=?,retryable=0,
                   retry_at=NULL,error_code=NULL,reconciliation_required=0,updated_at=?
                   WHERE session_id=? AND (lease_until IS NULL OR lease_until<?)""",
                (state, owner, now + lease_seconds, now, row["session_id"], now),
            ).rowcount
            if changed != 1:
                return None
            row = connection.execute("SELECT * FROM sessions WHERE session_id=?", (row["session_id"],)).fetchone()
            chunks = tuple(
                self._chunk(item) for item in connection.execute(
                    "SELECT * FROM chunks WHERE session_id=? ORDER BY chunk_index", (row["session_id"],)
                ).fetchall()
            )
            return ClaimedSession(
                session_id=row["session_id"], state=state, lease_owner=owner,
                create=json.loads(row["create_json"]), complete=json.loads(row["complete_json"]),
                terminology=json.loads(row["terminology_json"]),
                transcript=json.loads(row["transcript_json"]) if row["transcript_json"] else None,
                summary=json.loads(row["summary_json"]) if row["summary_json"] else None,
                chunks=chunks,
            )

    def fence_ambiguous_inference(self) -> int:
        """Fail closed when a lease expired across an in-flight provider stage."""
        now = self._clock()
        with self._transaction() as connection:
            return connection.execute(
                """UPDATE sessions SET state='reconciliation_required',retryable=0,
                   error_code='provider_outcome_ambiguous',reconciliation_required=1,
                   lease_owner=NULL,lease_until=NULL,updated_at=?
                   WHERE state IN ('transcribing','summarizing') AND lease_until<?""",
                (now, now),
            ).rowcount

    def _owned_update(self, session_id: str, owner: str, sql: str, values: tuple[Any, ...]) -> None:
        now = self._clock()
        with self._transaction() as connection:
            changed = connection.execute(
                f"UPDATE sessions SET {sql}, updated_at=? WHERE session_id=? AND lease_owner=?",
                (*values, now, session_id, owner),
            ).rowcount
            if changed != 1:
                raise StoreError("worker_lease_lost")

    def set_state(self, session_id: str, owner: str, state: str) -> None:
        self._owned_update(session_id, owner, "state=?", (state,))

    def persist_transcript(
        self, session_id: str, owner: str, value: dict[str, Any], request_uid: str,
        limiter: dict[str, Any],
    ) -> None:
        self._owned_update(
            session_id, owner,
            "transcript_json=?,transcript_request_uid=?,transcript_limiter_json=?,state='summarizing'",
            (self._canonical(value)[0], request_uid, self._canonical(limiter)[0]),
        )

    def persist_segment_receipt(
        self, session_id: str, owner: str, receipt: StoredSegmentReceipt
    ) -> bool:
        """Persist one immutable provider attempt; return ``True`` for an exact replay."""
        if receipt.session_id != session_id:
            raise StoreError("segment_receipt_session_mismatch", status_code=422)
        if not receipt.provider_request_uid or len(receipt.provider_request_uid) > 128:
            raise StoreError("segment_receipt_invalid", status_code=422)
        if not receipt.finish_reason or len(receipt.finish_reason) > 64:
            raise StoreError("segment_receipt_invalid", status_code=422)
        if not receipt.schema_version or len(receipt.schema_version) > 64:
            raise StoreError("segment_receipt_invalid", status_code=422)
        if receipt.accepted and receipt.finish_reason != "STOP":
            raise StoreError("segment_finish_reason_not_success")
        if receipt.accepted and receipt.transcript is None:
            raise StoreError("segment_receipt_invalid", status_code=422)
        if not receipt.accepted and receipt.transcript is not None:
            raise StoreError("failed_segment_content_forbidden", status_code=422)
        if receipt.inference_receipt_sha256 is not None and (
            len(receipt.inference_receipt_sha256) != 64
            or any(character not in "0123456789abcdef" for character in receipt.inference_receipt_sha256)
        ):
            raise StoreError("segment_inference_receipt_invalid", status_code=422)
        coverage_json, _ = self._bounded_metadata(
            receipt.coverage, code="segment_coverage_metadata_invalid"
        )
        limiter_json, _ = self._bounded_metadata(
            receipt.limiter, code="segment_limiter_metadata_invalid"
        )
        transcript_json: str | None = None
        transcript_sha256: str | None = None
        if receipt.transcript is not None:
            transcript_json, transcript_sha256 = self._canonical(receipt.transcript)
            if (
                receipt.transcript_receipt_sha256 is not None
                and receipt.transcript_receipt_sha256 != transcript_sha256
            ):
                raise StoreError("segment_transcript_receipt_mismatch")
        payload = {
            "session_id": session_id,
            "chunk_index": receipt.chunk_index,
            "source_sha256": receipt.source_sha256,
            "audio_start_ms": receipt.audio_start_ms,
            "audio_end_ms": receipt.audio_end_ms,
            "coverage_start_ms": receipt.coverage_start_ms,
            "coverage_end_ms": receipt.coverage_end_ms,
            "provider_request_uid": receipt.provider_request_uid,
            "finish_reason": receipt.finish_reason,
            "schema_version": receipt.schema_version,
            "accepted": receipt.accepted,
            "transcript_sha256": transcript_sha256,
            "inference_receipt_sha256": receipt.inference_receipt_sha256,
            "coverage": receipt.coverage,
            "limiter": receipt.limiter,
        }
        receipt_sha256 = self._canonical(payload)[1]
        now = self._clock()
        with self._transaction() as connection:
            source = connection.execute(
                """SELECT c.*,s.lease_owner FROM chunks c JOIN sessions s USING(session_id)
                   WHERE c.session_id=? AND c.chunk_index=?""",
                (session_id, receipt.chunk_index),
            ).fetchone()
            if source is None:
                raise StoreError("segment_source_missing")
            if source["lease_owner"] != owner:
                raise StoreError("worker_lease_lost")
            if (
                source["sha256"] != receipt.source_sha256
                or source["audio_start_ms"] != receipt.audio_start_ms
                or source["audio_end_ms"] != receipt.audio_end_ms
                or receipt.coverage_start_ms < receipt.audio_start_ms
                or receipt.coverage_end_ms > receipt.audio_end_ms
                or receipt.coverage_end_ms < receipt.coverage_start_ms
            ):
                raise StoreError("segment_source_mismatch")
            existing = connection.execute(
                """SELECT receipt_sha256 FROM segment_inference_receipts
                   WHERE session_id=? AND chunk_index=? AND provider_request_uid=?""",
                (session_id, receipt.chunk_index, receipt.provider_request_uid),
            ).fetchone()
            if existing is not None:
                if existing["receipt_sha256"] != receipt_sha256:
                    raise StoreError("segment_receipt_conflict")
                return True
            try:
                connection.execute(
                    """INSERT INTO segment_inference_receipts(
                       session_id,chunk_index,provider_request_uid,source_sha256,
                       audio_start_ms,audio_end_ms,coverage_start_ms,coverage_end_ms,
                       finish_reason,schema_version,accepted,transcript_json,transcript_sha256,
                       coverage_json,limiter_json,inference_receipt_sha256,
                       receipt_sha256,created_at
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        session_id, receipt.chunk_index, receipt.provider_request_uid,
                        receipt.source_sha256, receipt.audio_start_ms, receipt.audio_end_ms,
                        receipt.coverage_start_ms, receipt.coverage_end_ms, receipt.finish_reason,
                        receipt.schema_version, int(receipt.accepted), transcript_json,
                        transcript_sha256, coverage_json, limiter_json,
                        receipt.inference_receipt_sha256, receipt_sha256, now,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise StoreError("accepted_segment_receipt_conflict") from exc
            if receipt.accepted:
                # The accepted receipt and post-provider state transition are
                # one transaction. A crash after this point can safely reuse
                # the successful segment without replaying Gemini.
                connection.execute(
                    "UPDATE sessions SET state='normalizing',updated_at=? WHERE session_id=?",
                    (now, session_id),
                )
            else:
                # Keep a failed/ambiguous attempt in the in-flight state until
                # mark_error durably records its retry/reconciliation policy.
                # A crash in between must be fenced, never auto-replayed.
                connection.execute(
                    "UPDATE sessions SET updated_at=? WHERE session_id=?", (now, session_id)
                )
        return False

    def segment_receipts(
        self, session_id: str, *, accepted_only: bool = False
    ) -> tuple[StoredSegmentReceipt, ...]:
        where = " AND accepted=1" if accepted_only else ""
        with self._connect() as connection:
            rows = connection.execute(
                f"""SELECT * FROM segment_inference_receipts WHERE session_id=?{where}
                    ORDER BY chunk_index,created_at,provider_request_uid""",
                (session_id,),
            ).fetchall()
        return tuple(
            StoredSegmentReceipt(
                session_id=row["session_id"], chunk_index=row["chunk_index"],
                source_sha256=row["source_sha256"], audio_start_ms=row["audio_start_ms"],
                audio_end_ms=row["audio_end_ms"], coverage_start_ms=row["coverage_start_ms"],
                coverage_end_ms=row["coverage_end_ms"],
                provider_request_uid=row["provider_request_uid"], finish_reason=row["finish_reason"],
                schema_version=row["schema_version"], accepted=bool(row["accepted"]),
                transcript=json.loads(row["transcript_json"]) if row["transcript_json"] else None,
                coverage=json.loads(row["coverage_json"]), limiter=json.loads(row["limiter_json"]),
                transcript_receipt_sha256=row["transcript_sha256"],
                inference_receipt_sha256=row["inference_receipt_sha256"],
            )
            for row in rows
        )

    @staticmethod
    def _assemble_segments(receipts: tuple[StoredSegmentReceipt, ...]) -> dict[str, Any]:
        values = [receipt.transcript for receipt in receipts]
        if any(value is None or not isinstance(value.get("transcript"), str) for value in values):
            raise StoreError("segment_transcript_schema_invalid")
        languages = {value.get("language") for value in values if value is not None}
        if len(languages) != 1 or not next(iter(languages), None):
            raise StoreError("segment_language_ambiguous")
        uncertain: list[Any] = []
        for value in values:
            fragments = value.get("uncertain_fragments", []) if value is not None else []
            if not isinstance(fragments, list):
                raise StoreError("segment_transcript_schema_invalid")
            uncertain.extend(fragments)
        return {
            "transcript": "\n\n".join(value["transcript"].strip() for value in values if value),
            "language": next(iter(languages)),
            "uncertain_fragments": uncertain,
        }

    def persist_content_verification(
        self, session_id: str, owner: str, *, schema_version: str,
        verifier_version: str, verification: dict[str, Any], transcript: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Atomically assemble accepted segments and persist independent completeness evidence."""
        if not schema_version or len(schema_version) > 64 or not verifier_version or len(verifier_version) > 64:
            raise StoreError("content_verification_metadata_invalid", status_code=422)
        verification_json, _ = self._bounded_metadata(
            verification, code="content_verification_metadata_invalid"
        )
        now = self._clock()
        with self._transaction() as connection:
            session = connection.execute(
                "SELECT * FROM sessions WHERE session_id=?", (session_id,)
            ).fetchone()
            if session is None:
                raise StoreError("session_not_found", status_code=404)
            if session["lease_owner"] != owner:
                raise StoreError("worker_lease_lost")
            if not session["complete_sha256"]:
                raise StoreError("content_verification_transport_incomplete")
            source_rows = connection.execute(
                "SELECT * FROM chunks WHERE session_id=? ORDER BY chunk_index", (session_id,)
            ).fetchall()
            accepted_rows = connection.execute(
                """SELECT * FROM segment_inference_receipts
                   WHERE session_id=? AND accepted=1 ORDER BY chunk_index""",
                (session_id,),
            ).fetchall()
            if len(source_rows) != len(accepted_rows) or not source_rows:
                raise StoreError("content_coverage_incomplete")
            expected_start = source_rows[0]["audio_start_ms"]
            for expected_index, (source, accepted) in enumerate(
                zip(source_rows, accepted_rows, strict=True)
            ):
                if source["chunk_index"] != expected_index or accepted["chunk_index"] != expected_index:
                    raise StoreError("content_coverage_incomplete")
                if (
                    accepted["source_sha256"] != source["sha256"]
                    or accepted["audio_start_ms"] != source["audio_start_ms"]
                    or accepted["audio_end_ms"] != source["audio_end_ms"]
                    or accepted["coverage_start_ms"] != source["audio_start_ms"]
                    or accepted["coverage_end_ms"] != source["audio_end_ms"]
                    or source["audio_start_ms"] != expected_start
                    or accepted["finish_reason"] != "STOP"
                ):
                    raise StoreError("content_coverage_incomplete")
                expected_start = source["audio_end_ms"]
            receipts = tuple(
                StoredSegmentReceipt(
                    session_id=row["session_id"], chunk_index=row["chunk_index"],
                    source_sha256=row["source_sha256"], audio_start_ms=row["audio_start_ms"],
                    audio_end_ms=row["audio_end_ms"], coverage_start_ms=row["coverage_start_ms"],
                    coverage_end_ms=row["coverage_end_ms"],
                    provider_request_uid=row["provider_request_uid"], finish_reason=row["finish_reason"],
                    schema_version=row["schema_version"], accepted=True,
                    transcript=json.loads(row["transcript_json"]), coverage=json.loads(row["coverage_json"]),
                    limiter=json.loads(row["limiter_json"]),
                    transcript_receipt_sha256=row["transcript_sha256"],
                )
                for row in accepted_rows
            )
            assembled = self._assemble_segments(receipts)
            if transcript is not None and self._canonical(transcript)[1] != self._canonical(assembled)[1]:
                raise StoreError("aggregate_transcript_not_deterministic")
            transcript_json, transcript_sha256 = self._canonical(assembled)
            segment_receipts_sha256 = self._canonical(
                {"ordered_segment_receipts": [row["receipt_sha256"] for row in accepted_rows]}
            )[1]
            receipt_payload = {
                "session_id": session_id,
                "schema_version": schema_version,
                "verifier_version": verifier_version,
                "source_manifest_sha256": session["complete_sha256"],
                "transcript_sha256": transcript_sha256,
                "segment_receipts_sha256": segment_receipts_sha256,
                "segment_count": len(receipts),
                "coverage_start_ms": source_rows[0]["audio_start_ms"],
                "coverage_end_ms": source_rows[-1]["audio_end_ms"],
                "verification": verification,
            }
            receipt_sha256 = self._canonical(receipt_payload)[1]
            existing = connection.execute(
                """SELECT receipt_sha256,transcript_sha256,segment_receipts_sha256
                   FROM content_verification_receipts WHERE session_id=?""",
                (session_id,),
            ).fetchone()
            if existing is not None:
                if (
                    existing["receipt_sha256"] != receipt_sha256
                    or existing["transcript_sha256"] != transcript_sha256
                    or existing["segment_receipts_sha256"] != segment_receipts_sha256
                ):
                    raise StoreError("content_verification_receipt_conflict")
                return assembled
            connection.execute(
                """INSERT INTO content_verification_receipts(
                   session_id,schema_version,verifier_version,source_manifest_sha256,
                   transcript_sha256,segment_count,coverage_start_ms,coverage_end_ms,
                   verification_json,segment_receipts_sha256,receipt_sha256,created_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    session_id, schema_version, verifier_version, session["complete_sha256"],
                    transcript_sha256, len(receipts), source_rows[0]["audio_start_ms"],
                    source_rows[-1]["audio_end_ms"], verification_json,
                    segment_receipts_sha256, receipt_sha256, now,
                ),
            )
            connection.execute(
                """UPDATE sessions SET transcript_json=?,transcript_request_uid=NULL,
                   transcript_limiter_json=?,content_verified=1,state='summarizing',updated_at=?
                   WHERE session_id=?""",
                (
                    transcript_json,
                    self._canonical({"mode": "per_chunk", "segment_count": len(receipts)})[0],
                    now, session_id,
                ),
            )
            return assembled

    def persist_summary(
        self, session_id: str, owner: str, value: dict[str, Any], request_uid: str,
        limiter: dict[str, Any],
    ) -> None:
        with self._connect() as connection:
            verified = connection.execute(
                """SELECT 1 FROM sessions s JOIN content_verification_receipts c USING(session_id)
                   WHERE s.session_id=? AND s.content_verified=1""",
                (session_id,),
            ).fetchone()
        if verified is None:
            raise StoreError("content_not_verified")
        self._owned_update(
            session_id, owner,
            "summary_json=?,summary_request_uid=?,summary_limiter_json=?,state='publishing'",
            (self._canonical(value)[0], request_uid, self._canonical(limiter)[0]),
        )

    def publication_projection(self, session_id: str, owner: str) -> PublicationProjection:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM sessions WHERE session_id=? AND lease_owner=?", (session_id, owner)
            ).fetchone()
            content = connection.execute(
                """SELECT receipt_sha256 FROM content_verification_receipts
                   WHERE session_id=?""",
                (session_id,),
            ).fetchone()
            if (
                row is None or not row["transcript_json"] or not row["summary_json"]
                or not row["content_verified"] or content is None
            ):
                raise StoreError("publication_projection_incomplete")
            chunks = connection.execute(
                "SELECT chunk_index,sha256,duration_ms FROM chunks WHERE session_id=? ORDER BY chunk_index",
                (session_id,),
            ).fetchall()
        return PublicationProjection(
            session_id=session_id,
            create=json.loads(row["create_json"]), complete=json.loads(row["complete_json"]),
            terminology=json.loads(row["terminology_json"]),
            transport_chunks=tuple(dict(chunk) for chunk in chunks),
            transcript=json.loads(row["transcript_json"]), summary=json.loads(row["summary_json"]),
            transcription_request_uid=row["transcript_request_uid"],
            content_verification_receipt_sha256=content["receipt_sha256"],
            summary_request_uid=row["summary_request_uid"],
            transcription_limiter=json.loads(row["transcript_limiter_json"]),
            summary_limiter=json.loads(row["summary_limiter_json"]),
            model=row["model"],
        )

    def persist_github_verified(self, session_id: str, owner: str, *, url: str, commit_sha: str) -> None:
        self._owned_update(
            session_id, owner,
            "github_url=?,github_commit_sha=?,github_verified=1,publication_verified=1,state='verifying'",
            (url, commit_sha),
        )

    def authorize_purge(
        self, session_id: str, owner: str, *, policy_version: str
    ) -> bool:
        """Durably authorize deletion only after independent content and publication receipts."""
        if not policy_version or len(policy_version) > 64:
            raise StoreError("purge_policy_invalid", status_code=422)
        now = self._clock()
        with self._transaction() as connection:
            session = connection.execute(
                "SELECT * FROM sessions WHERE session_id=?", (session_id,)
            ).fetchone()
            content = connection.execute(
                "SELECT * FROM content_verification_receipts WHERE session_id=?", (session_id,)
            ).fetchone()
            if session is None:
                raise StoreError("session_not_found", status_code=404)
            if session["lease_owner"] != owner:
                raise StoreError("worker_lease_lost")
            if (
                not session["content_verified"] or content is None
                or not session["publication_verified"] or not session["github_verified"]
                or not session["github_commit_sha"]
            ):
                raise StoreError("purge_not_authorized")
            payload = {
                "session_id": session_id,
                "policy_version": policy_version,
                "content_receipt_sha256": content["receipt_sha256"],
                "publication_commit_sha": session["github_commit_sha"],
            }
            receipt_sha256 = self._canonical(payload)[1]
            existing = connection.execute(
                "SELECT receipt_sha256 FROM purge_authorization_receipts WHERE session_id=?",
                (session_id,),
            ).fetchone()
            if existing is not None:
                if existing["receipt_sha256"] != receipt_sha256:
                    raise StoreError("purge_authorization_conflict")
                return True
            chunk_paths = connection.execute(
                "SELECT path FROM chunks WHERE session_id=? ORDER BY chunk_index", (session_id,)
            ).fetchall()
            if not chunk_paths or any(not Path(row["path"]).is_file() for row in chunk_paths):
                raise StoreError("source_audio_missing")
            connection.execute(
                """INSERT INTO purge_authorization_receipts(
                   session_id,policy_version,content_receipt_sha256,publication_commit_sha,
                   receipt_sha256,created_at
                ) VALUES(?,?,?,?,?,?)""",
                (
                    session_id, policy_version, content["receipt_sha256"],
                    session["github_commit_sha"], receipt_sha256, now,
                ),
            )
            connection.execute(
                "UPDATE sessions SET purge_authorized=1,updated_at=? WHERE session_id=?",
                (now, session_id),
            )
            return False

    def finish_purge(self, session_id: str, owner: str) -> None:
        directory = self.session_directory(session_id)
        with self._connect() as connection:
            evidence = connection.execute(
                """SELECT 1 FROM sessions s JOIN audio_purge_receipts p USING(session_id)
                   WHERE s.session_id=? AND s.content_verified=1 AND s.publication_verified=1
                     AND s.purge_authorized=1 AND s.audio_purged=1""",
                (session_id,),
            ).fetchone()
        if evidence is None or (directory / "chunks").exists() or (directory / "normalized").exists():
            raise StoreError("audio_purge_not_verified")
        self._owned_update(
            session_id, owner,
            "state='published_verified',lease_owner=NULL,lease_until=NULL",
            (),
        )

    def mark_error(
        self, session_id: str, owner: str, *, code: str, retryable: bool,
        retry_at: float | None = None, reconciliation_required: bool = False,
    ) -> None:
        state = "reconciliation_required" if reconciliation_required else (
            "waiting_quota" if retryable and retry_at is not None else "retryable_error"
        )
        self._owned_update(
            session_id, owner,
            "state=?,retryable=?,retry_at=?,error_code=?,reconciliation_required=?,lease_owner=NULL,lease_until=NULL",
            (state, int(retryable), retry_at, code, int(reconciliation_required)),
        )

    def verification_state(self, session_id: str) -> VerificationState:
        with self._connect() as connection:
            row = connection.execute(
                """SELECT publication_verified,content_verified,purge_authorized,
                          audio_purged,legacy_unverified_purge
                   FROM sessions WHERE session_id=?""",
                (session_id,),
            ).fetchone()
        if row is None:
            raise StoreError("session_not_found", status_code=404)
        return VerificationState(
            publication_verified=bool(row["publication_verified"]),
            content_verified=bool(row["content_verified"]),
            purge_authorized=bool(row["purge_authorized"]),
            audio_purged=bool(row["audio_purged"]),
            legacy_unverified_purge=bool(row["legacy_unverified_purge"]),
        )

    def legacy_migration_audit(self) -> LegacyMigrationAudit | None:
        """Return bounded counters only; never identifiers or private content."""
        with self._connect() as connection:
            row = connection.execute(
                """SELECT migration_version,rows_examined,rows_truncated,
                          publication_verified_observed,audio_purged_observed,
                          legacy_unverified_purge_observed,
                          long_transcript_rows_observed,
                          suspicious_long_transcript_rows_observed,
                          finish_coverage_evidence_rows_observed,
                          transcript_without_finish_coverage_evidence_rows_observed
                   FROM legacy_content_migration_audit WHERE migration_version=?""",
                (self._CONTENT_MIGRATION,),
            ).fetchone()
        if row is None:
            return None
        return LegacyMigrationAudit(
            migration_version=row["migration_version"], rows_examined=row["rows_examined"],
            rows_truncated=bool(row["rows_truncated"]),
            publication_verified_observed=row["publication_verified_observed"],
            audio_purged_observed=row["audio_purged_observed"],
            legacy_unverified_purge_observed=row["legacy_unverified_purge_observed"],
            long_transcript_rows_observed=row["long_transcript_rows_observed"],
            suspicious_long_transcript_rows_observed=(
                row["suspicious_long_transcript_rows_observed"]
            ),
            finish_coverage_evidence_rows_observed=(
                row["finish_coverage_evidence_rows_observed"]
            ),
            transcript_without_finish_coverage_evidence_rows_observed=(
                row["transcript_without_finish_coverage_evidence_rows_observed"]
            ),
        )

    def purge_audio(self, session_id: str) -> None:
        """Delete audio only with a durable authorization chain.

        If the process crashed after filesystem deletion, the immutable
        authorization remains sufficient to verify absence and finish the
        purge receipt without replaying inference or publication.
        """
        with self._connect() as connection:
            row = connection.execute(
                """SELECT s.*,c.receipt_sha256 AS content_receipt_sha256,
                          a.receipt_sha256 AS authorization_receipt_sha256,
                          a.content_receipt_sha256 AS authorized_content_sha256,
                          a.publication_commit_sha AS authorized_commit_sha
                   FROM sessions s
                   LEFT JOIN content_verification_receipts c USING(session_id)
                   LEFT JOIN purge_authorization_receipts a USING(session_id)
                   WHERE s.session_id=?""",
                (session_id,),
            ).fetchone()
        if row is None:
            raise StoreError("session_not_found", status_code=404)
        if (
            not row["content_verified"] or not row["publication_verified"]
            or not row["purge_authorized"] or not row["content_receipt_sha256"]
            or not row["authorization_receipt_sha256"]
            or row["authorized_content_sha256"] != row["content_receipt_sha256"]
            or row["authorized_commit_sha"] != row["github_commit_sha"]
        ):
            raise StoreError("purge_not_authorized")
        directory = self.session_directory(session_id)
        for child in (directory / "chunks", directory / "normalized"):
            if child.exists():
                try:
                    shutil.rmtree(child)
                except OSError as exc:
                    raise StoreError("server_audio_purge_failed", status_code=500) from exc
            if child.exists():
                raise StoreError("server_audio_purge_failed", status_code=500)
        chunks_absent = not (directory / "chunks").exists()
        normalized_absent = not (directory / "normalized").exists()
        if not chunks_absent or not normalized_absent:
            raise StoreError("server_audio_purge_failed", status_code=500)
        payload = {
            "session_id": session_id,
            "authorization_receipt_sha256": row["authorization_receipt_sha256"],
            "chunks_absent": True,
            "normalized_absent": True,
        }
        receipt_sha256 = self._canonical(payload)[1]
        now = self._clock()
        with self._transaction() as connection:
            # Recheck the entire chain after the irreversible filesystem step.
            current = connection.execute(
                """SELECT s.content_verified,s.publication_verified,s.purge_authorized,
                          c.receipt_sha256 AS content_receipt_sha256,
                          a.receipt_sha256 AS authorization_receipt_sha256,
                          a.content_receipt_sha256 AS authorized_content_sha256,
                          a.publication_commit_sha,s.github_commit_sha
                   FROM sessions s
                   JOIN content_verification_receipts c USING(session_id)
                   JOIN purge_authorization_receipts a USING(session_id)
                   WHERE s.session_id=?""",
                (session_id,),
            ).fetchone()
            if (
                current is None or not current["content_verified"]
                or not current["publication_verified"] or not current["purge_authorized"]
                or current["authorization_receipt_sha256"] != row["authorization_receipt_sha256"]
                or current["authorized_content_sha256"] != current["content_receipt_sha256"]
                or current["publication_commit_sha"] != current["github_commit_sha"]
            ):
                raise StoreError("purge_authorization_lost", status_code=500)
            existing = connection.execute(
                "SELECT receipt_sha256 FROM audio_purge_receipts WHERE session_id=?", (session_id,)
            ).fetchone()
            if existing is not None and existing["receipt_sha256"] != receipt_sha256:
                raise StoreError("audio_purge_receipt_conflict")
            if existing is None:
                connection.execute(
                    """INSERT INTO audio_purge_receipts(
                       session_id,authorization_receipt_sha256,chunks_absent,
                       normalized_absent,receipt_sha256,created_at
                    ) VALUES(?,?,?,?,?,?)""",
                    (
                        session_id, row["authorization_receipt_sha256"], 1, 1,
                        receipt_sha256, now,
                    ),
                )
            connection.execute(
                """UPDATE sessions SET audio_purged=1,server_audio_purged=1,updated_at=?
                   WHERE session_id=? AND content_verified=1 AND publication_verified=1
                     AND purge_authorized=1""",
                (now, session_id),
            )

    def reap_expired(self, ttl_seconds: int) -> int:
        """Retain every durable session until publication readback and purge.

        The configured TTL is an admission/operations retention floor, not
        authority to destroy recoverable recordings. Small reconciliation
        receipts may remain indefinitely. Audio deletion requires the complete
        durable chain: content verification, publication readback, separate
        purge authorization, and verified physical absence in
        :meth:`purge_audio`.
        """
        if ttl_seconds < 0:
            raise ValueError("ttl_seconds must be non-negative")
        return 0
