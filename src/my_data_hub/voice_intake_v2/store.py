from __future__ import annotations

import json
import os
import shutil
import sqlite3
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

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
    transcription_request_uid: str
    summary_request_uid: str
    transcription_limiter: dict[str, Any]
    summary_limiter: dict[str, Any]
    model: str


class VoiceIntakeV2Store:
    """Small SQLite control ledger plus a private temporary audio spool."""

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
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS sessions (
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
                    retryable INTEGER NOT NULL DEFAULT 0,
                    retry_at REAL,
                    error_code TEXT,
                    reconciliation_required INTEGER NOT NULL DEFAULT 0,
                    lease_owner TEXT,
                    lease_until REAL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS chunks (
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
                );
                CREATE INDEX IF NOT EXISTS sessions_work_idx
                  ON sessions(state, lease_until, retry_at, updated_at);
                """
            )
        self._secure_database_files()

    @staticmethod
    def _canonical(value: dict[str, Any]) -> tuple[str, str]:
        import hashlib

        encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return encoded, hashlib.sha256(encoded.encode()).hexdigest()

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

    def record_chunk(self, receipt: ChunkReceipt) -> tuple[ChunkReceipt, StatusResponse]:
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
            complete = json.loads(row["complete_json"]) if row["complete_json"] else None
            inference_completed = int(row["transcript_json"] is not None) + int(row["summary_json"] is not None)
            return StatusResponse(
                session_id=session_id,
                state=row["state"],
                recording_finished=complete is not None,
                chunks_expected=complete["chunk_count"] if complete else None,
                chunks_received=aggregate["count"],
                bytes_received=aggregate["bytes"],
                recorded_audio_ms=complete["recorded_audio_ms"] if complete else None,
                auto_silence_skipped_ms=complete["auto_silence_skipped_ms"] if complete else None,
                inference_batches_completed=inference_completed,
                gemini_requests_completed=inference_completed,
                transcription_complete=row["transcript_json"] is not None,
                summary_complete=row["summary_json"] is not None,
                github_verified=bool(row["github_verified"]),
                server_audio_purged=bool(row["server_audio_purged"]),
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
            state = "normalizing" if row["transcript_json"] is None else (
                "summarizing" if row["summary_json"] is None else "publishing"
            )
            changed = connection.execute(
                """UPDATE sessions SET state=?,lease_owner=?,lease_until=?,updated_at=?
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

    def persist_summary(
        self, session_id: str, owner: str, value: dict[str, Any], request_uid: str,
        limiter: dict[str, Any],
    ) -> None:
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
            if row is None or not row["transcript_json"] or not row["summary_json"]:
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
            transcription_request_uid=row["transcript_request_uid"], summary_request_uid=row["summary_request_uid"],
            transcription_limiter=json.loads(row["transcript_limiter_json"]),
            summary_limiter=json.loads(row["summary_limiter_json"]),
            model=row["model"],
        )

    def persist_github_verified(self, session_id: str, owner: str, *, url: str, commit_sha: str) -> None:
        self._owned_update(
            session_id, owner,
            "github_url=?,github_commit_sha=?,github_verified=1,state='verifying'",
            (url, commit_sha),
        )

    def finish_purge(self, session_id: str, owner: str) -> None:
        self._owned_update(
            session_id, owner,
            "server_audio_purged=1,state='published_verified',lease_owner=NULL,lease_until=NULL",
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

    def purge_audio(self, session_id: str) -> None:
        directory = self.session_directory(session_id)
        for child in (directory / "chunks", directory / "normalized"):
            if child.exists():
                try:
                    shutil.rmtree(child)
                except OSError as exc:
                    raise StoreError("server_audio_purge_failed", status_code=500) from exc
            if child.exists():
                raise StoreError("server_audio_purge_failed", status_code=500)

    def reap_expired(self, ttl_seconds: int) -> int:
        """Retain every durable session until publication readback and purge.

        The configured TTL is an admission/operations retention floor, not
        authority to destroy recoverable recordings. Small reconciliation
        receipts may remain indefinitely; only exact GitHub readback permits
        audio deletion through :meth:`purge_audio`.
        """
        if ttl_seconds < 0:
            raise ValueError("ttl_seconds must be non-negative")
        return 0
