from __future__ import annotations

from contextlib import nullcontext
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import pytest

from my_data_hub.embeddings.contracts import EmbeddingJob
from my_data_hub.embeddings.documents import SearchDocument
from my_data_hub.embeddings.importer import EmbeddingImportConflict, PostgresEmbeddingImporter
from my_data_hub.embeddings.models import BGE_M3, E5_MULTILINGUAL_BASE
from my_data_hub.embeddings.worker import EmbeddingWorker

RUN_ID = UUID("22222222-2222-4222-8222-222222222222")
DOCUMENT_ID = UUID("11111111-1111-4111-8111-111111111111")
JOB_ID = UUID("33333333-3333-4333-8333-333333333333")
OUTBOX_ID = UUID("44444444-4444-4444-8444-444444444444")
NOW = datetime(2026, 8, 11, tzinfo=UTC)


class Encoder:
    def __init__(self, dimensions: int = 768) -> None:
        self.dimensions = dimensions

    def encode(self, texts, **kwargs):  # type: ignore[no-untyped-def]
        return [(1.0, *([0.0] * (self.dimensions - 1))) for _ in texts]


def job(*, model=E5_MULTILINGUAL_BASE) -> EmbeddingJob:  # type: ignore[no-untyped-def]
    return EmbeddingJob.create(
        document=SearchDocument(
            document_id=DOCUMENT_ID,
            representation_kind="blogger_compact_v1",
            actor_kind="person",
            display_name="Анна",
        ),
        model=model,
        canonical_revision=7,
    )


def manifest(*, model=E5_MULTILINGUAL_BASE):  # type: ignore[no-untyped-def]
    expected = job(model=model)
    return EmbeddingWorker(model=model, encoder=Encoder(model.dimensions)).run(
        run_id=RUN_ID,
        jobs=(expected,),
        started_at=NOW,
        completed_at=NOW,
    )


class FakeCursor:
    def __init__(self, connection: FakeConnection, *, conflict_hash: str | None = None) -> None:
        self.connection = connection
        self.conflict_hash = conflict_hash
        self._row: Any = None

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, sql: str, params: tuple[object, ...] = ()) -> FakeCursor:
        normalized = " ".join(sql.split())
        self.connection.statements.append((normalized, params))
        expected = self.connection.expected_job
        if normalized == "SET LOCAL ROLE mdh_canonical_committer":
            self._row = None
        elif "FROM sync.external_outbox" in normalized:
            self._row = self.connection.outbox
        elif "FROM search.embedding_job AS job" in normalized:
            self._row = (
                JOB_ID,
                DOCUMENT_ID,
                expected.document.representation_kind,
                expected.input_hash,
                self.connection.job_status,
                self.connection.result_hash,
                self.connection.document_is_current,
                self.connection.source_revision,
                expected.document.document_hash,
                expected.document.compact_text(),
                expected.document.representation_kind,
            )
        elif normalized.startswith("SELECT result_sha256 FROM search.embedding_"):
            stored = self.conflict_hash or self.connection.vector_hash
            self._row = (stored,) if stored is not None else None
        elif normalized.startswith("INSERT INTO search.embedding_"):
            self.connection.vector_hash = str(params[3])
            self._row = None
        elif normalized.startswith("UPDATE search.embedding_job AS previous"):
            self._row = None
        elif normalized.startswith("UPDATE search.embedding_job SET status='succeeded'"):
            self.connection.job_status = "succeeded"
            self.connection.result_hash = str(params[0])
            self._row = (JOB_ID,)
        elif normalized.startswith("UPDATE search.embedding_job SET status='cancelled'"):
            self.connection.job_status = "cancelled"
            self._row = (JOB_ID,)
        elif normalized.startswith("SELECT canonical_revision FROM hub.canonical_state"):
            self._row = (self.connection.revision,)
        elif normalized.startswith("SELECT hub.advance_canonical_revision"):
            self.connection.revision += 1
            self._row = (self.connection.revision,)
        elif normalized.startswith("INSERT INTO sync.external_outbox"):
            payload = params[2].obj  # psycopg Jsonb wrapper
            self.connection.outbox = (OUTBOX_ID, self.connection.revision, payload)
            self._row = (OUTBOX_ID,)
        else:
            raise AssertionError(f"unexpected SQL: {normalized}")
        return self

    def fetchone(self):  # type: ignore[no-untyped-def]
        return self._row


class FakeConnection:
    def __init__(
        self,
        *,
        expected_job: EmbeddingJob | None = None,
        conflict_hash: str | None = None,
        document_is_current: bool = True,
        source_revision: int | None = None,
    ) -> None:
        self.expected_job = expected_job or job()
        self.conflict_hash = conflict_hash
        self.document_is_current = document_is_current
        self.source_revision = (
            self.expected_job.canonical_revision if source_revision is None else source_revision
        )
        self.statements: list[tuple[str, tuple[object, ...]]] = []
        self.outbox: tuple[UUID, int, dict[str, object]] | None = None
        self.vector_hash: str | None = None
        self.job_status = "running"
        self.result_hash: str | None = None
        self.revision = 10
        self.rollbacks = 0

    def transaction(self):  # type: ignore[no-untyped-def]
        return nullcontext()

    def cursor(self) -> FakeCursor:
        return FakeCursor(self, conflict_hash=self.conflict_hash)

    def rollback(self) -> None:
        self.rollbacks += 1


def test_import_is_transactional_uses_768_halfvec_and_exact_replay_is_read_only() -> None:
    connection = FakeConnection()
    artifact = manifest()
    importer = PostgresEmbeddingImporter()

    first = importer.import_manifest(
        connection,
        manifest=artifact,
        expected_run_id=RUN_ID,
        jobs=(job(),),
    )
    assert first.replayed is False
    assert first.inserted_count == 1
    assert first.canonical_revision == 11
    assert connection.vector_hash == artifact.successful_results[0].vector_sha256
    assert any("%s::halfvec" in sql for sql, _ in connection.statements)
    assert any("SELECT hub.advance_canonical_revision" in sql for sql, _ in connection.statements)
    assert any("INSERT INTO sync.external_outbox" in sql for sql, _ in connection.statements)

    writes_before = sum(_is_write(sql) for sql, _ in connection.statements)
    replay = importer.import_manifest(
        connection,
        manifest=artifact,
        expected_run_id=RUN_ID,
        jobs=(job(),),
    )
    assert replay.replayed is True
    assert replay.canonical_revision == first.canonical_revision
    assert sum(_is_write(sql) for sql, _ in connection.statements) == writes_before


def test_conflicting_immutable_vector_rolls_back_before_revision_or_outbox() -> None:
    connection = FakeConnection(conflict_hash="f" * 64)
    with pytest.raises(EmbeddingImportConflict, match="immutable vector conflict"):
        PostgresEmbeddingImporter().import_manifest(
            connection,
            manifest=manifest(),
            expected_run_id=RUN_ID,
            jobs=(job(),),
        )
    assert connection.rollbacks == 1
    assert not any("advance_canonical_revision" in sql for sql, _ in connection.statements)
    assert not any("INSERT INTO sync.external_outbox" in sql for sql, _ in connection.statements)


def test_bge_import_routes_only_to_the_1024_halfvec_space() -> None:
    expected = job(model=BGE_M3)
    connection = FakeConnection(expected_job=expected)
    receipt = PostgresEmbeddingImporter().import_manifest(
        connection,
        manifest=manifest(model=BGE_M3),
        expected_run_id=RUN_ID,
        jobs=(expected,),
    )
    assert receipt.inserted_count == 1
    inserts = [sql for sql, _ in connection.statements if sql.startswith("INSERT INTO search.embedding_")]
    assert len(inserts) == 1
    assert inserts[0].startswith("INSERT INTO search.embedding_1024")
    assert "search.embedding_768" not in inserts[0]


def test_late_result_advances_job_receipt_as_stale_without_inserting_a_vector() -> None:
    expected = job()
    connection = FakeConnection(expected_job=expected, source_revision=8)
    receipt = PostgresEmbeddingImporter().import_manifest(
        connection,
        manifest=manifest(),
        expected_run_id=RUN_ID,
        jobs=(expected,),
    )
    assert receipt.inserted_count == 0
    assert receipt.stale_count == 1
    assert connection.job_status == "cancelled"
    assert connection.vector_hash is None
    assert receipt.canonical_revision == 11
    assert connection.outbox is not None


def test_import_rejects_run_or_job_input_contract_before_opening_transaction() -> None:
    artifact = manifest()
    connection = FakeConnection()
    with pytest.raises(EmbeddingImportConflict, match="run_id"):
        PostgresEmbeddingImporter().import_manifest(
            connection,
            manifest=artifact,
            expected_run_id=UUID("99999999-9999-4999-8999-999999999999"),
            jobs=(job(),),
        )
    assert connection.statements == []

    altered = job().model_copy(update={"input_hash": "0" * 64})
    with pytest.raises((EmbeddingImportConflict, ValueError), match="input_hash"):
        PostgresEmbeddingImporter().import_manifest(
            connection,
            manifest=artifact,
            expected_run_id=RUN_ID,
            jobs=(altered,),
        )
    assert connection.statements == []


def test_bge_runtime_resolves_the_exact_huggingface_snapshot() -> None:
    source = (
        __import__("pathlib").Path(__file__).resolve().parents[2]
        / "notebooks/templates/embedding_workers/bge_m3_runtime.py"
    ).read_text(encoding="utf-8")
    assert "snapshot_download" in source
    assert "revision=BGE_M3.revision" in source
    assert "str(snapshot_path), normalize_embeddings=True" in source
    assert "snapshot_path.name != BGE_M3.revision" in source


def _is_write(sql: str) -> bool:
    return sql.startswith(("INSERT ", "UPDATE ", "DELETE ")) or "advance_canonical_revision" in sql
