from __future__ import annotations

import hashlib
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest

from my_data_hub.embeddings.blogger_documents import CanonicalBloggerDocument
from my_data_hub.embeddings.contracts import EmbeddingJob
from my_data_hub.embeddings.documents import SearchDocument
from my_data_hub.embeddings.importer import EmbeddingImportReceipt
from my_data_hub.embeddings.master_stage import (
    EmbeddingStageContext,
    execute_embedding_production_stage,
)
from my_data_hub.embeddings.production import WORKER_ASSETS, EmbeddingProductionRequest
from my_data_hub.embeddings.worker import EmbeddingWorker
from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.master_runtime.contracts import MasterIdentity

ROOT = Path(__file__).resolve().parents[2]


class UnitEncoder:
    def __init__(self, dimensions: int) -> None:
        self.dimensions = dimensions

    def encode(self, texts, **kwargs):  # type: ignore[no-untyped-def]
        return [(1.0, *([0.0] * (self.dimensions - 1))) for _ in texts]


class FakeImporter:
    def __init__(self) -> None:
        self.calls = []
        self.connections = []

    def import_manifest(self, connection, **kwargs):  # type: ignore[no-untyped-def]
        self.calls.append(kwargs)
        self.connections.append(connection)
        manifest = kwargs["manifest"]
        return EmbeddingImportReceipt(
            artifact_id=manifest.artifact_id,
            outbox_id=UUID(f"20000000-0000-4000-8000-{len(self.calls):012d}"),
            canonical_revision=9 + len(self.calls),
            inserted_count=266,
            stale_count=0,
            failed_count=0,
            replayed=False,
        )


class FinalCursor:
    def __init__(self) -> None:
        self.query = ""

    def __enter__(self):
        return self

    def __exit__(self, *args):  # type: ignore[no-untyped-def]
        return None

    def execute(self, query, params=None):  # type: ignore[no-untyped-def]
        self.query = query
        return self

    def fetchall(self):
        return [
            (WORKER_ASSETS[0].model.exact_id, 266, 266),
            (WORKER_ASSETS[1].model.exact_id, 266, 266),
        ]

    def fetchone(self):
        return (11,)


class FinalConnection:
    def cursor(self):
        return FinalCursor()


def _request() -> EmbeddingProductionRequest:
    query = "калининград культура"
    return EmbeddingProductionRequest(
        request_id=UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
        idempotency_key_sha256="a" * 64,
        blogger_receipt_id=UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
        blogger_receipt_sha256="b" * 64,
        blogger_canonical_revision=9,
        blogger_checkpoint_id=UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc"),
        source_revision="d" * 40,
        probe_query=query,
        probe_query_sha256=hashlib.sha256(query.encode()).hexdigest(),
    )


def _documents() -> tuple[CanonicalBloggerDocument, ...]:
    return tuple(
        CanonicalBloggerDocument(
            actor_id=UUID(f"10000000-0000-4000-8000-{number:012d}"),
            document=SearchDocument(
                document_id=UUID(f"30000000-0000-4000-8000-{number:012d}"),
                actor_kind="person",
                display_name=f"Блогер {number}",
            ),
        )
        for number in range(1, 267)
    )


def _context(tmp_path: Path) -> EmbeddingStageContext:
    return EmbeddingStageContext(
        identity=MasterIdentity(
            UUID("40000000-0000-4000-8000-000000000001"),
            "50000000-0000-4000-8000-000000000001",
            9,
        ),
        operation_id=UUID("60000000-0000-4000-8000-000000000001"),
        request=_request(),
        database_url="postgresql://postgres@/hub?host=/kaggle/working/socket",
        remaining_seconds=10_300,
    )


def test_packaged_worker_assets_are_exact_generated_notebooks() -> None:
    assert (ROOT / "src/my_data_hub/embeddings/assets/e5-worker.json").read_bytes() == (
        ROOT / "notebooks/05-e5-blogger-embedding-worker/worker.ipynb"
    ).read_bytes()
    assert (ROOT / "src/my_data_hub/embeddings/assets/bge-worker.json").read_bytes() == (
        ROOT / "notebooks/06-bge-m3-blogger-embedding-worker/worker.ipynb"
    ).read_bytes()


def test_direct_stage_never_requires_or_reads_a_kaggle_credential(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("KAGGLE_API_TOKEN", raising=False)
    monkeypatch.setattr("my_data_hub.embeddings.master_stage._load_documents", lambda *_: _documents())

    class Exchange:
        def __init__(self) -> None:
            self.batches = []

        def stage(self, _connection, batch):  # type: ignore[no-untyped-def]
            self.batches.append(batch)

        def wait_result(self, _connection, **kwargs):  # type: ignore[no-untyped-def]
            batch = next(item for item in self.batches if item.metadata.task_run_id == kwargs["task_run_id"])
            now = datetime(2026, 8, 11, tzinfo=UTC)
            return EmbeddingWorker(
                model=batch.jobs[0].model, encoder=UnitEncoder(batch.jobs[0].model.dimensions)
            ).run(run_id=batch.metadata.task_run_id, jobs=batch.jobs, started_at=now, completed_at=now)

    class Runtime:
        def __init__(self) -> None:
            self.events = []

        def emit_donor_envelope(self, envelope):  # type: ignore[no-untyped-def]
            self.events.append(envelope)

    exchange = Exchange()
    runtime = Runtime()
    importer = FakeImporter()
    canonical_connections = [object(), object()]

    @contextmanager
    def canonical_connection_factory():  # type: ignore[no-untyped-def]
        yield canonical_connections.pop(0)

    receipt = execute_embedding_production_stage(
        _context(tmp_path), connection=FinalConnection(), exchange=exchange, runtime_client=runtime,
        importer=importer, canonical_connection_factory=canonical_connection_factory,
        lease_guard=lambda: None,
    )
    assert len(exchange.batches) == 2
    assert {event["event"] for event in runtime.events} == {"job.claimed", "job.completed"}
    assert all("jobs" not in event["progress"] and "vector" not in event["progress"] for event in runtime.events)
    assert len(receipt.imports) == 2


def test_launch_callback_is_metadata_only_and_stable_uid(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("my_data_hub.embeddings.master_stage._load_documents", lambda *_: _documents())
    from my_data_hub.embeddings.master_stage import _launch_metadata, _prepare

    prepared = _prepare(_context(tmp_path), _documents())
    metadata = [_launch_metadata(_context(tmp_path), item).model_dump(mode="json") for item in prepared]
    encoded = canonical_json_bytes(metadata)
    assert len(encoded) < 64 * 1024
    assert b"display_name" not in encoded
    assert b"public_description" not in encoded
    assert b"vector" not in encoded
    assert {item["job_count"] for item in metadata} == {267}
