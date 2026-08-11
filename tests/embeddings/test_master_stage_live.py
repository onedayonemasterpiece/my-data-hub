from __future__ import annotations

import hashlib
import json
from contextlib import contextmanager
from datetime import UTC, datetime
from itertools import pairwise
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest

from my_data_hub.embeddings.blogger_documents import CanonicalBloggerDocument
from my_data_hub.embeddings.contracts import EmbeddingJob
from my_data_hub.embeddings.documents import SearchDocument
from my_data_hub.embeddings.importer import EmbeddingImportReceipt
from my_data_hub.embeddings.master_stage import (
    EmbeddingStageContext,
    EmbeddingStageError,
    execute_embedding_production_stage,
)
from my_data_hub.embeddings.production import WORKER_ASSETS, EmbeddingProductionRequest
from my_data_hub.embeddings.worker import EmbeddingWorker
from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.master_runtime.contracts import MasterIdentity
from my_data_hub.providers.kaggle.adapter import KaggleProviderAdapter
from my_data_hub.providers.kaggle.contracts import KaggleProviderIdentity, KernelState

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
    wheel = tmp_path / "runtime.whl"
    wheel.write_bytes(b"exact-wheel")
    return EmbeddingStageContext(
        identity=MasterIdentity(
            UUID("40000000-0000-4000-8000-000000000001"),
            "50000000-0000-4000-8000-000000000001",
            9,
        ),
        operation_id=UUID("60000000-0000-4000-8000-000000000001"),
        request=_request(),
        database_url="postgresql://postgres@/hub?host=/kaggle/working/socket",
        wheel_path=wheel,
        wheel_sha256=hashlib.sha256(b"exact-wheel").hexdigest(),
        provider_owner="owner",
        remaining_seconds=10_300,
    )


def test_packaged_worker_assets_are_exact_generated_notebooks() -> None:
    assert (ROOT / "src/my_data_hub/embeddings/assets/e5-worker.json").read_bytes() == (
        ROOT / "notebooks/05-e5-blogger-embedding-worker/worker.ipynb"
    ).read_bytes()
    assert (ROOT / "src/my_data_hub/embeddings/assets/bge-worker.json").read_bytes() == (
        ROOT / "notebooks/06-bge-m3-blogger-embedding-worker/worker.ipynb"
    ).read_bytes()


def test_modern_token_blocks_before_any_provider_effect(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("KAGGLE_API_TOKEN", raising=False)
    adapter = object.__new__(KaggleProviderAdapter)
    with pytest.raises(EmbeddingStageError, match="modern Kaggle token"):
        execute_embedding_production_stage(
            _context(tmp_path),
            connection=FinalConnection(),
            adapter=adapter,
            canonical_connection_factory=lambda: pytest.fail("no canonical writes expected"),
            lease_guard=lambda: None,
        )


def test_single_adapter_launches_both_workers_and_imports_exact_artifacts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("KAGGLE_API_TOKEN", "modern-token")
    monkeypatch.setattr("my_data_hub.embeddings.master_stage._load_documents", lambda *_: _documents())
    adapter = object.__new__(KaggleProviderAdapter)
    adapter.identity = KaggleProviderIdentity(username="owner")
    jobs_by_dataset = {}
    jobs_by_task = {}
    launches = []

    def create_private_dataset(**kwargs):  # type: ignore[no-untyped-def]
        payload = json.loads(kwargs["files"]["embedding-jobs.json"])
        jobs_by_dataset[kwargs["intent"].provider_ref] = tuple(
            EmbeddingJob.model_validate(item) for item in payload["jobs"]
        )
        return SimpleNamespace(identity=SimpleNamespace(version=1, package_sha256="a" * 64))

    def reconcile_private_notebook_mutation(**kwargs):  # type: ignore[no-untyped-def]
        return None

    def push_private_notebook(**kwargs):  # type: ignore[no-untyped-def]
        task_id = kwargs["task_run_id"]
        dataset_ref = kwargs["dataset_sources"][0].rsplit("/", 1)[0]
        jobs_by_task[task_id] = jobs_by_dataset[dataset_ref]
        launches.append(task_id)
        run = SimpleNamespace(
            task_run_id=task_id,
            provider_ref=kwargs["intent"].provider_ref,
            provider_run_ref=f"{kwargs['intent'].provider_ref}/1",
            provider_kernel_id=len(launches),
            source_version=1,
            source_sha256=kwargs["intent"].arguments_sha256,
        )
        return SimpleNamespace(run=run)

    def read_run_status(run):  # type: ignore[no-untyped-def]
        assert len(launches) == 2  # both kernels launch before either poll
        return SimpleNamespace(state=KernelState.COMPLETE)

    def download(run, *, destination, **kwargs):  # type: ignore[no-untyped-def]
        assert kwargs["file_name"] == "embedding-result.json"
        assert kwargs["max_bytes"] == 32 * 1024 * 1024
        jobs = jobs_by_task[run.task_run_id]
        model = jobs[0].model
        now = datetime(2026, 8, 11, tzinfo=UTC)
        manifest = EmbeddingWorker(model=model, encoder=UnitEncoder(model.dimensions)).run(
            run_id=run.task_run_id, jobs=jobs, started_at=now, completed_at=now
        )
        (destination / "embedding-result.json").write_bytes(
            canonical_json_bytes(manifest.model_dump(mode="json"))
        )
        return SimpleNamespace(output_tree_sha256="e" * 64)

    adapter.create_private_dataset = create_private_dataset
    adapter.reconcile_private_notebook_mutation = reconcile_private_notebook_mutation
    adapter.push_private_notebook = push_private_notebook
    adapter.monotonic = lambda: 0.0
    adapter.sleep = lambda _seconds: None
    adapter.read_run_status = read_run_status
    adapter.download_exact_run_output_file = download
    importer = FakeImporter()
    canonical_connections = [object(), object()]

    @contextmanager
    def canonical_connection_factory():  # type: ignore[no-untyped-def]
        yield canonical_connections.pop(0)

    receipt = execute_embedding_production_stage(
        _context(tmp_path),
        connection=FinalConnection(),
        adapter=adapter,
        importer=importer,
        canonical_connection_factory=canonical_connection_factory,
        lease_guard=lambda: None,
    )
    assert len(launches) == 2 and len(set(launches)) == 2
    assert len(importer.calls) == 2
    assert all(len(call["jobs"]) == 267 for call in importer.calls)
    assert all(len(call["ephemeral_job_keys"]) == 1 for call in importer.calls)
    assert len({id(connection) for connection in importer.connections}) == 2
    assert receipt.canonical_revision == 11
    assert all(row["completed_documents"] == 266 for row in receipt.coverage)
    assert {
        row["dimensions"] for row in receipt.query_vector_receipts.values()
    } == {768, 1024}


def test_provider_polling_longer_than_one_lease_remains_continuously_guarded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("KAGGLE_API_TOKEN", "modern-token")
    monkeypatch.setattr("my_data_hub.embeddings.master_stage._load_documents", lambda *_: _documents())
    adapter = object.__new__(KaggleProviderAdapter)
    adapter.identity = KaggleProviderIdentity(username="owner")
    clock = [0.0]
    guard_observations: list[float] = []
    jobs_by_dataset = {}
    jobs_by_task = {}

    def create_private_dataset(**kwargs):  # type: ignore[no-untyped-def]
        payload = json.loads(kwargs["files"]["embedding-jobs.json"])
        jobs_by_dataset[kwargs["intent"].provider_ref] = tuple(
            EmbeddingJob.model_validate(item) for item in payload["jobs"]
        )
        return SimpleNamespace(identity=SimpleNamespace(version=1, package_sha256="a" * 64))

    def push_private_notebook(**kwargs):  # type: ignore[no-untyped-def]
        task_id = kwargs["task_run_id"]
        dataset_ref = kwargs["dataset_sources"][0].rsplit("/", 1)[0]
        jobs_by_task[task_id] = jobs_by_dataset[dataset_ref]
        return SimpleNamespace(run=SimpleNamespace(
            task_run_id=task_id,
            provider_ref=kwargs["intent"].provider_ref,
            provider_run_ref=f"{kwargs['intent'].provider_ref}/1",
            provider_kernel_id=len(jobs_by_task),
            source_version=1,
            source_sha256=kwargs["intent"].arguments_sha256,
        ))

    def read_run_status(_run):  # type: ignore[no-untyped-def]
        return SimpleNamespace(
            state=KernelState.COMPLETE if clock[0] > 135.0 else KernelState.RUNNING
        )

    def download(run, *, destination, **_kwargs):  # type: ignore[no-untyped-def]
        jobs = jobs_by_task[run.task_run_id]
        model = jobs[0].model
        now = datetime(2026, 8, 11, tzinfo=UTC)
        manifest = EmbeddingWorker(model=model, encoder=UnitEncoder(model.dimensions)).run(
            run_id=run.task_run_id, jobs=jobs, started_at=now, completed_at=now
        )
        (destination / "embedding-result.json").write_bytes(
            canonical_json_bytes(manifest.model_dump(mode="json"))
        )
        return SimpleNamespace(output_tree_sha256="e" * 64)

    adapter.create_private_dataset = create_private_dataset
    adapter.reconcile_private_notebook_mutation = lambda **_kwargs: None
    adapter.push_private_notebook = push_private_notebook
    adapter.monotonic = lambda: clock[0]
    adapter.sleep = lambda seconds: clock.__setitem__(0, clock[0] + seconds)
    adapter.read_run_status = read_run_status
    adapter.download_exact_run_output_file = download

    @contextmanager
    def canonical_connection_factory():  # type: ignore[no-untyped-def]
        yield object()

    execute_embedding_production_stage(
        _context(tmp_path),
        connection=FinalConnection(),
        adapter=adapter,
        importer=FakeImporter(),
        canonical_connection_factory=canonical_connection_factory,
        lease_guard=lambda: guard_observations.append(clock[0]),
    )

    assert clock[0] > 120.0
    assert guard_observations[0] == 0.0
    assert guard_observations[-1] == clock[0]
    assert max(right - left for left, right in pairwise(guard_observations)) <= 15.0
