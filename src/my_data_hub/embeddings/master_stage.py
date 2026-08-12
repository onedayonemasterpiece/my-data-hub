"""Gate K execution inside the ACTIVE Kaggle PostgreSQL master only."""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import Any
from uuid import UUID, uuid5

from my_data_hub.embeddings.blogger_documents import CanonicalBloggerDocument, build_compact_blogger_documents
from my_data_hub.embeddings.contracts import EmbeddingJob
from my_data_hub.embeddings.direct_plane import (
    EmbeddingLaunchMetadata, PostgresEmbeddingWorkerExchange, StagedEmbeddingBatch,
)
from my_data_hub.embeddings.documents import SearchDocument
from my_data_hub.embeddings.importer import EmbeddingImportReceipt, PostgresEmbeddingImporter
from my_data_hub.embeddings.models import BGE_M3, E5_MULTILINGUAL_BASE, EmbeddingModelContract
from my_data_hub.embeddings.production import (
    EXPECTED_BLOGGER_ROWS,
    WORKER_ASSETS,
    EmbeddingProductionRequest,
    EmbeddingProductionStageReceipt,
    embedding_worker_task_id,
)
from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.master_runtime.contracts import MasterIdentity

_QUERY_NAMESPACE = UUID("f2963298-c621-5786-9951-32f844a23aec")
_MODELS = (E5_MULTILINGUAL_BASE, BGE_M3)


class EmbeddingStageError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class EmbeddingStageContext:
    identity: MasterIdentity
    operation_id: UUID
    request: EmbeddingProductionRequest
    database_url: str
    remaining_seconds: float


@dataclass(frozen=True, slots=True)
class _PreparedModel:
    model: EmbeddingModelContract
    asset_index: int
    task_id: UUID
    jobs: tuple[EmbeddingJob, ...]
    query_job_key: str
    worker_source_sha256: str


def _query_job(request: EmbeddingProductionRequest, model: EmbeddingModelContract) -> EmbeddingJob:
    document = SearchDocument(
        document_id=uuid5(_QUERY_NAMESPACE, request.probe_query_sha256),
        representation_kind="blogger_search_query_v1",
        actor_kind="search_query",
        display_name=request.probe_query.strip(),
    )
    return EmbeddingJob.create(document=document, model=model, canonical_revision=request.blogger_canonical_revision)


def _load_documents(connection: Any, request: EmbeddingProductionRequest) -> tuple[CanonicalBloggerDocument, ...]:
    with connection.transaction(), connection.cursor() as cursor:
        revision = int(cursor.execute(
            "SELECT canonical_revision FROM hub.canonical_state WHERE singleton=true"
        ).fetchone()[0])
        if revision != request.blogger_canonical_revision:
            raise EmbeddingStageError("canonical revision differs from the verified blogger prerequisite")
        rows = cursor.execute(
            "SELECT blogger_id,display_name,actor_kind,public_description,geography_signal,project_id,"
            "public_accounts FROM region_talk.bloggers_ru_v1 ORDER BY blogger_id"
        ).fetchall()
        columns = (
            "blogger_id", "display_name", "actor_kind", "public_description", "geography_signal",
            "project_id", "public_accounts",
        )
    return build_compact_blogger_documents(
        (dict(zip(columns, row, strict=True)) for row in rows), expected_count=EXPECTED_BLOGGER_ROWS
    )


def _prepare(
    context: EmbeddingStageContext,
    documents: tuple[CanonicalBloggerDocument, ...],
) -> tuple[_PreparedModel, ...]:
    prepared: list[_PreparedModel] = []
    for index, model in enumerate(_MODELS):
        task_id = embedding_worker_task_id(context.request.request_id, model.exact_id)
        jobs = tuple(sorted((
            *(EmbeddingJob.create(
                document=item.document, model=model,
                canonical_revision=context.request.blogger_canonical_revision,
            ) for item in documents),
            _query_job(context.request, model),
        ), key=lambda item: item.job_key))
        prepared.append(_PreparedModel(
            model=model, asset_index=index, task_id=task_id, jobs=jobs,
            query_job_key=_query_job(context.request, model).job_key,
            worker_source_sha256=WORKER_ASSETS[index].primary_source_sha256,
        ))
    return tuple(prepared)


def _launch_metadata(context: EmbeddingStageContext, prepared: _PreparedModel) -> EmbeddingLaunchMetadata:
    payload = canonical_json_bytes({
        "schema_version": "embedding-jobs-batch.v1",
        "jobs": [job.model_dump(mode="json") for job in prepared.jobs],
    })
    return EmbeddingLaunchMetadata(
        schema_version="embedding-central-launch-metadata.v1",
        request_id=context.request.request_id,
        request_sha256=context.request.request_sha256,
        task_run_id=prepared.task_id,
        model_exact_id=prepared.model.exact_id,
        input_jobs_sha256=hashlib.sha256(payload).hexdigest(),
        job_count=len(prepared.jobs),
        worker_source_sha256=prepared.worker_source_sha256,
        worker_primary_source_sha256=prepared.worker_source_sha256,
        epoch=context.identity.epoch,
    )


def execute_embedding_production_stage(
    context: EmbeddingStageContext,
    *,
    connection: Any,
    exchange: PostgresEmbeddingWorkerExchange,
    runtime_client: Any,
    canonical_connection_factory: Callable[[], AbstractContextManager[Any]],
    lease_guard: Callable[[], None],
    importer: PostgresEmbeddingImporter | None = None,
) -> EmbeddingProductionStageReceipt:
    """Stage business bytes on the master and exchange only launch metadata with control."""

    if context.remaining_seconds < 10_200:
        raise EmbeddingStageError("ACTIVE lease lacks the bounded concurrent worker/checkpoint allocation")
    deadline = __import__("time").monotonic() + context.remaining_seconds - 60.0
    lease_guard()
    documents = _load_documents(connection, context.request)
    prepared_models = _prepare(context, documents)
    for prepared in prepared_models:
        metadata = _launch_metadata(context, prepared)
        exchange.stage(connection, StagedEmbeddingBatch(metadata=metadata, jobs=prepared.jobs))
        # Reuse CherryFlash's caller-stable event_uid semantics.  The envelope is
        # metadata only; documents, credentials and vectors never enter callbacks.
        runtime_client.emit_donor_envelope({
            "run_id": context.identity.run_id,
            "event": "job.claimed",
            "event_uid": f"embedding:{context.request.request_id}:{prepared.task_id}:claimed",
            "phase": "embedding_dispatch",
            "status": "claimed",
            "progress": metadata.model_dump(mode="json"),
        })

    import_engine = importer or PostgresEmbeddingImporter()
    workers: list[dict[str, Any]] = []
    imports: list[dict[str, Any]] = []
    query_vector_receipts: dict[str, dict[str, Any]] = {}
    actor_ids = {item.document.document_id: item.actor_id for item in documents}
    for prepared in prepared_models:
        metadata = _launch_metadata(context, prepared)
        manifest = exchange.wait_result(
            connection, request_id=context.request.request_id, task_run_id=prepared.task_id,
            expected_sha256=metadata.input_jobs_sha256, deadline=deadline, lease_guard=lease_guard,
        )
        query_results = [item for item in manifest.successful_results if item.job_key == prepared.query_job_key]
        if len(query_results) != 1:
            raise EmbeddingStageError("exact query vector is absent or duplicated")
        query_vector_receipts[prepared.model.exact_id] = {
            "query_sha256": context.request.probe_query_sha256,
            "vector_sha256": query_results[0].vector_sha256,
            "dimensions": query_results[0].dimensions,
        }
        lease_guard()
        with canonical_connection_factory() as canonical_connection:
            imported: EmbeddingImportReceipt = import_engine.import_manifest(
                canonical_connection, manifest=manifest, expected_run_id=prepared.task_id,
                jobs=prepared.jobs, actor_ids=actor_ids,
                ephemeral_job_keys=frozenset({prepared.query_job_key}),
                ephemeral_query_sha256=context.request.probe_query_sha256,
            )
        artifact_sha = hashlib.sha256(canonical_json_bytes(manifest.model_dump(mode="json"))).hexdigest()
        workers.append({
            "model_exact_id": prepared.model.exact_id, "task_run_id": str(prepared.task_id),
            "provider_status": "complete", "transport": "direct_active_master",
            "input_jobs_sha256": metadata.input_jobs_sha256, "artifact_sha256": artifact_sha,
            "artifact_id": str(manifest.artifact_id),
        })
        imports.append({
            "model_exact_id": prepared.model.exact_id, "artifact_id": str(imported.artifact_id),
            "run_id": str(prepared.task_id), "artifact_sha256": artifact_sha,
            "outbox_id": str(imported.outbox_id), "canonical_revision": imported.canonical_revision,
            "inserted_count": imported.inserted_count, "stale_count": imported.stale_count,
            "failed_count": imported.failed_count, "replayed": imported.replayed,
            "durability_state": imported.durability_state,
        })
        runtime_client.emit_donor_envelope({
            "run_id": context.identity.run_id,
            "event": "job.completed",
            "event_uid": f"embedding:{context.request.request_id}:{prepared.task_id}:completed",
            "phase": "embedding_import", "status": "completed",
            "progress": {"request_id": str(context.request.request_id),
                         "task_run_id": str(prepared.task_id),
                         "model_exact_id": prepared.model.exact_id,
                         "artifact_sha256": artifact_sha},
        })

    lease_guard()
    with connection.cursor() as cursor:
        coverage_rows = cursor.execute(
            "SELECT model.provider_model_id || '@' || model.exact_revision,count(document.search_document_id),"
            "count(document.search_document_id) FILTER (WHERE job.status='succeeded') "
            "FROM search.embedding_model model CROSS JOIN search.document document "
            "LEFT JOIN search.embedding_job job ON job.search_document_id=document.search_document_id "
            "AND job.model_id=model.model_id WHERE document.is_current "
            "GROUP BY model.model_key,model.provider_model_id,model.exact_revision ORDER BY model.model_key"
        ).fetchall()
        canonical_revision = int(cursor.execute(
            "SELECT canonical_revision FROM hub.canonical_state WHERE singleton=true"
        ).fetchone()[0])
    coverage = tuple({
        "model_exact_id": str(row[0]), "expected_documents": int(row[1]),
        "completed_documents": int(row[2]), "coverage": int(row[2]) / int(row[1]),
    } for row in coverage_rows)
    return EmbeddingProductionStageReceipt(
        request_id=context.request.request_id, request_sha256=context.request.request_sha256,
        master_instance_id=context.identity.master_instance_id, run_id=UUID(context.identity.run_id),
        epoch=context.identity.epoch, workers=tuple(workers), imports=tuple(imports), coverage=coverage,
        query_vector_receipts=query_vector_receipts, canonical_revision=canonical_revision,
    )
