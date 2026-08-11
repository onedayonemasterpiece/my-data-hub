"""Gate K execution inside the ACTIVE Kaggle PostgreSQL master only."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from importlib.resources import files as package_files
from pathlib import Path
from typing import Any
from uuid import UUID, uuid5

from my_data_hub.embeddings.blogger_documents import CanonicalBloggerDocument, build_compact_blogger_documents
from my_data_hub.embeddings.contracts import EmbeddingArtifactManifest, EmbeddingJob
from my_data_hub.embeddings.documents import SearchDocument
from my_data_hub.embeddings.importer import EmbeddingImportReceipt, PostgresEmbeddingImporter
from my_data_hub.embeddings.models import BGE_M3, E5_MULTILINGUAL_BASE, EmbeddingModelContract
from my_data_hub.embeddings.production import (
    EXPECTED_BLOGGER_ROWS,
    WORKER_ASSETS,
    EmbeddingProductionRequest,
    EmbeddingProductionStageReceipt,
    embedding_provider_authority,
)
from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.master_runtime.contracts import MasterIdentity
from my_data_hub.providers.kaggle.adapter import KaggleProviderAdapter, _canonical_notebook_source, mapping_sha256
from my_data_hub.providers.kaggle.contracts import (
    KernelState,
    MutationAction,
    ProviderEffectIntent,
)
from my_data_hub.providers.models import ControlClass

_NAMESPACE = UUID("ce45cd9b-511b-5b30-a3df-0b9a0b35a565")
_QUERY_NAMESPACE = UUID("f2963298-c621-5786-9951-32f844a23aec")
_MODELS = (E5_MULTILINGUAL_BASE, BGE_M3)
_MAX_ARTIFACT_BYTES = 32 * 1024 * 1024


class EmbeddingStageError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class EmbeddingStageContext:
    identity: MasterIdentity
    operation_id: UUID
    request: EmbeddingProductionRequest
    database_url: str
    wheel_path: Path
    wheel_sha256: str
    provider_owner: str
    remaining_seconds: float


@dataclass(frozen=True, slots=True)
class _PreparedModel:
    model: EmbeddingModelContract
    asset_index: int
    task_id: UUID
    dataset_ref: str
    notebook_ref: str
    jobs: tuple[EmbeddingJob, ...]
    query_job_key: str
    source: bytes
    source_sha256: str


def _requested_at(request_id: UUID) -> datetime:
    # Stable across process restarts; ProviderEffectIntent identity must not drift.
    return datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=request_id.int % (180 * 24 * 3600))


def _effect(request_id: UUID, suffix: str) -> UUID:
    return uuid5(_NAMESPACE, f"{request_id}:{suffix}")


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


def _render_source(asset_index: int, *, dataset_slug: str, task_id: UUID, wheel_sha256: str) -> bytes:
    name = "e5-worker.json" if asset_index == 0 else "bge-worker.json"
    body = json.loads(package_files("my_data_hub.embeddings.assets").joinpath(name).read_text(encoding="utf-8"))
    bootstrap = {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": (
            "import os\n"
            f"os.environ['MY_DATA_HUB_EMBEDDING_JOBS']='/kaggle/input/{dataset_slug}/embedding-jobs.json'\n"
            f"os.environ['MY_DATA_HUB_RUN_ID']='{task_id}'\n"
            f"os.environ['MY_DATA_HUB_WHEEL_PATH']='/kaggle/input/{dataset_slug}/my-data-hub.whl'\n"
            f"os.environ['MY_DATA_HUB_WHEEL_SHA256']='{wheel_sha256}'\n"
        ),
    }
    body["cells"].insert(0, bootstrap)
    return canonical_json_bytes(body)


def _prepare(
    context: EmbeddingStageContext,
    documents: tuple[CanonicalBloggerDocument, ...],
) -> tuple[_PreparedModel, ...]:
    authority = embedding_provider_authority(context.provider_owner, context.request.request_id)
    prepared: list[_PreparedModel] = []
    for index, model in enumerate(_MODELS):
        alias = "e5" if index == 0 else "bge"
        dataset_ref, task_id = authority[f"{alias}_input"]
        notebook_ref, notebook_task = authority[f"{alias}_worker"]
        if task_id != notebook_task:
            raise EmbeddingStageError("dataset and worker task identities differ")
        jobs = tuple(
            sorted(
                (
                    *(EmbeddingJob.create(
                        document=item.document,
                        model=model,
                        canonical_revision=context.request.blogger_canonical_revision,
                    ) for item in documents),
                    _query_job(context.request, model),
                ),
                key=lambda item: item.job_key,
            )
        )
        source = _render_source(
            index,
            dataset_slug=dataset_ref.split("/", 1)[1],
            task_id=task_id,
            wheel_sha256=context.wheel_sha256,
        )
        prepared.append(_PreparedModel(
            model=model,
            asset_index=index,
            task_id=task_id,
            dataset_ref=dataset_ref,
            notebook_ref=notebook_ref,
            jobs=jobs,
            query_job_key=_query_job(context.request, model).job_key,
            source=source,
            source_sha256=hashlib.sha256(_canonical_notebook_source(source, kernel_type="notebook")).hexdigest(),
        ))
    return tuple(prepared)


def _intent(
    context: EmbeddingStageContext,
    prepared: _PreparedModel,
    *,
    action: MutationAction,
    provider_ref: str,
    arguments: dict[str, Any],
    suffix: str,
) -> ProviderEffectIntent:
    return ProviderEffectIntent.create(
        operation_id=context.operation_id,
        effect_id=_effect(context.request.request_id, suffix),
        idempotency_key=f"embedding:{context.request.request_id}:{suffix}",
        task_id=prepared.task_id,
        action=action,
        provider_ref=provider_ref,
        arguments=arguments,
        requested_at=_requested_at(context.request.request_id),
    )


def execute_embedding_production_stage(
    context: EmbeddingStageContext,
    *,
    connection: Any,
    adapter: KaggleProviderAdapter,
    importer: PostgresEmbeddingImporter | None = None,
) -> EmbeddingProductionStageReceipt:
    """Dispatch both exact workers and import their exact artifacts transactionally."""

    if not os.environ.get("KAGGLE_API_TOKEN", "").strip():
        raise EmbeddingStageError("modern Kaggle token is absent")
    if not isinstance(adapter, KaggleProviderAdapter):
        raise EmbeddingStageError("Gate K requires the repository's single concrete Kaggle adapter")
    if context.remaining_seconds < 10_200:
        raise EmbeddingStageError("ACTIVE lease lacks the bounded concurrent worker/checkpoint allocation")
    active_deadline = adapter.monotonic() + context.remaining_seconds
    if context.wheel_path.is_symlink() or not context.wheel_path.is_file():
        raise EmbeddingStageError("exact runtime wheel is absent")
    wheel = context.wheel_path.read_bytes()
    if hashlib.sha256(wheel).hexdigest() != context.wheel_sha256:
        raise EmbeddingStageError("runtime wheel hash differs")

    # Corpus/token validation is complete before the first provider effect.
    documents = _load_documents(connection, context.request)
    prepared_models = _prepare(context, documents)
    launched: list[tuple[_PreparedModel, Any, Any, str]] = []
    for prepared in prepared_models:
        jobs_payload = canonical_json_bytes({
            "schema_version": "embedding-jobs-batch.v1",
            "jobs": [job.model_dump(mode="json") for job in prepared.jobs],
        })
        dataset_files = {"embedding-jobs.json": jobs_payload, "my-data-hub.whl": wheel}
        dataset_intent = _intent(
            context,
            prepared,
            action=MutationAction.CREATE_DATASET,
            provider_ref=prepared.dataset_ref,
            suffix=f"{prepared.asset_index}:dataset",
            arguments={
                "content_tree_sha256": mapping_sha256(dataset_files),
                "control_class": ControlClass.ORCHESTRATOR_PROTECTED.value,
                "disposable": False,
            },
        )
        dataset = adapter.create_private_dataset(
            intent=dataset_intent,
            files=dataset_files,
            title=prepared.dataset_ref.split("/", 1)[1],
            control_class=ControlClass.ORCHESTRATOR_PROTECTED,
            disposable=False,
        )
        dataset_source = f"{prepared.dataset_ref}/{dataset.identity.version}"
        notebook_intent = _intent(
            context,
            prepared,
            action=MutationAction.PUSH_NOTEBOOK,
            provider_ref=prepared.notebook_ref,
            suffix=f"{prepared.asset_index}:worker",
            arguments={
                "task_run_id": str(prepared.task_id),
                "source_sha256": prepared.source_sha256,
                "dataset_sources": (dataset_source,),
                "control_class": ControlClass.ORCHESTRATOR_PROTECTED.value,
                "disposable": False,
            },
        )
        notebook = adapter.reconcile_private_notebook_mutation(
            intent=notebook_intent,
            task_run_id=prepared.task_id,
            expected_source_sha256=prepared.source_sha256,
            dataset_sources=(dataset_source,),
            control_class=ControlClass.ORCHESTRATOR_PROTECTED,
            disposable=False,
        )
        if notebook is None:
            notebook = adapter.push_private_notebook(
                intent=notebook_intent,
                task_run_id=prepared.task_id,
                source=prepared.source,
                title=prepared.notebook_ref.split("/", 1)[1],
                code_file="worker.ipynb",
                kernel_type="notebook",
                language="python",
                control_class=ControlClass.ORCHESTRATOR_PROTECTED,
                disposable=False,
                dataset_sources=(dataset_source,),
                enable_internet=True,
                timeout_seconds=9_000,
            )
        launched.append((prepared, dataset, notebook, hashlib.sha256(jobs_payload).hexdigest()))

    import_engine = importer or PostgresEmbeddingImporter()
    workers: list[dict[str, Any]] = []
    imports: list[dict[str, Any]] = []
    query_vector_receipts: dict[str, dict[str, Any]] = {}
    actor_ids = {item.document.document_id: item.actor_id for item in documents}
    pending = {item[2].run.task_run_id: item[2].run for item in launched}
    poll_started = adapter.monotonic()
    poll_budget = min(9_000.0, active_deadline - poll_started - 60.0)
    if poll_budget <= 0:
        raise EmbeddingStageError("provider preparation exhausted the ACTIVE-stage deadline")
    for _poll in range(601):
        for task_id, run in tuple(pending.items()):
            observed = adapter.read_run_status(run)
            if observed.state is KernelState.COMPLETE:
                pending.pop(task_id)
            elif observed.state is KernelState.FAILED:
                raise EmbeddingStageError("embedding worker reached a failed provider terminal state")
        if not pending:
            break
        if adapter.monotonic() - poll_started + 15 > poll_budget:
            raise EmbeddingStageError("embedding workers exceeded the shared ACTIVE-stage deadline")
        adapter.sleep(15)
    if pending:
        raise EmbeddingStageError("embedding workers did not complete within bounded polling")
    for prepared, dataset, notebook, jobs_sha256 in launched:
        with tempfile.TemporaryDirectory(prefix="mdh-embedding-output-") as temporary:
            destination = Path(temporary)
            output = adapter.download_exact_run_output_file(
                notebook.run,
                destination=destination,
                file_name="embedding-result.json",
                max_bytes=_MAX_ARTIFACT_BYTES,
            )
            raw = (destination / "embedding-result.json").read_bytes()
        manifest = EmbeddingArtifactManifest.model_validate_json(raw)
        if raw != canonical_json_bytes(manifest.model_dump(mode="json")):
            raise EmbeddingStageError("embedding artifact is not canonical JSON")
        query_results = [item for item in manifest.successful_results if item.job_key == prepared.query_job_key]
        if len(query_results) != 1:
            raise EmbeddingStageError("exact query vector is absent or duplicated")
        query_vector_receipts[prepared.model.exact_id] = {
            "query_sha256": context.request.probe_query_sha256,
            "vector_sha256": query_results[0].vector_sha256,
            "dimensions": query_results[0].dimensions,
        }
        imported: EmbeddingImportReceipt = import_engine.import_manifest(
            connection,
            manifest=manifest,
            expected_run_id=prepared.task_id,
            jobs=prepared.jobs,
            actor_ids=actor_ids,
            ephemeral_job_keys=frozenset({prepared.query_job_key}),
            ephemeral_query_sha256=context.request.probe_query_sha256,
        )
        workers.append({
            "model_exact_id": prepared.model.exact_id,
            "task_run_id": str(prepared.task_id),
            "provider_ref": notebook.run.provider_ref,
            "provider_run_ref": notebook.run.provider_run_ref,
            "provider_kernel_id": notebook.run.provider_kernel_id,
            "source_version": notebook.run.source_version,
            "source_sha256": notebook.run.source_sha256,
            "primary_source_sha256": WORKER_ASSETS[prepared.asset_index].primary_source_sha256,
            "provider_status": "complete",
            "privacy": "private",
            "control_class": "orchestrator_protected",
            "output_tree_sha256": output.output_tree_sha256,
            "artifact_sha256": hashlib.sha256(raw).hexdigest(),
            "artifact_id": str(manifest.artifact_id),
            "artifact_run_id": str(manifest.run_id),
            "input_dataset": {
                "provider_ref": prepared.dataset_ref,
                "provider_version": dataset.identity.version,
                "package_sha256": dataset.identity.package_sha256,
                "jobs_sha256": jobs_sha256,
            },
        })
        imports.append({
            "model_exact_id": prepared.model.exact_id,
            "artifact_id": str(imported.artifact_id),
            "run_id": str(prepared.task_id),
            "artifact_sha256": hashlib.sha256(raw).hexdigest(),
            "outbox_id": str(imported.outbox_id),
            "canonical_revision": imported.canonical_revision,
            "inserted_count": imported.inserted_count,
            "stale_count": imported.stale_count,
            "failed_count": imported.failed_count,
            "replayed": imported.replayed,
            "durability_state": imported.durability_state,
        })

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
        request_id=context.request.request_id,
        request_sha256=context.request.request_sha256,
        master_instance_id=context.identity.master_instance_id,
        run_id=UUID(context.identity.run_id),
        epoch=context.identity.epoch,
        workers=tuple(workers),
        imports=tuple(imports),
        coverage=coverage,
        query_vector_receipts=query_vector_receipts,
        canonical_revision=canonical_revision,
    )
