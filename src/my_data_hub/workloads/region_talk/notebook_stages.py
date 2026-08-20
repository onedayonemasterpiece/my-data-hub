"""Fail-closed execution contracts for isolated Region Talk stage notebooks.

The pure vector-fusion transform executes in-tree.  Model/media/editorial stages
execute only through an explicitly attached runtime which supplies its exact
producer identity.  Missing inputs or runtimes are retryable failures, never a
successful evidence receipt.
"""

from __future__ import annotations

import hashlib
import importlib
import os
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, model_validator

from my_data_hub.embeddings.models import BGE_M3, E5_MULTILINGUAL_BASE
from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.workloads.region_talk.transforms.evidence import (
    BGE_M3_CONTRACT,
    BGE_M3_MODEL_ID,
    E5_CONTRACT,
    E5_MODEL_ID,
    fuse_vector_evidence,
)
from my_data_hub.workloads.region_talk.transforms.models import (
    VectorEvidence,
    VectorFusionRequest,
)

from .stage_dispatch import (
    StageExecutionPayload,
    StageResultMetadata,
    StageWorkerDirectResultReceipt,
    StageWorkerDirectResultRequest,
    StageWorkerPayloadFetchRequest,
    StageWorkerStatus,
    StageWorkPayloadReceipt,
)
from .stage_execution import STAGE_BY_KEY, work_item_id


class RegionTalkStageRuntimeUnavailable(RuntimeError):
    """Exact contract was accepted but required verified runtime/input is absent."""

    code = "HEAVY_RUNTIME_NOT_ATTACHED"
    retryable = True


class LegacyStageNotebookPayload(BaseModel):
    """Pre-0027 fixture accepted only to preserve fail-closed replay tests."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: str
    stage_run_id: UUID
    candidate_revision: int = Field(ge=1)
    revision_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    reason: str = Field(min_length=1, max_length=200)
    publication_dispatch: bool
    notification_dispatch: bool

    @model_validator(mode="after")
    def exact_safe_payload(self) -> LegacyStageNotebookPayload:
        if self.schema_version != "region-talk-stage-work-payload.v1":
            raise ValueError("stage payload schema differs")
        if self.publication_dispatch or self.notification_dispatch:
            raise ValueError("stage payload cannot enable publication or notification")
        return self


StageNotebookPayload = StageExecutionPayload | LegacyStageNotebookPayload
_PAYLOAD_ADAPTER = TypeAdapter(StageNotebookPayload)


class StageNotebookWorkItem(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    work_item_id: UUID
    subject_type: str
    subject_id: UUID
    input_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    payload: dict[str, Any]


class AttachedStageRuntime(Protocol):
    """One exact runtime seam injected by a private stage Notebook."""

    @property
    def producer_exact_id(self) -> str: ...

    def execute(
        self,
        *,
        stage: str,
        contract_version: str,
        subject_id: UUID,
        input_fingerprint: str,
        payload: StageExecutionPayload,
    ) -> dict[str, Any]: ...


class DirectStageWorkerFunctions(Protocol):
    """Exact direct-master boundary available only inside a private worker."""

    def fetch_payload(
        self,
        *,
        worker_task_run_id: UUID,
        effect_id: UUID,
        request: StageWorkerPayloadFetchRequest,
    ) -> StageWorkPayloadReceipt: ...

    def submit_result(
        self,
        *,
        worker_task_run_id: UUID,
        effect_id: UUID,
        request: StageWorkerDirectResultRequest,
    ) -> StageWorkerDirectResultReceipt: ...


class DirectStageCredentialCheckpoint(Protocol):
    """Private Notebook rotation hook; no capability crosses control journals."""

    def __call__(
        self,
        functions: DirectStageWorkerFunctions,
        request: StageWorkerPayloadFetchRequest,
        *,
        phase: str,
    ) -> tuple[DirectStageWorkerFunctions, StageWorkerPayloadFetchRequest]: ...


def attached_stage_runtime_from_env(stage: str) -> AttachedStageRuntime | None:
    """Load one explicitly attached, execution-pin-owned runtime factory.

    The value is a Python ``module:factory`` reference.  It is intentionally
    read only inside the private worker process; the remote MCP has no path to
    set it and no provider credential is accepted by this contract.
    """

    raw = os.getenv("MY_DATA_HUB_REGION_TALK_STAGE_RUNTIME", "").strip()
    if not raw:
        return None
    if raw.count(":") != 1:
        raise ValueError("attached stage runtime must be module:factory")
    module_name, attribute = raw.split(":", 1)
    if (
        not module_name
        or not attribute.isidentifier()
        or any(not part.isidentifier() for part in module_name.split("."))
    ):
        raise ValueError("attached stage runtime reference is invalid")
    factory = getattr(importlib.import_module(module_name), attribute)
    runtime = factory(stage=stage)
    if not isinstance(getattr(runtime, "producer_exact_id", None), str) or not callable(
        getattr(runtime, "execute", None)
    ):
        raise ValueError("attached stage runtime does not implement the exact seam")
    return runtime


def stage_model_identity(stage: str) -> dict[str, Any]:
    if stage == "e5_embedding":
        return {
            "model_id": E5_MULTILINGUAL_BASE.model_key,
            "model_revision": E5_MULTILINGUAL_BASE.revision,
            "encoder_contract": E5_MULTILINGUAL_BASE.encoder_contract_version,
        }
    if stage == "bge_m3_embedding":
        return {
            "model_id": BGE_M3.model_key,
            "model_revision": BGE_M3.revision,
            "encoder_contract": BGE_M3.encoder_contract_version,
        }
    return {
        "model_id": None,
        "model_revision": None,
        "encoder_contract": None,
    }


def _producer_for(stage: str) -> str:
    identity = stage_model_identity(stage)
    if identity["model_id"]:
        return f"{identity['model_id']}@{identity['model_revision']}"
    return f"my-data-hub:{stage}@{STAGE_BY_KEY[stage].contract_version}"


def _metrics_from_upstream(payload: StageExecutionPayload, stage: str) -> dict[str, Any]:
    for upstream in payload.upstream_results:
        if upstream.stage == stage:
            metadata = StageResultMetadata.model_validate(upstream.result_metadata)
            if metadata.input_fingerprint != upstream.input_fingerprint:
                raise ValueError("upstream metadata input fingerprint differs")
            return metadata.metrics
    raise RegionTalkStageRuntimeUnavailable(f"{stage} upstream result is unavailable")


def _score_map(rows: Any, *, stage: str) -> dict[str, float]:
    if not isinstance(rows, list) or not rows:
        raise ValueError(f"{stage} score rows are absent")
    result: dict[str, float] = {}
    for item in rows:
        if not isinstance(item, dict) or item.get("stage") != stage:
            continue
        label = item.get("label")
        value = item.get("value")
        if (
            not isinstance(label, str)
            or not isinstance(value, (float, int))
            or isinstance(value, bool)
            or not 0 <= float(value) <= 1
            or label in result
        ):
            raise ValueError(f"{stage} score row is invalid")
        result[label] = round(float(value), 6)
    if not result:
        raise ValueError(f"{stage} scores are absent")
    return result


def _vector_fusion(payload: StageExecutionPayload) -> dict[str, Any]:
    data = payload.input_data
    if data.get("schema_version") != "region-talk-vector-fusion-input.v1":
        raise ValueError("vector fusion input schema differs")
    e5_metrics = _metrics_from_upstream(payload, "e5_embedding")
    bge_metrics = _metrics_from_upstream(payload, "bge_m3_embedding")
    rows = data.get("scores")
    e5_scores = _score_map(rows, stage="e5_embedding")
    bge_scores = _score_map(rows, stage="bge_m3_embedding")
    text_hash = str(e5_metrics.get("text_sha256", ""))
    if text_hash != bge_metrics.get("text_sha256"):
        raise ValueError("embedding text hashes differ")

    def evidence(metrics: dict[str, Any], *, contract: str, model: str, scores: dict[str, float]) -> VectorEvidence:
        return VectorEvidence(
            contract_version=contract,
            model_id=model,
            text_hash=text_hash,
            semantic_bank_version=str(metrics.get("semantic_bank_version", "")),
            semantic_bank_hash=str(metrics.get("semantic_bank_hash", "")),
            evidence_fingerprint=str(metrics.get("evidence_fingerprint", "")),
            scores=scores,
        )

    e5 = evidence(e5_metrics, contract=E5_CONTRACT, model=E5_MODEL_ID, scores=e5_scores)
    bge = evidence(bge_metrics, contract=BGE_M3_CONTRACT, model=BGE_M3_MODEL_ID, scores=bge_scores)
    result = fuse_vector_evidence(
        VectorFusionRequest(
            schema_version="region-talk.vector-fusion.v1",
            text_hash=text_hash,
            expected_e5_fingerprint=e5.evidence_fingerprint,
            expected_bge_m3_fingerprint=bge.evidence_fingerprint,
            e5=e5,
            bge_m3=bge,
        )
    )
    if result.status != "fused_e5_bge_m3":
        raise RegionTalkStageRuntimeUnavailable(
            "vector fusion lacks exact-current embedding evidence: " + ",".join(result.reasons)
        )
    return result.model_dump(mode="json")


def process_region_talk_stage_item(
    work_item: dict[str, Any],
    *,
    stage: str,
    contract_version: str,
    runtime: AttachedStageRuntime | None = None,
) -> dict[str, Any]:
    """Execute one exact stage input or raise a retryable unavailable result.

    Successful output is the common ``region-talk-stage-result-metadata.v1``
    object.  The surrounding Notebook result builder independently records its
    exact output fingerprint.
    """

    definition = STAGE_BY_KEY.get(stage)
    if definition is None or definition.contract_version != contract_version:
        raise ValueError("notebook stage/contract differs from the fixed DAG")
    parsed = StageNotebookWorkItem.model_validate(work_item)
    if parsed.subject_type != "region_talk.candidate":
        raise ValueError("work request subject differs from Region Talk candidate")
    payload = _PAYLOAD_ADAPTER.validate_python(parsed.payload)
    expected_id = work_item_id(
        run_id=payload.stage_run_id,
        candidate_id=parsed.subject_id,
        revision=payload.candidate_revision,
        stage=stage,
        input_fingerprint=parsed.input_fingerprint,
    )
    if parsed.work_item_id != expected_id:
        raise ValueError("work request identity is not deterministic")
    if isinstance(payload, LegacyStageNotebookPayload):
        model = stage_model_identity(stage)
        suffix = (
            f"; exact model {model['model_id']}@{model['model_revision']}"
            if model["model_id"]
            else "; donor model/revision remains unverified"
        )
        raise RegionTalkStageRuntimeUnavailable(
            f"{stage} accepted its typed input but no verified heavyweight runtime is attached{suffix}"
        )
    if payload.candidate_id != parsed.subject_id:
        raise ValueError("execution payload candidate differs")
    if payload.input_fingerprint != parsed.input_fingerprint:
        raise ValueError("execution payload input fingerprint differs")
    data_schema = payload.input_data.get("schema_version")
    if data_schema == "region-talk-stage-text-input.v1":
        text = payload.input_data.get("text")
        text_sha256 = payload.input_data.get("text_sha256")
        if (
            not isinstance(text, str)
            or len(text.encode("utf-8")) > 256 * 1024
            or hashlib.sha256(text.encode("utf-8")).hexdigest() != text_sha256
        ):
            raise ValueError("stage text input hash or size differs")
    if data_schema == "region-talk-image-input.v1":
        if payload.input_data.get("availability") != "AVAILABLE":
            raise RegionTalkStageRuntimeUnavailable("verified private image artifact is unavailable")
        if not isinstance(payload.input_data.get("artifact_sha256"), str):
            raise ValueError("available image input lacks artifact_sha256")

    if stage == "vector_fusion":
        metrics = _vector_fusion(payload)
        producer = _producer_for(stage)
    else:
        if runtime is None:
            model = stage_model_identity(stage)
            suffix = (
                f"; exact model {model['model_id']}@{model['model_revision']}"
                if model["model_id"]
                else "; exact donor runtime remains unattached"
            )
            raise RegionTalkStageRuntimeUnavailable(
                f"{stage} accepted its exact input but no verified runtime is attached{suffix}"
            )
        metrics = runtime.execute(
            stage=stage,
            contract_version=contract_version,
            subject_id=parsed.subject_id,
            input_fingerprint=parsed.input_fingerprint,
            payload=payload,
        )
        producer = runtime.producer_exact_id
        if not isinstance(metrics, dict):
            raise ValueError("attached runtime returned non-object metrics")

    metadata = StageResultMetadata(
        stage=stage,
        contract_version=contract_version,
        subject_type="region_talk.candidate",
        subject_id=parsed.subject_id,
        candidate_revision=payload.candidate_revision,
        revision_fingerprint=payload.revision_fingerprint,
        input_fingerprint=parsed.input_fingerprint,
        producer_exact_id=producer,
        metrics=metrics,
        artifact_sha256=None,
    )
    # Re-encode now so cyclic/non-canonical custom mappings cannot cross the
    # Notebook boundary even if a runtime returned one.
    hashlib.sha256(canonical_json_bytes(metadata.model_dump(mode="json"))).hexdigest()
    return metadata.model_dump(mode="json")


def execute_direct_region_talk_stage_worker(
    functions: DirectStageWorkerFunctions,
    request: StageWorkerPayloadFetchRequest,
    *,
    runtime: AttachedStageRuntime | None = None,
    credential_checkpoint: DirectStageCredentialCheckpoint | None = None,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> StageWorkerDirectResultReceipt:
    """Fetch, execute and land one task-bound stage result without central data transit."""

    if credential_checkpoint is not None:
        functions, request = credential_checkpoint(
            functions,
            request,
            phase="before_fetch",
        )
    fetched = functions.fetch_payload(
        worker_task_run_id=request.worker_task_run_id,
        effect_id=request.effect_id,
        request=request,
    )
    if (
        fetched.worker_task_run_id != request.worker_task_run_id
        or fetched.dispatch_id != request.dispatch_id
        or fetched.effect_id != request.effect_id
        or fetched.worker_binding_sha256 != request.worker_binding_sha256
    ):
        raise ValueError("fetched payload differs from the exact worker request")
    item = {
        "work_item_id": str(fetched.work_item_id),
        "subject_type": fetched.subject_type,
        "subject_id": str(fetched.subject_id),
        "input_fingerprint": fetched.input_fingerprint,
        "payload": fetched.payload.model_dump(mode="json"),
    }
    try:
        result_metadata = StageResultMetadata.model_validate(
            process_region_talk_stage_item(
                item,
                stage=fetched.stage,
                contract_version=fetched.contract_version,
                runtime=runtime,
            )
        )
        status = StageWorkerStatus.SUCCEEDED
    except RegionTalkStageRuntimeUnavailable as exc:
        status = StageWorkerStatus.FAILED_RETRYABLE
        result_metadata = _failure_metadata(
            fetched,
            code=exc.code,
            message=str(exc),
            retryable=True,
        )
    except ValueError as exc:
        status = StageWorkerStatus.FAILED_TERMINAL
        result_metadata = _failure_metadata(
            fetched,
            code="INVALID_STAGE_INPUT",
            message=str(exc),
            retryable=False,
        )
    metadata_sha256 = hashlib.sha256(
        canonical_json_bytes(result_metadata.model_dump(mode="json"))
    ).hexdigest()
    if credential_checkpoint is not None:
        functions, current_request = credential_checkpoint(
            functions,
            request,
            phase="before_submit",
        )
        if (
            current_request.worker_task_run_id != fetched.worker_task_run_id
            or current_request.dispatch_id != fetched.dispatch_id
            or current_request.effect_id != fetched.effect_id
        ):
            raise ValueError("rotated worker checkpoint changed fixed work identity")
        request = current_request
    submission = StageWorkerDirectResultRequest(
        worker_task_run_id=fetched.worker_task_run_id,
        dispatch_id=fetched.dispatch_id,
        effect_id=fetched.effect_id,
        worker_binding_sha256=request.worker_binding_sha256,
        work_item_id=fetched.work_item_id,
        attempt=fetched.attempt,
        result_status=status,
        result_metadata=result_metadata,
        metadata_sha256=metadata_sha256,
        result_sha256=result_metadata.artifact_sha256 or metadata_sha256,
        completed_at=clock().astimezone(UTC),
    )
    return functions.submit_result(
        worker_task_run_id=fetched.worker_task_run_id,
        effect_id=fetched.effect_id,
        request=submission,
    )


def _failure_metadata(
    fetched: StageWorkPayloadReceipt,
    *,
    code: str,
    message: str,
    retryable: bool,
) -> StageResultMetadata:
    return StageResultMetadata(
        stage=fetched.stage,
        contract_version=fetched.contract_version,
        subject_type=fetched.subject_type,
        subject_id=fetched.subject_id,
        candidate_revision=fetched.payload.candidate_revision,
        revision_fingerprint=fetched.payload.revision_fingerprint,
        input_fingerprint=fetched.input_fingerprint,
        producer_exact_id=f"my-data-hub:{fetched.stage}@{fetched.contract_version}",
        metrics={
            "failure_code": code,
            "failure_message_sha256": hashlib.sha256(message.encode("utf-8")).hexdigest(),
            "retryable": retryable,
        },
    )


__all__ = [
    "AttachedStageRuntime",
    "DirectStageCredentialCheckpoint",
    "DirectStageWorkerFunctions",
    "RegionTalkStageRuntimeUnavailable",
    "StageNotebookPayload",
    "StageNotebookWorkItem",
    "attached_stage_runtime_from_env",
    "execute_direct_region_talk_stage_worker",
    "process_region_talk_stage_item",
    "stage_model_identity",
]
