"""Fail-closed contracts for isolated Region Talk heavyweight notebooks.

These adapters replace ambiguous ``NotImplementedError`` shells.  They accept
only stage work requests emitted by :mod:`stage_execution` and return a typed
unavailable error until an exact heavyweight runtime is attached.  E5 and
BGE-M3 expose the already pinned repository model identities; no result is
reported as successful merely because a model is named.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from my_data_hub.embeddings.models import BGE_M3, E5_MULTILINGUAL_BASE

from .stage_execution import STAGE_BY_KEY, work_item_id


class RegionTalkStageRuntimeUnavailable(RuntimeError):
    """Exact contract was accepted but no verified heavyweight runtime exists."""

    code = "HEAVY_RUNTIME_NOT_ATTACHED"
    retryable = True


class StageNotebookPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: str
    stage_run_id: UUID
    candidate_revision: int = Field(ge=1)
    revision_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    reason: str = Field(min_length=1, max_length=200)
    publication_dispatch: bool
    notification_dispatch: bool

    @model_validator(mode="after")
    def exact_safe_payload(self) -> StageNotebookPayload:
        if self.schema_version != "region-talk-stage-work-payload.v1":
            raise ValueError("stage payload schema differs")
        if self.publication_dispatch or self.notification_dispatch:
            raise ValueError("stage payload cannot enable publication or notification")
        return self


class StageNotebookWorkItem(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    work_item_id: UUID
    subject_type: str
    subject_id: UUID
    input_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    payload: StageNotebookPayload

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


def process_region_talk_stage_item(
    work_item: dict[str, Any], *, stage: str, contract_version: str
) -> dict[str, Any]:
    """Validate an exact request, then fail without fabricating heavy evidence."""

    definition = STAGE_BY_KEY.get(stage)
    if definition is None or definition.contract_version != contract_version:
        raise ValueError("notebook stage/contract differs from the fixed DAG")
    parsed = StageNotebookWorkItem.model_validate(work_item)
    if parsed.subject_type != "region_talk.candidate":
        raise ValueError("work request subject differs from Region Talk candidate")
    expected_id = work_item_id(
        run_id=parsed.payload.stage_run_id,
        candidate_id=parsed.subject_id,
        revision=parsed.payload.candidate_revision,
        stage=stage,
        input_fingerprint=parsed.input_fingerprint,
    )
    if parsed.work_item_id != expected_id:
        raise ValueError("work request identity is not deterministic")
    model = stage_model_identity(stage)
    suffix = (
        f"; exact model {model['model_id']}@{model['model_revision']}"
        if model["model_id"]
        else "; donor model/revision remains unverified"
    )
    raise RegionTalkStageRuntimeUnavailable(
        f"{stage} accepted its typed input but no verified heavyweight runtime is attached"
        f"{suffix}"
    )


__all__ = [
    "RegionTalkStageRuntimeUnavailable",
    "StageNotebookPayload",
    "StageNotebookWorkItem",
    "process_region_talk_stage_item",
    "stage_model_identity",
]
