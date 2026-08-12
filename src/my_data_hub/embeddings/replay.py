from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field

from my_data_hub.embeddings.contracts import EmbeddingVectorResult


class ReplayAction(StrEnum):
    INSERTED = "inserted"
    REPLACED_CURRENT = "replaced_current"
    EXACT_NOOP = "exact_noop"
    CONFLICT = "conflict"
    STALE_RESULT = "stale_result"


class StoredVector(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    result: EmbeddingVectorResult
    is_current: bool


class VectorReplayState(BaseModel):
    """Immutable projection used to plan transactional imports on the master."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    records: tuple[StoredVector, ...] = ()


class ReplayDecision(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    action: ReplayAction
    state: VectorReplayState
    stale_job_keys: tuple[str, ...] = ()
    reason: str = Field(min_length=1, max_length=500)


def apply_vector_result(
    state: VectorReplayState,
    incoming: EmbeddingVectorResult,
) -> ReplayDecision:
    """Pure replay model; it emits a decision and never writes canonical state."""

    same_job = next((item for item in state.records if item.result.job_key == incoming.job_key), None)
    if same_job is not None:
        if same_job.result == incoming:
            return ReplayDecision(
                action=ReplayAction.EXACT_NOOP,
                state=state,
                reason="identical immutable job result already exists",
            )
        return ReplayDecision(
            action=ReplayAction.CONFLICT,
            state=state,
            reason="same job key carries a different immutable result",
        )

    identity = (
        incoming.document_id,
        incoming.representation_kind,
        incoming.model_key,
        incoming.model_revision,
        incoming.vector_space,
    )
    current = next(
        (
            item
            for item in state.records
            if item.is_current
            and (
                item.result.document_id,
                item.result.representation_kind,
                item.result.model_key,
                item.result.model_revision,
                item.result.vector_space,
            )
            == identity
        ),
        None,
    )
    if current is not None and incoming.canonical_revision < current.result.canonical_revision:
        return ReplayDecision(
            action=ReplayAction.STALE_RESULT,
            state=state,
            reason="incoming result predates the current canonical revision",
        )
    if current is not None and incoming.canonical_revision == current.result.canonical_revision:
        return ReplayDecision(
            action=ReplayAction.CONFLICT,
            state=state,
            reason="same canonical revision produced a different input hash",
        )

    stale_keys: tuple[str, ...] = ()
    records: list[StoredVector] = []
    for item in state.records:
        if current is not None and item.result.job_key == current.result.job_key:
            records.append(StoredVector(result=item.result, is_current=False))
            stale_keys = (item.result.job_key,)
        else:
            records.append(item)
    records.append(StoredVector(result=incoming, is_current=True))
    records.sort(key=lambda item: item.result.job_key)
    return ReplayDecision(
        action=(ReplayAction.REPLACED_CURRENT if current is not None else ReplayAction.INSERTED),
        state=VectorReplayState(records=tuple(records)),
        stale_job_keys=stale_keys,
        reason=(
            "newer document hash supersedes the previous current vector"
            if current is not None
            else "first current vector for this model space and representation"
        ),
    )
