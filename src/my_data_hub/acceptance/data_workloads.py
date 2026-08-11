"""Resumable metadata-only core for operational data-workload acceptance.

This module deliberately cannot emit a live PASS.  It sequences already-owned
H1/H3/H5 production boundaries and produces a bounded ``EVIDENCE_READY`` bundle
for a later, exact-source Kaggle evidence Notebook.  Raw YDB rows, public
business projections, vectors, SQL, credentials, DSNs and signed bearer
receipts are outside every model in this module.
"""

from __future__ import annotations

import hashlib
from datetime import datetime
from enum import StrEnum
from typing import Any, Literal, Protocol
from uuid import UUID, uuid5

from pydantic import BaseModel, ConfigDict, Field, model_validator

from my_data_hub.embeddings.production import WORKER_ASSETS
from my_data_hub.hashing import canonical_json_bytes

SHA256_PATTERN = r"^[a-f0-9]{64}$"
COMMIT_PATTERN = r"^[a-f0-9]{40}$"
EXPECTED_ROWS = 266
E5_EXACT_ID = WORKER_ASSETS[0].model.exact_id
BGE_EXACT_ID = WORKER_ASSETS[1].model.exact_id
_NAMESPACE = UUID("a99f4a79-755a-5f57-bec0-d2574399ee0f")


def _sha(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


class CheckpointEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    checkpoint_id: UUID
    generation: int = Field(ge=1)
    exact_version_ref: str = Field(pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[1-9][0-9]*$")
    manifest_sha256: str = Field(pattern=SHA256_PATTERN)
    canonical_revision: int = Field(ge=1)
    status: Literal["VERIFIED"] = "VERIFIED"


class MasterEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    master_instance_id: UUID
    epoch: int = Field(ge=1)
    canonical_revision: int = Field(ge=1)
    state: Literal["ACTIVE"] = "ACTIVE"


class MutationAcceptance(BaseModel):
    """Metadata response for a mutation whose identity was persisted first."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    operation_id: str = Field(min_length=1, max_length=300)
    request_sha256: str = Field(pattern=SHA256_PATTERN)
    outcome: Literal["accepted", "replayed", "ambiguous", "rejected"]
    state: str = Field(min_length=1, max_length=100)
    response_sha256: str = Field(pattern=SHA256_PATTERN)


class DuplicateReviewEvidence(BaseModel):
    """Owner-reviewable identities only; source payload columns are forbidden."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    export_batch_id: UUID
    source_request_id: UUID
    source_operation_id: UUID
    source_request_sha256: str = Field(pattern=SHA256_PATTERN)
    duplicate_group_count: int = Field(ge=1, le=1330)
    duplicate_groups_pending: int = Field(ge=1, le=1330)
    identity_set_sha256: str = Field(pattern=SHA256_PATTERN)
    member_record_id_set_sha256: str = Field(pattern=SHA256_PATTERN)
    review_projection_sha256: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode="after")
    def every_group_is_pending(self) -> DuplicateReviewEvidence:
        if self.duplicate_group_count != self.duplicate_groups_pending:
            raise ValueError("initial duplicate review must cover every pending group")
        return self


class OwnerDuplicateAuthorization(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    authorization_id: UUID
    authorized_by_sha256: str = Field(pattern=SHA256_PATTERN)
    authorized_at: datetime
    source_request_id: UUID
    source_operation_id: UUID
    source_request_sha256: str = Field(pattern=SHA256_PATTERN)
    export_batch_id: UUID
    decision_count: int = Field(ge=1, le=1330)
    identity_set_sha256: str = Field(pattern=SHA256_PATTERN)
    member_record_id_set_sha256: str = Field(pattern=SHA256_PATTERN)
    envelope_sha256: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode="after")
    def aware_authorization(self) -> OwnerDuplicateAuthorization:
        if self.authorized_at.tzinfo is None:
            raise ValueError("owner authorization time must be timezone-aware")
        return self

    def binds(self, review: DuplicateReviewEvidence) -> bool:
        return (
            self.source_request_id == review.source_request_id
            and self.source_operation_id == review.source_operation_id
            and self.source_request_sha256 == review.source_request_sha256
            and self.export_batch_id == review.export_batch_id
            and self.decision_count == review.duplicate_group_count
            and self.identity_set_sha256 == review.identity_set_sha256
            and self.member_record_id_set_sha256 == review.member_record_id_set_sha256
        )


class BloggerQuarantineEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    request_id: UUID
    request_sha256: str = Field(pattern=SHA256_PATTERN)
    operation_id: UUID
    export_batch_id: UUID
    failure_code: Literal["BloggerMigrationQuarantined"]
    row_count: Literal[266] = EXPECTED_ROWS
    raw_count: Literal[266] = EXPECTED_ROWS
    dispositioned_count: Literal[266] = EXPECTED_ROWS
    undispositioned_count: Literal[0] = 0
    quarantined_count: int = Field(ge=1, le=266)
    logical_sha256: str = Field(pattern=SHA256_PATTERN)
    record_id_set_sha256: str = Field(pattern=SHA256_PATTERN)
    canonical_outcome_sha256: str = Field(pattern=SHA256_PATTERN)
    duplicate_group_count: int = Field(ge=1, le=1330)
    duplicate_groups_pending: int = Field(ge=1, le=1330)

    @model_validator(mode="after")
    def exact_quarantine(self) -> BloggerQuarantineEvidence:
        if self.duplicate_group_count != self.duplicate_groups_pending:
            raise ValueError("quarantine evidence does not leave every duplicate unresolved")
        return self


class BloggerTerminalEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    request_id: UUID
    request_sha256: str = Field(pattern=SHA256_PATTERN)
    receipt_sha256: str = Field(pattern=SHA256_PATTERN)
    operation_id: UUID
    import_schema: Literal["region-talk-ydb-bloggers-import-receipt.v3"]
    export_batch_id: UUID
    row_count: Literal[266] = EXPECTED_ROWS
    distinct_record_ids: Literal[266] = EXPECTED_ROWS
    dispositions: dict[str, int]
    raw_count: Literal[266] = EXPECTED_ROWS
    dispositioned_count: Literal[266] = EXPECTED_ROWS
    undispositioned_count: Literal[0] = 0
    quarantined_count: Literal[0] = 0
    duplicate_group_count: int = Field(ge=1, le=1330)
    duplicate_groups_pending: Literal[0] = 0
    replayed_count: Literal[266] = EXPECTED_ROWS
    actor_count: int = Field(ge=1, le=266)
    account_count: int = Field(ge=0, le=2128)
    logical_sha256: str = Field(pattern=SHA256_PATTERN)
    record_id_set_sha256: str = Field(pattern=SHA256_PATTERN)
    canonical_outcome_sha256: str = Field(pattern=SHA256_PATTERN)
    source_master_instance_id: UUID
    source_run_id: UUID
    source_epoch: int = Field(ge=1)
    canonical_revision: int = Field(ge=1)
    checkpoint: CheckpointEvidence

    @model_validator(mode="after")
    def lossless_terminal(self) -> BloggerTerminalEvidence:
        if sum(self.dispositions.values()) != EXPECTED_ROWS:
            raise ValueError("final blogger dispositions are not lossless")
        if self.dispositions.get("deduplicated", 0) < 1:
            raise ValueError("FM16 requires an observed explicit duplicate replay")
        if self.checkpoint.canonical_revision != self.canonical_revision:
            raise ValueError("final blogger checkpoint revision differs")
        return self


class BloggerRequestObservation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    request_id: UUID
    request_sha256: str = Field(pattern=SHA256_PATTERN)
    state: Literal["REQUESTED", "CLAIMED", "IMPORT_COMMITTED", "QUARANTINED", "CHECKPOINT_VERIFIED", "FAILED"]
    quarantine: BloggerQuarantineEvidence | None = None
    terminal: BloggerTerminalEvidence | None = None

    @model_validator(mode="after")
    def terminal_shape(self) -> BloggerRequestObservation:
        if self.state == "QUARANTINED" and self.quarantine is None:
            raise ValueError("quarantined blogger request lacks exact evidence")
        if self.state == "CHECKPOINT_VERIFIED" and self.terminal is None:
            raise ValueError("verified blogger request lacks exact evidence")
        return self


class BloggerAccountingEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    export_batch_id: UUID
    expected_row_count: Literal[266] = EXPECTED_ROWS
    status: Literal["accepted"] = "accepted"
    logical_sha256: str = Field(pattern=SHA256_PATTERN)
    record_id_set_sha256: str = Field(pattern=SHA256_PATTERN)
    canonical_outcome_sha256: str = Field(pattern=SHA256_PATTERN)
    raw_count: Literal[266] = EXPECTED_ROWS
    dispositioned_count: Literal[266] = EXPECTED_ROWS
    undispositioned_count: Literal[0] = 0
    quarantined_count: Literal[0] = 0
    duplicate_groups_pending: Literal[0] = 0
    actor_count: int = Field(ge=1, le=266)
    account_count: int = Field(ge=0, le=2128)
    canonical_revision: int = Field(ge=1)
    checkpoint_required: Literal[True] = True

    @property
    def equality_sha256(self) -> str:
        return _sha(self.model_dump(mode="json"))


class RestoreObservation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    operation_id: str = Field(min_length=1, max_length=300)
    state: Literal["REQUESTED", "RUNNING", "DURABLE_COMPLETE", "FAILED", "FENCED", "ORPHANED"]


class EmbeddingModelEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    model_exact_id: str = Field(min_length=1, max_length=400)
    task_run_id: UUID
    provider_ref: str = Field(pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
    provider_run_ref: str = Field(pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[1-9][0-9]*$")
    provider_kernel_id: int = Field(ge=1)
    source_sha256: str = Field(pattern=SHA256_PATTERN)
    primary_source_sha256: str = Field(pattern=SHA256_PATTERN)
    artifact_id: UUID
    artifact_sha256: str = Field(pattern=SHA256_PATTERN)
    inserted_count: Literal[266] = EXPECTED_ROWS
    stale_count: Literal[0] = 0
    failed_count: Literal[0] = 0
    expected_documents: Literal[266] = EXPECTED_ROWS
    completed_documents: Literal[266] = EXPECTED_ROWS
    coverage: Literal[1.0] = 1.0
    checkpoint_required: Literal[True] = True


class EmbeddingTerminalEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    request_id: UUID
    request_sha256: str = Field(pattern=SHA256_PATTERN)
    state: Literal["CHECKPOINT_VERIFIED"] = "CHECKPOINT_VERIFIED"
    blogger_export_batch_id: UUID
    blogger_canonical_revision: int = Field(ge=1)
    canonical_revision: int = Field(ge=1)
    models: tuple[EmbeddingModelEvidence, EmbeddingModelEvidence]
    checkpoint: CheckpointEvidence

    @model_validator(mode="after")
    def exact_two_models(self) -> EmbeddingTerminalEvidence:
        if {item.model_exact_id for item in self.models} != {E5_EXACT_ID, BGE_EXACT_ID}:
            raise ValueError("embedding evidence does not contain both exact model spaces")
        if len({item.task_run_id for item in self.models}) != 2:
            raise ValueError("embedding workers reused one task identity")
        if self.canonical_revision < self.blogger_canonical_revision:
            raise ValueError("embedding revision predates blogger prerequisite")
        if self.checkpoint.canonical_revision != self.canonical_revision:
            raise ValueError("embedding checkpoint revision differs")
        for item in self.models:
            asset = next(value for value in WORKER_ASSETS if value.model.exact_id == item.model_exact_id)
            if item.primary_source_sha256 != asset.primary_source_sha256:
                raise ValueError("embedding worker source differs from the pinned asset")
        return self

    def for_model(self, model_exact_id: str) -> EmbeddingModelEvidence:
        return next(item for item in self.models if item.model_exact_id == model_exact_id)


class EmbeddingRequestObservation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    request_id: UUID
    request_sha256: str = Field(pattern=SHA256_PATTERN)
    state: Literal["REQUESTED", "CLAIMED", "STAGE_COMMITTED", "CHECKPOINT_VERIFIED", "FAILED"]
    terminal: EmbeddingTerminalEvidence | None = None

    @model_validator(mode="after")
    def exact_terminal(self) -> EmbeddingRequestObservation:
        if self.state == "CHECKPOINT_VERIFIED" and self.terminal is None:
            raise ValueError("verified embedding request lacks terminal evidence")
        return self


class FixedChangeIntent(BaseModel):
    """Named SQL-free fixture mutation rendered only by the H1 adapter."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    contract: Literal["fm21_hub_project_fixture.v1"] = "fm21_hub_project_fixture.v1"
    action: Literal["insert", "delete"]
    fixture_project_id: UUID
    fixture_sha256: str = Field(pattern=SHA256_PATTERN)
    expected_revision: int = Field(ge=1)
    idempotency_key_sha256: str = Field(pattern=SHA256_PATTERN)

    @property
    def request_sha256(self) -> str:
        return _sha(self.model_dump(mode="json"))


class ChangePreviewEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    operation_id: str = Field(pattern=SHA256_PATTERN)
    request_sha256: str = Field(pattern=SHA256_PATTERN)
    action: Literal["insert", "delete"]
    affected_rows: Literal[0, 1]
    expected_revision: int = Field(ge=1)
    pre_change_checkpoint_id: UUID
    preview_receipt_sha256: str = Field(pattern=SHA256_PATTERN)


class ChangeApplyEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    operation_id: str = Field(pattern=SHA256_PATTERN)
    outcome: Literal["accepted", "replayed", "ambiguous", "rejected"]
    affected_rows: Literal[1] | None = None
    committed_revision: int | None = Field(default=None, ge=1)
    response_sha256: str = Field(pattern=SHA256_PATTERN)


class ChangeStatusEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    operation_id: str = Field(pattern=SHA256_PATTERN)
    state: Literal[
        "PREVIEWED",
        "APPLYING",
        "COMMITTED_PENDING_CHECKPOINT",
        "CHECKPOINTING",
        "CHECKPOINT_VERIFIED",
        "DURABLE_COMPLETE",
        "FAILED",
    ]
    expected_revision: int = Field(ge=1)
    committed_revision: int | None = Field(default=None, ge=1)
    pre_change_checkpoint_id: UUID
    post_change_checkpoint: CheckpointEvidence | None = None

    @model_validator(mode="after")
    def durable_has_checkpoint(self) -> ChangeStatusEvidence:
        if self.state == "DURABLE_COMPLETE" and self.post_change_checkpoint is None:
            raise ValueError("durable change lacks a post-change checkpoint")
        if (
            self.state == "DURABLE_COMPLETE"
            and self.post_change_checkpoint is not None
            and self.post_change_checkpoint.canonical_revision != self.committed_revision
        ):
            raise ValueError("durable change checkpoint revision differs")
        return self


class DataWorkloadPlan(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["my-data-hub-operational-data-workload-plan.v1"] = (
        "my-data-hub-operational-data-workload-plan.v1"
    )
    matrix_id: UUID
    source_commit: str = Field(pattern=COMMIT_PATTERN)
    blogger_project_id: UUID
    blogger_snapshot_at: datetime
    blogger_source_revision: str = Field(pattern=COMMIT_PATTERN)
    embedding_probe_query_sha256: str = Field(pattern=SHA256_PATTERN)
    require_duplicate_replay: Literal[True] = True

    @model_validator(mode="after")
    def aware_snapshot(self) -> DataWorkloadPlan:
        if self.blogger_snapshot_at.tzinfo is None:
            raise ValueError("blogger snapshot must be timezone-aware")
        return self

    def identity(self, suffix: str) -> UUID:
        return uuid5(_NAMESPACE, f"{self.matrix_id}:{suffix}")

    def key_sha256(self, suffix: str) -> str:
        return hashlib.sha256(f"h6:{self.matrix_id}:{suffix}".encode()).hexdigest()


class DataPhase(StrEnum):
    INITIAL = "INITIAL"
    FM16_V1_RUNNING = "FM16_V1_RUNNING"
    FM16_V1_AMBIGUOUS = "FM16_V1_AMBIGUOUS"
    AWAITING_OWNER_AUTHORIZATION = "AWAITING_OWNER_AUTHORIZATION"
    FM16_V2_RUNNING = "FM16_V2_RUNNING"
    FM16_V2_AMBIGUOUS = "FM16_V2_AMBIGUOUS"
    FM16_COMPLETE = "FM16_COMPLETE"
    FM17_RESTORE_RUNNING = "FM17_RESTORE_RUNNING"
    FM17_RESTORE_AMBIGUOUS = "FM17_RESTORE_AMBIGUOUS"
    FM17_COMPLETE = "FM17_COMPLETE"
    FM18_19_RUNNING = "FM18_19_RUNNING"
    FM18_19_AMBIGUOUS = "FM18_19_AMBIGUOUS"
    FM18_19_COMPLETE = "FM18_19_COMPLETE"
    FM21_INSERT_PREVIEWED = "FM21_INSERT_PREVIEWED"
    FM21_INSERT_APPLYING = "FM21_INSERT_APPLYING"
    FM21_INSERT_AMBIGUOUS = "FM21_INSERT_AMBIGUOUS"
    FM21_INSERT_COMPLETE = "FM21_INSERT_COMPLETE"
    FM21_DELETE_PREVIEWED = "FM21_DELETE_PREVIEWED"
    FM21_DELETE_APPLYING = "FM21_DELETE_APPLYING"
    FM21_DELETE_AMBIGUOUS = "FM21_DELETE_AMBIGUOUS"
    FM21_DELETE_COMPLETE = "FM21_DELETE_COMPLETE"
    EVIDENCE_READY = "EVIDENCE_READY"
    FAILED = "FAILED"


class DataWorkloadState(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["my-data-hub-operational-data-workload-state.v1"] = (
        "my-data-hub-operational-data-workload-state.v1"
    )
    matrix_id: UUID
    plan_sha256: str = Field(pattern=SHA256_PATTERN)
    phase: DataPhase = DataPhase.INITIAL
    mutations_started: int = Field(default=0, ge=0)
    v1_request_id: UUID | None = None
    v1_request_sha256: str | None = Field(default=None, pattern=SHA256_PATTERN)
    quarantine: BloggerQuarantineEvidence | None = None
    duplicate_review: DuplicateReviewEvidence | None = None
    owner_authorization_sha256: str | None = Field(default=None, pattern=SHA256_PATTERN)
    v2_request_id: UUID | None = None
    v2_request_sha256: str | None = Field(default=None, pattern=SHA256_PATTERN)
    blogger_terminal: BloggerTerminalEvidence | None = None
    pre_restore_accounting: BloggerAccountingEvidence | None = None
    restore_operation_id: str | None = Field(default=None, min_length=1, max_length=300)
    restore_idempotency_key_sha256: str | None = Field(default=None, pattern=SHA256_PATTERN)
    restore_request_sha256: str | None = Field(default=None, pattern=SHA256_PATTERN)
    restored_master: MasterEvidence | None = None
    post_restore_accounting: BloggerAccountingEvidence | None = None
    embedding_request_id: UUID | None = None
    embedding_request_sha256: str | None = Field(default=None, pattern=SHA256_PATTERN)
    embedding_terminal: EmbeddingTerminalEvidence | None = None
    fixture_project_id: UUID | None = None
    fixture_sha256: str | None = Field(default=None, pattern=SHA256_PATTERN)
    insert_preview: ChangePreviewEvidence | None = None
    insert_status: ChangeStatusEvidence | None = None
    delete_preview: ChangePreviewEvidence | None = None
    delete_status: ChangeStatusEvidence | None = None
    final_zero_preview: ChangePreviewEvidence | None = None
    failure_code: str | None = Field(default=None, pattern=r"^[A-Z0-9_]+$")
    resumable: bool = True

    @classmethod
    def initial(cls, plan: DataWorkloadPlan) -> DataWorkloadState:
        return cls(matrix_id=plan.matrix_id, plan_sha256=_sha(plan.model_dump(mode="json")))


class RequirementEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    requirement_id: Literal["FM16", "FM17", "FM18", "FM19", "FM21"]
    assertion_evidence_sha256: dict[str, str]
    operation_ids: tuple[str, ...]

    @model_validator(mode="after")
    def hashes_only(self) -> RequirementEvidence:
        if not self.assertion_evidence_sha256 or any(
            not name or len(value) != 64 or any(char not in "0123456789abcdef" for char in value)
            for name, value in self.assertion_evidence_sha256.items()
        ):
            raise ValueError("requirement evidence hashes are invalid")
        if not self.operation_ids or len(set(self.operation_ids)) != len(self.operation_ids):
            raise ValueError("operation identities must be non-empty and unique")
        return self


class DataWorkloadEvidenceBundle(BaseModel):
    """Input to later live evidence execution. It is intentionally not PASS."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["my-data-hub-operational-data-evidence.v1"] = "my-data-hub-operational-data-evidence.v1"
    matrix_id: UUID
    source_commit: str = Field(pattern=COMMIT_PATTERN)
    outcome: Literal["EVIDENCE_READY"] = "EVIDENCE_READY"
    live_evidence: Literal[False] = False
    requirements: tuple[
        RequirementEvidence,
        RequirementEvidence,
        RequirementEvidence,
        RequirementEvidence,
        RequirementEvidence,
    ]

    @model_validator(mode="after")
    def exact_requirements(self) -> DataWorkloadEvidenceBundle:
        if tuple(item.requirement_id for item in self.requirements) != ("FM16", "FM17", "FM18", "FM19", "FM21"):
            raise ValueError("data evidence requirements must be exact and ordered")
        return self

    @property
    def bundle_sha256(self) -> str:
        return _sha(self.model_dump(mode="json"))


class DataWorkloadExecutionResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    outcome: Literal["PROGRESS", "AWAITING_OWNER_AUTHORIZATION", "FAIL", "EVIDENCE_READY"]
    state: DataWorkloadState
    evidence: DataWorkloadEvidenceBundle | None = None
    failure_code: str | None = Field(default=None, pattern=r"^[A-Z0-9_]+$")
    resumable: bool = True

    @model_validator(mode="after")
    def exact_result(self) -> DataWorkloadExecutionResult:
        if (self.outcome == "EVIDENCE_READY") != (self.evidence is not None):
            raise ValueError("only EVIDENCE_READY may carry an evidence bundle")
        if self.outcome == "FAIL" and self.failure_code is None:
            raise ValueError("FAIL requires a typed failure code")
        return self


class DataWorkloadStateStore(Protocol):
    def persist(self, state: DataWorkloadState) -> None: ...


class DataWorkloadGateway(Protocol):
    """Integration adapter. All methods exchange bounded metadata only."""

    async def start_blogger_v1(
        self, *, request_id: UUID, intent_sha256: str, plan: DataWorkloadPlan
    ) -> MutationAcceptance: ...
    async def observe_blogger(self, request_id: UUID) -> BloggerRequestObservation: ...
    async def duplicate_review(self, request_id: UUID) -> DuplicateReviewEvidence: ...
    async def start_blogger_v2(
        self, *, request_id: UUID, intent_sha256: str, authorization: OwnerDuplicateAuthorization
    ) -> MutationAcceptance: ...
    async def migration_accounting(self, export_batch_id: UUID) -> BloggerAccountingEvidence: ...
    async def start_restore(
        self, *, idempotency_key_sha256: str, checkpoint: CheckpointEvidence, expected_epoch: int
    ) -> MutationAcceptance: ...
    async def observe_restore(self, operation_id: str) -> RestoreObservation: ...
    async def active_master(self) -> MasterEvidence: ...
    async def start_embedding(
        self,
        *,
        request_id: UUID,
        intent_sha256: str,
        blogger: BloggerTerminalEvidence,
        probe_query_sha256: str,
    ) -> MutationAcceptance: ...
    async def observe_embedding(self, request_id: UUID) -> EmbeddingRequestObservation: ...
    async def preview_fixed_change(self, intent: FixedChangeIntent) -> ChangePreviewEvidence: ...
    async def apply_fixed_change(self, preview: ChangePreviewEvidence) -> ChangeApplyEvidence: ...
    async def fixed_change_status(self, operation_id: str) -> ChangeStatusEvidence: ...


class DataWorkloadStateMachine:
    """One persisted transition per call; safe for crash/restart orchestration."""

    def __init__(self, store: DataWorkloadStateStore) -> None:
        self.store = store

    def _persist(self, state: DataWorkloadState) -> DataWorkloadState:
        self.store.persist(state)
        return state

    @staticmethod
    def _result(
        state: DataWorkloadState,
        outcome: Literal["PROGRESS", "AWAITING_OWNER_AUTHORIZATION"] = "PROGRESS",
    ) -> DataWorkloadExecutionResult:
        return DataWorkloadExecutionResult(outcome=outcome, state=state)

    def _fail(self, state: DataWorkloadState, code: str, *, resumable: bool) -> DataWorkloadExecutionResult:
        failed = self._persist(
            state.model_copy(update={"phase": DataPhase.FAILED, "failure_code": code, "resumable": resumable})
        )
        return DataWorkloadExecutionResult(outcome="FAIL", state=failed, failure_code=code, resumable=resumable)

    @staticmethod
    def _acceptance_phase(acceptance: MutationAcceptance, running: DataPhase, ambiguous: DataPhase) -> DataPhase | None:
        if acceptance.outcome in {"accepted", "replayed"}:
            return running
        if acceptance.outcome == "ambiguous":
            return ambiguous
        return None

    async def advance(
        self,
        plan: DataWorkloadPlan,
        state: DataWorkloadState,
        gateway: DataWorkloadGateway,
        *,
        owner_authorization: OwnerDuplicateAuthorization | None = None,
    ) -> DataWorkloadExecutionResult:
        if state.matrix_id != plan.matrix_id or state.plan_sha256 != _sha(plan.model_dump(mode="json")):
            raise ValueError("persisted data-workload state differs from the exact plan")
        phase = state.phase
        if phase == DataPhase.FAILED:
            if not state.resumable:
                return DataWorkloadExecutionResult(
                    outcome="FAIL", state=state, failure_code=state.failure_code, resumable=False
                )
            # Ambiguous operations are resumed from their known identity.
            phase = self._resume_phase(state)
            state = self._persist(state.model_copy(update={"phase": phase, "failure_code": None}))

        if phase == DataPhase.INITIAL:
            request_id = plan.identity("fm16:v1")
            intent_sha = _sha({"request_id": str(request_id), "plan": plan.model_dump(mode="json")})
            planned = self._persist(
                state.model_copy(
                    update={
                        "phase": DataPhase.FM16_V1_AMBIGUOUS,
                        "v1_request_id": request_id,
                    }
                )
            )
            accepted = await gateway.start_blogger_v1(request_id=request_id, intent_sha256=intent_sha, plan=plan)
            if accepted.operation_id != str(request_id):
                return self._fail(planned, "FM16_V1_OPERATION_MISMATCH", resumable=False)
            next_phase = self._acceptance_phase(accepted, DataPhase.FM16_V1_RUNNING, DataPhase.FM16_V1_AMBIGUOUS)
            if next_phase is None:
                return self._fail(planned, "FM16_V1_REQUEST_REJECTED", resumable=False)
            updated = self._persist(
                planned.model_copy(
                    update={
                        "phase": next_phase,
                        "v1_request_sha256": accepted.request_sha256,
                        "mutations_started": planned.mutations_started + 1,
                    }
                )
            )
            if accepted.outcome == "ambiguous":
                return self._fail(updated, "FM16_V1_REQUEST_AMBIGUOUS", resumable=True)
            return self._result(updated)

        if phase == DataPhase.FM16_V1_AMBIGUOUS:
            assert state.v1_request_id is not None
            intent_sha = _sha({"request_id": str(state.v1_request_id), "plan": plan.model_dump(mode="json")})
            accepted = await gateway.start_blogger_v1(
                request_id=state.v1_request_id, intent_sha256=intent_sha, plan=plan
            )
            if accepted.operation_id != str(state.v1_request_id):
                return self._fail(state, "FM16_V1_OPERATION_MISMATCH", resumable=False)
            if state.v1_request_sha256 not in {None, accepted.request_sha256}:
                return self._fail(state, "FM16_V1_REQUEST_REPLAY_MISMATCH", resumable=False)
            if accepted.outcome == "rejected":
                return self._fail(state, "FM16_V1_REQUEST_REJECTED", resumable=False)
            replayed = self._persist(
                state.model_copy(
                    update={
                        "phase": (
                            DataPhase.FM16_V1_AMBIGUOUS
                            if accepted.outcome == "ambiguous"
                            else DataPhase.FM16_V1_RUNNING
                        ),
                        "v1_request_sha256": accepted.request_sha256,
                    }
                )
            )
            if accepted.outcome == "ambiguous":
                return self._fail(replayed, "FM16_V1_REQUEST_AMBIGUOUS", resumable=True)
            return self._result(replayed)

        if phase == DataPhase.FM16_V1_RUNNING:
            assert state.v1_request_id is not None
            observed = await gateway.observe_blogger(state.v1_request_id)
            if observed.request_id != state.v1_request_id or observed.request_sha256 != state.v1_request_sha256:
                return self._fail(state, "FM16_V1_OBSERVATION_MISMATCH", resumable=False)
            if observed.state in {"REQUESTED", "CLAIMED", "IMPORT_COMMITTED"}:
                return self._result(state)
            if observed.state not in {"QUARANTINED", "FAILED"} or observed.quarantine is None:
                return self._fail(state, "FM16_V1_DID_NOT_QUARANTINE_DUPLICATES", resumable=False)
            if (
                observed.quarantine.request_id != observed.request_id
                or observed.quarantine.request_sha256 != observed.request_sha256
            ):
                return self._fail(state, "FM16_V1_QUARANTINE_MISMATCH", resumable=False)
            review = await gateway.duplicate_review(state.v1_request_id)
            if (
                review.export_batch_id != observed.quarantine.export_batch_id
                or review.source_request_id != observed.quarantine.request_id
                or review.source_operation_id != observed.quarantine.operation_id
                or review.source_request_sha256 != observed.quarantine.request_sha256
                or review.duplicate_group_count != observed.quarantine.duplicate_group_count
            ):
                return self._fail(state, "FM16_DUPLICATE_REVIEW_BATCH_MISMATCH", resumable=False)
            waiting = self._persist(
                state.model_copy(
                    update={
                        "phase": DataPhase.AWAITING_OWNER_AUTHORIZATION,
                        "v1_request_sha256": observed.request_sha256,
                        "quarantine": observed.quarantine,
                        "duplicate_review": review,
                    }
                )
            )
            return self._result(waiting, "AWAITING_OWNER_AUTHORIZATION")

        if phase == DataPhase.AWAITING_OWNER_AUTHORIZATION:
            if owner_authorization is None:
                return self._result(state, "AWAITING_OWNER_AUTHORIZATION")
            assert state.duplicate_review is not None
            if not owner_authorization.binds(state.duplicate_review):
                return DataWorkloadExecutionResult(
                    outcome="FAIL", state=state, failure_code="FM16_OWNER_AUTHORIZATION_MISMATCH", resumable=True
                )
            request_id = plan.identity("fm16:v2")
            intent_sha = _sha(
                {
                    "request_id": str(request_id),
                    "plan_sha256": state.plan_sha256,
                    "envelope_sha256": owner_authorization.envelope_sha256,
                }
            )
            planned = self._persist(
                state.model_copy(
                    update={
                        "phase": DataPhase.FM16_V2_AMBIGUOUS,
                        "v2_request_id": request_id,
                        "owner_authorization_sha256": owner_authorization.envelope_sha256,
                    }
                )
            )
            accepted = await gateway.start_blogger_v2(
                request_id=request_id, intent_sha256=intent_sha, authorization=owner_authorization
            )
            if accepted.operation_id != str(request_id):
                return self._fail(planned, "FM16_V2_OPERATION_MISMATCH", resumable=False)
            next_phase = self._acceptance_phase(accepted, DataPhase.FM16_V2_RUNNING, DataPhase.FM16_V2_AMBIGUOUS)
            if next_phase is None:
                return self._fail(planned, "FM16_V2_REQUEST_REJECTED", resumable=False)
            updated = self._persist(
                planned.model_copy(
                    update={
                        "phase": next_phase,
                        "v2_request_sha256": accepted.request_sha256,
                        "mutations_started": planned.mutations_started + 1,
                    }
                )
            )
            if accepted.outcome == "ambiguous":
                return self._fail(updated, "FM16_V2_REQUEST_AMBIGUOUS", resumable=True)
            return self._result(updated)

        if phase == DataPhase.FM16_V2_AMBIGUOUS:
            if owner_authorization is None:
                return self._result(state, "AWAITING_OWNER_AUTHORIZATION")
            assert state.v2_request_id is not None
            intent_sha = _sha(
                {
                    "request_id": str(state.v2_request_id),
                    "plan_sha256": state.plan_sha256,
                    "envelope_sha256": owner_authorization.envelope_sha256,
                }
            )
            accepted = await gateway.start_blogger_v2(
                request_id=state.v2_request_id,
                intent_sha256=intent_sha,
                authorization=owner_authorization,
            )
            if accepted.operation_id != str(state.v2_request_id):
                return self._fail(state, "FM16_V2_OPERATION_MISMATCH", resumable=False)
            if state.v2_request_sha256 not in {None, accepted.request_sha256}:
                return self._fail(state, "FM16_V2_REQUEST_REPLAY_MISMATCH", resumable=False)
            if accepted.outcome == "rejected":
                return self._fail(state, "FM16_V2_REQUEST_REJECTED", resumable=False)
            replayed = self._persist(
                state.model_copy(
                    update={
                        "phase": (
                            DataPhase.FM16_V2_AMBIGUOUS
                            if accepted.outcome == "ambiguous"
                            else DataPhase.FM16_V2_RUNNING
                        ),
                        "v2_request_sha256": accepted.request_sha256,
                    }
                )
            )
            if accepted.outcome == "ambiguous":
                return self._fail(replayed, "FM16_V2_REQUEST_AMBIGUOUS", resumable=True)
            return self._result(replayed)

        if phase == DataPhase.FM16_V2_RUNNING:
            assert state.v2_request_id is not None
            observed = await gateway.observe_blogger(state.v2_request_id)
            if observed.request_id != state.v2_request_id or observed.request_sha256 != state.v2_request_sha256:
                return self._fail(state, "FM16_V2_OBSERVATION_MISMATCH", resumable=False)
            if observed.state in {"REQUESTED", "CLAIMED", "IMPORT_COMMITTED"}:
                return self._result(state)
            if observed.state != "CHECKPOINT_VERIFIED" or observed.terminal is None:
                return self._fail(state, "FM16_V2_NOT_CHECKPOINT_VERIFIED", resumable=False)
            terminal = observed.terminal
            if terminal.request_id != observed.request_id or terminal.request_sha256 != observed.request_sha256:
                return self._fail(state, "FM16_V2_TERMINAL_MISMATCH", resumable=False)
            assert state.quarantine is not None
            if terminal.export_batch_id != state.quarantine.export_batch_id:
                return self._fail(state, "FM16_V2_EXPORT_BATCH_MISMATCH", resumable=False)
            completed = self._persist(
                state.model_copy(
                    update={
                        "phase": DataPhase.FM16_COMPLETE,
                        "v2_request_sha256": observed.request_sha256,
                        "blogger_terminal": terminal,
                    }
                )
            )
            return self._result(completed)

        if phase == DataPhase.FM16_COMPLETE or (
            phase == DataPhase.FM17_RESTORE_AMBIGUOUS and state.restore_operation_id is None
        ):
            assert state.blogger_terminal is not None
            before = state.pre_restore_accounting or await gateway.migration_accounting(
                state.blogger_terminal.export_batch_id
            )
            if before.canonical_revision != state.blogger_terminal.canonical_revision:
                return self._fail(state, "FM17_PRE_RESTORE_REVISION_MISMATCH", resumable=False)
            idempotency_key_sha256 = hashlib.sha256(
                canonical_json_bytes(
                    {
                        "matrix_id": str(plan.matrix_id),
                        "checkpoint_id": str(state.blogger_terminal.checkpoint.checkpoint_id),
                        "kind": "fm17-cold-restore",
                    }
                )
            ).hexdigest()
            planned = self._persist(
                state.model_copy(
                    update={
                        "phase": DataPhase.FM17_RESTORE_AMBIGUOUS,
                        "pre_restore_accounting": before,
                        "restore_idempotency_key_sha256": idempotency_key_sha256,
                    }
                )
            )
            accepted = await gateway.start_restore(
                idempotency_key_sha256=idempotency_key_sha256,
                checkpoint=state.blogger_terminal.checkpoint,
                expected_epoch=state.blogger_terminal.source_epoch,
            )
            next_phase = self._acceptance_phase(
                accepted, DataPhase.FM17_RESTORE_RUNNING, DataPhase.FM17_RESTORE_AMBIGUOUS
            )
            if next_phase is None:
                return self._fail(planned, "FM17_RESTORE_REJECTED", resumable=False)
            updated = self._persist(
                planned.model_copy(
                    update={
                        "phase": next_phase,
                        "restore_operation_id": accepted.operation_id,
                        "restore_request_sha256": accepted.request_sha256,
                        "mutations_started": planned.mutations_started + 1,
                    }
                )
            )
            if accepted.outcome == "ambiguous":
                return self._fail(updated, "FM17_RESTORE_AMBIGUOUS", resumable=True)
            return self._result(updated)

        if phase in {DataPhase.FM17_RESTORE_RUNNING, DataPhase.FM17_RESTORE_AMBIGUOUS}:
            assert state.restore_operation_id and state.pre_restore_accounting and state.blogger_terminal
            observed = await gateway.observe_restore(state.restore_operation_id)
            if observed.operation_id != state.restore_operation_id:
                return self._fail(state, "FM17_RESTORE_OBSERVATION_MISMATCH", resumable=False)
            if observed.state in {"REQUESTED", "RUNNING"}:
                return self._result(state)
            if observed.state != "DURABLE_COMPLETE":
                return self._fail(state, "FM17_RESTORE_NOT_DURABLE", resumable=False)
            master = await gateway.active_master()
            after = await gateway.migration_accounting(state.blogger_terminal.export_batch_id)
            if (
                after.equality_sha256 != state.pre_restore_accounting.equality_sha256
                or master.canonical_revision != after.canonical_revision
                or master.epoch <= state.blogger_terminal.source_epoch
                or master.master_instance_id == state.blogger_terminal.source_master_instance_id
            ):
                return self._fail(state, "FM17_ACCOUNTING_CHANGED_AFTER_RESTORE", resumable=False)
            completed = self._persist(
                state.model_copy(
                    update={
                        "phase": DataPhase.FM17_COMPLETE,
                        "restored_master": master,
                        "post_restore_accounting": after,
                    }
                )
            )
            return self._result(completed)

        if phase == DataPhase.FM17_COMPLETE:
            assert state.blogger_terminal is not None
            request_id = plan.identity("fm18-19:embedding")
            intent_sha = _sha(
                {
                    "request_id": str(request_id),
                    "blogger_request_sha256": state.blogger_terminal.request_sha256,
                    "checkpoint_id": str(state.blogger_terminal.checkpoint.checkpoint_id),
                    "probe_query_sha256": plan.embedding_probe_query_sha256,
                }
            )
            planned = self._persist(
                state.model_copy(
                    update={
                        "phase": DataPhase.FM18_19_AMBIGUOUS,
                        "embedding_request_id": request_id,
                    }
                )
            )
            accepted = await gateway.start_embedding(
                request_id=request_id,
                intent_sha256=intent_sha,
                blogger=state.blogger_terminal,
                probe_query_sha256=plan.embedding_probe_query_sha256,
            )
            if accepted.operation_id != str(request_id):
                return self._fail(planned, "FM18_19_OPERATION_MISMATCH", resumable=False)
            next_phase = self._acceptance_phase(accepted, DataPhase.FM18_19_RUNNING, DataPhase.FM18_19_AMBIGUOUS)
            if next_phase is None:
                return self._fail(planned, "FM18_19_REQUEST_REJECTED", resumable=False)
            updated = self._persist(
                planned.model_copy(
                    update={
                        "phase": next_phase,
                        "embedding_request_sha256": accepted.request_sha256,
                        "mutations_started": planned.mutations_started + 1,
                    }
                )
            )
            if accepted.outcome == "ambiguous":
                return self._fail(updated, "FM18_19_REQUEST_AMBIGUOUS", resumable=True)
            return self._result(updated)

        if phase == DataPhase.FM18_19_AMBIGUOUS:
            assert state.embedding_request_id and state.blogger_terminal
            intent_sha = _sha(
                {
                    "request_id": str(state.embedding_request_id),
                    "blogger_request_sha256": state.blogger_terminal.request_sha256,
                    "checkpoint_id": str(state.blogger_terminal.checkpoint.checkpoint_id),
                    "probe_query_sha256": plan.embedding_probe_query_sha256,
                }
            )
            accepted = await gateway.start_embedding(
                request_id=state.embedding_request_id,
                intent_sha256=intent_sha,
                blogger=state.blogger_terminal,
                probe_query_sha256=plan.embedding_probe_query_sha256,
            )
            if accepted.operation_id != str(state.embedding_request_id):
                return self._fail(state, "FM18_19_OPERATION_MISMATCH", resumable=False)
            if state.embedding_request_sha256 not in {None, accepted.request_sha256}:
                return self._fail(state, "FM18_19_REQUEST_REPLAY_MISMATCH", resumable=False)
            if accepted.outcome == "rejected":
                return self._fail(state, "FM18_19_REQUEST_REJECTED", resumable=False)
            replayed = self._persist(
                state.model_copy(
                    update={
                        "phase": (
                            DataPhase.FM18_19_AMBIGUOUS
                            if accepted.outcome == "ambiguous"
                            else DataPhase.FM18_19_RUNNING
                        ),
                        "embedding_request_sha256": accepted.request_sha256,
                    }
                )
            )
            if accepted.outcome == "ambiguous":
                return self._fail(replayed, "FM18_19_REQUEST_AMBIGUOUS", resumable=True)
            return self._result(replayed)

        if phase == DataPhase.FM18_19_RUNNING:
            assert state.embedding_request_id and state.blogger_terminal
            observed = await gateway.observe_embedding(state.embedding_request_id)
            if (
                observed.request_id != state.embedding_request_id
                or observed.request_sha256 != state.embedding_request_sha256
            ):
                return self._fail(state, "FM18_19_OBSERVATION_MISMATCH", resumable=False)
            if observed.state in {"REQUESTED", "CLAIMED", "STAGE_COMMITTED"}:
                return self._result(state)
            if observed.state != "CHECKPOINT_VERIFIED" or observed.terminal is None:
                return self._fail(state, "FM18_19_NOT_CHECKPOINT_VERIFIED", resumable=False)
            terminal = observed.terminal
            if (
                terminal.request_id != observed.request_id
                or terminal.request_sha256 != observed.request_sha256
                or terminal.blogger_export_batch_id != state.blogger_terminal.export_batch_id
                or terminal.blogger_canonical_revision != state.blogger_terminal.canonical_revision
            ):
                return self._fail(state, "FM18_19_BLOGGER_PREREQUISITE_MISMATCH", resumable=False)
            completed = self._persist(
                state.model_copy(
                    update={
                        "phase": DataPhase.FM18_19_COMPLETE,
                        "embedding_request_sha256": observed.request_sha256,
                        "embedding_terminal": terminal,
                    }
                )
            )
            return self._result(completed)

        if phase == DataPhase.FM18_19_COMPLETE:
            assert state.embedding_terminal is not None
            fixture_id = plan.identity("fm21:project")
            fixture_sha = _sha(
                {
                    "contract": "fm21_hub_project_fixture.v1",
                    "matrix_id": str(plan.matrix_id),
                    "project_id": str(fixture_id),
                }
            )
            intent = FixedChangeIntent(
                action="insert",
                fixture_project_id=fixture_id,
                fixture_sha256=fixture_sha,
                expected_revision=state.embedding_terminal.canonical_revision,
                idempotency_key_sha256=plan.key_sha256("fm21:insert"),
            )
            preview = await gateway.preview_fixed_change(intent)
            if (
                preview.request_sha256 != intent.request_sha256
                or preview.action != intent.action
                or preview.expected_revision != intent.expected_revision
                or preview.pre_change_checkpoint_id != state.embedding_terminal.checkpoint.checkpoint_id
                or preview.affected_rows != 1
            ):
                return self._fail(state, "FM21_INSERT_PREVIEW_NOT_EXACT", resumable=False)
            previewed = self._persist(
                state.model_copy(
                    update={
                        "phase": DataPhase.FM21_INSERT_PREVIEWED,
                        "fixture_project_id": fixture_id,
                        "fixture_sha256": fixture_sha,
                        "insert_preview": preview,
                    }
                )
            )
            return self._result(previewed)

        if phase == DataPhase.FM21_INSERT_PREVIEWED:
            assert state.insert_preview is not None
            applying = self._persist(state.model_copy(update={"phase": DataPhase.FM21_INSERT_AMBIGUOUS}))
            applied = await gateway.apply_fixed_change(state.insert_preview)
            if applied.operation_id != state.insert_preview.operation_id:
                return self._fail(applying, "FM21_INSERT_OPERATION_MISMATCH", resumable=False)
            if applied.outcome == "rejected":
                return self._fail(applying, "FM21_INSERT_REJECTED", resumable=False)
            next_phase = (
                DataPhase.FM21_INSERT_AMBIGUOUS if applied.outcome == "ambiguous" else DataPhase.FM21_INSERT_APPLYING
            )
            updated = self._persist(
                applying.model_copy(update={"phase": next_phase, "mutations_started": applying.mutations_started + 1})
            )
            if applied.outcome == "ambiguous":
                return self._fail(updated, "FM21_INSERT_AMBIGUOUS", resumable=True)
            return self._result(updated)

        if phase in {DataPhase.FM21_INSERT_APPLYING, DataPhase.FM21_INSERT_AMBIGUOUS}:
            assert state.insert_preview is not None
            status = await gateway.fixed_change_status(state.insert_preview.operation_id)
            if (
                status.operation_id != state.insert_preview.operation_id
                or status.expected_revision != state.insert_preview.expected_revision
                or status.pre_change_checkpoint_id != state.insert_preview.pre_change_checkpoint_id
            ):
                return self._fail(state, "FM21_INSERT_STATUS_MISMATCH", resumable=False)
            if status.state in {
                "PREVIEWED",
                "APPLYING",
                "COMMITTED_PENDING_CHECKPOINT",
                "CHECKPOINTING",
                "CHECKPOINT_VERIFIED",
            }:
                return self._result(state)
            if status.state != "DURABLE_COMPLETE" or status.committed_revision != status.expected_revision + 1:
                return self._fail(state, "FM21_INSERT_NOT_DURABLE", resumable=False)
            complete = self._persist(
                state.model_copy(update={"phase": DataPhase.FM21_INSERT_COMPLETE, "insert_status": status})
            )
            return self._result(complete)

        if phase == DataPhase.FM21_INSERT_COMPLETE:
            assert state.fixture_project_id and state.fixture_sha256 and state.insert_status
            intent = FixedChangeIntent(
                action="delete",
                fixture_project_id=state.fixture_project_id,
                fixture_sha256=state.fixture_sha256,
                expected_revision=int(state.insert_status.committed_revision),
                idempotency_key_sha256=plan.key_sha256("fm21:delete"),
            )
            preview = await gateway.preview_fixed_change(intent)
            assert state.insert_status.post_change_checkpoint is not None
            if (
                preview.request_sha256 != intent.request_sha256
                or preview.action != intent.action
                or preview.expected_revision != intent.expected_revision
                or preview.pre_change_checkpoint_id != state.insert_status.post_change_checkpoint.checkpoint_id
                or preview.affected_rows != 1
            ):
                return self._fail(state, "FM21_DELETE_PREVIEW_DID_NOT_FIND_FIXTURE", resumable=True)
            previewed = self._persist(
                state.model_copy(update={"phase": DataPhase.FM21_DELETE_PREVIEWED, "delete_preview": preview})
            )
            return self._result(previewed)

        if phase == DataPhase.FM21_DELETE_PREVIEWED:
            assert state.delete_preview is not None
            applying = self._persist(state.model_copy(update={"phase": DataPhase.FM21_DELETE_AMBIGUOUS}))
            applied = await gateway.apply_fixed_change(state.delete_preview)
            if applied.operation_id != state.delete_preview.operation_id:
                return self._fail(applying, "FM21_DELETE_OPERATION_MISMATCH", resumable=False)
            if applied.outcome == "rejected":
                return self._fail(applying, "FM21_DELETE_REJECTED", resumable=True)
            next_phase = (
                DataPhase.FM21_DELETE_AMBIGUOUS if applied.outcome == "ambiguous" else DataPhase.FM21_DELETE_APPLYING
            )
            updated = self._persist(
                applying.model_copy(update={"phase": next_phase, "mutations_started": applying.mutations_started + 1})
            )
            if applied.outcome == "ambiguous":
                return self._fail(updated, "FM21_DELETE_AMBIGUOUS", resumable=True)
            return self._result(updated)

        if phase in {DataPhase.FM21_DELETE_APPLYING, DataPhase.FM21_DELETE_AMBIGUOUS}:
            assert state.delete_preview is not None
            status = await gateway.fixed_change_status(state.delete_preview.operation_id)
            if (
                status.operation_id != state.delete_preview.operation_id
                or status.expected_revision != state.delete_preview.expected_revision
                or status.pre_change_checkpoint_id != state.delete_preview.pre_change_checkpoint_id
            ):
                return self._fail(state, "FM21_DELETE_STATUS_MISMATCH", resumable=True)
            if status.state in {
                "PREVIEWED",
                "APPLYING",
                "COMMITTED_PENDING_CHECKPOINT",
                "CHECKPOINTING",
                "CHECKPOINT_VERIFIED",
            }:
                return self._result(state)
            if status.state != "DURABLE_COMPLETE" or status.committed_revision != status.expected_revision + 1:
                return self._fail(state, "FM21_DELETE_NOT_DURABLE", resumable=True)
            complete = self._persist(
                state.model_copy(update={"phase": DataPhase.FM21_DELETE_COMPLETE, "delete_status": status})
            )
            return self._result(complete)

        if phase == DataPhase.FM21_DELETE_COMPLETE:
            assert state.fixture_project_id and state.fixture_sha256 and state.delete_status
            intent = FixedChangeIntent(
                action="delete",
                fixture_project_id=state.fixture_project_id,
                fixture_sha256=state.fixture_sha256,
                expected_revision=int(state.delete_status.committed_revision),
                idempotency_key_sha256=plan.key_sha256("fm21:verify-absent"),
            )
            preview = await gateway.preview_fixed_change(intent)
            assert state.delete_status.post_change_checkpoint is not None
            if (
                preview.request_sha256 != intent.request_sha256
                or preview.action != intent.action
                or preview.expected_revision != intent.expected_revision
                or preview.pre_change_checkpoint_id != state.delete_status.post_change_checkpoint.checkpoint_id
                or preview.affected_rows != 0
            ):
                return self._fail(state, "FM21_FIXTURE_CLEANUP_NOT_PROVEN", resumable=True)
            ready = self._persist(
                state.model_copy(update={"phase": DataPhase.EVIDENCE_READY, "final_zero_preview": preview})
            )
            bundle = self._bundle(plan, ready)
            return DataWorkloadExecutionResult(outcome="EVIDENCE_READY", state=ready, evidence=bundle)

        if phase == DataPhase.EVIDENCE_READY:
            return DataWorkloadExecutionResult(
                outcome="EVIDENCE_READY", state=state, evidence=self._bundle(plan, state)
            )
        raise RuntimeError(f"unsupported data-workload phase: {phase}")

    @staticmethod
    def _resume_phase(state: DataWorkloadState) -> DataPhase:
        mapping = {
            "FM16_V1_REQUEST_AMBIGUOUS": DataPhase.FM16_V1_AMBIGUOUS,
            "FM16_V2_REQUEST_AMBIGUOUS": DataPhase.FM16_V2_AMBIGUOUS,
            "FM17_RESTORE_AMBIGUOUS": DataPhase.FM17_RESTORE_AMBIGUOUS,
            "FM18_19_REQUEST_AMBIGUOUS": DataPhase.FM18_19_AMBIGUOUS,
            "FM21_INSERT_AMBIGUOUS": DataPhase.FM21_INSERT_AMBIGUOUS,
            "FM21_DELETE_AMBIGUOUS": DataPhase.FM21_DELETE_AMBIGUOUS,
            "FM21_DELETE_PREVIEW_DID_NOT_FIND_FIXTURE": DataPhase.FM21_INSERT_COMPLETE,
            "FM21_DELETE_REJECTED": DataPhase.FM21_DELETE_PREVIEWED,
            "FM21_DELETE_NOT_DURABLE": DataPhase.FM21_DELETE_AMBIGUOUS,
            "FM21_FIXTURE_CLEANUP_NOT_PROVEN": DataPhase.FM21_DELETE_COMPLETE,
            "FM16_OWNER_AUTHORIZATION_MISMATCH": DataPhase.AWAITING_OWNER_AUTHORIZATION,
        }
        if state.failure_code not in mapping:
            raise RuntimeError("resumable state has no exact recovery phase")
        return mapping[state.failure_code]

    @staticmethod
    def _bundle(plan: DataWorkloadPlan, state: DataWorkloadState) -> DataWorkloadEvidenceBundle:
        assert state.quarantine and state.duplicate_review and state.blogger_terminal
        assert state.pre_restore_accounting and state.post_restore_accounting and state.restored_master
        assert state.embedding_terminal and state.insert_status and state.delete_status and state.final_zero_preview
        e5 = state.embedding_terminal.for_model(E5_EXACT_ID)
        bge = state.embedding_terminal.for_model(BGE_EXACT_ID)
        fm16 = RequirementEvidence(
            requirement_id="FM16",
            assertion_evidence_sha256={
                "full_export_accounted": _sha(
                    {
                        "row_count": state.blogger_terminal.row_count,
                        "dispositions": state.blogger_terminal.dispositions,
                        "logical_sha256": state.blogger_terminal.logical_sha256,
                    }
                ),
                "transactional_import": _sha(
                    {
                        "request_sha256": state.blogger_terminal.request_sha256,
                        "canonical_revision": state.blogger_terminal.canonical_revision,
                    }
                ),
                "quarantine_accounted": _sha(
                    {
                        "quarantine": state.quarantine.model_dump(mode="json"),
                        "review_sha256": state.duplicate_review.review_projection_sha256,
                        "authorization_sha256": state.owner_authorization_sha256,
                    }
                ),
                "checkpoint_verified": _sha(state.blogger_terminal.checkpoint.model_dump(mode="json")),
            },
            operation_ids=(str(state.quarantine.operation_id), str(state.blogger_terminal.operation_id)),
        )
        fm17 = RequirementEvidence(
            requirement_id="FM17",
            assertion_evidence_sha256={
                "cold_restore_complete": _sha(state.restored_master.model_dump(mode="json")),
                "row_count_equal": _sha(
                    {
                        "before": state.pre_restore_accounting.raw_count,
                        "after": state.post_restore_accounting.raw_count,
                    }
                ),
                "logical_hash_equal": _sha(
                    {
                        "before": state.pre_restore_accounting.logical_sha256,
                        "after": state.post_restore_accounting.logical_sha256,
                    }
                ),
            },
            operation_ids=(str(state.restore_operation_id),),
        )

        def model_requirement(
            requirement_id: Literal["FM18", "FM19"], item: EmbeddingModelEvidence
        ) -> RequirementEvidence:
            exact_name = "exact_e5_model" if requirement_id == "FM18" else "exact_bge_m3_model"
            return RequirementEvidence(
                requirement_id=requirement_id,
                assertion_evidence_sha256={
                    exact_name: _sha(
                        {
                            "model_exact_id": item.model_exact_id,
                            "primary_source_sha256": item.primary_source_sha256,
                        }
                    ),
                    "corpus_accounted": _sha(
                        {
                            "expected": item.expected_documents,
                            "completed": item.completed_documents,
                            "coverage": item.coverage,
                        }
                    ),
                    "transactional_import": _sha(
                        {
                            "artifact_id": str(item.artifact_id),
                            "artifact_sha256": item.artifact_sha256,
                            "inserted_count": item.inserted_count,
                            "failed_count": item.failed_count,
                        }
                    ),
                    "checkpoint_required": _sha(
                        {
                            "checkpoint_required": item.checkpoint_required,
                            "checkpoint": state.embedding_terminal.checkpoint.model_dump(mode="json"),
                        }
                    ),
                },
                operation_ids=(str(state.embedding_terminal.request_id), str(item.task_run_id)),
            )

        fm21 = RequirementEvidence(
            requirement_id="FM21",
            assertion_evidence_sha256={
                "disposable_row": _sha(
                    {
                        "fixture_project_id": str(state.fixture_project_id),
                        "fixture_sha256": state.fixture_sha256,
                    }
                ),
                "preview_bound": _sha(
                    {
                        "insert": state.insert_preview.model_dump(mode="json") if state.insert_preview else None,
                        "delete": state.delete_preview.model_dump(mode="json") if state.delete_preview else None,
                        "absent": state.final_zero_preview.model_dump(mode="json"),
                    }
                ),
                "apply_bound": _sha(
                    {
                        "insert_operation_id": state.insert_status.operation_id,
                        "delete_operation_id": state.delete_status.operation_id,
                    }
                ),
                "post_checkpoint_verified": _sha(
                    {
                        "insert": state.insert_status.post_change_checkpoint.model_dump(mode="json"),  # type: ignore[union-attr]
                        "delete": state.delete_status.post_change_checkpoint.model_dump(mode="json"),  # type: ignore[union-attr]
                    }
                ),
                "durable_receipt": _sha(
                    {
                        "insert_state": state.insert_status.state,
                        "delete_state": state.delete_status.state,
                        "insert_revision": state.insert_status.committed_revision,
                        "delete_revision": state.delete_status.committed_revision,
                    }
                ),
            },
            operation_ids=(state.insert_status.operation_id, state.delete_status.operation_id),
        )
        return DataWorkloadEvidenceBundle(
            matrix_id=plan.matrix_id,
            source_commit=plan.source_commit,
            requirements=(fm16, fm17, model_requirement("FM18", e5), model_requirement("FM19", bge), fm21),
        )
