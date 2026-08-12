"""Fixed, task-owned checkpoint acceptance operations for FM05, FM14 and FM15.

The coordinator persists a metadata-only intent before invoking any provider
effect.  Provider ports expose scenario-specific idempotent ``ensure_*`` calls;
there is deliberately no generic byte upload or caller-selected fault mode.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from typing import Any, Literal, Protocol
from uuid import UUID, uuid5

from pydantic import BaseModel, ConfigDict, Field, model_validator

from my_data_hub.hashing import canonical_json_bytes

ACCEPTANCE_RECEIPT_SCHEMA = "my-data-hub-checkpoint-acceptance-receipt.v1"
ACCEPTANCE_INTENT_SCHEMA = "my-data-hub-checkpoint-acceptance-intent.v1"
ACCEPTANCE_TIMEOUT_SECONDS = 900
ACCEPTANCE_MAX_ATTEMPTS = 3
MAX_ACCEPTANCE_METADATA_BYTES = 64 * 1024
_CANDIDATE_NAMESPACE = UUID("81423c74-3df2-5454-b00a-497c813f2c43")

Scenario = Literal["FM05", "FM14", "FM15"]
EvidenceClass = Literal["injected", "live"]
StageOutcome = Literal["succeeded", "rejected_expected"]


def checkpoint_acceptance_candidate_id(scenario: Scenario, operation_id: UUID, task_run_id: UUID) -> UUID:
    """Deterministic task-owned candidate identity shared with restart recovery."""

    return uuid5(_CANDIDATE_NAMESPACE, f"{scenario}:{operation_id}:{task_run_id}")


class CheckpointAcceptanceError(RuntimeError):
    """A fixed acceptance operation violated its durable safety contract."""


class CheckpointAcceptanceHead(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    generation: int = Field(ge=0)
    current_checkpoint_id: UUID | None = None
    previous_checkpoint_id: UUID | None = None

    @property
    def snapshot_sha256(self) -> str:
        return hashlib.sha256(canonical_json_bytes(self.model_dump(mode="json"))).hexdigest()


class _AcceptanceRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    operation_id: UUID
    task_run_id: UUID
    idempotency_key: str = Field(min_length=8, max_length=200)
    source_revision: str = Field(pattern=r"^[0-9a-f]{40}$")


class EmptyCheckpointRoundtripRequest(_AcceptanceRequest):
    schema_version: Literal["my-data-hub-checkpoint-acceptance-fm05.v1"] = "my-data-hub-checkpoint-acceptance-fm05.v1"


class CorruptCheckpointRejectionRequest(_AcceptanceRequest):
    schema_version: Literal["my-data-hub-checkpoint-acceptance-fm14.v1"] = "my-data-hub-checkpoint-acceptance-fm14.v1"


class ForcedRestoreFailureRequest(_AcceptanceRequest):
    schema_version: Literal["my-data-hub-checkpoint-acceptance-fm15.v1"] = "my-data-hub-checkpoint-acceptance-fm15.v1"


class CheckpointAcceptanceIntent(BaseModel):
    """Durable pre-effect record containing only bounded identities and HEAD."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["my-data-hub-checkpoint-acceptance-intent.v1"] = ACCEPTANCE_INTENT_SCHEMA
    scenario: Scenario
    operation_id: UUID
    task_run_id: UUID
    idempotency_key_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_revision: str = Field(pattern=r"^[0-9a-f]{40}$")
    candidate_checkpoint_id: UUID
    evidence_class: EvidenceClass
    initial_head: CheckpointAcceptanceHead
    timeout_seconds: Literal[900] = ACCEPTANCE_TIMEOUT_SECONDS
    max_attempts: Literal[3] = ACCEPTANCE_MAX_ATTEMPTS

    @property
    def intent_sha256(self) -> str:
        return hashlib.sha256(canonical_json_bytes(self.model_dump(mode="json"))).hexdigest()


class CheckpointAcceptanceStageReceipt(BaseModel):
    """Bounded metadata returned by one fixed idempotent provider effect."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    stage: str = Field(pattern=r"^[a-z][a-z0-9_]{2,63}$")
    candidate_checkpoint_id: UUID
    task_owned: Literal[True] = True
    disposable_candidate: bool = False
    outcome: StageOutcome
    detail_code: str = Field(pattern=r"^[A-Z][A-Z0-9_]{2,99}$")
    provider_receipt_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    manifest_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    package_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    expected_content_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    observed_content_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    exact_version_ref: str | None = Field(default=None, min_length=1, max_length=512)
    canonical_revision: int | None = Field(default=None, ge=0)
    canonical_row_count: int | None = Field(default=None, ge=0)


class CheckpointAcceptanceReceipt(BaseModel):
    """Terminal bounded receipt; injected contract evidence is never labelled live."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["my-data-hub-checkpoint-acceptance-receipt.v1"] = ACCEPTANCE_RECEIPT_SCHEMA
    scenario: Scenario
    verdict: Literal["CONTRACT_PASS", "LIVE_PASS"]
    evidence_class: EvidenceClass
    operation_id: UUID
    task_run_id: UUID
    candidate_checkpoint_id: UUID
    intent_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    initial_head: CheckpointAcceptanceHead
    final_head: CheckpointAcceptanceHead
    head_unchanged: bool
    stages: tuple[CheckpointAcceptanceStageReceipt, ...] = Field(min_length=2, max_length=5)
    completed_at: datetime

    @model_validator(mode="after")
    def evidence_verdict(self) -> CheckpointAcceptanceReceipt:
        if self.completed_at.tzinfo is None:
            raise ValueError("checkpoint acceptance completion must be timezone-aware")
        if (self.evidence_class == "live") != (self.verdict == "LIVE_PASS"):
            raise ValueError("checkpoint acceptance verdict overstates its evidence class")
        if self.scenario == "FM05" and self.head_unchanged:
            raise ValueError("FM05 must advance the exact checkpoint HEAD")
        if self.scenario in {"FM14", "FM15"} and not self.head_unchanged:
            raise ValueError("negative checkpoint acceptance changed canonical HEAD")
        return self

    @property
    def receipt_sha256(self) -> str:
        return hashlib.sha256(canonical_json_bytes(self.model_dump(mode="json"))).hexdigest()


class DurableAcceptanceOperation(BaseModel):
    """Projection supplied by a durable journal implementation."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    intent: CheckpointAcceptanceIntent
    state: Literal["INTENT_COMMITTED", "RUNNING", "DURABLE_COMPLETE", "FAILED"]
    stages: tuple[CheckpointAcceptanceStageReceipt, ...] = ()
    attempts: int = Field(default=0, ge=0, le=ACCEPTANCE_MAX_ATTEMPTS)
    receipt: CheckpointAcceptanceReceipt | None = None
    failure_code: str | None = Field(default=None, pattern=r"^[A-Z][A-Z0-9_]{2,99}$")


class CheckpointAcceptanceJournal(Protocol):
    """Durable metadata journal; implementations must reject conflicting replay."""

    def ensure_intent(self, intent: CheckpointAcceptanceIntent) -> DurableAcceptanceOperation: ...

    def operation(self, operation_id: UUID) -> DurableAcceptanceOperation | None: ...

    def record_stage(
        self,
        operation_id: UUID,
        intent_sha256: str,
        receipt: CheckpointAcceptanceStageReceipt,
    ) -> DurableAcceptanceOperation: ...

    def complete(
        self,
        operation_id: UUID,
        intent_sha256: str,
        receipt: CheckpointAcceptanceReceipt,
    ) -> DurableAcceptanceOperation: ...

    def record_attempt_failure(
        self,
        operation_id: UUID,
        intent_sha256: str,
        failure_code: str,
    ) -> DurableAcceptanceOperation: ...


class TaskOwnedCheckpointEffects(Protocol):
    """Scenario-specific idempotent effects; no method accepts package bytes or a mode."""

    @property
    def evidence_class(self) -> EvidenceClass: ...

    def head(self) -> CheckpointAcceptanceHead: ...

    def ensure_fm05_empty_candidate(self, intent: CheckpointAcceptanceIntent) -> CheckpointAcceptanceStageReceipt: ...

    def ensure_fm05_private_upload(self, intent: CheckpointAcceptanceIntent) -> CheckpointAcceptanceStageReceipt: ...

    def ensure_fm05_exact_readback(self, intent: CheckpointAcceptanceIntent) -> CheckpointAcceptanceStageReceipt: ...

    def ensure_fm05_independent_restore(
        self, intent: CheckpointAcceptanceIntent
    ) -> CheckpointAcceptanceStageReceipt: ...

    def ensure_fm05_cas_promotion(self, intent: CheckpointAcceptanceIntent) -> CheckpointAcceptanceStageReceipt: ...

    def ensure_fm14_corrupted_candidate(
        self, intent: CheckpointAcceptanceIntent
    ) -> CheckpointAcceptanceStageReceipt: ...

    def ensure_fm14_hash_mismatch_rejection(
        self, intent: CheckpointAcceptanceIntent
    ) -> CheckpointAcceptanceStageReceipt: ...

    def ensure_fm15_restore_failure_candidate(
        self, intent: CheckpointAcceptanceIntent
    ) -> CheckpointAcceptanceStageReceipt: ...

    def ensure_fm15_exact_readback(self, intent: CheckpointAcceptanceIntent) -> CheckpointAcceptanceStageReceipt: ...

    def ensure_fm15_forced_restore_rejection(
        self, intent: CheckpointAcceptanceIntent
    ) -> CheckpointAcceptanceStageReceipt: ...


_EXPECTED_STAGES: dict[Scenario, tuple[tuple[str, str, StageOutcome], ...]] = {
    "FM05": (
        ("empty_candidate", "EMPTY_CANDIDATE_CREATED", "succeeded"),
        ("private_upload", "PRIVATE_CANDIDATE_UPLOADED", "succeeded"),
        ("exact_readback", "EXACT_READBACK_VERIFIED", "succeeded"),
        ("independent_restore", "INDEPENDENT_RESTORE_VERIFIED", "succeeded"),
        ("cas_promotion", "HEAD_CAS_PROMOTED", "succeeded"),
    ),
    "FM14": (
        ("corrupted_candidate", "TASK_OWNED_CORRUPTION_CANDIDATE_CREATED", "succeeded"),
        ("hash_mismatch_rejection", "EXACT_READBACK_HASH_MISMATCH_REJECTED", "rejected_expected"),
    ),
    "FM15": (
        ("restore_failure_candidate", "TASK_OWNED_RESTORE_FAILURE_CANDIDATE_CREATED", "succeeded"),
        ("exact_readback", "EXACT_READBACK_VERIFIED", "succeeded"),
        ("forced_restore_rejection", "FORCED_DISPOSABLE_RESTORE_FAILURE_REJECTED", "rejected_expected"),
    ),
}


class CheckpointAcceptanceCoordinator:
    """Execute the three fixed operations with durable intent and exact HEAD checks."""

    def __init__(
        self,
        *,
        journal: CheckpointAcceptanceJournal,
        effects: TaskOwnedCheckpointEffects,
        now: Any = lambda: datetime.now(UTC),
    ) -> None:
        self.journal = journal
        self.effects = effects
        self.now = now

    def run_empty_roundtrip(self, request: EmptyCheckpointRoundtripRequest) -> CheckpointAcceptanceReceipt:
        return self._run("FM05", request)

    def run_corruption_rejection(self, request: CorruptCheckpointRejectionRequest) -> CheckpointAcceptanceReceipt:
        return self._run("FM14", request)

    def run_forced_restore_failure(self, request: ForcedRestoreFailureRequest) -> CheckpointAcceptanceReceipt:
        return self._run("FM15", request)

    def _intent(
        self,
        scenario: Scenario,
        request: _AcceptanceRequest,
        initial_head: CheckpointAcceptanceHead,
    ) -> CheckpointAcceptanceIntent:
        candidate_id = checkpoint_acceptance_candidate_id(scenario, request.operation_id, request.task_run_id)
        if candidate_id in {
            initial_head.current_checkpoint_id,
            initial_head.previous_checkpoint_id,
        }:
            raise CheckpointAcceptanceError("task-owned candidate collides with protected HEAD")
        return CheckpointAcceptanceIntent(
            scenario=scenario,
            operation_id=request.operation_id,
            task_run_id=request.task_run_id,
            idempotency_key_sha256=hashlib.sha256(request.idempotency_key.encode()).hexdigest(),
            source_revision=request.source_revision,
            candidate_checkpoint_id=candidate_id,
            evidence_class=self.effects.evidence_class,
            initial_head=initial_head,
        )

    def _run(
        self,
        scenario: Scenario,
        request: _AcceptanceRequest,
    ) -> CheckpointAcceptanceReceipt:
        existing = self.journal.operation(request.operation_id)
        if existing is None:
            intent = self._intent(scenario, request, self.effects.head())
        else:
            intent = existing.intent
            self._assert_request_binding(scenario, request, intent)
            if existing.state == "DURABLE_COMPLETE":
                if existing.receipt is None:
                    raise CheckpointAcceptanceError("durable completion lacks its exact receipt")
                return existing.receipt
            if existing.state == "FAILED":
                raise CheckpointAcceptanceError(
                    f"checkpoint acceptance is terminally failed: {existing.failure_code or 'UNKNOWN'}"
                )
        operation = self.journal.ensure_intent(intent)
        if operation.intent.intent_sha256 != intent.intent_sha256:
            raise CheckpointAcceptanceError("operation identity was reused for different intent")
        intent = operation.intent
        if len(canonical_json_bytes(intent.model_dump(mode="json"))) > MAX_ACCEPTANCE_METADATA_BYTES:
            raise CheckpointAcceptanceError("checkpoint acceptance intent exceeds 64 KiB")

        calls = self._effect_calls(scenario)
        collected: list[CheckpointAcceptanceStageReceipt] = []
        try:
            for index, effect in enumerate(calls):
                self._assert_head_before_stage(intent, scenario, index)
                stage = effect(intent)
                self._validate_stage(intent, scenario, index, stage)
                self.journal.record_stage(intent.operation_id, intent.intent_sha256, stage)
                collected.append(stage)
                self._assert_head_after_stage(intent, scenario, index)
        except Exception as exc:
            failure_code = (type(exc).__name__.lstrip("_") or "ERROR").upper()[:100]
            failed = self.journal.record_attempt_failure(
                intent.operation_id,
                intent.intent_sha256,
                failure_code,
            )
            if failed.state == "FAILED":
                raise CheckpointAcceptanceError(
                    f"checkpoint acceptance exhausted its fixed attempts: {failure_code}"
                ) from exc
            raise

        final_head = self.effects.head()
        unchanged = final_head == intent.initial_head
        receipt = CheckpointAcceptanceReceipt(
            scenario=scenario,
            verdict="LIVE_PASS" if intent.evidence_class == "live" else "CONTRACT_PASS",
            evidence_class=intent.evidence_class,
            operation_id=intent.operation_id,
            task_run_id=intent.task_run_id,
            candidate_checkpoint_id=intent.candidate_checkpoint_id,
            intent_sha256=intent.intent_sha256,
            initial_head=intent.initial_head,
            final_head=final_head,
            head_unchanged=unchanged,
            stages=tuple(collected),
            completed_at=self.now().astimezone(UTC),
        )
        if len(canonical_json_bytes(receipt.model_dump(mode="json"))) > MAX_ACCEPTANCE_METADATA_BYTES:
            raise CheckpointAcceptanceError("checkpoint acceptance receipt exceeds 64 KiB")
        completed = self.journal.complete(intent.operation_id, intent.intent_sha256, receipt)
        if completed.receipt is None or completed.receipt.receipt_sha256 != receipt.receipt_sha256:
            raise CheckpointAcceptanceError("durable journal completed with a different receipt")
        return completed.receipt

    def _assert_request_binding(
        self,
        scenario: Scenario,
        request: _AcceptanceRequest,
        intent: CheckpointAcceptanceIntent,
    ) -> None:
        expected_candidate = uuid5(
            _CANDIDATE_NAMESPACE,
            f"{scenario}:{request.operation_id}:{request.task_run_id}",
        )
        if (
            intent.scenario != scenario
            or intent.operation_id != request.operation_id
            or intent.task_run_id != request.task_run_id
            or intent.idempotency_key_sha256 != hashlib.sha256(request.idempotency_key.encode()).hexdigest()
            or intent.source_revision != request.source_revision
            or intent.candidate_checkpoint_id != expected_candidate
            or intent.evidence_class != self.effects.evidence_class
        ):
            raise CheckpointAcceptanceError("operation identity was reused for a different request")

    def _effect_calls(self, scenario: Scenario) -> tuple[Any, ...]:
        if scenario == "FM05":
            return (
                self.effects.ensure_fm05_empty_candidate,
                self.effects.ensure_fm05_private_upload,
                self.effects.ensure_fm05_exact_readback,
                self.effects.ensure_fm05_independent_restore,
                self.effects.ensure_fm05_cas_promotion,
            )
        if scenario == "FM14":
            return (
                self.effects.ensure_fm14_corrupted_candidate,
                self.effects.ensure_fm14_hash_mismatch_rejection,
            )
        return (
            self.effects.ensure_fm15_restore_failure_candidate,
            self.effects.ensure_fm15_exact_readback,
            self.effects.ensure_fm15_forced_restore_rejection,
        )

    def _validate_stage(
        self,
        intent: CheckpointAcceptanceIntent,
        scenario: Scenario,
        index: int,
        receipt: CheckpointAcceptanceStageReceipt,
    ) -> None:
        expected_stage, expected_detail, expected_outcome = _EXPECTED_STAGES[scenario][index]
        if (
            receipt.candidate_checkpoint_id != intent.candidate_checkpoint_id
            or receipt.stage != expected_stage
            or receipt.detail_code != expected_detail
            or receipt.outcome != expected_outcome
        ):
            raise CheckpointAcceptanceError("provider stage differs from the fixed task-owned contract")
        if (
            scenario == "FM05"
            and index == 0
            and (
                receipt.canonical_revision != 0
                or receipt.canonical_row_count != 0
                or receipt.manifest_sha256 is None
                or receipt.package_sha256 is None
            )
        ):
            raise CheckpointAcceptanceError("FM05 candidate is not the fixed empty checkpoint")
        if index == 0 and (
            (scenario == "FM05" and receipt.disposable_candidate)
            or (scenario in {"FM14", "FM15"} and not receipt.disposable_candidate)
        ):
            raise CheckpointAcceptanceError("candidate disposal class differs from its fixed scenario")
        if scenario == "FM05" and index == 1 and receipt.exact_version_ref is None:
            raise CheckpointAcceptanceError("FM05 upload lacks an exact private version")
        if (scenario, index) in {("FM05", 2), ("FM15", 1)} and (
            receipt.expected_content_sha256 is None
            or receipt.expected_content_sha256 != receipt.observed_content_sha256
        ):
            raise CheckpointAcceptanceError("exact checkpoint readback hash did not match")
        if (
            scenario == "FM14"
            and index == 1
            and (
                receipt.expected_content_sha256 is None
                or receipt.observed_content_sha256 is None
                or receipt.expected_content_sha256 == receipt.observed_content_sha256
            )
        ):
            raise CheckpointAcceptanceError("FM14 did not prove a deterministic hash mismatch")

    def _assert_head_before_stage(
        self,
        intent: CheckpointAcceptanceIntent,
        scenario: Scenario,
        index: int,
    ) -> None:
        observed = self.effects.head()
        if scenario != "FM05":
            if observed != intent.initial_head:
                raise CheckpointAcceptanceError("checkpoint HEAD changed before safe terminal stage")
            return
        expected_post = CheckpointAcceptanceHead(
            generation=intent.initial_head.generation + 1,
            current_checkpoint_id=intent.candidate_checkpoint_id,
            previous_checkpoint_id=intent.initial_head.current_checkpoint_id,
        )
        if observed not in (intent.initial_head, expected_post):
            raise CheckpointAcceptanceError("checkpoint HEAD changed outside the exact FM05 CAS")

    def _assert_head_after_stage(
        self,
        intent: CheckpointAcceptanceIntent,
        scenario: Scenario,
        index: int,
    ) -> None:
        observed = self.effects.head()
        if scenario == "FM05":
            expected = CheckpointAcceptanceHead(
                generation=intent.initial_head.generation + 1,
                current_checkpoint_id=intent.candidate_checkpoint_id,
                previous_checkpoint_id=intent.initial_head.current_checkpoint_id,
            )
            allowed = (expected,) if index == 4 else (intent.initial_head, expected)
            if observed not in allowed:
                raise CheckpointAcceptanceError("FM05 CAS did not produce the exact next HEAD")
        elif observed != intent.initial_head:
            raise CheckpointAcceptanceError("non-promoted candidate changed current or previous HEAD")
