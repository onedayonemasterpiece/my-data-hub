from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal
from uuid import UUID, uuid4

import pytest
from jsonschema import Draft202012Validator, FormatChecker

from my_data_hub.checkpoints.acceptance import (
    CheckpointAcceptanceCoordinator,
    CheckpointAcceptanceError,
    CheckpointAcceptanceHead,
    CheckpointAcceptanceIntent,
    CheckpointAcceptanceReceipt,
    CheckpointAcceptanceStageReceipt,
    CorruptCheckpointRejectionRequest,
    DurableAcceptanceOperation,
    EmptyCheckpointRoundtripRequest,
    EvidenceClass,
    ForcedRestoreFailureRequest,
    StageOutcome,
)

NOW = datetime(2026, 8, 11, 12, tzinfo=UTC)
SOURCE_REVISION = "a" * 40
CURRENT = UUID("11111111-1111-4111-8111-111111111111")
PREVIOUS = UUID("22222222-2222-4222-8222-222222222222")


class FakeJournal:
    def __init__(self, events: list[str], *, lose_complete_once: bool = False) -> None:
        self.events = events
        self.operations: dict[UUID, DurableAcceptanceOperation] = {}
        self.lose_complete_once = lose_complete_once
        self.complete_lost = False

    def ensure_intent(self, intent: CheckpointAcceptanceIntent) -> DurableAcceptanceOperation:
        self.events.append("journal.intent")
        existing = self.operations.get(intent.operation_id)
        if existing is not None:
            if existing.intent.intent_sha256 != intent.intent_sha256:
                raise ValueError("idempotency conflict")
            return existing
        value = DurableAcceptanceOperation(intent=intent, state="INTENT_COMMITTED")
        self.operations[intent.operation_id] = value
        return value

    def operation(self, operation_id: UUID) -> DurableAcceptanceOperation | None:
        return self.operations.get(operation_id)

    def record_stage(
        self,
        operation_id: UUID,
        intent_sha256: str,
        receipt: CheckpointAcceptanceStageReceipt,
    ) -> DurableAcceptanceOperation:
        current = self.operations[operation_id]
        assert current.intent.intent_sha256 == intent_sha256
        by_name = {item.stage: item for item in current.stages}
        if receipt.stage in by_name:
            assert by_name[receipt.stage] == receipt
            return current
        value = current.model_copy(update={"state": "RUNNING", "stages": (*current.stages, receipt)})
        self.operations[operation_id] = value
        self.events.append(f"journal.stage:{receipt.stage}")
        return value

    def complete(
        self,
        operation_id: UUID,
        intent_sha256: str,
        receipt: CheckpointAcceptanceReceipt,
    ) -> DurableAcceptanceOperation:
        current = self.operations[operation_id]
        assert current.intent.intent_sha256 == intent_sha256
        if current.receipt is not None:
            assert current.receipt.receipt_sha256 == receipt.receipt_sha256
            return current
        value = current.model_copy(
            update={"state": "DURABLE_COMPLETE", "receipt": receipt}
        )
        self.operations[operation_id] = value
        self.events.append("journal.complete")
        if self.lose_complete_once and not self.complete_lost:
            self.complete_lost = True
            raise ConnectionError("completion committed but response was lost")
        return value

    def record_attempt_failure(
        self,
        operation_id: UUID,
        intent_sha256: str,
        failure_code: str,
    ) -> DurableAcceptanceOperation:
        current = self.operations[operation_id]
        assert current.intent.intent_sha256 == intent_sha256
        attempts = min(3, current.attempts + 1)
        value = current.model_copy(
            update={
                "attempts": attempts,
                "state": "FAILED" if attempts == 3 else "RUNNING",
                "failure_code": failure_code,
            }
        )
        self.operations[operation_id] = value
        self.events.append(f"journal.failure:{failure_code}")
        return value


class FakeEffects:
    def __init__(
        self,
        events: list[str],
        *,
        head: CheckpointAcceptanceHead,
        lose_once_after: str | None = None,
    ) -> None:
        self.events = events
        self._head = head
        self._evidence_class: EvidenceClass = "injected"
        self.receipts: dict[tuple[UUID, str], CheckpointAcceptanceStageReceipt] = {}
        self.physical_effects: dict[str, int] = {}
        self.lose_once_after = lose_once_after
        self.lost = False

    @property
    def evidence_class(self) -> EvidenceClass:
        return self._evidence_class

    def head(self) -> CheckpointAcceptanceHead:
        return self._head

    def _receipt(
        self,
        intent: CheckpointAcceptanceIntent,
        stage: str,
        detail: str,
        outcome: StageOutcome = "succeeded",
        **metadata: object,
    ) -> CheckpointAcceptanceStageReceipt:
        key = (intent.operation_id, stage)
        if key not in self.receipts:
            self.events.append(f"effect:{stage}")
            self.physical_effects[stage] = self.physical_effects.get(stage, 0) + 1
            payload = f"{intent.intent_sha256}:{stage}:{detail}".encode()
            self.receipts[key] = CheckpointAcceptanceStageReceipt(
                stage=stage,
                candidate_checkpoint_id=intent.candidate_checkpoint_id,
                task_owned=True,
                outcome=outcome,
                detail_code=detail,
                provider_receipt_sha256=hashlib.sha256(payload).hexdigest(),
                **metadata,
            )
        receipt = self.receipts[key]
        if self.lose_once_after == stage and not self.lost:
            self.lost = True
            raise ConnectionError("effect completed but its response was lost")
        return receipt

    def ensure_fm05_empty_candidate(
        self, intent: CheckpointAcceptanceIntent
    ) -> CheckpointAcceptanceStageReceipt:
        return self._receipt(
            intent,
            "empty_candidate",
            "EMPTY_CANDIDATE_CREATED",
            manifest_sha256="a" * 64,
            package_sha256="b" * 64,
            canonical_revision=0,
            canonical_row_count=0,
        )

    def ensure_fm05_private_upload(
        self, intent: CheckpointAcceptanceIntent
    ) -> CheckpointAcceptanceStageReceipt:
        return self._receipt(
            intent,
            "private_upload",
            "PRIVATE_CANDIDATE_UPLOADED",
            exact_version_ref=f"task-owned/{intent.candidate_checkpoint_id}/1",
        )

    def ensure_fm05_exact_readback(
        self, intent: CheckpointAcceptanceIntent
    ) -> CheckpointAcceptanceStageReceipt:
        return self._receipt(
            intent,
            "exact_readback",
            "EXACT_READBACK_VERIFIED",
            expected_content_sha256="c" * 64,
            observed_content_sha256="c" * 64,
        )

    def ensure_fm05_independent_restore(
        self, intent: CheckpointAcceptanceIntent
    ) -> CheckpointAcceptanceStageReceipt:
        return self._receipt(
            intent, "independent_restore", "INDEPENDENT_RESTORE_VERIFIED"
        )

    def ensure_fm05_cas_promotion(
        self, intent: CheckpointAcceptanceIntent
    ) -> CheckpointAcceptanceStageReceipt:
        expected = CheckpointAcceptanceHead(
            generation=intent.initial_head.generation + 1,
            current_checkpoint_id=intent.candidate_checkpoint_id,
            previous_checkpoint_id=intent.initial_head.current_checkpoint_id,
        )
        if self._head == intent.initial_head:
            self._head = expected
        elif self._head != expected:
            raise ValueError("CAS conflict")
        return self._receipt(intent, "cas_promotion", "HEAD_CAS_PROMOTED")

    def ensure_fm14_corrupted_candidate(
        self, intent: CheckpointAcceptanceIntent
    ) -> CheckpointAcceptanceStageReceipt:
        return self._receipt(
            intent,
            "corrupted_candidate",
            "TASK_OWNED_CORRUPTION_CANDIDATE_CREATED",
            disposable_candidate=True,
            manifest_sha256="d" * 64,
            package_sha256="e" * 64,
        )

    def ensure_fm14_hash_mismatch_rejection(
        self, intent: CheckpointAcceptanceIntent
    ) -> CheckpointAcceptanceStageReceipt:
        return self._receipt(
            intent,
            "hash_mismatch_rejection",
            "EXACT_READBACK_HASH_MISMATCH_REJECTED",
            "rejected_expected",
            expected_content_sha256="f" * 64,
            observed_content_sha256="0" * 64,
        )

    def ensure_fm15_restore_failure_candidate(
        self, intent: CheckpointAcceptanceIntent
    ) -> CheckpointAcceptanceStageReceipt:
        return self._receipt(
            intent,
            "restore_failure_candidate",
            "TASK_OWNED_RESTORE_FAILURE_CANDIDATE_CREATED",
            disposable_candidate=True,
            manifest_sha256="1" * 64,
            package_sha256="2" * 64,
        )

    def ensure_fm15_exact_readback(
        self, intent: CheckpointAcceptanceIntent
    ) -> CheckpointAcceptanceStageReceipt:
        return self._receipt(
            intent,
            "exact_readback",
            "EXACT_READBACK_VERIFIED",
            expected_content_sha256="3" * 64,
            observed_content_sha256="3" * 64,
        )

    def ensure_fm15_forced_restore_rejection(
        self, intent: CheckpointAcceptanceIntent
    ) -> CheckpointAcceptanceStageReceipt:
        return self._receipt(
            intent,
            "forced_restore_rejection",
            "FORCED_DISPOSABLE_RESTORE_FAILURE_REJECTED",
            "rejected_expected",
        )


def _head() -> CheckpointAcceptanceHead:
    return CheckpointAcceptanceHead(
        generation=7,
        current_checkpoint_id=CURRENT,
        previous_checkpoint_id=PREVIOUS,
    )


def _coordinator(
    *, lose_once_after: str | None = None, lose_complete_once: bool = False
) -> tuple[CheckpointAcceptanceCoordinator, FakeJournal, FakeEffects, list[str]]:
    events: list[str] = []
    journal = FakeJournal(events, lose_complete_once=lose_complete_once)
    effects = FakeEffects(events, head=_head(), lose_once_after=lose_once_after)
    coordinator = CheckpointAcceptanceCoordinator(
        journal=journal, effects=effects, now=lambda: NOW
    )
    return coordinator, journal, effects, events


def test_fm05_empty_candidate_roundtrip_commits_intent_before_effect_and_cas_promotes() -> None:
    coordinator, _journal, effects, events = _coordinator()
    request = EmptyCheckpointRoundtripRequest(
        operation_id=uuid4(), task_run_id=uuid4(),
        idempotency_key="fm05-empty-roundtrip", source_revision=SOURCE_REVISION,
    )
    receipt = coordinator.run_empty_roundtrip(request)

    assert events.index("journal.intent") < events.index("effect:empty_candidate")
    assert receipt.scenario == "FM05" and receipt.verdict == "CONTRACT_PASS"
    assert receipt.evidence_class == "injected" and receipt.head_unchanged is False
    assert receipt.final_head.generation == receipt.initial_head.generation + 1
    assert receipt.final_head.current_checkpoint_id == receipt.candidate_checkpoint_id
    assert receipt.final_head.previous_checkpoint_id == CURRENT
    assert effects.physical_effects == {
        "empty_candidate": 1, "private_upload": 1, "exact_readback": 1,
        "independent_restore": 1, "cas_promotion": 1,
    }


@pytest.mark.parametrize("lost_stage", ["private_upload", "cas_promotion"])
def test_fm05_lost_effect_response_resumes_without_duplicate_effect(lost_stage: str) -> None:
    coordinator, journal, effects, _events = _coordinator(lose_once_after=lost_stage)
    request = EmptyCheckpointRoundtripRequest(
        operation_id=uuid4(), task_run_id=uuid4(),
        idempotency_key="fm05-response-loss", source_revision=SOURCE_REVISION,
    )
    with pytest.raises(ConnectionError, match="response was lost"):
        coordinator.run_empty_roundtrip(request)
    assert journal.operation(request.operation_id).state == "RUNNING"  # type: ignore[union-attr]

    receipt = coordinator.run_empty_roundtrip(request)
    replay = coordinator.run_empty_roundtrip(request)
    assert replay.receipt_sha256 == receipt.receipt_sha256
    assert effects.physical_effects[lost_stage] == 1
    assert set(effects.physical_effects.values()) == {1}


def test_lost_terminal_journal_response_reconciles_exact_durable_receipt() -> None:
    coordinator, journal, effects, _events = _coordinator(lose_complete_once=True)
    request = CorruptCheckpointRejectionRequest(
        operation_id=uuid4(), task_run_id=uuid4(),
        idempotency_key="fm14-terminal-response-loss", source_revision=SOURCE_REVISION,
    )
    with pytest.raises(ConnectionError, match="completion committed"):
        coordinator.run_corruption_rejection(request)
    stored = journal.operation(request.operation_id)
    assert stored is not None and stored.state == "DURABLE_COMPLETE" and stored.receipt is not None
    replay = coordinator.run_corruption_rejection(request)
    assert replay.receipt_sha256 == stored.receipt.receipt_sha256
    assert set(effects.physical_effects.values()) == {1}


@pytest.mark.parametrize(
    ("scenario", "acceptance_request"),
    [
        (
            "FM14",
            CorruptCheckpointRejectionRequest(
                operation_id=uuid4(), task_run_id=uuid4(),
                idempotency_key="fm14-corruption", source_revision=SOURCE_REVISION,
            ),
        ),
        (
            "FM15",
            ForcedRestoreFailureRequest(
                operation_id=uuid4(), task_run_id=uuid4(),
                idempotency_key="fm15-restore-failure", source_revision=SOURCE_REVISION,
            ),
        ),
    ],
)
def test_negative_candidate_is_disposable_and_head_remains_exact(
    scenario: Literal["FM14", "FM15"],
    acceptance_request: CorruptCheckpointRejectionRequest | ForcedRestoreFailureRequest,
) -> None:
    coordinator, _journal, effects, events = _coordinator()
    if scenario == "FM14":
        assert isinstance(acceptance_request, CorruptCheckpointRejectionRequest)
        receipt = coordinator.run_corruption_rejection(acceptance_request)
        mismatch = receipt.stages[-1]
        assert mismatch.expected_content_sha256 != mismatch.observed_content_sha256
    else:
        assert isinstance(acceptance_request, ForcedRestoreFailureRequest)
        receipt = coordinator.run_forced_restore_failure(acceptance_request)
        assert receipt.stages[-1].detail_code == "FORCED_DISPOSABLE_RESTORE_FAILURE_REJECTED"

    assert events.index("journal.intent") < min(
        index for index, event in enumerate(events) if event.startswith("effect:")
    )
    assert receipt.scenario == scenario and receipt.verdict == "CONTRACT_PASS"
    assert receipt.head_unchanged is True and receipt.final_head == _head()
    assert receipt.candidate_checkpoint_id not in {CURRENT, PREVIOUS}
    assert effects.head() == _head()


def test_wrong_task_owned_stage_identity_and_head_drift_fail_closed() -> None:
    coordinator, _journal, effects, _events = _coordinator()
    request = CorruptCheckpointRejectionRequest(
        operation_id=uuid4(), task_run_id=uuid4(),
        idempotency_key="fm14-wrong-candidate", source_revision=SOURCE_REVISION,
    )
    original = effects.ensure_fm14_corrupted_candidate

    def wrong(intent: CheckpointAcceptanceIntent) -> CheckpointAcceptanceStageReceipt:
        return original(intent).model_copy(update={"candidate_checkpoint_id": uuid4()})

    effects.ensure_fm14_corrupted_candidate = wrong  # type: ignore[method-assign]
    with pytest.raises(CheckpointAcceptanceError, match="fixed task-owned"):
        coordinator.run_corruption_rejection(request)

    coordinator, _journal, effects, _events = _coordinator()
    original_rejection = effects.ensure_fm14_hash_mismatch_rejection

    def drift(intent: CheckpointAcceptanceIntent) -> CheckpointAcceptanceStageReceipt:
        effects._head = _head().model_copy(update={"generation": 8})
        return original_rejection(intent)

    effects.ensure_fm14_hash_mismatch_rejection = drift  # type: ignore[method-assign]
    with pytest.raises(CheckpointAcceptanceError, match="non-promoted candidate changed"):
        coordinator.run_corruption_rejection(
            request.model_copy(update={"operation_id": uuid4()})
        )


def test_fixed_attempt_budget_terminalizes_repeated_contract_violation() -> None:
    coordinator, journal, effects, _events = _coordinator()
    request = CorruptCheckpointRejectionRequest(
        operation_id=uuid4(), task_run_id=uuid4(),
        idempotency_key="fm14-attempt-budget", source_revision=SOURCE_REVISION,
    )
    original = effects.ensure_fm14_corrupted_candidate

    def wrong(intent: CheckpointAcceptanceIntent) -> CheckpointAcceptanceStageReceipt:
        return original(intent).model_copy(update={"candidate_checkpoint_id": uuid4()})

    effects.ensure_fm14_corrupted_candidate = wrong  # type: ignore[method-assign]
    for _attempt in range(2):
        with pytest.raises(CheckpointAcceptanceError, match="fixed task-owned"):
            coordinator.run_corruption_rejection(request)
    with pytest.raises(CheckpointAcceptanceError, match="exhausted its fixed attempts"):
        coordinator.run_corruption_rejection(request)
    operation = journal.operation(request.operation_id)
    assert operation is not None and operation.state == "FAILED" and operation.attempts == 3
    with pytest.raises(CheckpointAcceptanceError, match="terminally failed"):
        coordinator.run_corruption_rejection(request)


def test_receipt_schema_example_is_strict_and_injected_never_claims_live() -> None:
    root = Path(__file__).resolve().parents[2]
    pairs = (
        ("checkpoint-acceptance-fm05-request.v1", EmptyCheckpointRoundtripRequest),
        ("checkpoint-acceptance-fm14-request.v1", CorruptCheckpointRejectionRequest),
        ("checkpoint-acceptance-fm15-request.v1", ForcedRestoreFailureRequest),
        ("checkpoint-acceptance-intent.v1", CheckpointAcceptanceIntent),
        ("checkpoint-acceptance-receipt.v1", CheckpointAcceptanceReceipt),
    )
    examples: dict[str, object] = {}
    for name, model in pairs:
        schema = json.loads((root / f"schemas/{name}.schema.json").read_text())
        example = json.loads((root / f"examples/contracts/{name}.example.json").read_text())
        Draft202012Validator.check_schema(schema)
        Draft202012Validator(schema, format_checker=FormatChecker()).validate(example)
        model.model_validate(example)
        examples[name] = example
    example = examples["checkpoint-acceptance-receipt.v1"]
    assert isinstance(example, dict)
    parsed = CheckpointAcceptanceReceipt.model_validate(example)
    assert parsed.verdict == "CONTRACT_PASS" and parsed.evidence_class == "injected"
    with pytest.raises(ValueError, match="overstates"):
        CheckpointAcceptanceReceipt.model_validate({**example, "verdict": "LIVE_PASS"})
