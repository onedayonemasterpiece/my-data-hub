from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

import my_data_hub.checkpoints.acceptance_runtime as acceptance_runtime
from my_data_hub.checkpoints.acceptance import (
    CheckpointAcceptanceError,
    CheckpointAcceptanceHead,
    CheckpointAcceptanceIntent,
    CheckpointAcceptanceReceipt,
    CheckpointAcceptanceStageReceipt,
)
from my_data_hub.checkpoints.acceptance_runtime import (
    CheckpointAcceptanceRuntimeBinding,
    ControlLedgerCheckpointAcceptanceJournal,
    KaggleTaskOwnedCheckpointEffects,
)
from my_data_hub.checkpoints.registry import CheckpointHead
from my_data_hub.control_plane.ledger import ControlLedger
from my_data_hub.control_plane.ledger.errors import IdempotencyConflict
from my_data_hub.providers.kaggle.retry import BoundedRetry, RetryPolicy

NOW = datetime(2026, 8, 11, 12, tzinfo=UTC)


def _intent(scenario: str = "FM14") -> CheckpointAcceptanceIntent:
    return CheckpointAcceptanceIntent(
        scenario=scenario,
        operation_id=UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
        task_run_id=UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
        idempotency_key_sha256="1" * 64,
        source_revision="2" * 40,
        candidate_checkpoint_id=UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc"),
        evidence_class="injected",
        initial_head=CheckpointAcceptanceHead(generation=0),
    )


def _stage(intent: CheckpointAcceptanceIntent) -> CheckpointAcceptanceStageReceipt:
    return CheckpointAcceptanceStageReceipt(
        stage="corrupted_candidate",
        candidate_checkpoint_id=intent.candidate_checkpoint_id,
        task_owned=True,
        disposable_candidate=True,
        outcome="succeeded",
        detail_code="TASK_OWNED_CORRUPTION_CANDIDATE_CREATED",
        provider_receipt_sha256="3" * 64,
    )


def test_control_ledger_acceptance_journal_survives_restart_and_replay(tmp_path: Path) -> None:
    ledger = ControlLedger(tmp_path / "control.sqlite3")
    journal = ControlLedgerCheckpointAcceptanceJournal(ledger)
    intent = _intent()

    assert journal.ensure_intent(intent).state == "INTENT_COMMITTED"
    first = journal.record_stage(intent.operation_id, intent.intent_sha256, _stage(intent))
    assert first.state == "RUNNING"
    assert journal.record_stage(intent.operation_id, intent.intent_sha256, _stage(intent)) == first

    restarted = ControlLedgerCheckpointAcceptanceJournal(ControlLedger(ledger.path))
    recovered = restarted.operation(intent.operation_id)
    assert recovered is not None
    assert recovered.intent == intent
    assert recovered.stages == (_stage(intent),)


def test_control_ledger_acceptance_journal_persists_terminal_receipt(tmp_path: Path) -> None:
    journal = ControlLedgerCheckpointAcceptanceJournal(ControlLedger(tmp_path / "control.sqlite3"))
    intent = _intent()
    journal.ensure_intent(intent)
    stage = _stage(intent)
    journal.record_stage(intent.operation_id, intent.intent_sha256, stage)
    receipt = CheckpointAcceptanceReceipt(
        scenario="FM14",
        verdict="CONTRACT_PASS",
        evidence_class="injected",
        operation_id=intent.operation_id,
        task_run_id=intent.task_run_id,
        candidate_checkpoint_id=intent.candidate_checkpoint_id,
        intent_sha256=intent.intent_sha256,
        initial_head=intent.initial_head,
        final_head=intent.initial_head,
        head_unchanged=True,
        stages=(
            stage,
            CheckpointAcceptanceStageReceipt(
                stage="hash_mismatch_rejection",
                candidate_checkpoint_id=intent.candidate_checkpoint_id,
                task_owned=True,
                disposable_candidate=True,
                outcome="rejected_expected",
                detail_code="EXACT_READBACK_HASH_MISMATCH_REJECTED",
                provider_receipt_sha256="4" * 64,
                expected_content_sha256="5" * 64,
                observed_content_sha256="6" * 64,
            ),
        ),
        completed_at=NOW,
    )
    completed = journal.complete(intent.operation_id, intent.intent_sha256, receipt)
    assert completed.state == "DURABLE_COMPLETE"
    assert journal.complete(intent.operation_id, intent.intent_sha256, receipt) == completed


def test_control_ledger_acceptance_journal_third_failure_is_terminal(tmp_path: Path) -> None:
    journal = ControlLedgerCheckpointAcceptanceJournal(ControlLedger(tmp_path / "control.sqlite3"))
    intent = _intent()
    journal.ensure_intent(intent)
    for number in range(1, 4):
        result = journal.record_attempt_failure(intent.operation_id, intent.intent_sha256, "TIMEOUT")
        assert result.attempts == number
    assert result.state == "FAILED"
    assert journal.record_attempt_failure(intent.operation_id, intent.intent_sha256, "TIMEOUT") == result


def test_control_ledger_acceptance_journal_rejects_conflicting_binding(tmp_path: Path) -> None:
    journal = ControlLedgerCheckpointAcceptanceJournal(ControlLedger(tmp_path / "control.sqlite3"))
    intent = _intent()
    journal.ensure_intent(intent)
    conflicting = intent.model_copy(update={"candidate_checkpoint_id": uuid4()})
    with pytest.raises(IdempotencyConflict):
        journal.ensure_intent(conflicting)


class _Registry:
    dataset_ref = "owner/fm14-evidence"

    @property
    def head(self) -> CheckpointHead:
        return CheckpointHead()


class _InjectedAdapter:
    def __init__(self) -> None:
        self.identity = SimpleNamespace(username="owner")
        self.api = SimpleNamespace()
        self.journal = SimpleNamespace()
        self.calls = 0
        self.retry = BoundedRetry(
            RetryPolicy(),
            sleep=lambda _seconds: None,
            monotonic=lambda: 0.0,
            wall_clock=lambda: NOW,
        )

    def current_private_dataset_version(self, *, provider_ref: str) -> None:
        self.calls += 1
        return None


def test_expired_absolute_deadline_blocks_before_provider_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime_root = tmp_path / "kaggle-working"
    runtime_root.mkdir()
    monkeypatch.setattr(acceptance_runtime, "_RUNTIME_ROOT", runtime_root)
    template = runtime_root / "h6-test-template"
    working = runtime_root / "h6-test-working"
    template.mkdir(parents=True, exist_ok=True)
    adapter = _InjectedAdapter()
    binding = CheckpointAcceptanceRuntimeBinding(
        scenario="FM14",
        operation_id=_intent().operation_id,
        task_run_id=_intent().task_run_id,
        source_revision=_intent().source_revision,
        started_at=NOW,
        dataset_ref="owner/fm14-evidence",
        notebook_ref="owner/fm14-verifier",
        template_directory=template,
        working_directory=working,
    )
    effects = KaggleTaskOwnedCheckpointEffects(
        adapter=adapter,  # type: ignore[arg-type]
        registry=_Registry(),  # type: ignore[arg-type]
        binding=binding,
        clock=lambda: NOW + timedelta(seconds=901),
    )
    assert effects.evidence_class == "injected"
    with pytest.raises(CheckpointAcceptanceError, match="deadline expired"):
        effects.ensure_fm14_corrupted_candidate(_intent())
    assert adapter.calls == 0
