from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from my_data_hub.control_plane.ledger import ControlLedger, IdempotencyConflict
from my_data_hub.providers.kaggle import ControlLedgerKaggleJournal
from my_data_hub.providers.kaggle.contracts import (
    EffectOutcome,
    MutationAction,
    ProviderEffectIntent,
    ProviderEffectReceipt,
    TaskResourceClaim,
)
from my_data_hub.providers.models import ControlClass, ProviderFingerprint, ProviderKind


def make_intent() -> ProviderEffectIntent:
    return ProviderEffectIntent.create(
        operation_id=uuid4(), effect_id=uuid4(), idempotency_key="journal-effect-key",
        task_id=uuid4(), action=MutationAction.CREATE_DATASET,
        provider_ref="owner/private-canary", arguments={"private": True},
        requested_at=datetime.now(UTC),
    )


def test_control_journal_persists_intent_receipt_and_exact_cleanup_claim(tmp_path) -> None:  # type: ignore[no-untyped-def]
    journal = ControlLedgerKaggleJournal(ControlLedger(tmp_path / "control.sqlite3"))
    intent = make_intent()
    journal.persist_intent(intent)
    journal.persist_intent(intent)
    receipt = ProviderEffectReceipt(
        operation_id=intent.operation_id, effect_id=intent.effect_id, action=intent.action,
        provider_ref=intent.provider_ref, outcome=EffectOutcome.APPLIED, attempts=1,
        provider_version=1, observed_at=datetime.now(UTC), detail_code="created",
    )
    journal.persist_receipt(receipt)
    journal.persist_receipt(receipt)
    claim = TaskResourceClaim.create(
        task_id=intent.task_id, effect_id=intent.effect_id, provider_ref=intent.provider_ref,
        kind=ProviderKind.DATASET, control_class=ControlClass.MCP_MANAGED, disposable=True,
        fingerprint=ProviderFingerprint(value="a" * 64), provider_version=1,
        registered_at=datetime.now(UTC),
    )
    journal.persist_resource_claim(claim)
    journal.persist_resource_claim(claim)
    journal.assert_resource_claim(claim)
    conflicting_claim = TaskResourceClaim.create(
        task_id=claim.task_id,
        effect_id=claim.effect_id,
        provider_ref=claim.provider_ref,
        kind=claim.kind,
        control_class=claim.control_class,
        disposable=claim.disposable,
        fingerprint=ProviderFingerprint(value="b" * 64),
        provider_version=claim.provider_version,
        registered_at=claim.registered_at,
    )
    with pytest.raises(IdempotencyConflict):
        journal.persist_resource_claim(conflicting_claim)
    with pytest.raises(PermissionError):
        journal.assert_resource_claim(claim.model_copy(update={"provider_version": 2}))


def test_control_journal_rejects_idempotency_conflict(tmp_path) -> None:  # type: ignore[no-untyped-def]
    journal = ControlLedgerKaggleJournal(ControlLedger(tmp_path / "control.sqlite3"))
    intent = make_intent()
    journal.persist_intent(intent)
    with pytest.raises(IdempotencyConflict):
        journal.persist_intent(intent.model_copy(update={"provider_ref": "owner/different"}))
