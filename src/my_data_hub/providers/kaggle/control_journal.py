from __future__ import annotations

from my_data_hub.control_plane.ledger import ControlLedger

from .contracts import ProviderEffectIntent, ProviderEffectReceipt, TaskResourceClaim


class ControlLedgerKaggleJournal:
    """Durable provider journal backed by the non-canonical control ledger."""

    def __init__(self, ledger: ControlLedger) -> None:
        self.ledger = ledger

    def persist_intent(self, intent: ProviderEffectIntent) -> None:
        self.ledger.persist_provider_effect_intent(intent.model_dump(mode="json"))

    def persist_receipt(self, receipt: ProviderEffectReceipt) -> None:
        self.ledger.persist_provider_effect_receipt(
            str(receipt.effect_id), receipt.model_dump(mode="json")
        )

    def persist_resource_claim(self, claim: TaskResourceClaim) -> None:
        self.ledger.persist_provider_resource_claim(claim.model_dump(mode="json"))

    def assert_resource_claim(self, claim: TaskResourceClaim) -> None:
        self.ledger.assert_provider_resource_claim(
            claim.claim_sha256, claim.model_dump(mode="json")
        )
