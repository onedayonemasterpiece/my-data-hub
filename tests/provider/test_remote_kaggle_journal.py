from __future__ import annotations

import json
from datetime import UTC, datetime
from uuid import UUID

import pytest

from my_data_hub.providers.kaggle import (
    AuthenticatedControlPlaneClient,
    ControlPlaneMetadataError,
    ControlPlaneRuntimeIdentity,
    EffectOutcome,
    MetadataHttpResponse,
    MutationAction,
    ProviderEffectIntent,
    ProviderEffectReceipt,
    RemoteControlLedgerKaggleJournal,
    TaskResourceClaim,
)
from my_data_hub.providers.models import ControlClass, ProviderFingerprint, ProviderKind

RUN_ID = UUID("11111111-1111-4111-8111-111111111111")
ATTEMPT_ID = UUID("22222222-2222-4222-8222-222222222222")
MASTER_ID = UUID("33333333-3333-4333-8333-333333333333")
OPERATION_ID = UUID("44444444-4444-4444-8444-444444444444")
EFFECT_ID = UUID("55555555-5555-4555-8555-555555555555")
NOW = datetime(2026, 8, 11, tzinfo=UTC)


class CaptureTransport:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def request(self, **kwargs: object) -> MetadataHttpResponse:
        self.calls.append(kwargs)
        path = str(kwargs["url"])
        body = b'{"authorized":true}' if path.endswith("/assert") else b"{}"
        return MetadataHttpResponse(200, body)


class ClaimTransport(CaptureTransport):
    def __init__(self, claim: TaskResourceClaim) -> None:
        super().__init__()
        self.claim = claim

    def request(self, **kwargs: object) -> MetadataHttpResponse:
        self.calls.append(kwargs)
        return MetadataHttpResponse(
            200,
            json.dumps({"claim": self.claim.model_dump(mode="json")}).encode(),
        )


def _client(transport: CaptureTransport) -> AuthenticatedControlPlaneClient:
    return AuthenticatedControlPlaneClient(
        base_url="https://control.example.test",
        bearer_token="runtime-token-that-is-long-enough",
        runtime_identity=ControlPlaneRuntimeIdentity(
            run_id=RUN_ID,
            attempt_id=ATTEMPT_ID,
            master_instance_id=MASTER_ID,
            epoch=7,
        ),
        transport=transport,
    )


def test_remote_journal_sends_only_metadata_with_exact_runtime_headers() -> None:
    transport = CaptureTransport()
    journal = RemoteControlLedgerKaggleJournal(_client(transport))
    intent = ProviderEffectIntent.create(
        operation_id=OPERATION_ID,
        effect_id=EFFECT_ID,
        idempotency_key="checkpoint-effect-identity",
        task_id=RUN_ID,
        action=MutationAction.CREATE_DATASET,
        provider_ref="owner/private-checkpoints",
        arguments={"content_tree_sha256": "a" * 64},
        requested_at=NOW,
    )
    journal.persist_intent(intent)
    receipt = ProviderEffectReceipt(
        operation_id=OPERATION_ID,
        effect_id=EFFECT_ID,
        action=MutationAction.CREATE_DATASET,
        provider_ref="owner/private-checkpoints",
        outcome=EffectOutcome.APPLIED,
        attempts=1,
        provider_version=1,
        observed_at=NOW,
        detail_code="created",
    )
    journal.persist_receipt(receipt)
    claim = TaskResourceClaim.create(
        task_id=RUN_ID,
        effect_id=EFFECT_ID,
        provider_ref="owner/private-checkpoints",
        kind=ProviderKind.DATASET,
        control_class=ControlClass.ORCHESTRATOR_PROTECTED,
        disposable=False,
        fingerprint=ProviderFingerprint(value="b" * 64),
        provider_version=1,
        registered_at=NOW,
    )
    journal.persist_resource_claim(claim)
    journal.assert_resource_claim(claim)

    assert [str(call["url"]).removeprefix("https://control.example.test") for call in transport.calls] == [
        "/internal/provider-journal/intents",
        "/internal/provider-journal/receipts",
        "/internal/provider-journal/resource-claims",
        "/internal/provider-journal/resource-claims/assert",
    ]
    for call in transport.calls:
        headers = call["headers"]
        assert isinstance(headers, dict)
        assert headers["Authorization"] == "Bearer runtime-token-that-is-long-enough"
        assert headers["X-MDH-Run-ID"] == str(RUN_ID)
        assert headers["X-MDH-Attempt-ID"] == str(ATTEMPT_ID)
        assert headers["X-MDH-Master-Instance-ID"] == str(MASTER_ID)
        assert headers["X-MDH-Epoch"] == "7"
        encoded = call["body"]
        assert isinstance(encoded, bytes)
        payload = json.loads(encoded)
        flattened = json.dumps(payload).casefold()
        assert "postgresql://" not in flattened
        assert "pgdata" not in flattened


def test_metadata_client_rejects_checkpoint_bytes_and_database_urls_before_transport() -> None:
    transport = CaptureTransport()
    client = _client(transport)
    with pytest.raises(ControlPlaneMetadataError, match="binary"):
        client.post("/internal/checkpoints/candidates", {"archive": b"archive"})
    with pytest.raises(ControlPlaneMetadataError, match="data-plane"):
        client.post("/internal/checkpoints/candidates", {"database_url": "postgresql://secret"})
    assert transport.calls == []


def test_control_client_rejects_non_https_and_unbound_runtime_epoch() -> None:
    transport = CaptureTransport()
    with pytest.raises(ValueError, match="HTTPS"):
        AuthenticatedControlPlaneClient(
            base_url="http://control.example.test",
            bearer_token="runtime-token-that-is-long-enough",
            runtime_identity=ControlPlaneRuntimeIdentity(RUN_ID, ATTEMPT_ID, MASTER_ID, 1),
            transport=transport,
        )
    with pytest.raises(ValueError, match="positive"):
        ControlPlaneRuntimeIdentity(RUN_ID, ATTEMPT_ID, MASTER_ID, 0)


def test_remote_journal_resolves_exact_durable_permanent_claim_for_next_version() -> None:
    claim = TaskResourceClaim.create(
        task_id=RUN_ID,
        effect_id=EFFECT_ID,
        provider_ref="owner/private-checkpoints",
        kind=ProviderKind.DATASET,
        control_class=ControlClass.ORCHESTRATOR_PROTECTED,
        disposable=False,
        fingerprint=ProviderFingerprint(value="b" * 64),
        provider_version=7,
        registered_at=NOW,
    )
    transport = ClaimTransport(claim)
    resolved = RemoteControlLedgerKaggleJournal(_client(transport)).current_resource_claim(
        provider_ref=claim.provider_ref,
        kind=ProviderKind.DATASET,
        control_class=ControlClass.ORCHESTRATOR_PROTECTED,
    )
    assert resolved == claim
    call = transport.calls[0]
    assert str(call["url"]).endswith("/internal/provider-journal/resource-claims/current")
    assert json.loads(call["body"]) == {  # type: ignore[arg-type]
        "provider_ref": "owner/private-checkpoints",
        "kind": "dataset",
        "control_class": "orchestrator_protected",
    }
