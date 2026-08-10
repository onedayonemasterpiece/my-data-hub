from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from pydantic import ValidationError

from my_data_hub.providers import (
    BoundedInventory,
    ControlClass,
    InventoryLimits,
    InventoryPage,
    ObservedProviderResource,
    OperationLedger,
    Origin,
    ProviderAction,
    ProviderFingerprint,
    ProviderKind,
    ProviderOperation,
    ProviderPolicy,
    ProviderRegistry,
    ProviderResource,
    ResourceLease,
)
from my_data_hub.providers.exchange import ExchangeValidationError, manifest_sha256, validate_exchange_manifest
from my_data_hub.providers.inventory import InventoryBoundExceeded, InventoryProtocolError
from my_data_hub.providers.kaggle import (
    CanaryCleanupReceipt,
    DatasetCanaryReceipt,
    KagglePrivateCanaryAdapter,
    NotebookCanaryReceipt,
)
from my_data_hub.providers.models import IdempotencyConflict, LeaseDenied, StaleFingerprint
from my_data_hub.providers.policy import PolicyDenied

NOW = datetime(2026, 8, 9, 20, tzinfo=UTC)
HASH_A = "a" * 64
HASH_B = "b" * 64


def observed(ref: str, *, kind: ProviderKind = ProviderKind.DATASET, fingerprint: str = HASH_A):
    return ObservedProviderResource(
        provider="kaggle",
        provider_ref=ref,
        kind=kind,
        owner=ref.split("/", 1)[0],
        private=True,
        fingerprint=ProviderFingerprint(value=fingerprint),
        state="complete",
        observed_at=NOW,
    )


class Pages:
    def __init__(self, pages: dict[str | None, InventoryPage]):
        self.pages = pages
        self.calls: list[tuple[str | None, int]] = []

    def list_resources(self, *, kind: ProviderKind, cursor: str | None, limit: int) -> InventoryPage:
        self.calls.append((cursor, limit))
        return self.pages[cursor]


def test_inventory_is_bounded_and_unknown_names_stay_external_read_only() -> None:
    adapter = Pages(
        {
            None: InventoryPage(resources=(observed("alice/mdh-mcp-managed-looking"),), next_cursor="page-2"),
            "page-2": InventoryPage(resources=(observed("alice/worker-backup-looking"),)),
        }
    )
    resources = BoundedInventory(
        adapter,
        ProviderRegistry(),
        InventoryLimits(page_size=1, max_pages=2, max_resources=2),
    ).collect(ProviderKind.DATASET)

    assert [item.control_class for item in resources] == [
        ControlClass.EXTERNAL_READ_ONLY,
        ControlClass.EXTERNAL_READ_ONLY,
    ]
    assert adapter.calls == [(None, 1), ("page-2", 1)]


def test_inventory_rejects_repeated_cursor_and_page_budget() -> None:
    repeated = Pages(
        {
            None: InventoryPage(resources=(), next_cursor="again"),
            "again": InventoryPage(resources=(), next_cursor="again"),
        }
    )
    with pytest.raises(InventoryProtocolError, match="cursor"):
        BoundedInventory(repeated, ProviderRegistry()).collect(ProviderKind.DATASET)

    endless = Pages(
        {
            None: InventoryPage(resources=(), next_cursor="2"),
            "2": InventoryPage(resources=(), next_cursor="3"),
        }
    )
    with pytest.raises(InventoryBoundExceeded, match="page"):
        BoundedInventory(endless, ProviderRegistry(), InventoryLimits(max_pages=2)).collect(ProviderKind.DATASET)


def test_explicit_protected_registration_survives_rediscovery_and_denies_content_and_mutation() -> None:
    registry = ProviderRegistry()
    protected = registry.register_protected(
        provider="kaggle",
        provider_ref="prod/region-talk-worker",
        kind=ProviderKind.NOTEBOOK,
        owner="prod",
        fingerprint=ProviderFingerprint(value=HASH_A),
        private=True,
        workload="region-talk",
        observed_at=NOW,
    )
    rediscovered = registry.resolve_discovery(
        observed("prod/renamed-mcp-looking", kind=ProviderKind.NOTEBOOK, fingerprint=HASH_B).model_copy(
            update={"provider_ref": protected.provider_ref}
        )
    )
    assert rediscovered.control_class == ControlClass.ORCHESTRATOR_PROTECTED
    assert rediscovered.origin == Origin.ORCHESTRATOR
    assert rediscovered.fingerprint == ProviderFingerprint(value=HASH_B)

    policy = ProviderPolicy()
    policy.authorize(rediscovered, ProviderAction.READ_STATUS, principal="normal-kaggle-writer", now=NOW)
    for action in ProviderAction:
        if action in {ProviderAction.LIST, ProviderAction.READ_STATUS}:
            continue
        with pytest.raises(PolicyDenied) as denied:
            policy.authorize(rediscovered, action, principal="normal-kaggle-writer", now=NOW)
        assert denied.value.code == "PROTECTED_RESOURCE_DENIED"

    with pytest.raises(PolicyDenied, match="reclassified"):
        registry.adopt(
            rediscovered,
            target=ControlClass.MCP_MANAGED,
            expected_fingerprint=ProviderFingerprint(value=HASH_B),
        )


def managed_resource() -> ProviderResource:
    return ProviderResource(
        provider="kaggle",
        provider_ref="sandbox/disposable",
        kind=ProviderKind.DATASET,
        owner="sandbox",
        origin=Origin.MCP,
        control_class=ControlClass.MCP_MANAGED,
        private=True,
        fingerprint=ProviderFingerprint(value=HASH_A),
        state="complete",
        observed_at=NOW,
    )


def lease(*, principal: str = "agent-a", expires: datetime = NOW + timedelta(minutes=5)) -> ResourceLease:
    return ResourceLease(
        lease_id=uuid4(),
        provider_ref="sandbox/disposable",
        principal=principal,
        fencing_token=7,
        acquired_at=NOW - timedelta(minutes=1),
        expires_at=expires,
    )


def test_mutation_requires_exact_fingerprint_and_current_owned_lease() -> None:
    policy = ProviderPolicy()
    resource = managed_resource()
    held = lease()
    policy.authorize(
        resource,
        ProviderAction.CREATE_VERSION,
        principal="agent-a",
        now=NOW,
        expected_fingerprint=ProviderFingerprint(value=HASH_A),
        lease=held,
    )
    with pytest.raises(StaleFingerprint):
        policy.authorize(
            resource,
            ProviderAction.CREATE_VERSION,
            principal="agent-a",
            now=NOW,
            expected_fingerprint=ProviderFingerprint(value=HASH_B),
            lease=held,
        )
    with pytest.raises(LeaseDenied):
        policy.authorize(
            resource,
            ProviderAction.DELETE,
            principal="agent-b",
            now=NOW,
            expected_fingerprint=ProviderFingerprint(value=HASH_A),
            lease=held,
        )


def test_operation_idempotency_is_bound_to_exact_request_hash() -> None:
    held = lease()
    first = ProviderOperation.create(
        operation_id=uuid4(),
        idempotency_key="stable-key-0001",
        principal="agent-a",
        provider_ref=held.provider_ref,
        action=ProviderAction.CREATE_VERSION,
        expected_fingerprint=ProviderFingerprint(value=HASH_A),
        lease_id=held.lease_id,
        fencing_token=held.fencing_token,
        arguments={"version_note": "canary"},
        requested_at=NOW,
    )
    replay = first.model_copy(update={"operation_id": uuid4()})
    ledger = OperationLedger()
    assert ledger.record(first) is first
    assert ledger.record(replay) is first

    conflicting = ProviderOperation.create(
        operation_id=uuid4(),
        idempotency_key=first.idempotency_key,
        principal="agent-a",
        provider_ref=held.provider_ref,
        action=ProviderAction.DELETE,
        expected_fingerprint=ProviderFingerprint(value=HASH_A),
        lease_id=held.lease_id,
        fencing_token=held.fencing_token,
        arguments={},
        requested_at=NOW,
    )
    with pytest.raises(IdempotencyConflict):
        ledger.record(conflicting)


def exchange_payload(content: bytes = b"hello") -> tuple[dict[str, object], dict[str, bytes]]:
    payload: dict[str, object] = {
        "contract_version": "my-data-hub-kaggle-exchange.v1",
        "package_id": str(uuid4()),
        "control_class": "mcp_exchange",
        "private": True,
        "dataset_ref": "sandbox/exchange-canary",
        "dataset_version": 1,
        "created_at": NOW.isoformat().replace("+00:00", "Z"),
        "expires_at": (NOW + timedelta(days=1)).isoformat().replace("+00:00", "Z"),
        "created_by": "agent-a",
        "purpose": "bounded test exchange",
        "target_project": "my-data-hub",
        "intended_recipients": ["agent-b"],
        "sensitivity": "internal",
        "files": [
            {
                "path": "payload/result.txt",
                "media_type": "text/plain",
                "byte_size": len(content),
                "sha256": hashlib.sha256(content).hexdigest(),
                "executable": False,
            }
        ],
    }
    payload["manifest_sha256"] = manifest_sha256(payload)
    return payload, {"payload/result.txt": content}


def test_exchange_validates_manifest_recipient_ttl_paths_and_exact_file_hashes() -> None:
    payload, files = exchange_payload()
    manifest = validate_exchange_manifest(payload, recipient="agent-b", file_contents=files, now=NOW)
    assert manifest.private is True

    with pytest.raises(ExchangeValidationError, match="recipient"):
        validate_exchange_manifest(payload, recipient="agent-c", file_contents=files, now=NOW)
    with pytest.raises(ExchangeValidationError, match="expired"):
        validate_exchange_manifest(payload, recipient="agent-b", file_contents=files, now=NOW + timedelta(days=2))
    with pytest.raises(ExchangeValidationError, match="wrong hash"):
        validate_exchange_manifest(
            payload,
            recipient="agent-b",
            file_contents={"payload/result.txt": b"HELLO"},
            now=NOW,
        )

    traversing, traversing_files = exchange_payload()
    traversing["files"][0]["path"] = "../secret"  # type: ignore[index]
    traversing["manifest_sha256"] = manifest_sha256(traversing)
    traversing_files = {"../secret": b"hello"}
    with pytest.raises(ValidationError, match="traversal"):
        validate_exchange_manifest(traversing, recipient="agent-b", file_contents=traversing_files, now=NOW)

    tampered = dict(payload)
    tampered["purpose"] = "changed after signing"
    with pytest.raises(ExchangeValidationError, match="manifest_sha256"):
        validate_exchange_manifest(tampered, recipient="agent-b", file_contents=files, now=NOW)


def cleanup(ref: str, *, outcome: str = "deleted") -> CanaryCleanupReceipt:
    return CanaryCleanupReceipt(
        provider_ref=ref,
        requested_at=NOW,
        observed_at=NOW + timedelta(seconds=1),
        outcome=outcome,
        provider_receipt_sha256=HASH_A,
    )


def test_private_canary_receipts_require_exact_hash_privacy_and_cleanup() -> None:
    dataset = DatasetCanaryReceipt(
        canary_id=uuid4(),
        provider_ref="sandbox/dataset-canary",
        privacy="private",
        expected_package_sha256=HASH_A,
        readback_package_sha256=HASH_A,
        cleanup=cleanup("sandbox/dataset-canary"),
        completed_at=NOW + timedelta(seconds=2),
    )
    assert dataset.cleanup.outcome == "deleted"
    with pytest.raises(ValidationError, match="readback hash"):
        DatasetCanaryReceipt.model_validate({**dataset.model_dump(), "readback_package_sha256": HASH_B})

    notebook = NotebookCanaryReceipt(
        canary_id=uuid4(),
        provider_ref="sandbox/notebook-canary",
        privacy="private",
        expected_source_sha256=HASH_A,
        readback_source_sha256=HASH_A,
        expected_output_sha256=HASH_B,
        readback_output_sha256=HASH_B,
        terminal_state="complete",
        cleanup=cleanup("sandbox/notebook-canary"),
        completed_at=NOW + timedelta(seconds=2),
    )
    assert notebook.privacy == "private"
    with pytest.raises(ValidationError, match="cleanup"):
        DatasetCanaryReceipt.model_validate({**dataset.model_dump(), "cleanup": cleanup("sandbox/other").model_dump()})


def test_provider_surface_structurally_omits_public_creation_and_cancellation() -> None:
    protocol = {member for member in KagglePrivateCanaryAdapter.__dict__ if not member.startswith("_")}
    assert "create_private_dataset" in protocol
    assert not any("public" in member for member in protocol)
    assert not any("cancel" in member for member in protocol)
    assert not any("cancel" in action.value for action in ProviderAction)
