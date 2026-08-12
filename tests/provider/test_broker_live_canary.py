from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest
from pydantic import ValidationError

from my_data_hub.hashing import canonical_json_bytes, sha256_value
from my_data_hub.providers.inventory import InventoryPage
from my_data_hub.providers.kaggle.contracts import (
    BrokeredBlobGrant,
    EffectOutcome,
    KaggleDatasetIdentity,
    KaggleKernelRunIdentity,
    KaggleKernelSourceIdentity,
    NotebookMutationResult,
    ProviderEffectReceipt,
    TaskResourceClaim,
)
from my_data_hub.providers.models import ControlClass, ProviderFingerprint, ProviderKind
from scripts.provider.broker_live_canary import (
    PRODUCER_RESULT,
    VERIFIER_RESULT,
    AtomicCanaryState,
    BrokerLiveCanary,
    BrokerLiveCanaryReceipt,
    _payload,
    build_producer_source,
    build_verifier_source,
)

NOW = datetime(2026, 8, 11, 12, tzinfo=UTC)
CANARY_ID = UUID("11111111-2222-4333-8444-555555555555")


class FakeJournal:
    def __init__(self) -> None:
        self.claims: dict[str, TaskResourceClaim] = {}
        self.intents: list[object] = []
        self.receipts: list[object] = []

    def persist_intent(self, intent: object) -> None:
        self.intents.append(intent)

    def persist_receipt(self, receipt: object) -> None:
        self.receipts.append(receipt)

    def persist_resource_claim(self, claim: TaskResourceClaim) -> None:
        self.claims[claim.claim_sha256] = claim

    def assert_resource_claim(self, claim: TaskResourceClaim) -> None:
        assert self.claims.get(claim.claim_sha256, claim) == claim


class FakeAdapter:
    """A provider double: the orchestrator must label its receipt SIMULATED."""

    def __init__(self, journal: FakeJournal) -> None:
        self.journal = journal
        self.grant = BrokeredBlobGrant(
            blob_token="opaque-secret-token",
            create_url="https://upload.example.test/blob?X-Goog-Signature=secret",
        )
        self.resources: dict[ProviderKind, set[str]] = {
            ProviderKind.DATASET: set(),
            ProviderKind.NOTEBOOK: set(),
        }
        self.pushed: list[dict[str, object]] = []
        self.finalized = 0
        self.reconciled = 0
        self.deleted: list[str] = []

    def provider_identity(self) -> object:
        return SimpleNamespace(username="owner")

    def start_brokered_dataset_blob(self, **kwargs: object) -> BrokeredBlobGrant:
        assert kwargs["file_name"] == "broker-canary.bin"
        assert kwargs["content_length"] == 4096
        return self.grant

    def push_private_notebook(self, **kwargs: object) -> NotebookMutationResult:
        intent = kwargs["intent"]
        source = kwargs["source"]
        task_run_id = kwargs["task_run_id"]
        ref = intent.provider_ref
        self.pushed.append(dict(kwargs))
        self.resources[ProviderKind.NOTEBOOK].add(ref)
        source_sha = hashlib.sha256(source).hexdigest()
        fingerprint = ProviderFingerprint(value=sha256_value({"ref": ref, "source": source_sha}))
        source_identity = KaggleKernelSourceIdentity(
            provider_ref=ref,
            source_version=1,
            privacy="private",
            source_sha256=source_sha,
            fingerprint=fingerprint,
            observed_at=NOW,
        )
        run = KaggleKernelRunIdentity(
            task_run_id=task_run_id,
            provider_ref=ref,
            source_version=1,
            source_sha256=source_sha,
            provider_kernel_id=100 + len(self.pushed),
            provider_run_ref=f"{ref}/1",
            started_at=NOW,
        )
        claim = TaskResourceClaim.create(
            task_id=intent.task_id,
            effect_id=intent.effect_id,
            provider_ref=ref,
            kind=ProviderKind.NOTEBOOK,
            control_class=ControlClass.MCP_MANAGED,
            disposable=True,
            fingerprint=fingerprint,
            provider_version=1,
            registered_at=intent.requested_at,
        )
        self.journal.persist_resource_claim(claim)
        effect = ProviderEffectReceipt(
            operation_id=intent.operation_id,
            effect_id=intent.effect_id,
            action=intent.action,
            provider_ref=ref,
            outcome=EffectOutcome.APPLIED,
            attempts=1,
            observed_fingerprint=fingerprint,
            provider_version=1,
            observed_at=NOW,
            detail_code="private_notebook_pushed_and_run",
        )
        return NotebookMutationResult(source=source_identity, run=run, claim=claim, effect=effect)

    def poll_run(self, run: object, policy: object = None) -> object:
        return SimpleNamespace(state=SimpleNamespace(value="complete"))

    def read_exact_run_output(self, run: KaggleKernelRunIdentity) -> object:
        return SimpleNamespace(output_tree_sha256="a" * 64, receipt_sha256="b" * 64)

    def download_exact_run_output_file(
        self, run: KaggleKernelRunIdentity, *, destination: Path, file_name: str, max_bytes: int
    ) -> object:
        destination.mkdir(parents=True, exist_ok=True)
        payload = _payload(CANARY_ID, 4096)
        if file_name == PRODUCER_RESULT:
            result = {
                "schema_version": "my-data-hub-broker-producer-result.v1",
                "canary_id": str(CANARY_ID),
                "task_run_id": str(run.task_run_id),
                "byte_size": 4096,
                "file_sha256": hashlib.sha256(payload).hexdigest(),
                "direct_put": True,
                "credential_env_present": [],
                "credential_files_present": [],
            }
        else:
            assert file_name == VERIFIER_RESULT
            result = {
                "schema_version": "my-data-hub-broker-verifier-result.v1",
                "canary_id": str(CANARY_ID),
                "task_run_id": str(run.task_run_id),
                "exact_version_ref": f"owner/mdh-broker-canary-{CANARY_ID.hex[:12]}/1",
                "byte_size": 4096,
                "file_sha256": hashlib.sha256(payload).hexdigest(),
                "credential_env_present": [],
                "credential_files_present": [],
            }
        (destination / file_name).write_bytes(canonical_json_bytes(result))
        return SimpleNamespace(output_tree_sha256="c" * 64)

    def finalize_brokered_checkpoint_dataset(self, **kwargs: object) -> int:
        self.finalized += 1
        assert kwargs["expected_previous_version"] is None
        assert kwargs["files"][0].blob_token == self.grant.blob_token
        self.resources[ProviderKind.DATASET].add(str(kwargs["provider_ref"]))
        return 1

    def reconcile_brokered_checkpoint_dataset(self, **kwargs: object) -> bool:
        self.reconciled += 1
        assert kwargs["version"] == 1
        return True

    def read_private_dataset(self, *, provider_ref: str, version: int) -> KaggleDatasetIdentity:
        package_sha = sha256_value(
            {
                "files": [
                    {
                        "path": "broker-canary.bin",
                        "byte_size": 4096,
                        "sha256": hashlib.sha256(_payload(CANARY_ID, 4096)).hexdigest(),
                    }
                ]
            }
        )
        fingerprint = ProviderFingerprint(
            value=sha256_value(
                {"provider_ref": provider_ref, "version": version, "privacy": "private", "package_sha256": package_sha}
            )
        )
        return KaggleDatasetIdentity(
            provider_ref=provider_ref,
            version=version,
            privacy="private",
            package_sha256=package_sha,
            fingerprint=fingerprint,
            observed_at=NOW,
        )

    def delete_task_created_resource(self, *, intent: object, claim: TaskResourceClaim) -> ProviderEffectReceipt:
        self.resources[claim.kind].discard(claim.provider_ref)
        self.deleted.append(claim.provider_ref)
        return ProviderEffectReceipt(
            operation_id=intent.operation_id,
            effect_id=intent.effect_id,
            action=intent.action,
            provider_ref=claim.provider_ref,
            outcome=EffectOutcome.APPLIED,
            attempts=1,
            observed_at=NOW,
            detail_code="task_created_resource_absent",
        )

    def list_resources(self, *, kind: ProviderKind, cursor: str | None, limit: int) -> InventoryPage:
        assert cursor is None and limit == 100
        assert not self.resources[kind]
        return InventoryPage(resources=(), next_cursor=None)


def test_full_broker_flow_is_exact_secret_free_and_fake_cannot_claim_live(tmp_path: Path) -> None:
    journal = FakeJournal()
    adapter = FakeAdapter(journal)
    state_path = tmp_path / "canary-state.json"
    receipt = BrokerLiveCanary(
        adapter=adapter,
        journal=journal,
        state=AtomicCanaryState(state_path),
        commit_sha="d" * 40,
        canary_id=CANARY_ID,
        payload_bytes=4096,
        live_provider=False,
        clock=lambda: NOW,
    ).run()

    assert receipt.outcome == "SIMULATED"
    assert receipt.live_provider_mutations is False
    assert adapter.finalized == 1 and adapter.reconciled == 1
    assert len(adapter.pushed) == 2
    assert adapter.pushed[0]["enable_internet"] is True
    assert adapter.pushed[0]["dataset_sources"] == ()
    assert adapter.pushed[1]["enable_internet"] is False
    assert adapter.pushed[1]["dataset_sources"] == (receipt.dataset.exact_version_ref,)
    assert set(adapter.deleted) == {
        receipt.dataset.provider_ref,
        receipt.producer.provider_ref,
        receipt.verifier.provider_ref,
    }
    encoded = canonical_json_bytes(receipt.model_dump(mode="json"))
    assert adapter.grant.create_url.encode() not in encoded
    assert adapter.grant.blob_token.encode() not in encoded
    assert b"create_url" not in encoded and b"blob_token" not in encoded
    state = state_path.read_bytes()
    assert adapter.grant.create_url.encode() not in state
    assert adapter.grant.blob_token.encode() not in state

    forged = receipt.model_dump(mode="json")
    forged.update(
        {"evidence_origin": "test", "execution_mode": "test", "outcome": "PASS", "live_provider_mutations": False}
    )
    with pytest.raises(ValidationError, match="only observed live provider mutations"):
        BrokerLiveCanaryReceipt.model_validate(forged)


def test_notebook_sources_are_credential_free_and_use_only_the_signed_put_capability() -> None:
    producer = build_producer_source(
        canary_id=CANARY_ID,
        task_run_id=UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
        provider_ref="owner/mdh-broker-producer-source",
        create_url="https://upload.example.test/blob?signature=secret",
        byte_size=4096,
    ).decode()
    verifier = build_verifier_source(
        canary_id=CANARY_ID,
        task_run_id=UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
        provider_ref="owner/mdh-broker-verifier-source",
        exact_version_ref="owner/mdh-broker-canary-source/7",
        byte_size=4096,
        file_sha256="a" * 64,
    ).decode()

    compile(producer, "producer-run.py", "exec")
    compile(verifier, "verifier-run.py", "exec")
    assert "HTTPSConnection" in producer and 'connection.request("PUT"' in producer
    assert "KAGGLE_USERNAME" in producer and "KAGGLE_API_TOKEN" in producer
    assert "import kaggle" not in producer.casefold()
    assert "kagglehub" not in producer.casefold()
    assert "CREATE_URL" not in verifier
    assert "owner/mdh-broker-canary-source/7" in verifier
    assert "KAGGLE_USERNAME" in verifier and "import kaggle" not in verifier.casefold()


def test_example_is_explicitly_not_live() -> None:
    root = Path(__file__).resolve().parents[2]
    payload = json.loads((root / "examples/contracts/broker-live-canary-receipt.v1.example.json").read_bytes())
    receipt = BrokerLiveCanaryReceipt.model_validate(payload)
    assert (receipt.evidence_origin, receipt.execution_mode, receipt.outcome, receipt.live_provider_mutations) == (
        "example",
        "not_run",
        "NOT_RUN",
        False,
    )
