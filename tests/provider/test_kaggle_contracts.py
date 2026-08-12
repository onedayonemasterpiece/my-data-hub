from __future__ import annotations

import importlib.metadata
import inspect
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
from pydantic import ValidationError

from my_data_hub.providers import ControlClass, ProviderFingerprint, ProviderKind
from my_data_hub.providers.kaggle import (
    BoundedRetry,
    EffectOutcome,
    KaggleKernelRunIdentity,
    KaggleProviderAdapter,
    KaggleRetryExhausted,
    MutationAction,
    ProviderEffectIntent,
    RetryClass,
    RetryPolicy,
    TaskResourceClaim,
    classify_failure,
    compatibility_inventory,
    parse_retry_after,
)
from my_data_hub.providers.kaggle.contracts import ProviderEffectJournal

NOW = datetime(2026, 8, 10, 12, tzinfo=UTC)
HASH_A = "a" * 64


class HttpFailure(RuntimeError):
    def __init__(self, status: int, headers: dict[str, str] | None = None) -> None:
        self.response = SimpleNamespace(status_code=status, headers=headers or {})
        super().__init__(f"http {status}")


class FixedRandom:
    def __init__(self, value: float) -> None:
        self.value = value

    def uniform(self, a: float, b: float) -> float:
        assert a <= self.value <= b
        return self.value


class Time:
    def __init__(self) -> None:
        self.value = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.value

    def sleep(self, delay: float) -> None:
        self.sleeps.append(delay)
        self.value += delay


def test_retry_after_and_jitter_are_bounded_and_deterministic() -> None:
    timeline = Time()
    retry = BoundedRetry(
        RetryPolicy(
            max_attempts=3,
            initial_delay_seconds=1,
            max_delay_seconds=8,
            max_retry_after_seconds=8,
            max_elapsed_seconds=20,
            jitter_ratio=0.25,
        ),
        sleep=timeline.sleep,
        monotonic=timeline.monotonic,
        wall_clock=lambda: NOW,
        random_source=FixedRandom(2.0),
    )
    calls = 0

    def operation() -> str:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise HttpFailure(429, {"Retry-After": "9999"})
        return "ok"

    result, attempts = retry.call("bounded", operation)
    assert result == "ok"
    assert attempts == 3
    assert timeline.sleeps == [8.0, 8.0]

    http_date = "Mon, 10 Aug 2026 12:00:07 GMT"
    assert parse_retry_after(http_date, now=NOW) == 7
    assert parse_retry_after("not-a-delay", now=NOW) is None


def test_retry_taxonomy_is_fail_closed_and_elapsed_budget_is_terminal() -> None:
    assert classify_failure(HttpFailure(503), now=NOW).retry_class == RetryClass.SERVER
    assert classify_failure(HttpFailure(503), now=NOW).retryable is True
    assert classify_failure(HttpFailure(401), now=NOW).retryable is False
    assert classify_failure(HttpFailure(403), now=NOW).retry_class == RetryClass.AUTHORIZATION
    assert classify_failure(HttpFailure(404), now=NOW).retry_class == RetryClass.NOT_FOUND
    assert classify_failure(ValueError("bad request"), now=NOW).retryable is False

    timeline = Time()
    retry = BoundedRetry(
        RetryPolicy(
            max_attempts=5,
            initial_delay_seconds=4,
            max_delay_seconds=8,
            max_retry_after_seconds=8,
            max_elapsed_seconds=5,
            jitter_ratio=0,
        ),
        sleep=timeline.sleep,
        monotonic=timeline.monotonic,
        wall_clock=lambda: NOW,
    )
    with pytest.raises(KaggleRetryExhausted, match="bounded retry"):
        retry.call("server", lambda: (_ for _ in ()).throw(HttpFailure(503)))
    assert timeline.sleeps == [4.0]


def test_effect_intent_and_task_claim_hashes_bind_exact_values() -> None:
    task_id = uuid4()
    intent = ProviderEffectIntent.create(
        operation_id=uuid4(),
        effect_id=uuid4(),
        idempotency_key="stable-effect-key",
        task_id=task_id,
        action=MutationAction.CREATE_DATASET,
        provider_ref="owner/private-data",
        arguments={"sha256": HASH_A},
        requested_at=NOW,
    )
    changed = intent.model_dump()
    changed["provider_ref"] = "owner/different-data"
    with pytest.raises(ValidationError, match="request_sha256"):
        ProviderEffectIntent.model_validate(changed)

    claim = TaskResourceClaim.create(
        task_id=task_id,
        effect_id=intent.effect_id,
        provider_ref=intent.provider_ref,
        kind=ProviderKind.DATASET,
        control_class=ControlClass.MCP_MANAGED,
        disposable=True,
        fingerprint=ProviderFingerprint(value=HASH_A),
        provider_version=1,
        registered_at=NOW,
    )
    tampered = claim.model_dump()
    tampered["provider_version"] = 2
    with pytest.raises(ValidationError, match="claim_sha256"):
        TaskResourceClaim.model_validate(tampered)


def test_output_receipt_binds_exact_run_and_source() -> None:
    run = KaggleKernelRunIdentity(
        task_run_id=uuid4(),
        provider_ref="owner/private-kernel",
        source_version=7,
        source_sha256=HASH_A,
        provider_kernel_id=42,
        provider_run_ref="owner/private-kernel/7",
        started_at=NOW,
    )
    assert run.source_version == 7
    with pytest.raises(ValidationError, match="exact Kaggle source version"):
        KaggleKernelRunIdentity.model_validate({**run.model_dump(), "provider_run_ref": "owner/private-kernel/8"})


def test_persist_intent_interface_owns_no_ledger() -> None:
    required = {
        name for name, member in ProviderEffectJournal.__dict__.items() if callable(member) and not name.startswith("_")
    }
    assert required == {
        "persist_intent",
        "persist_receipt",
        "persist_resource_claim",
        "assert_resource_claim",
    }
    assert not hasattr(KaggleProviderAdapter, "ledger")
    assert "import sqlite" not in inspect.getsource(KaggleProviderAdapter).casefold()


def test_donor_inventory_is_complete_and_exact() -> None:
    inventory = compatibility_inventory()
    contracts = " ".join(item.reused_contract for item in inventory).casefold()
    assert len(inventory) == 6
    assert {item.donor_commit for item in inventory} == {"416d17e689acf0a4f69f2b4d1db5dad5b46c4bca"}
    assert len({(item.source_path, item.blob_sha) for item in inventory}) == len(inventory)
    assert all(item.blob_sha != item.donor_commit for item in inventory)
    for marker in ("dataset", "kernel", "output", "callback", "heartbeat", "bge", "e5"):
        assert marker in contracts


def test_exactly_one_concrete_adapter_and_no_direct_fallback_surface() -> None:
    source = inspect.getsource(__import__("my_data_hub.providers.kaggle.adapter", fromlist=["*"]))
    assert source.count("class KaggleProviderAdapter:") == 1
    assert "DirectKaggleClient" not in source
    assert "requests." not in source
    assert "subprocess" not in source
    public = {name for name in KaggleProviderAdapter.__dict__ if not name.startswith("_")}
    assert not any("public" in name or "cancel" in name for name in public)


def test_installed_official_kaggle_224_surface_matches_protocol() -> None:
    try:
        version = importlib.metadata.version("kaggle")
    except importlib.metadata.PackageNotFoundError:
        pytest.skip("provider extra is not installed in this test environment")
    assert version == "2.2.4"
    from kaggle.api.kaggle_api_extended import KaggleApi

    expected_parameters = {
        "dataset_list_with_response": {"mine", "page_size", "page_token"},
        "kernels_list_with_response": {"mine", "page_size", "page_token"},
        "dataset_status": {"dataset", "format"},
        "dataset_create_new": {"folder", "public", "quiet", "convert_to_csv", "dir_mode"},
        "dataset_create_version": {
            "folder",
            "version_notes",
            "quiet",
            "convert_to_csv",
            "delete_old_versions",
            "dir_mode",
        },
        "dataset_download_files": {"dataset", "path", "force", "quiet", "unzip"},
        "dataset_download_file": {"dataset", "file_name", "path", "force", "quiet"},
        "dataset_delete": {"owner_slug", "dataset_slug", "no_confirm"},
        "kernels_push": {"folder", "timeout", "acc"},
        "kernels_pull": {"kernel", "path", "metadata", "quiet"},
        "kernels_status": {"kernel"},
        "kernels_output": {"kernel", "path", "file_pattern", "force", "quiet", "page_token", "page_size"},
        "kernels_delete": {"kernel", "no_confirm"},
    }
    for method_name, required in expected_parameters.items():
        parameters = set(inspect.signature(getattr(KaggleApi, method_name)).parameters)
        assert required <= parameters


def test_permanent_resource_claim_is_structurally_not_disposable() -> None:
    claim = TaskResourceClaim.create(
        task_id=uuid4(),
        effect_id=uuid4(),
        provider_ref="owner/master-runtime",
        kind=ProviderKind.NOTEBOOK,
        control_class=ControlClass.ORCHESTRATOR_PROTECTED,
        disposable=False,
        fingerprint=ProviderFingerprint(value=HASH_A),
        provider_version=1,
        registered_at=NOW + timedelta(seconds=1),
    )
    assert claim.disposable is False
    assert EffectOutcome.NOT_FOUND.value == "not_found"
