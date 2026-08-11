from __future__ import annotations

from types import SimpleNamespace

import pytest

from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.providers.kaggle.adapter import KaggleProviderAdapter
from my_data_hub.providers.kaggle.contracts import (
    BrokeredBlobGrant,
    BrokeredDatasetFile,
    KaggleAmbiguousMutation,
    KaggleContractError,
    KaggleProviderIdentity,
)


class _SdkObject:
    pass


class _BlobType:
    DATASET = "dataset"


SDK_TYPES = (_BlobType, _SdkObject, _SdkObject, _SdkObject, _SdkObject, _SdkObject)


class _Journal:
    def persist_intent(self, intent: object) -> None:
        pass

    def persist_receipt(self, receipt: object) -> None:
        pass

    def persist_resource_claim(self, claim: object) -> None:
        pass

    def assert_resource_claim(self, claim: object) -> None:
        pass


class _Context:
    def __init__(self, value: object) -> None:
        self.value = value

    def __enter__(self) -> object:
        return self.value

    def __exit__(self, *args: object) -> None:
        pass


class _BrokeredApi:
    CONFIG_NAME_USER = "username"

    def __init__(self) -> None:
        self.current_version: int | None = None
        self.version_files: dict[int, list[SimpleNamespace]] = {}
        self.blob_metadata: dict[str, tuple[str, int]] = {}
        self.start_calls: list[object] = []
        self.create_calls: list[object] = []
        self.version_calls: list[object] = []
        self.list_calls: list[str] = []
        self.start_failure: Exception | None = None
        self.raise_after_version_apply: Exception | None = None
        self.blob_api_client = SimpleNamespace(start_blob_upload=self._start_blob_upload)
        self.dataset_api_client = SimpleNamespace(
            create_dataset=self._create_dataset,
            create_dataset_version=self._create_dataset_version,
        )
        self.blobs = SimpleNamespace(blob_api_client=self.blob_api_client)
        self.datasets = SimpleNamespace(dataset_api_client=self.dataset_api_client)

    def build_kaggle_client(self) -> _Context:
        return _Context(self)

    def dataset_list_with_response(self, **_kwargs: object) -> SimpleNamespace:
        rows = []
        if self.current_version is not None:
            rows.append(
                SimpleNamespace(
                    ref="owner/checkpoint-data",
                    is_private=True,
                    current_version_number=self.current_version,
                    status="ready",
                )
            )
        return SimpleNamespace(datasets=rows, next_page_token=None)

    def dataset_list_files(
        self,
        dataset: str,
        page_token: str | None = None,
        page_size: int = 20,
    ) -> SimpleNamespace:
        assert page_token is None
        assert page_size == 100
        self.list_calls.append(dataset)
        version = int(dataset.rsplit("/", 1)[1])
        return SimpleNamespace(files=self.version_files.get(version, []), next_page_token=None, error_message="")

    def _start_blob_upload(self, request: object) -> SimpleNamespace:
        self.start_calls.append(request)
        if self.start_failure is not None:
            raise self.start_failure
        return SimpleNamespace(token="opaque-token", create_url="https://uploads.example.test/signed?secret=value")

    def _metadata_from_files(self, files: list[object]) -> list[SimpleNamespace]:
        return [
            SimpleNamespace(
                name=self.blob_metadata[file.token][0],
                total_bytes=self.blob_metadata[file.token][1],
                description=file.description,
            )
            for file in files
        ]

    def _create_dataset(self, request: object) -> SimpleNamespace:
        self.create_calls.append(request)
        self.current_version = 1
        self.version_files[1] = self._metadata_from_files(request.files)
        return SimpleNamespace(status="ok", url="https://www.kaggle.com/datasets/owner/checkpoint-data")

    def _create_dataset_version(self, request: object) -> SimpleNamespace:
        self.version_calls.append(request)
        self.current_version = (self.current_version or 0) + 1
        self.version_files[self.current_version] = self._metadata_from_files(request.body.files)
        if self.raise_after_version_apply is not None:
            raise self.raise_after_version_apply
        return SimpleNamespace(status="ok", url="https://www.kaggle.com/datasets/owner/checkpoint-data")


def _description(*, total_bytes: int, file_sha256: str = "b" * 64) -> str:
    return canonical_json_bytes(
        {
            "operation_id": "operation-123",
            "master_run_ref": "owner/postgres-master/7",
            "epoch": 9,
            "manifest_sha256": "a" * 64,
            "file_sha256": file_sha256,
            "total_bytes": total_bytes,
        }
    ).decode()


def _file(*, token: str = "opaque-token", size: int = 123) -> BrokeredDatasetFile:
    return BrokeredDatasetFile(
        name="physical/base.tar.gz",
        total_bytes=size,
        description=_description(total_bytes=size),
        blob_token=token,
    )


def _adapter(monkeypatch: pytest.MonkeyPatch) -> tuple[KaggleProviderAdapter, _BrokeredApi]:
    monkeypatch.setattr("my_data_hub.providers.kaggle.adapter._brokered_sdk_types", lambda: SDK_TYPES)
    api = _BrokeredApi()
    return (
        KaggleProviderAdapter(
            api,  # type: ignore[arg-type]
            identity=KaggleProviderIdentity(username="owner"),
            journal=_Journal(),  # type: ignore[arg-type]
            sleep=lambda _seconds: None,
        ),
        api,
    )


def test_blob_start_uses_one_official_sdk_request_and_redacts_capabilities(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, api = _adapter(monkeypatch)

    grant = adapter.start_brokered_dataset_blob(
        file_name="physical/base.tar.gz",
        content_length=123,
        content_type="application/octet-stream",
        last_modified_epoch_seconds=1_786_400_000,
    )

    assert isinstance(grant, BrokeredBlobGrant)
    assert grant.blob_token == "opaque-token"
    assert grant.create_url.startswith("https://uploads.example.test/")
    assert "opaque-token" not in repr(grant)
    assert grant.create_url not in repr(grant)
    assert len(api.start_calls) == 1
    request = api.start_calls[0]
    assert (
        request.type,
        request.name,
        request.content_length,
        request.content_type,
        request.last_modified_epoch_seconds,
    ) == (
        "dataset",
        "physical/base.tar.gz",
        123,
        "application/octet-stream",
        1_786_400_000,
    )


def test_blob_start_never_retries_or_leaks_provider_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, api = _adapter(monkeypatch)
    secret = "opaque-token https://uploads.example.test/signed?secret=value"
    api.start_failure = RuntimeError(secret)

    with pytest.raises(KaggleAmbiguousMutation) as captured:
        adapter.start_brokered_dataset_blob(
            file_name="physical/base.tar.gz",
            content_length=123,
            content_type="application/octet-stream",
            last_modified_epoch_seconds=1_786_400_000,
        )

    assert len(api.start_calls) == 1
    assert secret not in str(captured.value)
    assert captured.value.__cause__ is None


def test_finalize_private_create_forwards_tokens_and_exact_descriptions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, api = _adapter(monkeypatch)
    file = _file()
    api.blob_metadata[file.blob_token] = (file.name, file.total_bytes)

    version = adapter.finalize_brokered_checkpoint_dataset(
        provider_ref="owner/checkpoint-data",
        title="Private checkpoints",
        files=(file,),
        version_notes="checkpoint epoch 9",
        expected_previous_version=None,
    )

    assert version == 1
    assert len(api.create_calls) == 1
    request = api.create_calls[0]
    assert request.owner_slug == "owner"
    assert request.slug == "checkpoint-data"
    assert request.title == "Private checkpoints"
    assert request.license_name == "CC0-1.0"
    assert request.is_private is True
    assert request.files[0].token == file.blob_token
    assert request.files[0].description == file.description
    assert api.list_calls == ["owner/checkpoint-data/1"]


def test_lost_version_response_reconciles_once_without_duplicate_or_download(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, api = _adapter(monkeypatch)
    api.current_version = 1
    api.version_files[1] = [SimpleNamespace(name="old", total_bytes=1, description="old")]
    file = _file(token="opaque-version-token")
    api.blob_metadata[file.blob_token] = (file.name, file.total_bytes)
    secret = f"{file.blob_token} https://uploads.example.test/private"
    api.raise_after_version_apply = RuntimeError(secret)

    version = adapter.finalize_brokered_checkpoint_dataset(
        provider_ref="owner/checkpoint-data",
        title="Private checkpoints",
        files=(file,),
        version_notes="checkpoint epoch 9",
        expected_previous_version=1,
    )

    assert version == 2
    assert len(api.version_calls) == 1
    assert api.version_calls[0].body.version_notes == "checkpoint epoch 9"
    assert api.version_calls[0].body.delete_old_versions is False
    assert api.list_calls == ["owner/checkpoint-data/2"]
    assert not hasattr(api, "dataset_download_files")

    # Re-entering after the lost response reconciles the exact expected version
    # before any second mutation can be issued.
    assert (
        adapter.finalize_brokered_checkpoint_dataset(
            provider_ref="owner/checkpoint-data",
            title="Private checkpoints",
            files=(file,),
            version_notes="checkpoint epoch 9",
            expected_previous_version=1,
        )
        == 2
    )
    assert len(api.version_calls) == 1


def test_reconcile_requires_current_exact_numeric_version_and_all_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, api = _adapter(monkeypatch)
    file = _file()
    expected = ((file.name, file.total_bytes, file.description),)
    api.current_version = 2
    api.version_files[2] = [
        SimpleNamespace(name=file.name, total_bytes=file.total_bytes, description=file.description)
    ]

    assert adapter.reconcile_brokered_checkpoint_dataset(
        provider_ref="owner/checkpoint-data", version=2, expected_files=expected
    )
    assert not adapter.reconcile_brokered_checkpoint_dataset(
        provider_ref="owner/checkpoint-data", version=1, expected_files=expected
    )
    api.version_files[2][0].description = _description(total_bytes=file.total_bytes, file_sha256="c" * 64)
    assert not adapter.reconcile_brokered_checkpoint_dataset(
        provider_ref="owner/checkpoint-data", version=2, expected_files=expected
    )
    assert all(call in {"owner/checkpoint-data/2"} for call in api.list_calls)


def test_finalize_rejects_noncanonical_or_unbound_description_before_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, api = _adapter(monkeypatch)
    file = BrokeredDatasetFile(
        name="physical/base.tar.gz",
        total_bytes=123,
        description='{ "operation_id": "not-canonical" }',
        blob_token="must-not-appear",
    )

    with pytest.raises(KaggleContractError) as captured:
        adapter.finalize_brokered_checkpoint_dataset(
            provider_ref="owner/checkpoint-data",
            title="Private checkpoints",
            files=(file,),
            version_notes="checkpoint epoch 9",
            expected_previous_version=None,
        )

    assert "must-not-appear" not in str(captured.value)
    assert not api.create_calls


def test_unreconciled_finalize_error_redacts_tokens_and_urls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, api = _adapter(monkeypatch)
    file = _file(token="opaque-create-token")
    api.blob_metadata[file.blob_token] = (file.name, file.total_bytes)
    secret = f"{file.blob_token} https://uploads.example.test/private"

    def fail_before_apply(_request: object) -> None:
        api.create_calls.append(_request)
        raise RuntimeError(secret)

    api.dataset_api_client.create_dataset = fail_before_apply
    with pytest.raises(KaggleAmbiguousMutation) as captured:
        adapter.finalize_brokered_checkpoint_dataset(
            provider_ref="owner/checkpoint-data",
            title="Private checkpoints",
            files=(file,),
            version_notes="checkpoint epoch 9",
            expected_previous_version=None,
        )

    assert len(api.create_calls) == 1
    assert secret not in str(captured.value)
    assert file.blob_token not in repr(file)
    assert captured.value.__cause__ is None
