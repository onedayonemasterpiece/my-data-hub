from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Literal, Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator


class PrivateDatasetSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    provider_ref: str = Field(min_length=3, max_length=500)
    version: int = Field(ge=1)
    privacy: Literal["private"]
    package_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    observed_at: datetime


class PrivateNotebookSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    provider_ref: str = Field(min_length=3, max_length=500)
    privacy: Literal["private"]
    source_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    output_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    state: Literal["queued", "running", "complete", "failed"]
    observed_at: datetime

    @model_validator(mode="after")
    def completed_notebook_has_output_fingerprint(self) -> PrivateNotebookSnapshot:
        if self.state == "complete" and self.output_sha256 is None:
            raise ValueError("completed notebook snapshot requires an output hash")
        return self


class CanaryCleanupReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    provider_ref: str = Field(min_length=3, max_length=500)
    requested_at: datetime
    observed_at: datetime
    outcome: Literal["deleted", "not_found", "failed"]
    provider_receipt_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")

    @model_validator(mode="after")
    def observation_follows_request(self) -> CanaryCleanupReceipt:
        if self.observed_at < self.requested_at:
            raise ValueError("cleanup observation precedes request")
        return self


class DatasetCanaryReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    canary_id: UUID
    provider_ref: str = Field(min_length=3, max_length=500)
    privacy: Literal["private"]
    expected_package_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    readback_package_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    cleanup: CanaryCleanupReceipt
    completed_at: datetime

    @model_validator(mode="after")
    def exact_private_readback_and_cleanup(self) -> DatasetCanaryReceipt:
        if self.expected_package_sha256 != self.readback_package_sha256:
            raise ValueError("dataset canary readback hash differs from the exact input")
        if self.cleanup.provider_ref != self.provider_ref:
            raise ValueError("cleanup receipt belongs to a different dataset")
        if self.cleanup.outcome not in {"deleted", "not_found"}:
            raise ValueError("dataset canary cleanup was not proven")
        if self.completed_at < self.cleanup.observed_at:
            raise ValueError("canary completed before cleanup observation")
        return self


class NotebookCanaryReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    canary_id: UUID
    provider_ref: str = Field(min_length=3, max_length=500)
    privacy: Literal["private"]
    expected_source_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    readback_source_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    expected_output_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    readback_output_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    terminal_state: Literal["complete"]
    cleanup: CanaryCleanupReceipt
    completed_at: datetime

    @model_validator(mode="after")
    def exact_private_readback_and_cleanup(self) -> NotebookCanaryReceipt:
        if self.expected_source_sha256 != self.readback_source_sha256:
            raise ValueError("notebook canary source hash differs from the exact input")
        if self.expected_output_sha256 != self.readback_output_sha256:
            raise ValueError("notebook canary output hash differs from the exact expected output")
        if self.cleanup.provider_ref != self.provider_ref:
            raise ValueError("cleanup receipt belongs to a different notebook")
        if self.cleanup.outcome not in {"deleted", "not_found"}:
            raise ValueError("notebook canary cleanup was not proven")
        if self.completed_at < self.cleanup.observed_at:
            raise ValueError("canary completed before cleanup observation")
        return self


class KagglePrivateCanaryAdapter(Protocol):
    """Compatibility boundary containing only proven, private lifecycle methods.

    There is deliberately no public-dataset creation method and no cancellation
    method. Implementations must be separately credentialed and integration-tested.
    """

    def create_private_dataset(
        self,
        *,
        canary_id: UUID,
        files: Mapping[str, bytes],
        idempotency_key: str,
    ) -> PrivateDatasetSnapshot: ...

    def read_private_dataset(self, *, provider_ref: str, version: int) -> PrivateDatasetSnapshot: ...

    def delete_private_dataset(self, *, provider_ref: str, expected_version: int) -> CanaryCleanupReceipt: ...

    def push_private_notebook(
        self,
        *,
        canary_id: UUID,
        source: bytes,
        idempotency_key: str,
    ) -> PrivateNotebookSnapshot: ...

    def run_private_notebook(self, *, provider_ref: str, expected_source_sha256: str) -> PrivateNotebookSnapshot: ...

    def read_private_notebook(self, *, provider_ref: str) -> PrivateNotebookSnapshot: ...

    def delete_private_notebook(self, *, provider_ref: str, expected_source_sha256: str) -> CanaryCleanupReceipt: ...
