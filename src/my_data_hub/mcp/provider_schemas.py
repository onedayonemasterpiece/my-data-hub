from __future__ import annotations

from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from my_data_hub.providers.exchange import ExchangeManifest


class _ExactProviderPayload(BaseModel):
    """Closed MCP input model; provider arguments are never an open JSON object."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class ProviderDatasetInput(_ExactProviderPayload):
    resource_ref: str = Field(
        min_length=3,
        max_length=300,
        pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$",
    )
    provider_version: int = Field(ge=1)
    claim_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    control_class: Literal["mcp_managed", "mcp_exchange"]


class ProviderBinaryFile(_ExactProviderPayload):
    """One binary-safe file carried directly by a JSON MCP tool call."""

    encoding: Literal["base64"]
    content_base64: str = Field(min_length=4, max_length=349_528)
    byte_size: int = Field(ge=1, le=262_144)
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")


class ProviderCreatePayload(_ExactProviderPayload):
    kind: Literal["dataset"]
    task_id: UUID
    effect_id: UUID
    idempotency_key: str = Field(min_length=8, max_length=300)
    title: str = Field(min_length=1, max_length=200)
    disposable: bool
    files: dict[str, str | ProviderBinaryFile] = Field(min_length=1, max_length=100)
    exchange_manifest: ExchangeManifest | None = None


class ProviderVersionPayload(_ExactProviderPayload):
    kind: Literal["dataset"]
    task_id: UUID
    effect_id: UUID
    idempotency_key: str = Field(min_length=8, max_length=300)
    claim_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    version_notes: str = Field(min_length=1, max_length=1000)
    files: dict[str, str | ProviderBinaryFile] = Field(min_length=1, max_length=100)
    exchange_manifest: ExchangeManifest | None = None


class ProviderRunPayload(_ExactProviderPayload):
    kind: Literal["notebook"]
    task_id: UUID
    effect_id: UUID
    idempotency_key: str = Field(min_length=8, max_length=300)
    task_run_id: UUID
    title: str = Field(min_length=1, max_length=200)
    code_file: str = Field(min_length=1, max_length=300)
    kernel_type: Literal["script", "notebook"]
    language: Literal["python", "r", "julia"]
    source_utf8: str = Field(min_length=1, max_length=262_144)
    dataset_inputs: list[ProviderDatasetInput] = Field(max_length=16)
    disposable: bool
    timeout_seconds: int | None = Field(default=None, ge=1, le=3600)


class ProviderReadPayload(_ExactProviderPayload):
    kind: Literal["dataset", "notebook"]
    claim_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")


class ProviderListPayload(_ExactProviderPayload):
    kind: Literal["dataset"]
    claim_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    cursor: int = Field(default=0, ge=0)
    limit: int = Field(default=50, ge=1, le=50)


class ProviderDownloadPayload(_ExactProviderPayload):
    kind: Literal["dataset"]
    claim_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    path: str = Field(min_length=1, max_length=1000)
    offset: int = Field(default=0, ge=0)
    max_bytes: int = Field(default=131_072, ge=1, le=131_072)


class ProviderDeletePayload(_ExactProviderPayload):
    kind: Literal["dataset", "notebook"]
    task_id: UUID
    effect_id: UUID
    idempotency_key: str = Field(min_length=8, max_length=300)
    claim_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
