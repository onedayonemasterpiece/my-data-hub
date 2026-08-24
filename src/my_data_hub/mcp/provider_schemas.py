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


class ProviderExpectedOutput(_ExactProviderPayload):
    """One caller-declared, top-level file expected from a notebook run."""

    path: str = Field(
        min_length=1,
        max_length=200,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,199}$",
        description="Top-level /kaggle/working output basename; directories are forbidden.",
    )
    max_bytes: int = Field(
        ge=1,
        le=8_388_608,
        description="Hard maximum accepted size for this output file.",
    )
    media_type: str = Field(
        min_length=3,
        max_length=100,
        pattern=r"^[A-Za-z0-9.+-]+/[A-Za-z0-9.+-]+$",
    )


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
    title: str = Field(
        min_length=5,
        max_length=80,
        description="Kaggle notebook title; must equal the slug part of resource_ref exactly.",
    )
    code_file: str = Field(
        min_length=1,
        max_length=300,
        description="Relative executable path, for example main.py; traversal is forbidden.",
    )
    kernel_type: Literal["script", "notebook"]
    language: Literal["python", "r", "julia"]
    source_utf8: str = Field(
        min_length=1,
        max_length=262_144,
        description="Executable UTF-8 source that must embed the exact task_run_id string.",
    )
    dataset_inputs: list[ProviderDatasetInput] = Field(max_length=16)
    disposable: bool
    enable_internet: bool = Field(
        default=False,
        description="Enable Kaggle internet only for disposable runs with no attached Dataset inputs.",
    )
    accelerator: Literal["none", "gpu"] = Field(
        default="none",
        description="Bounded compute class; gpu requests Kaggle's provider-selected GPU and does not promise a model.",
    )
    expected_outputs: list[ProviderExpectedOutput] = Field(
        default_factory=list,
        max_length=32,
        description="Declared top-level outputs that list/download may expose after the run.",
    )
    timeout_seconds: int | None = Field(default=None, ge=1, le=3600)


class ProviderReadPayload(_ExactProviderPayload):
    kind: Literal["dataset", "notebook"]
    claim_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")


class ProviderListPayload(_ExactProviderPayload):
    kind: Literal["dataset", "notebook"]
    claim_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    cursor: int = Field(default=0, ge=0)
    limit: int = Field(default=50, ge=1, le=50)


class ProviderDownloadPayload(_ExactProviderPayload):
    kind: Literal["dataset", "notebook"]
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


class ProviderUploadFile(_ExactProviderPayload):
    path: str = Field(min_length=1, max_length=1000)
    byte_size: int = Field(ge=0, le=67_108_864)
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")


class ProviderUploadStartPayload(_ExactProviderPayload):
    kind: Literal["dataset"]
    upload_id: UUID
    task_id: UUID
    effect_id: UUID
    idempotency_key: str = Field(min_length=8, max_length=300)
    title: str = Field(min_length=6, max_length=50)
    disposable: bool
    files: list[ProviderUploadFile] = Field(min_length=1, max_length=100)
    ttl_seconds: int = Field(default=3600, ge=300, le=86_400)


class ProviderUploadChunkPayload(_ExactProviderPayload):
    upload_id: UUID
    task_id: UUID
    path: str = Field(min_length=1, max_length=1000)
    offset: int = Field(ge=0, le=67_108_863)
    encoding: Literal["base64"]
    content_base64: str = Field(min_length=4, max_length=32_768)
    byte_size: int = Field(ge=1, le=24_576)
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")


class ProviderUploadReferencePayload(_ExactProviderPayload):
    upload_id: UUID
    task_id: UUID
