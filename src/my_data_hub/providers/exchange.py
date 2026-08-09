from __future__ import annotations

import hashlib
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import PurePosixPath
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from my_data_hub.hashing import canonical_json_bytes

MAX_EXCHANGE_TTL = timedelta(days=7)


class ExchangeValidationError(ValueError):
    pass


class ExchangeFile(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    path: str = Field(min_length=1, max_length=1000)
    media_type: str = Field(min_length=1, max_length=200)
    byte_size: int = Field(ge=0, le=10_737_418_240)
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    executable: bool

    @field_validator("path")
    @classmethod
    def path_is_normalized_relative_posix(cls, value: str) -> str:
        if "\\" in value or value.startswith("/") or "\x00" in value:
            raise ValueError("exchange path must be a relative POSIX path")
        parts = value.split("/")
        if any(part in {"", ".", ".."} for part in parts):
            raise ValueError("exchange path must be normalized and traversal-free")
        normalized = PurePosixPath(value).as_posix()
        if normalized != value:
            raise ValueError("exchange path must already be normalized")
        return value


class ExchangeSource(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    repository: str | None = Field(default=None, max_length=500)
    commit: str | None = Field(default=None, pattern=r"^[a-f0-9]{7,64}$")
    parent_package_id: UUID | None = None

    @model_validator(mode="after")
    def at_least_one_source_field(self) -> ExchangeSource:
        if self.repository is None and self.commit is None and self.parent_package_id is None:
            raise ValueError("source must contain at least one value")
        return self


class ExchangeManifest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    contract_version: Literal["my-data-hub-kaggle-exchange.v1"]
    package_id: UUID
    control_class: Literal["mcp_exchange"]
    private: Literal[True]
    dataset_ref: str = Field(pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", max_length=300)
    dataset_version: int = Field(ge=1)
    created_at: datetime
    expires_at: datetime
    created_by: str = Field(min_length=1, max_length=300)
    purpose: str = Field(min_length=1, max_length=1000)
    target_project: str = Field(min_length=1, max_length=200)
    intended_recipients: tuple[str, ...] = Field(min_length=1, max_length=20)
    sensitivity: Literal["public_source", "internal", "confidential_encrypted"]
    source: ExchangeSource | None = None
    instructions: str | None = Field(default=None, max_length=10_000)
    files: tuple[ExchangeFile, ...] = Field(min_length=1, max_length=1000)
    manifest_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")

    @field_validator("created_at", "expires_at")
    @classmethod
    def timestamps_are_timezone_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("exchange timestamps must include a timezone")
        return value

    @field_validator("intended_recipients")
    @classmethod
    def recipients_are_explicit_and_unique(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not item.strip() or item != item.strip() or len(item) > 300 for item in value):
            raise ValueError("recipients must be non-empty normalized principal identifiers")
        if len(value) != len(set(value)):
            raise ValueError("intended recipients must be unique")
        return value

    @model_validator(mode="after")
    def manifest_invariants(self) -> ExchangeManifest:
        if self.expires_at <= self.created_at:
            raise ValueError("exchange expiry must follow creation")
        if self.expires_at - self.created_at > MAX_EXCHANGE_TTL:
            raise ValueError("exchange TTL exceeds the maximum")
        paths = [file.path for file in self.files]
        if len(paths) != len(set(paths)):
            raise ValueError("exchange file paths must be unique")
        return self


def manifest_sha256(payload: Mapping[str, Any]) -> str:
    unsigned = dict(payload)
    unsigned.pop("manifest_sha256", None)
    return hashlib.sha256(canonical_json_bytes(unsigned)).hexdigest()


def validate_exchange_manifest(
    payload: Mapping[str, Any],
    *,
    recipient: str,
    file_contents: Mapping[str, bytes],
    now: datetime | None = None,
) -> ExchangeManifest:
    """Validate exact manifest bytes model, TTL, recipient and every payload hash."""

    expected_manifest_hash = payload.get("manifest_sha256")
    if not isinstance(expected_manifest_hash, str) or manifest_sha256(payload) != expected_manifest_hash:
        raise ExchangeValidationError("manifest_sha256 does not match the canonical manifest")

    manifest = ExchangeManifest.model_validate(payload)
    clock = now or datetime.now(UTC)
    if clock.tzinfo is None or clock.utcoffset() is None:
        raise ExchangeValidationError("validation clock must include a timezone")
    if clock < manifest.created_at:
        raise ExchangeValidationError("exchange package is not active yet")
    if clock >= manifest.expires_at:
        raise ExchangeValidationError("exchange package has expired")
    if recipient not in manifest.intended_recipients:
        raise ExchangeValidationError("principal is not an intended recipient")

    declared = {entry.path: entry for entry in manifest.files}
    if set(file_contents) != set(declared):
        raise ExchangeValidationError("payload file set differs from the manifest")
    for path, content in file_contents.items():
        if not isinstance(content, bytes):
            raise ExchangeValidationError(f"payload file {path!r} is not bytes")
        entry = declared[path]
        if len(content) != entry.byte_size:
            raise ExchangeValidationError(f"payload file {path!r} has the wrong size")
        if hashlib.sha256(content).hexdigest() != entry.sha256:
            raise ExchangeValidationError(f"payload file {path!r} has the wrong hash")
    return manifest
