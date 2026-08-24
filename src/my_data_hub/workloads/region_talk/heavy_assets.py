"""Offline-only asset admission for Region Talk heavy runtimes."""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
from pathlib import Path, PurePosixPath
from typing import Literal

from pydantic import Field, model_validator

from my_data_hub.hashing import canonical_json_bytes

from .heavy_contracts import SHA256_PATTERN, HeavyRuntimeUnavailable, StrictModel, canonical_sha256


class PythonDistributionAsset(StrictModel):
    distribution: str = Field(pattern=r"^[A-Za-z0-9_.-]{1,100}$")
    version: str | None = Field(default=None, max_length=100)
    wheel_filename: str | None = Field(default=None, max_length=300)
    wheel_sha256: str | None = Field(default=None, pattern=SHA256_PATTERN)
    status: Literal["resolved", "unresolved"]

    @model_validator(mode="after")
    def exact_resolution(self) -> PythonDistributionAsset:
        exact = self.version is not None and self.wheel_filename is not None and self.wheel_sha256 is not None
        if exact != (self.status == "resolved"):
            raise ValueError("distribution resolution fields differ from status")
        if self.wheel_filename is not None and PurePosixPath(self.wheel_filename).name != self.wheel_filename:
            raise ValueError("wheel filename must be a safe basename")
        return self


class ModelFileAsset(StrictModel):
    logical_name: str = Field(pattern=r"^[a-z][a-z0-9_.-]{0,99}$")
    provider_ref: str = Field(min_length=1, max_length=500)
    filename: str = Field(min_length=1, max_length=300)
    byte_size: int | None = Field(default=None, ge=1, le=10_000_000_000)
    sha256: str | None = Field(default=None, pattern=SHA256_PATTERN)
    status: Literal["provider_available_unverified", "resolved", "unresolved"]

    @model_validator(mode="after")
    def exact_resolution(self) -> ModelFileAsset:
        if self.status == "resolved" and (self.byte_size is None or self.sha256 is None):
            raise ValueError("resolved model file lacks exact size/hash")
        if self.status != "resolved" and self.sha256 is not None:
            raise ValueError("unresolved model file cannot claim an exact hash")
        if PurePosixPath(self.filename).name != self.filename:
            raise ValueError("model filename must be a safe basename")
        return self


class RemoteModelAsset(StrictModel):
    logical_name: str = Field(pattern=r"^[a-z][a-z0-9_.-]{0,99}$")
    provider: str = Field(min_length=1, max_length=100)
    model_id: str = Field(min_length=1, max_length=200)
    immutable_revision: str | None = Field(default=None, max_length=300)
    request_contract_sha256: str = Field(pattern=SHA256_PATTERN)
    status: Literal["resolved", "unresolved"]

    @model_validator(mode="after")
    def exact_resolution(self) -> RemoteModelAsset:
        if (self.immutable_revision is not None) != (self.status == "resolved"):
            raise ValueError("remote model immutable revision differs from status")
        return self


class HeavyRuntimeAssetManifest(StrictModel):
    schema_version: Literal["region-talk-heavy-runtime-assets.v1"]
    source_revision: str = Field(pattern=r"^[a-f0-9]{40}$")
    runtime_image_identity: str = Field(min_length=1, max_length=500)
    network_install_allowed: Literal[False]
    network_model_download_allowed: Literal[False]
    production_ready: bool
    distributions: tuple[PythonDistributionAsset, ...] = Field(min_length=1, max_length=100)
    models: tuple[ModelFileAsset, ...] = Field(min_length=1, max_length=100)
    remote_models: tuple[RemoteModelAsset, ...] = Field(min_length=1, max_length=20)
    shadow_fixture_sha256: str | None = Field(default=None, pattern=SHA256_PATTERN)
    smoke_receipt_sha256: str | None = Field(default=None, pattern=SHA256_PATTERN)
    manifest_sha256: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode="after")
    def exact_manifest(self) -> HeavyRuntimeAssetManifest:
        if len({item.distribution.lower() for item in self.distributions}) != len(self.distributions):
            raise ValueError("distribution assets must be unique")
        if len({item.logical_name for item in self.models}) != len(self.models):
            raise ValueError("model assets must be unique")
        if len({item.logical_name for item in self.remote_models}) != len(self.remote_models):
            raise ValueError("remote model assets must be unique")
        if self.production_ready and (
            any(item.status != "resolved" for item in self.distributions)
            or any(item.status != "resolved" for item in self.models)
            or any(item.status != "resolved" for item in self.remote_models)
            or self.shadow_fixture_sha256 is None
            or self.smoke_receipt_sha256 is None
        ):
            raise ValueError("production-ready assets require complete exact evidence")
        payload = self.model_dump(mode="json")
        payload.pop("manifest_sha256")
        if canonical_sha256(payload) != self.manifest_sha256:
            raise ValueError("heavy runtime asset manifest_sha256 differs")
        return self

    def require_production_ready(self) -> None:
        if not self.production_ready:
            raise HeavyRuntimeUnavailable("heavy runtime asset manifest is not production-ready")

    def verify_local(self, root: Path) -> None:
        self.require_production_ready()
        if not root.is_absolute() or root.is_symlink() or not root.is_dir():
            raise HeavyRuntimeUnavailable("heavy runtime asset root is unsafe")
        for item in self.distributions:
            assert item.wheel_filename is not None and item.wheel_sha256 is not None and item.version is not None
            wheel = root / "wheels" / item.wheel_filename
            if (
                wheel.is_symlink()
                or not wheel.is_file()
                or hashlib.sha256(wheel.read_bytes()).hexdigest() != item.wheel_sha256
            ):
                raise HeavyRuntimeUnavailable(f"offline wheel differs: {item.distribution}")
            try:
                observed = importlib.metadata.version(item.distribution)
            except importlib.metadata.PackageNotFoundError as exc:
                raise HeavyRuntimeUnavailable(f"distribution is not installed: {item.distribution}") from exc
            if observed != item.version:
                raise HeavyRuntimeUnavailable(f"distribution version differs: {item.distribution}")
        for item in self.models:
            model = root / "models" / item.logical_name / item.filename
            assert item.sha256 is not None and item.byte_size is not None
            if (
                model.is_symlink()
                or not model.is_file()
                or model.stat().st_size != item.byte_size
                or hashlib.sha256(model.read_bytes()).hexdigest() != item.sha256
            ):
                raise HeavyRuntimeUnavailable(f"offline model asset differs: {item.logical_name}")


def load_asset_manifest(path: Path) -> HeavyRuntimeAssetManifest:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 1024 * 1024:
        raise HeavyRuntimeUnavailable("heavy runtime asset manifest is absent or unsafe")
    body = path.read_bytes()
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise HeavyRuntimeUnavailable("heavy runtime asset manifest is invalid JSON") from exc
    if not isinstance(value, dict) or body != canonical_json_bytes(value):
        raise HeavyRuntimeUnavailable("heavy runtime asset manifest is not canonical JSON")
    return HeavyRuntimeAssetManifest.model_validate(value)


__all__ = [
    "HeavyRuntimeAssetManifest",
    "ModelFileAsset",
    "PythonDistributionAsset",
    "RemoteModelAsset",
    "load_asset_manifest",
]
