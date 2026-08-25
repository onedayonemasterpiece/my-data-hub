from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal, Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from my_data_hub.hashing import sha256_value
from my_data_hub.providers.models import ControlClass, ProviderFingerprint, ProviderKind

KAGGLE_API_PACKAGE = "kaggle"
KAGGLE_API_VERSION = "2.2.4"
CONTROL_MANIFEST_NAME = "my-data-hub-resource.json"
RUN_RECEIPT_NAME = "my-data-hub-run-receipt.json"


class KaggleProviderError(RuntimeError):
    """Fail-closed base error for the single Kaggle adapter."""


class KaggleDependencyError(KaggleProviderError):
    pass


class KaggleIdentityError(KaggleProviderError):
    pass


class KaggleContractError(KaggleProviderError):
    pass


class KagglePolicyError(KaggleProviderError):
    pass


class KaggleRetryExhausted(KaggleProviderError):
    pass


class KaggleAmbiguousMutation(KaggleProviderError):
    pass


class KaggleNotFound(KaggleProviderError):
    pass


class KaggleTerminalFailure(KaggleProviderError):
    pass


class KagglePollingTimeout(KaggleProviderError):
    pass


@dataclass(frozen=True, slots=True)
class BrokeredBlobGrant:
    """One opaque SDK upload grant; capabilities are absent from its repr."""

    blob_token: str = field(repr=False)
    create_url: str = field(repr=False)


@dataclass(frozen=True, slots=True)
class BrokeredDatasetFile:
    """One already-uploaded blob authorized for a Dataset finalization."""

    name: str
    total_bytes: int
    description: str
    blob_token: str = field(repr=False)


class RetryClass(StrEnum):
    RATE_LIMIT = "rate_limit"
    SERVER = "server"
    TIMEOUT = "timeout"
    CONNECTION = "connection"
    AUTHENTICATION = "authentication"
    AUTHORIZATION = "authorization"
    NOT_FOUND = "not_found"
    CONFLICT = "conflict"
    INVALID_REQUEST = "invalid_request"
    UNKNOWN = "unknown"


class MutationAction(StrEnum):
    CREATE_DATASET = "create_dataset"
    VERSION_DATASET = "version_dataset"
    PUSH_NOTEBOOK = "push_notebook"
    DELETE_DATASET = "delete_dataset"
    DELETE_NOTEBOOK = "delete_notebook"


class EffectOutcome(StrEnum):
    APPLIED = "applied"
    ALREADY_APPLIED = "already_applied"
    NOT_FOUND = "not_found"
    UNCERTAIN = "uncertain"


class KernelState(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETE = "complete"
    FAILED = "failed"
    UNKNOWN = "unknown"


class KaggleProviderIdentity(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    username: str = Field(min_length=1, max_length=300)
    package: Literal["kaggle"] = "kaggle"
    package_version: Literal["2.2.4"] = "2.2.4"
    authenticated: Literal[True] = True


class ProviderEffectIntent(BaseModel):
    """Persist-before-side-effect payload; persistence is owned by the caller."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    operation_id: UUID
    effect_id: UUID
    idempotency_key: str = Field(min_length=8, max_length=300)
    task_id: UUID
    action: MutationAction
    provider_ref: str = Field(pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", max_length=300)
    expected_fingerprint: ProviderFingerprint | None = None
    arguments_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    request_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    requested_at: datetime

    @classmethod
    def create(
        cls,
        *,
        operation_id: UUID,
        effect_id: UUID,
        idempotency_key: str,
        task_id: UUID,
        action: MutationAction,
        provider_ref: str,
        arguments: Mapping[str, Any],
        requested_at: datetime,
        expected_fingerprint: ProviderFingerprint | None = None,
    ) -> ProviderEffectIntent:
        arguments_sha256 = sha256_value(dict(arguments))
        unsigned = {
            "operation_id": operation_id,
            "effect_id": effect_id,
            "idempotency_key": idempotency_key,
            "task_id": task_id,
            "action": action,
            "provider_ref": provider_ref,
            "expected_fingerprint": expected_fingerprint,
            "arguments_sha256": arguments_sha256,
            "requested_at": requested_at,
        }
        draft = cls.model_construct(**unsigned, request_sha256="0" * 64)
        return cls(**unsigned, request_sha256=draft.calculated_request_sha256())

    def calculated_request_sha256(self) -> str:
        return sha256_value(
            {
                "task_id": str(self.task_id),
                "action": self.action.value,
                "provider_ref": self.provider_ref,
                "expected_fingerprint": (
                    self.expected_fingerprint.model_dump(mode="json") if self.expected_fingerprint else None
                ),
                "arguments_sha256": self.arguments_sha256,
            }
        )

    @model_validator(mode="after")
    def request_hash_is_exact(self) -> ProviderEffectIntent:
        if self.request_sha256 != self.calculated_request_sha256():
            raise ValueError("request_sha256 does not match the provider effect intent")
        return self


class ProviderEffectReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    operation_id: UUID
    effect_id: UUID
    action: MutationAction
    provider_ref: str = Field(pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", max_length=300)
    outcome: EffectOutcome
    attempts: int = Field(ge=0, le=20)
    observed_fingerprint: ProviderFingerprint | None = None
    provider_version: int | None = Field(default=None, ge=1)
    observed_at: datetime
    detail_code: str = Field(min_length=1, max_length=100)


class ProviderEffectJournal(Protocol):
    """Durability seam. Implementations must commit before returning from persist_intent."""

    def persist_intent(self, intent: ProviderEffectIntent) -> None: ...

    def persist_receipt(self, receipt: ProviderEffectReceipt) -> None: ...

    def persist_resource_claim(self, claim: TaskResourceClaim) -> None: ...

    def assert_resource_claim(self, claim: TaskResourceClaim) -> None: ...


class TaskResourceClaim(BaseModel):
    """Exact cleanup authority recorded after a successful task-created effect.

    Slugs, names and prefixes are deliberately absent from authorization logic.
    The claim hash binds the resource identity, creating task, exact provider
    fingerprint and source/version identity.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    task_id: UUID
    effect_id: UUID
    provider_ref: str = Field(pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", max_length=300)
    kind: ProviderKind
    control_class: Literal[
        ControlClass.ORCHESTRATOR_PROTECTED,
        ControlClass.MCP_MANAGED,
        ControlClass.MCP_EXCHANGE,
    ]
    disposable: bool
    fingerprint: ProviderFingerprint
    provider_version: int = Field(ge=1)
    registered_at: datetime
    claim_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")

    @classmethod
    def create(
        cls,
        *,
        task_id: UUID,
        effect_id: UUID,
        provider_ref: str,
        kind: ProviderKind,
        control_class: Literal[
            ControlClass.ORCHESTRATOR_PROTECTED,
            ControlClass.MCP_MANAGED,
            ControlClass.MCP_EXCHANGE,
        ],
        disposable: bool,
        fingerprint: ProviderFingerprint,
        provider_version: int,
        registered_at: datetime,
    ) -> TaskResourceClaim:
        unsigned = {
            "task_id": task_id,
            "effect_id": effect_id,
            "provider_ref": provider_ref,
            "kind": kind,
            "control_class": control_class,
            "disposable": disposable,
            "fingerprint": fingerprint,
            "provider_version": provider_version,
            "registered_at": registered_at,
        }
        draft = cls.model_construct(**unsigned, claim_sha256="0" * 64)
        return cls(**unsigned, claim_sha256=draft.calculated_claim_sha256())

    def calculated_claim_sha256(self) -> str:
        return sha256_value(
            {
                "task_id": str(self.task_id),
                "effect_id": str(self.effect_id),
                "provider_ref": self.provider_ref,
                "kind": self.kind.value,
                "control_class": self.control_class.value,
                "disposable": self.disposable,
                "fingerprint": self.fingerprint.model_dump(mode="json"),
                "provider_version": self.provider_version,
                "registered_at": self.registered_at.isoformat(),
            }
        )

    @model_validator(mode="after")
    def claim_hash_is_exact(self) -> TaskResourceClaim:
        if self.claim_sha256 != self.calculated_claim_sha256():
            raise ValueError("claim_sha256 does not match the task resource claim")
        return self


class KaggleDatasetIdentity(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    provider_ref: str = Field(pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", max_length=300)
    version: int = Field(ge=1)
    privacy: Literal["private"]
    package_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    fingerprint: ProviderFingerprint
    observed_at: datetime


class KaggleDatasetFileObservation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    path: str = Field(min_length=1, max_length=1000)
    byte_size: int = Field(ge=0, le=68_719_476_736)
    provider_hash: str | None = Field(default=None, max_length=200)


class KaggleDatasetSummary(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    provider_ref: str = Field(pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", max_length=300)
    title: str = Field(min_length=1, max_length=500)
    provider_version: int = Field(ge=1)
    visibility: Literal["public", "owner_private"]
    license: str = Field(min_length=1, max_length=500)
    total_bytes: int = Field(ge=0, le=68_719_476_736)
    terms_acceptance_required: bool = False


class KaggleDatasetInspection(KaggleDatasetSummary):
    files: tuple[KaggleDatasetFileObservation, ...] = Field(max_length=10_000)
    files_manifest_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    attach_mode: Literal["native_exact", "native_guarded"]


class KaggleDatasetFileContent(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    provider_ref: str = Field(pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", max_length=300)
    provider_version: int = Field(ge=1)
    path: str = Field(min_length=1, max_length=1000)
    byte_size: int = Field(ge=0, le=67_108_864)
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    content: bytes = Field(repr=False)


class KaggleNotebookSummary(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    provider_ref: str = Field(pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", max_length=300)
    title: str = Field(min_length=1, max_length=500)
    source_version: int = Field(ge=1)
    private: Literal[True] = True
    provider_status: str = Field(min_length=1, max_length=100)


class KaggleNotebookSource(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    provider_ref: str = Field(pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", max_length=300)
    source_version: int = Field(ge=1)
    code_file: str = Field(min_length=1, max_length=1000)
    kernel_type: Literal["script", "notebook"]
    language: str = Field(min_length=1, max_length=30)
    source_utf8: str = Field(min_length=1, max_length=262_144)
    source_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    private: Literal[True] = True


class KaggleRunLog(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    content: bytes = Field(repr=False)
    byte_size: int = Field(ge=0, le=1_048_576)
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")


@dataclass(frozen=True, slots=True)
class ExactDatasetBatchFile:
    """One verified regular file; content is intentionally absent from repr."""

    path: str
    byte_size: int
    sha256: str
    content: bytes = field(repr=False)


@dataclass(frozen=True, slots=True)
class ExactDatasetBatch:
    """Bounded readback of an exact private MCP Dataset version."""

    identity: KaggleDatasetIdentity
    files: tuple[ExactDatasetBatchFile, ...]


class KaggleKernelSourceIdentity(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    provider_ref: str = Field(pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", max_length=300)
    source_version: int = Field(ge=1)
    privacy: Literal["private"]
    source_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    fingerprint: ProviderFingerprint
    observed_at: datetime


class KaggleKernelRunIdentity(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    task_run_id: UUID
    provider_ref: str = Field(pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", max_length=300)
    source_version: int = Field(ge=1)
    source_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    provider_kernel_id: int = Field(ge=1)
    provider_run_ref: str = Field(min_length=3, max_length=500)
    started_at: datetime

    @model_validator(mode="after")
    def provider_run_ref_is_exact_version(self) -> KaggleKernelRunIdentity:
        expected = f"{self.provider_ref}/{self.source_version}"
        if self.provider_run_ref != expected:
            raise ValueError("provider_run_ref must bind the exact Kaggle source version")
        return self


class KaggleKernelStatus(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    run: KaggleKernelRunIdentity
    state: KernelState
    provider_status: str = Field(min_length=1, max_length=100)
    failure_message: str | None = Field(default=None, max_length=4000)
    observed_at: datetime


class KaggleKernelOutputIdentity(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    run: KaggleKernelRunIdentity
    terminal_state: Literal[KernelState.COMPLETE]
    output_tree_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    receipt_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    file_count: int = Field(ge=1, le=10000)
    observed_at: datetime


class KaggleKernelOutputTreeIdentity(BaseModel):
    """Exact numeric run output copied into a caller-owned provider-side directory."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    run: KaggleKernelRunIdentity
    terminal_state: Literal[KernelState.COMPLETE]
    output_tree_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    file_count: int = Field(ge=1, le=10000)
    observed_at: datetime


class KaggleKernelFailureOutputIdentity(BaseModel):
    """One bounded receipt read from an exact terminal failed Kaggle run."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    run: KaggleKernelRunIdentity
    terminal_state: Literal[KernelState.FAILED]
    provider_status: Literal["failed", "error"]
    output_tree_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    receipt_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    file_count: Literal[1] = 1
    observed_at: datetime


class DatasetMutationResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    identity: KaggleDatasetIdentity
    claim: TaskResourceClaim
    effect: ProviderEffectReceipt


class NotebookMutationResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    source: KaggleKernelSourceIdentity
    run: KaggleKernelRunIdentity
    claim: TaskResourceClaim
    effect: ProviderEffectReceipt


class PrivateAccessProof(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    provider_ref: str = Field(pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", max_length=300)
    provider_version: int = Field(ge=1)
    authenticated_readback_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    unauthenticated_http_status: Literal[401, 403, 404]
    denial_class: Literal["authentication", "authorization", "not_found"]
    observed_at: datetime


class PollPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    interval_seconds: float = Field(default=15.0, ge=0, le=300)
    timeout_seconds: float = Field(default=1800.0, gt=0, le=43200)
    max_polls: int = Field(default=240, ge=1, le=2000)


class KaggleApiProtocol(Protocol):
    """Methods used from the official ``kaggle==2.2.4`` KaggleApi."""

    CONFIG_NAME_USER: str

    def authenticate(self) -> None: ...

    def get_config_value(self, name: str) -> str | None: ...

    def dataset_list_with_response(
        self,
        *,
        mine: bool,
        page_size: int,
        page_token: str | None,
        search: str | None = None,
        sort_by: str | None = None,
    ) -> Any: ...

    def kernels_list_with_response(
        self,
        *,
        mine: bool,
        page_size: int,
        page_token: str | None,
        search: str | None = None,
    ) -> Any: ...

    def dataset_status(self, dataset: str, format: str | None = None) -> str: ...

    def dataset_list_files(
        self,
        dataset: str,
        page_token: str | None = None,
        page_size: int = 20,
    ) -> Any: ...

    def dataset_metadata(self, dataset: str, path: str) -> str: ...

    def build_kaggle_client(self) -> Any: ...

    def dataset_create_new(
        self,
        folder: str,
        public: bool = False,
        quiet: bool = False,
        convert_to_csv: bool = True,
        dir_mode: str = "skip",
        ignore_patterns: list[str] | None = None,
    ) -> Any: ...

    def dataset_create_version(
        self,
        folder: str,
        version_notes: str,
        quiet: bool = False,
        convert_to_csv: bool = True,
        delete_old_versions: bool = False,
        dir_mode: str = "skip",
        ignore_patterns: list[str] | None = None,
    ) -> Any: ...

    def dataset_download_files(
        self,
        dataset: str,
        path: str | None = None,
        force: bool = False,
        quiet: bool = True,
        unzip: bool = False,
        licenses: list[str] | None = None,
    ) -> None: ...

    def dataset_download_file(
        self,
        dataset: str,
        file_name: str,
        path: str | None = None,
        force: bool = False,
        quiet: bool = True,
        licenses: list[str] | None = None,
    ) -> bool: ...

    def dataset_delete(self, owner_slug: str | None, dataset_slug: str, no_confirm: bool = False) -> bool: ...

    def kernels_push(self, folder: str, timeout: str | None = None, acc: str | None = None) -> Any: ...

    def kernels_pull(self, kernel: str, path: str, metadata: bool = False, quiet: bool = True) -> str: ...

    def kernels_status(self, kernel: str) -> Any: ...

    def kernels_output(
        self,
        kernel: str,
        path: str,
        file_pattern: str | None = None,
        force: bool = False,
        quiet: bool = True,
        page_token: str | None = None,
        page_size: int = 20,
    ) -> tuple[list[str], str]: ...

    def kernels_logs(self, kernel: str | None) -> str: ...

    def kernels_delete(self, kernel: str, no_confirm: bool = False) -> None: ...


class UnauthenticatedDatasetProbe(Protocol):
    """Hook supplied by real-provider tests; it must not carry Kaggle credentials."""

    def read_dataset(self, provider_ref: str, version: int) -> None: ...


Clock = Callable[[], datetime]
MonotonicClock = Callable[[], float]
Sleeper = Callable[[float], None]
TreeDownloader = Callable[[str, Path], None]
