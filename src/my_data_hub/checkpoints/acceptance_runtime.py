"""Production adapters for the fixed FM05/FM14/FM15 checkpoint operations.

Only metadata is written to the control ledger.  Checkpoint packages and Kaggle
outputs stay below the owner-configured provider working directory.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import stat
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlsplit
from uuid import UUID, uuid5

from pydantic import BaseModel, ConfigDict, Field, model_validator

from my_data_hub.control_plane.ledger.models import EffectState
from my_data_hub.control_plane.ledger.store import ControlLedger
from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.providers.kaggle.adapter import (
    KaggleProviderAdapter,
    _canonical_notebook_source,
    directory_sha256,
    tree_sha256,
)
from my_data_hub.providers.kaggle.contracts import (
    DatasetMutationResult,
    EffectOutcome,
    KaggleAmbiguousMutation,
    KaggleDatasetIdentity,
    KaggleTerminalFailure,
    MutationAction,
    PollPolicy,
    ProviderEffectIntent,
)
from my_data_hub.providers.kaggle.control_journal import (
    AuthenticatedControlPlaneClient,
    ControlPlaneAcceptanceIdentity,
    RemoteControlLedgerKaggleJournal,
)
from my_data_hub.providers.models import ControlClass, ProviderKind

from .acceptance import (
    ACCEPTANCE_MAX_ATTEMPTS,
    ACCEPTANCE_TIMEOUT_SECONDS,
    CheckpointAcceptanceCoordinator,
    CheckpointAcceptanceError,
    CheckpointAcceptanceHead,
    CheckpointAcceptanceIntent,
    CheckpointAcceptanceReceipt,
    CheckpointAcceptanceStageReceipt,
    CorruptCheckpointRejectionRequest,
    DurableAcceptanceOperation,
    EmptyCheckpointRoundtripRequest,
    EvidenceClass,
    ForcedRestoreFailureRequest,
    Scenario,
)
from .kaggle_runtime import (
    CHECKPOINT_MANIFEST_NAME,
    KaggleCheckpointDatasetProvider,
    KaggleCheckpointRestoreVerifier,
    KaggleCheckpointVerifierAssets,
    RemoteControlCheckpointRegistry,
)
from .manifest import CheckpointManifest, build_manifest, load_and_verify, sha256_file, write_manifest
from .registry import CheckpointRegistryContract

_JOURNAL_KIND_PREFIX = "checkpoint_acceptance"
_JOURNAL_SCHEMA = "my-data-hub-checkpoint-acceptance-journal.v1"
_EFFECT_NAMESPACE = UUID("012bb305-cad2-5236-9f7e-33774492a721")
_FIXED_CORRUPTION_PATH = "physical/base.tar.gz"
_FM15_RECEIPT_NAME = "checkpoint-acceptance-fm15-failure.json"
_RUNTIME_ROOT = Path("/kaggle/working")

_STAGES: dict[Scenario, tuple[str, ...]] = {
    "FM05": ("empty_candidate", "private_upload", "exact_readback", "independent_restore", "cas_promotion"),
    "FM14": ("corrupted_candidate", "hash_mismatch_rejection"),
    "FM15": ("restore_failure_candidate", "exact_readback", "forced_restore_rejection"),
}


class CheckpointAcceptanceCapabilityError(CheckpointAcceptanceError):
    """A required safe production seam is absent; no provider mutation is allowed."""


class CheckpointAcceptanceEntrypointBlocker(CheckpointAcceptanceCapabilityError):
    """Named pre-mutation production capability blocker for exit status 78."""

    def __init__(self, code: str) -> None:
        if re.fullmatch(r"[A-Z][A-Z0-9_]{2,119}", code) is None:
            raise ValueError("checkpoint acceptance blocker code is invalid")
        self.code = code
        super().__init__(code)


class CheckpointAcceptanceRuntimeAmbiguity(CheckpointAcceptanceError):
    """The mutation phase may have started and must never be reported BLOCKED."""


class CheckpointAcceptanceControlIdentity(BaseModel):
    """Dedicated owner-task authority; it never impersonates a master epoch."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    authority_kind: Literal["acceptance-task"] = "acceptance-task"
    request_id: UUID
    task_run_id: UUID
    attempt_id: UUID
    scope: Literal["acceptance:operate"] = "acceptance:operate"

    @model_validator(mode="after")
    def task_bound(self) -> CheckpointAcceptanceControlIdentity:
        if self.request_id != self.task_run_id:
            raise ValueError("checkpoint control authority must be task-bound")
        return self


class CheckpointAcceptanceVerifierAsset(BaseModel):
    """Owner-reviewed verifier source identity; source bytes remain in Kaggle."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    dataset_version_ref: str = Field(
        pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[1-9][0-9]*$",
        max_length=320,
    )
    claim_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    path: Path
    source_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    code_file: Literal["worker.py"] = "worker.py"


class CheckpointAcceptanceProductionConfig(BaseModel):
    """Strict metadata-only input for one fixed production checkpoint scenario."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["my-data-hub-checkpoint-acceptance-production-config.v1"]
    scenario: Scenario
    operation_id: UUID
    task_run_id: UUID
    source_revision: str = Field(pattern=r"^[a-f0-9]{40}$")
    started_at: datetime
    provider_owner: str = Field(pattern=r"^[A-Za-z0-9_.-]{1,80}$")
    dataset_ref: str = Field(pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", max_length=300)
    evidence_notebook_ref: str = Field(
        pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$",
        max_length=300,
    )
    verifier_notebook_ref: str | None = Field(
        default=None,
        pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$",
        max_length=300,
    )
    template_dataset_version_ref: str = Field(
        pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[1-9][0-9]*$",
        max_length=320,
    )
    template_claim_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    template_directory: Path
    template_manifest_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    template_content_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    verifier: CheckpointAcceptanceVerifierAsset | None = None
    status_dataset_version_ref: str = Field(
        pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[1-9][0-9]*$",
        max_length=320,
    )
    status_config_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    status_helper_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    working_directory: Path
    control_base_url: str = Field(min_length=12, max_length=500)
    control_identity: CheckpointAcceptanceControlIdentity
    timeout_seconds: Literal[900]

    @model_validator(mode="after")
    def exact_production_binding(self) -> CheckpointAcceptanceProductionConfig:
        if self.started_at.tzinfo is None or self.started_at.utcoffset() is None:
            raise ValueError("checkpoint acceptance start must be timezone-aware")
        if (
            self.control_identity.request_id != self.task_run_id
            or self.control_identity.task_run_id != self.task_run_id
        ):
            raise ValueError("control acceptance identity must equal the task run id")
        output_refs = [self.dataset_ref, self.evidence_notebook_ref]
        if self.verifier_notebook_ref is not None:
            output_refs.append(self.verifier_notebook_ref)
        if any(ref.split("/", 1)[0] != self.provider_owner for ref in output_refs):
            raise ValueError("checkpoint acceptance resources must have the fixed provider owner")
        input_refs = [self.template_dataset_version_ref, self.status_dataset_version_ref]
        if self.verifier is not None:
            input_refs.append(self.verifier.dataset_version_ref)
        if any(ref.split("/", 1)[0] != self.provider_owner for ref in input_refs):
            raise ValueError("checkpoint acceptance input assets must have the fixed provider owner")
        if (self.scenario in {"FM05", "FM15"}) != (self.verifier is not None):
            raise ValueError("only restore scenarios require the fixed verifier asset")
        if (self.scenario in {"FM05", "FM15"}) != (self.verifier_notebook_ref is not None):
            raise ValueError("only restore scenarios require the fixed verifier Notebook")
        parsed = urlsplit(self.control_base_url)
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or parsed.path not in {"", "/"}
        ):
            raise ValueError("control base URL must be credential-free HTTPS origin")
        paths = [
            (self.template_directory, "template"),
            (self.working_directory, "working"),
        ]
        if self.verifier is not None:
            paths.append((self.verifier.path, "verifier"))
        for path, label in paths:
            _assert_runtime_path(path, label, allow_root=False)
        if self.template_directory.resolve() == self.working_directory.resolve():
            raise ValueError("template and working directories must differ")
        return self

    @property
    def config_sha256(self) -> str:
        return _metadata_sha256(self.model_dump(mode="json"))

    @property
    def journal_path(self) -> Path:
        return self.working_directory / "checkpoint-acceptance-control.sqlite3"


class CheckpointAcceptanceProviderLocator(BaseModel):
    """Bounded provider identities sufficient for outer evidence reconciliation."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    provider_owner: str = Field(pattern=r"^[A-Za-z0-9_.-]{1,80}$")
    dataset_ref: str = Field(pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", max_length=300)
    evidence_notebook_ref: str = Field(
        pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$",
        max_length=300,
    )
    verifier_notebook_ref: str | None = Field(
        default=None,
        pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$",
        max_length=300,
    )
    template_dataset_version_ref: str = Field(
        pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[1-9][0-9]*$",
        max_length=320,
    )
    template_claim_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    verifier_dataset_version_ref: str | None = Field(
        default=None,
        pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[1-9][0-9]*$",
        max_length=320,
    )
    verifier_claim_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    exact_version_refs: tuple[str, ...] = Field(default=(), max_length=5)

    @model_validator(mode="after")
    def exact_refs(self) -> CheckpointAcceptanceProviderLocator:
        if (
            self.dataset_ref.split("/", 1)[0] != self.provider_owner
            or self.evidence_notebook_ref.split("/", 1)[0] != self.provider_owner
            or (
                self.verifier_notebook_ref is not None
                and self.verifier_notebook_ref.split("/", 1)[0] != self.provider_owner
            )
            or self.template_dataset_version_ref.split("/", 1)[0] != self.provider_owner
            or (
                self.verifier_dataset_version_ref is not None
                and self.verifier_dataset_version_ref.split("/", 1)[0] != self.provider_owner
            )
            or (self.verifier_dataset_version_ref is None) != (self.verifier_claim_sha256 is None)
            or (self.verifier_dataset_version_ref is None) != (self.verifier_notebook_ref is None)
            or len(set(self.exact_version_refs)) != len(self.exact_version_refs)
            or any(
                re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[1-9][0-9]*", ref) is None
                for ref in self.exact_version_refs
            )
        ):
            raise ValueError("checkpoint acceptance provider locator is not exact")
        return self


class CheckpointAcceptanceOperationalResult(BaseModel):
    """Terminal metadata-only Notebook output; never a matrix PASS claim."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["my-data-hub-checkpoint-acceptance-operational-result.v1"] = (
        "my-data-hub-checkpoint-acceptance-operational-result.v1"
    )
    scenario: Scenario
    outcome: Literal["LIVE_EVIDENCE_READY", "BLOCKED", "FAIL"]
    live_evidence: bool
    operation_id: UUID
    task_run_id: UUID
    source_revision: str = Field(pattern=r"^[a-f0-9]{40}$")
    config_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    mutations_started: int = Field(ge=0, le=5)
    locator: CheckpointAcceptanceProviderLocator
    candidate_checkpoint_id: UUID | None = None
    intent_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    verdict: Literal["LIVE_PASS"] | None = None
    evidence_class: Literal["live"] | None = None
    initial_head: CheckpointAcceptanceHead | None = None
    final_head: CheckpointAcceptanceHead | None = None
    head_unchanged: bool | None = None
    stages: tuple[CheckpointAcceptanceStageReceipt, ...] = Field(default=(), max_length=5)
    receipt: CheckpointAcceptanceReceipt | None = None
    receipt_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    blocker_code: str | None = Field(default=None, pattern=r"^[A-Z][A-Z0-9_]{2,119}$")
    failure_code: str | None = Field(default=None, pattern=r"^[A-Z][A-Z0-9_]{2,119}$")
    completed_at: datetime

    @model_validator(mode="after")
    def exact_outcome_shape(self) -> CheckpointAcceptanceOperationalResult:
        receipt_fields = (
            self.candidate_checkpoint_id,
            self.intent_sha256,
            self.verdict,
            self.evidence_class,
            self.initial_head,
            self.final_head,
            self.head_unchanged,
            self.receipt,
            self.receipt_sha256,
        )
        if self.completed_at.tzinfo is None or self.completed_at.utcoffset() is None:
            raise ValueError("checkpoint acceptance result time must be timezone-aware")
        if self.outcome == "LIVE_EVIDENCE_READY":
            if (
                not self.live_evidence
                or self.mutations_started < 1
                or not self.locator.exact_version_refs
                or self.blocker_code is not None
                or self.failure_code is not None
                or any(value is None for value in receipt_fields)
                or self.receipt is None
                or self.receipt.verdict != "LIVE_PASS"
                or self.receipt.evidence_class != "live"
                or self.receipt.receipt_sha256 != self.receipt_sha256
                or self.stages != self.receipt.stages
                or self.candidate_checkpoint_id != self.receipt.candidate_checkpoint_id
                or self.intent_sha256 != self.receipt.intent_sha256
                or self.initial_head != self.receipt.initial_head
                or self.final_head != self.receipt.final_head
                or self.head_unchanged != self.receipt.head_unchanged
            ):
                raise ValueError("ready result lacks one exact durable live receipt")
        elif self.outcome == "BLOCKED":
            if self.live_evidence or self.mutations_started != 0 or not self.blocker_code:
                raise ValueError("blocked result must be pre-mutation with a named blocker")
            if any(value is not None for value in receipt_fields) or self.stages:
                raise ValueError("blocked result cannot include acceptance receipt claims")
        else:
            if (
                self.live_evidence
                or self.mutations_started < 1
                or not self.failure_code
                or self.blocker_code is not None
                or any(value is not None for value in receipt_fields)
                or self.stages
            ):
                raise ValueError("failed result must conservatively mark the mutation phase")
        return self


@dataclass(frozen=True, slots=True)
class ProductionCheckpointAcceptanceRuntime:
    """Exact assembled production runtime and its deterministic request."""

    config: CheckpointAcceptanceProductionConfig
    coordinator: CheckpointAcceptanceCoordinator
    request: EmptyCheckpointRoundtripRequest | CorruptCheckpointRejectionRequest | ForcedRestoreFailureRequest

    def run(self) -> CheckpointAcceptanceReceipt:
        if self.config.scenario == "FM05":
            assert isinstance(self.request, EmptyCheckpointRoundtripRequest)
            return self.coordinator.run_empty_roundtrip(self.request)
        if self.config.scenario == "FM14":
            assert isinstance(self.request, CorruptCheckpointRejectionRequest)
            return self.coordinator.run_corruption_rejection(self.request)
        assert isinstance(self.request, ForcedRestoreFailureRequest)
        return self.coordinator.run_forced_restore_failure(self.request)


def _effect_key(operation_id: UUID, kind: str) -> str:
    return f"checkpoint-acceptance:{operation_id}:{kind}"


def _effect_id(operation_id: UUID, kind: str) -> str:
    return str(uuid5(_EFFECT_NAMESPACE, f"{operation_id}:{kind}"))


def _metadata_sha256(value: object) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _assert_runtime_path(path: Path, label: str, *, allow_root: bool) -> None:
    if not path.is_absolute() or path.is_symlink():
        raise ValueError(f"checkpoint acceptance {label} path must be absolute and non-symlink")
    root = _RUNTIME_ROOT.resolve()
    resolved = path.resolve()
    if path != resolved:
        raise ValueError(f"checkpoint acceptance {label} path must be normalized")
    if (not allow_root and resolved == root) or (resolved != root and root not in resolved.parents):
        raise ValueError(f"checkpoint acceptance {label} path must stay below /kaggle/working")
    current = path
    while current != _RUNTIME_ROOT and current != current.parent:
        if current.exists() and current.is_symlink():
            raise ValueError(f"checkpoint acceptance {label} path contains a symbolic link")
        current = current.parent


def _required_runtime_token(environ: Mapping[str, str]) -> str:
    token = environ.get("MY_DATA_HUB_RUN_SECRET", "")
    if not 24 <= len(token) <= 4096 or any(char.isspace() for char in token):
        raise CheckpointAcceptanceEntrypointBlocker("CHECKPOINT_CONTROL_RUNTIME_TOKEN_MISSING")
    return token


def _read_bounded_regular_file(path: Path, *, max_bytes: int) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or not 1 <= metadata.st_size <= max_bytes:
            raise ValueError("checkpoint acceptance asset is absent or unbounded")
        with os.fdopen(descriptor, "rb", closefd=False) as handle:
            payload = handle.read(max_bytes + 1)
        if not 1 <= len(payload) <= max_bytes:
            raise ValueError("checkpoint acceptance asset changed or exceeds its bound")
        return payload
    finally:
        os.close(descriptor)


class ControlLedgerCheckpointAcceptanceJournal:
    """Map the acceptance journal onto append-only operation/effect records.

    Stage, attempt and terminal receipts use deterministic general-ledger
    effects.  Thus a crash after recording a provider result but before the
    coordinator sees the response is reconciled without a new provider effect.
    """

    def __init__(self, ledger: ControlLedger) -> None:
        self.ledger = ledger

    def ensure_intent(self, intent: CheckpointAcceptanceIntent) -> DurableAcceptanceOperation:
        payload = intent.model_dump(mode="json")
        record, _created = self.ledger.ensure_operation(
            operation_id=str(intent.operation_id),
            idempotency_key=(f"checkpoint-acceptance:{intent.scenario}:{intent.idempotency_key_sha256}"),
            operation_kind=f"{_JOURNAL_KIND_PREFIX}.{intent.scenario}.v1",
            intent=payload,
            initial_state="INTENT_COMMITTED",
            identity={"schema_version": _JOURNAL_SCHEMA, "intent": payload},
        )
        if record.operation_id != str(intent.operation_id) or record.identity != {
            "schema_version": _JOURNAL_SCHEMA,
            "intent": payload,
        }:
            raise CheckpointAcceptanceError("durable acceptance operation identity conflicts")
        operation = self.operation(intent.operation_id)
        assert operation is not None
        return operation

    def operation(self, operation_id: UUID) -> DurableAcceptanceOperation | None:
        record = self.ledger.get_operation(str(operation_id))
        if record is None:
            return None
        if not record.operation_kind.startswith(f"{_JOURNAL_KIND_PREFIX}."):
            raise CheckpointAcceptanceError("operation is not a checkpoint acceptance operation")
        try:
            if set(record.identity) != {"schema_version", "intent"}:
                raise ValueError("identity fields differ")
            if record.identity["schema_version"] != _JOURNAL_SCHEMA:
                raise ValueError("journal schema differs")
            intent = CheckpointAcceptanceIntent.model_validate(record.identity["intent"])
        except (KeyError, TypeError, ValueError) as exc:
            raise CheckpointAcceptanceError("durable acceptance intent is invalid") from exc
        if intent.operation_id != operation_id:
            raise CheckpointAcceptanceError("durable acceptance intent has another operation id")
        if record.operation_kind != f"{_JOURNAL_KIND_PREFIX}.{intent.scenario}.v1" or record.idempotency_key != (
            f"checkpoint-acceptance:{intent.scenario}:{intent.idempotency_key_sha256}"
        ):
            raise CheckpointAcceptanceError("durable acceptance operation binding differs")
        stages: list[CheckpointAcceptanceStageReceipt] = []
        for stage in _STAGES[intent.scenario]:
            effect = self.ledger.get_effect_by_idempotency_key(_effect_key(operation_id, f"stage:{stage}"))
            if effect is None:
                continue
            if effect.state is not EffectState.APPLIED or effect.receipt is None:
                raise CheckpointAcceptanceError("acceptance stage journal is incomplete")
            stages.append(CheckpointAcceptanceStageReceipt.model_validate(effect.receipt["receipt"]))
        attempts = 0
        failure_code: str | None = None
        for number in range(1, ACCEPTANCE_MAX_ATTEMPTS + 1):
            effect = self.ledger.get_effect_by_idempotency_key(_effect_key(operation_id, f"attempt-failure:{number}"))
            if effect is None:
                break
            if effect.state is not EffectState.APPLIED or effect.receipt is None:
                raise CheckpointAcceptanceError("acceptance failure journal is incomplete")
            attempts = number
            failure_code = str(effect.receipt["failure_code"])
        receipt: CheckpointAcceptanceReceipt | None = None
        terminal = self.ledger.get_effect_by_idempotency_key(_effect_key(operation_id, "terminal"))
        if terminal is not None:
            if terminal.state is not EffectState.APPLIED or terminal.receipt is None:
                raise CheckpointAcceptanceError("acceptance terminal journal is incomplete")
            receipt = CheckpointAcceptanceReceipt.model_validate(terminal.receipt["receipt"])
        states = {"INTENT_COMMITTED", "RUNNING", "DURABLE_COMPLETE", "FAILED"}
        if record.state not in states:
            raise CheckpointAcceptanceError("durable acceptance operation state is invalid")
        return DurableAcceptanceOperation(
            intent=intent,
            state=record.state,
            stages=tuple(stages),
            attempts=attempts,
            receipt=receipt,
            failure_code=failure_code,
        )

    def record_stage(
        self,
        operation_id: UUID,
        intent_sha256: str,
        receipt: CheckpointAcceptanceStageReceipt,
    ) -> DurableAcceptanceOperation:
        operation = self._bound(operation_id, intent_sha256)
        if operation.state in {"DURABLE_COMPLETE", "FAILED"}:
            raise CheckpointAcceptanceError("cannot append a stage to a terminal operation")
        expected = _STAGES[operation.intent.scenario]
        if receipt.stage not in expected:
            raise CheckpointAcceptanceError("stage does not belong to the durable scenario")
        recorded = {item.stage: item for item in operation.stages}
        if receipt.stage in recorded:
            if recorded[receipt.stage] != receipt:
                raise CheckpointAcceptanceError("acceptance stage replay has a different receipt")
            return operation
        if len(operation.stages) >= len(expected) or receipt.stage != expected[len(operation.stages)]:
            raise CheckpointAcceptanceError("acceptance stages must be committed in fixed order")
        self._record_effect(
            operation_id,
            f"stage:{receipt.stage}",
            {"intent_sha256": intent_sha256, "receipt_sha256": _metadata_sha256(receipt.model_dump(mode="json"))},
            {"receipt": receipt.model_dump(mode="json")},
        )
        current = self.ledger.get_operation(str(operation_id))
        assert current is not None
        if current.state == "INTENT_COMMITTED":
            self.ledger.transition_operation(
                str(operation_id),
                expected_state="INTENT_COMMITTED",
                new_state="RUNNING",
                metadata={"reason": "first_acceptance_stage"},
            )
        result = self.operation(operation_id)
        assert result is not None
        return result

    def complete(
        self,
        operation_id: UUID,
        intent_sha256: str,
        receipt: CheckpointAcceptanceReceipt,
    ) -> DurableAcceptanceOperation:
        self._bound(operation_id, intent_sha256)
        if receipt.intent_sha256 != intent_sha256 or receipt.operation_id != operation_id:
            raise CheckpointAcceptanceError("terminal acceptance receipt is not bound to its intent")
        self._record_effect(
            operation_id,
            "terminal",
            {"intent_sha256": intent_sha256, "receipt_sha256": receipt.receipt_sha256},
            {"receipt": receipt.model_dump(mode="json")},
        )
        current = self.ledger.get_operation(str(operation_id))
        assert current is not None
        if current.state in {"INTENT_COMMITTED", "RUNNING"}:
            self.ledger.transition_operation(
                str(operation_id),
                expected_state=current.state,
                new_state="DURABLE_COMPLETE",
                metadata={"receipt_sha256": receipt.receipt_sha256},
            )
        elif current.state != "DURABLE_COMPLETE":
            raise CheckpointAcceptanceError("failed operation cannot be completed")
        result = self.operation(operation_id)
        assert result is not None
        return result

    def record_attempt_failure(
        self,
        operation_id: UUID,
        intent_sha256: str,
        failure_code: str,
    ) -> DurableAcceptanceOperation:
        operation = self._bound(operation_id, intent_sha256)
        if operation.state == "FAILED":
            return operation
        if operation.state == "DURABLE_COMPLETE":
            raise CheckpointAcceptanceError("completed operation cannot record a failure")
        number = operation.attempts + 1
        if number > ACCEPTANCE_MAX_ATTEMPTS:
            raise CheckpointAcceptanceError("acceptance attempt counter exceeded its fixed bound")
        self._record_effect(
            operation_id,
            f"attempt-failure:{number}",
            {"intent_sha256": intent_sha256, "attempt": number, "failure_code": failure_code},
            {"attempt": number, "failure_code": failure_code},
        )
        current = self.ledger.get_operation(str(operation_id))
        assert current is not None
        if number == ACCEPTANCE_MAX_ATTEMPTS and current.state != "FAILED":
            self.ledger.transition_operation(
                str(operation_id),
                expected_state=current.state,
                new_state="FAILED",
                metadata={"attempt": number, "failure_code": failure_code},
            )
        elif current.state == "INTENT_COMMITTED":
            self.ledger.transition_operation(
                str(operation_id),
                expected_state="INTENT_COMMITTED",
                new_state="RUNNING",
                metadata={"reason": "acceptance_attempt_failed", "attempt": number},
            )
        result = self.operation(operation_id)
        assert result is not None
        return result

    def _bound(self, operation_id: UUID, intent_sha256: str) -> DurableAcceptanceOperation:
        operation = self.operation(operation_id)
        if operation is None or operation.intent.intent_sha256 != intent_sha256:
            raise CheckpointAcceptanceError("acceptance journal call is not bound to the exact intent")
        return operation

    def _record_effect(
        self,
        operation_id: UUID,
        kind: str,
        identity: dict[str, object],
        receipt: dict[str, object],
    ) -> None:
        effect_id = _effect_id(operation_id, kind)
        effect, _ = self.ledger.plan_effect(
            effect_id=effect_id,
            operation_id=str(operation_id),
            idempotency_key=_effect_key(operation_id, kind),
            effect_kind=f"checkpoint_acceptance.{kind}",
            exact_identity=identity,
        )
        if effect.state is EffectState.PLANNED:
            effect = self.ledger.claim_effect(effect_id) or effect
        if effect.state is EffectState.IN_PROGRESS:
            self.ledger.complete_effect(effect_id, receipt)
        elif effect.state is EffectState.APPLIED:
            if effect.receipt != receipt:
                raise CheckpointAcceptanceError("acceptance journal replay has a different receipt")
        else:
            raise CheckpointAcceptanceError("acceptance journal effect is not resumable")


@dataclass(frozen=True, slots=True)
class CheckpointAcceptanceRuntimeBinding:
    """Owner-fixed identity and provider-side assets for exactly one scenario."""

    scenario: Scenario
    operation_id: UUID
    task_run_id: UUID
    source_revision: str
    started_at: datetime
    dataset_ref: str
    notebook_ref: str
    template_directory: Path
    working_directory: Path
    verifier_source: bytes = b""
    verifier_code_file: str = "worker.py"

    def __post_init__(self) -> None:
        if self.started_at.tzinfo is None or self.started_at.utcoffset() is None:
            raise ValueError("acceptance start must be timezone-aware")
        if len(self.dataset_ref.split("/")) != 2 or len(self.notebook_ref.split("/")) != 2:
            raise ValueError("acceptance resources must be exact owner/slug refs")
        if not self.source_revision or len(self.source_revision) != 40:
            raise ValueError("acceptance binding requires an exact source revision")
        for path, label in ((self.template_directory, "template"), (self.working_directory, "working")):
            if not path.is_absolute() or path.is_symlink():
                raise ValueError(f"acceptance {label} path must be absolute and non-symlink")
            resolved = path.resolve()
            root = _RUNTIME_ROOT.resolve()
            if resolved == root or root not in resolved.parents:
                raise ValueError(f"acceptance {label} bytes must stay below /kaggle/working")
        if not self.template_directory.is_dir():
            raise ValueError("acceptance template directory is missing")
        if self.scenario in {"FM05", "FM15"} and not self.verifier_source:
            raise ValueError("restore scenarios require fixed verifier source")

    @property
    def deadline_at(self) -> datetime:
        return self.started_at.astimezone(UTC) + timedelta(seconds=ACCEPTANCE_TIMEOUT_SECONDS)


def build_production_checkpoint_acceptance_runtime(
    config: CheckpointAcceptanceProductionConfig,
    *,
    environ: Mapping[str, str] | None = None,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    control_transport: Any | None = None,
    adapter_factory: Callable[..., KaggleProviderAdapter] = KaggleProviderAdapter.from_environment,
    ledger_factory: Callable[[Path], ControlLedger] = ControlLedger,
) -> ProductionCheckpointAcceptanceRuntime:
    """Preflight and assemble one exact official production acceptance runtime.

    The modern control token is read only from ``MY_DATA_HUB_RUN_SECRET``.  A
    runtime-authenticated, read-only HEAD resolution happens before local
    journal creation and before any provider mutation.  Test injections cannot
    produce a live runtime because the concrete official types are checked.
    """

    values = os.environ if environ is None else environ
    token = _required_runtime_token(values)
    now = clock()
    if now.tzinfo is None or now.utcoffset() is None:
        raise CheckpointAcceptanceEntrypointBlocker("CHECKPOINT_RUNTIME_CLOCK_INVALID")
    remaining = (config.started_at.astimezone(UTC) + timedelta(seconds=ACCEPTANCE_TIMEOUT_SECONDS)) - now.astimezone(
        UTC
    )
    if remaining.total_seconds() <= 0 or remaining.total_seconds() > ACCEPTANCE_TIMEOUT_SECONDS:
        raise CheckpointAcceptanceEntrypointBlocker("CHECKPOINT_ACCEPTANCE_DEADLINE_INVALID")

    try:
        client = AuthenticatedControlPlaneClient(
            base_url=config.control_base_url,
            bearer_token=token,
            runtime_identity=ControlPlaneAcceptanceIdentity(
                request_id=config.control_identity.request_id,
                task_run_id=config.control_identity.task_run_id,
                attempt_id=config.control_identity.attempt_id,
            ),
            transport=control_transport,
        )
        registry = RemoteControlCheckpointRegistry(
            client,
            operation_id=str(config.operation_id),
            dataset_ref=config.dataset_ref,
        )
        # This is the modern-token/control authorization preflight.  It is a
        # bounded metadata GET and cannot change the registry or provider.
        registry.resolve_head()
    except Exception as exc:
        raise CheckpointAcceptanceEntrypointBlocker("CHECKPOINT_CONTROL_AUTH_PREFLIGHT_FAILED") from exc

    try:
        manifest = load_and_verify(
            config.template_directory / CHECKPOINT_MANIFEST_NAME,
            config.template_directory,
        )
        if (
            manifest.manifest_sha256 != config.template_manifest_sha256
            or directory_sha256(config.template_directory) != config.template_content_sha256
            or manifest.canonical_revision != 0
            or sum(manifest.restore_probe.row_counts.values()) != 0
        ):
            raise ValueError("template identity differs")
        verifier_source = b""
        verifier_code_file = "worker.py"
        if config.verifier is not None:
            path = config.verifier.path
            verifier_source = _read_bounded_regular_file(path, max_bytes=1024 * 1024)
            if hashlib.sha256(verifier_source).hexdigest() != config.verifier.source_sha256:
                raise ValueError("verifier identity differs")
            verifier_code_file = config.verifier.code_file
        binding = CheckpointAcceptanceRuntimeBinding(
            scenario=config.scenario,
            operation_id=config.operation_id,
            task_run_id=config.task_run_id,
            source_revision=config.source_revision,
            started_at=config.started_at,
            dataset_ref=config.dataset_ref,
            notebook_ref=config.verifier_notebook_ref or config.evidence_notebook_ref,
            template_directory=config.template_directory,
            working_directory=config.working_directory,
            verifier_source=verifier_source,
            verifier_code_file=verifier_code_file,
        )
    except Exception as exc:
        raise CheckpointAcceptanceEntrypointBlocker("CHECKPOINT_ACCEPTANCE_ASSET_PREFLIGHT_FAILED") from exc

    remote_journal = RemoteControlLedgerKaggleJournal(client)
    try:
        adapter = adapter_factory(journal=remote_journal, clock=clock)
        api_type = type(adapter.api)
        if (
            type(adapter) is not KaggleProviderAdapter
            or not api_type.__module__.startswith("kaggle.")
            or api_type.__name__ != "KaggleApi"
            or type(adapter.journal) is not RemoteControlLedgerKaggleJournal
            or type(registry) is not RemoteControlCheckpointRegistry
        ):
            raise TypeError("injected checkpoint adapter cannot produce live evidence")
        effects = KaggleTaskOwnedCheckpointEffects(
            adapter=adapter,
            registry=registry,
            binding=binding,
            clock=clock,
        )
        if effects.evidence_class != "live":
            raise TypeError("checkpoint runtime is not exact official live evidence")
    except Exception as exc:
        raise CheckpointAcceptanceEntrypointBlocker("CHECKPOINT_KAGGLE_AUTH_PREFLIGHT_FAILED") from exc

    journal_path = config.journal_path
    try:
        if journal_path.exists():
            if journal_path.is_symlink() or not journal_path.is_file():
                raise ValueError("checkpoint acceptance journal is not a regular file")
            if journal_path.stat().st_mode & 0o777 != 0o600:
                raise ValueError("checkpoint acceptance journal mode differs from 0600")
        ledger = ledger_factory(journal_path)
        journal = ControlLedgerCheckpointAcceptanceJournal(ledger)
    except Exception as exc:
        raise CheckpointAcceptanceRuntimeAmbiguity("checkpoint acceptance journal initialization failed") from exc

    idempotency_key = (
        f"checkpoint-acceptance:{config.scenario}:{config.operation_id}:{config.task_run_id}:{config.source_revision}"
    )
    request_type: type[
        EmptyCheckpointRoundtripRequest | CorruptCheckpointRejectionRequest | ForcedRestoreFailureRequest
    ]
    if config.scenario == "FM05":
        request_type = EmptyCheckpointRoundtripRequest
    elif config.scenario == "FM14":
        request_type = CorruptCheckpointRejectionRequest
    else:
        request_type = ForcedRestoreFailureRequest
    request = request_type(
        operation_id=config.operation_id,
        task_run_id=config.task_run_id,
        idempotency_key=idempotency_key,
        source_revision=config.source_revision,
    )
    return ProductionCheckpointAcceptanceRuntime(
        config=config,
        coordinator=CheckpointAcceptanceCoordinator(journal=journal, effects=effects, now=clock),
        request=request,
    )


class KaggleTaskOwnedCheckpointEffects:
    """Fixed production effects over one official adapter and remote registry.

    Public methods accept only the already-authorized intent.  Dataset refs,
    verifier source, package template, corruption path and deadline are fixed by
    :class:`CheckpointAcceptanceRuntimeBinding` and cannot be selected by a
    scenario request.
    """

    def __init__(
        self,
        *,
        adapter: KaggleProviderAdapter,
        registry: CheckpointRegistryContract,
        binding: CheckpointAcceptanceRuntimeBinding,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.adapter = adapter
        self.registry = registry
        self.binding = binding
        self.clock = clock
        registry_ref = getattr(registry, "dataset_ref", binding.dataset_ref)
        if registry_ref != binding.dataset_ref:
            raise ValueError("checkpoint registry and task-owned Dataset refs differ")
        registry_operation = getattr(registry, "operation_id", str(binding.operation_id))
        if str(registry_operation) != str(binding.operation_id):
            raise ValueError("checkpoint registry and acceptance operation ids differ")
        if adapter.identity.username != binding.dataset_ref.split("/", 1)[0]:
            raise ValueError("task-owned Dataset is not owned by the authenticated Kaggle identity")
        if binding.notebook_ref.split("/", 1)[0] != adapter.identity.username:
            raise ValueError("task-owned Notebook is not owned by the authenticated Kaggle identity")
        claim = None
        claim_source = getattr(adapter.journal, "current_resource_claim", None)
        if binding.scenario == "FM05" and callable(claim_source):
            try:
                claim = claim_source(
                    provider_ref=binding.dataset_ref,
                    kind=ProviderKind.DATASET,
                    control_class=ControlClass.ORCHESTRATOR_PROTECTED,
                )
            except Exception:
                # An absent first-version claim is valid.  Existing provider
                # state is checked by upload_candidate before any mutation.
                claim = None
        self._provider = KaggleCheckpointDatasetProvider(
            adapter,
            dataset_ref=binding.dataset_ref,
            operation_id=binding.operation_id,
            resource_task_id=binding.task_run_id,
            claim=claim,
        )

    @property
    def evidence_class(self) -> EvidenceClass:
        api_type = type(self.adapter.api)
        official = (
            type(self.adapter) is KaggleProviderAdapter
            and api_type.__module__.startswith("kaggle.")
            and api_type.__name__ == "KaggleApi"
            and type(self.adapter.journal) is RemoteControlLedgerKaggleJournal
            and type(self.registry) is RemoteControlCheckpointRegistry
        )
        return "live" if official else "injected"

    def head(self) -> CheckpointAcceptanceHead:
        value = self.registry.head
        return CheckpointAcceptanceHead(
            generation=value.generation,
            current_checkpoint_id=value.current,
            previous_checkpoint_id=value.previous,
        )

    def ensure_fm05_empty_candidate(self, intent: CheckpointAcceptanceIntent) -> CheckpointAcceptanceStageReceipt:
        manifest, package = self._ensure_candidate(intent, corrupt=False)
        promoted = CheckpointAcceptanceHead(
            generation=intent.initial_head.generation + 1,
            current_checkpoint_id=intent.candidate_checkpoint_id,
            previous_checkpoint_id=intent.initial_head.current_checkpoint_id,
        )
        if self.head() == intent.initial_head:
            self.registry.add_candidate(manifest)
        elif self.head() != promoted:
            raise CheckpointAcceptanceError("FM05 candidate replay observed an unrelated HEAD")
        return self._stage(
            intent,
            "empty_candidate",
            "EMPTY_CANDIDATE_CREATED",
            manifest=manifest,
            package_sha=tree_sha256(package),
            disposable=False,
            canonical_revision=manifest.canonical_revision,
            canonical_row_count=sum(manifest.restore_probe.row_counts.values()),
        )

    def ensure_fm05_private_upload(self, intent: CheckpointAcceptanceIntent) -> CheckpointAcceptanceStageReceipt:
        manifest, package = self._ensure_candidate(intent, corrupt=False)
        exact_ref = self._provider_call(lambda: self._provider.upload_candidate(package, manifest))
        self.registry.uploaded(intent.candidate_checkpoint_id, exact_ref)
        package_sha = tree_sha256(package)
        recorder = getattr(self.registry, "package_uploaded", None)
        if callable(recorder):
            recorder(intent.candidate_checkpoint_id, package_sha)
        return self._stage(
            intent,
            "private_upload",
            "PRIVATE_CANDIDATE_UPLOADED",
            manifest=manifest,
            package_sha=package_sha,
            exact_ref=exact_ref,
            disposable=False,
            provider={"exact_version_ref": exact_ref, "package_sha256": package_sha},
        )

    def ensure_fm05_exact_readback(self, intent: CheckpointAcceptanceIntent) -> CheckpointAcceptanceStageReceipt:
        manifest, package = self._ensure_candidate(intent, corrupt=False)
        exact_ref = self._exact_ref(intent)
        identity, observed = self._readback(exact_ref, intent, "fm05")
        expected = directory_sha256(package)
        if observed != expected:
            raise CheckpointAcceptanceError("FM05 exact provider readback differs from the candidate")
        read_manifest = load_and_verify(
            self._readback_dir(intent, "fm05") / CHECKPOINT_MANIFEST_NAME, self._readback_dir(intent, "fm05")
        )
        if read_manifest.manifest_sha256 != manifest.manifest_sha256:
            raise CheckpointAcceptanceError("FM05 exact readback manifest differs")
        self.registry.readback_verified(intent.candidate_checkpoint_id)
        return self._stage(
            intent,
            "exact_readback",
            "EXACT_READBACK_VERIFIED",
            manifest=manifest,
            package_sha=observed,
            exact_ref=exact_ref,
            disposable=False,
            expected=expected,
            observed=observed,
            provider=identity.model_dump(mode="json"),
        )

    def ensure_fm05_independent_restore(self, intent: CheckpointAcceptanceIntent) -> CheckpointAcceptanceStageReceipt:
        manifest, _package = self._ensure_candidate(intent, corrupt=False)
        exact_ref = self._exact_ref(intent)
        identity = self._provider_call(
            lambda: self.adapter.read_private_dataset(
                provider_ref=self.binding.dataset_ref, version=int(exact_ref.rsplit("/", 1)[1])
            )
        )
        assets = KaggleCheckpointVerifierAssets(
            notebook_ref=self.binding.notebook_ref,
            notebook_source=self.binding.verifier_source,
            code_file=self.binding.verifier_code_file,
            kernel_type="script" if self.binding.verifier_code_file.endswith(".py") else "notebook",
            timeout_seconds=max(60, min(600, int(self._remaining()))),
        )
        output_root = self.binding.working_directory / "verifier-output"
        output_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        verifier = KaggleCheckpointRestoreVerifier(
            self.adapter,
            assets,
            output_directory=output_root,
            operation_id=intent.operation_id,
            authorization_task_id=intent.task_run_id,
            poll_policy=self._poll_policy(),
            metadata_only_output=True,
        )
        receipt = self._provider_call(
            lambda: verifier.verify_restore(exact_version_ref=exact_ref, dataset_identity=identity, manifest=manifest)
        )
        self.registry.restore_verified(intent.candidate_checkpoint_id)
        return self._stage(
            intent,
            "independent_restore",
            "INDEPENDENT_RESTORE_VERIFIED",
            manifest=manifest,
            package_sha=identity.package_sha256,
            exact_ref=exact_ref,
            disposable=False,
            provider=receipt,
        )

    def ensure_fm05_cas_promotion(self, intent: CheckpointAcceptanceIntent) -> CheckpointAcceptanceStageReceipt:
        self._assert_intent(intent, "FM05")
        before = self.head()
        expected = CheckpointAcceptanceHead(
            generation=intent.initial_head.generation + 1,
            current_checkpoint_id=intent.candidate_checkpoint_id,
            previous_checkpoint_id=intent.initial_head.current_checkpoint_id,
        )
        if before == intent.initial_head:
            promoted = self.registry.promote(
                intent.candidate_checkpoint_id, expected_generation=intent.initial_head.generation
            )
            before = CheckpointAcceptanceHead(
                generation=promoted.generation,
                current_checkpoint_id=promoted.current,
                previous_checkpoint_id=promoted.previous,
            )
        if before != expected:
            raise CheckpointAcceptanceError("FM05 promotion did not reconcile to the exact CAS result")
        return self._stage(
            intent,
            "cas_promotion",
            "HEAD_CAS_PROMOTED",
            disposable=False,
            provider={
                "head": before.model_dump(mode="json"),
                "initial_head_sha256": intent.initial_head.snapshot_sha256,
            },
        )

    def ensure_fm14_corrupted_candidate(self, intent: CheckpointAcceptanceIntent) -> CheckpointAcceptanceStageReceipt:
        manifest, package = self._ensure_candidate(intent, corrupt=True)
        self.registry.add_candidate(manifest)
        exact_ref, mutation = self._upload_disposable(intent, package)
        expected, observed = self._corruption_hashes(package, manifest)
        return self._stage(
            intent,
            "corrupted_candidate",
            "TASK_OWNED_CORRUPTION_CANDIDATE_CREATED",
            manifest=manifest,
            package_sha=tree_sha256(package),
            exact_ref=exact_ref,
            disposable=True,
            expected=expected,
            observed=observed,
            provider={
                "exact_version_ref": exact_ref,
                "effect_receipt": mutation.effect.model_dump(mode="json"),
                "resource_claim_sha256": mutation.claim.claim_sha256,
            },
        )

    def ensure_fm14_hash_mismatch_rejection(
        self, intent: CheckpointAcceptanceIntent
    ) -> CheckpointAcceptanceStageReceipt:
        manifest, _package = self._ensure_candidate(intent, corrupt=True)
        exact_ref = self._exact_ref(intent)
        _identity, _ = self._readback(exact_ref, intent, "fm14")
        readback = self._readback_dir(intent, "fm14")
        expected, observed = self._corruption_hashes(readback, manifest)
        if expected == observed:
            raise CheckpointAcceptanceError("FM14 exact readback did not contain the fixed corruption")
        self.registry.reject(intent.candidate_checkpoint_id, "FM14_EXACT_READBACK_HASH_MISMATCH")
        return self._stage(
            intent,
            "hash_mismatch_rejection",
            "EXACT_READBACK_HASH_MISMATCH_REJECTED",
            manifest=manifest,
            package_sha=tree_sha256(readback),
            exact_ref=exact_ref,
            disposable=True,
            outcome="rejected_expected",
            expected=expected,
            observed=observed,
            provider={"exact_version_ref": exact_ref, "rejection": "FM14_EXACT_READBACK_HASH_MISMATCH"},
        )

    def ensure_fm15_restore_failure_candidate(
        self, intent: CheckpointAcceptanceIntent
    ) -> CheckpointAcceptanceStageReceipt:
        self._assert_fm15_capability(intent)
        manifest, package = self._ensure_candidate(intent, corrupt=False)
        self.registry.add_candidate(manifest)
        exact_ref, mutation = self._upload_disposable(intent, package)
        return self._stage(
            intent,
            "restore_failure_candidate",
            "TASK_OWNED_RESTORE_FAILURE_CANDIDATE_CREATED",
            manifest=manifest,
            package_sha=tree_sha256(package),
            exact_ref=exact_ref,
            disposable=True,
            provider={
                "exact_version_ref": exact_ref,
                "effect_receipt": mutation.effect.model_dump(mode="json"),
                "resource_claim_sha256": mutation.claim.claim_sha256,
            },
        )

    def ensure_fm15_exact_readback(self, intent: CheckpointAcceptanceIntent) -> CheckpointAcceptanceStageReceipt:
        manifest, package = self._ensure_candidate(intent, corrupt=False)
        exact_ref = self._exact_ref(intent)
        identity, observed = self._readback(exact_ref, intent, "fm15")
        expected = directory_sha256(package)
        if observed != expected:
            raise CheckpointAcceptanceError("FM15 exact readback differs from its disposable candidate")
        readback_manifest = load_and_verify(
            self._readback_dir(intent, "fm15") / CHECKPOINT_MANIFEST_NAME,
            self._readback_dir(intent, "fm15"),
        )
        if readback_manifest.manifest_sha256 != manifest.manifest_sha256:
            raise CheckpointAcceptanceError("FM15 exact readback manifest differs")
        return self._stage(
            intent,
            "exact_readback",
            "EXACT_READBACK_VERIFIED",
            manifest=manifest,
            package_sha=observed,
            exact_ref=exact_ref,
            disposable=True,
            expected=expected,
            observed=observed,
            provider=identity.model_dump(mode="json"),
        )

    def ensure_fm15_forced_restore_rejection(
        self, intent: CheckpointAcceptanceIntent
    ) -> CheckpointAcceptanceStageReceipt:
        self._assert_intent(intent, "FM15")
        manifest, package = self._ensure_candidate(intent, corrupt=False)
        expected_content_sha256 = directory_sha256(package)
        exact_ref = self._exact_ref(intent)
        run_id = uuid5(_EFFECT_NAMESPACE, f"fm15-verifier:{intent.operation_id}:{intent.task_run_id}")
        source = self._fm15_source(
            intent,
            run_id,
            exact_ref,
            manifest_sha256=manifest.manifest_sha256,
            expected_content_sha256=expected_content_sha256,
        )
        source_sha = hashlib.sha256(_canonical_notebook_source(source, kernel_type="script")).hexdigest()
        arguments = {
            "task_run_id": str(run_id),
            "source_sha256": source_sha,
            "dataset_sources": (exact_ref,),
            "control_class": ControlClass.MCP_MANAGED.value,
            "disposable": True,
        }
        effect = ProviderEffectIntent.create(
            operation_id=intent.operation_id,
            effect_id=uuid5(_EFFECT_NAMESPACE, f"fm15-notebook:{intent.operation_id}"),
            idempotency_key=f"checkpoint-acceptance:FM15:notebook:{intent.operation_id}",
            task_id=intent.task_run_id,
            action=MutationAction.PUSH_NOTEBOOK,
            provider_ref=self.binding.notebook_ref,
            arguments=arguments,
            requested_at=self.binding.started_at,
        )
        launched = self._provider_call(
            lambda: self.adapter.reconcile_private_notebook_mutation(
                intent=effect,
                task_run_id=run_id,
                expected_source_sha256=source_sha,
                dataset_sources=(exact_ref,),
                control_class=ControlClass.MCP_MANAGED,
                disposable=True,
            )
        )
        if launched is None:
            launched = self._provider_call(
                lambda: self.adapter.push_private_notebook(
                    intent=effect,
                    task_run_id=run_id,
                    source=source,
                    title=self.binding.notebook_ref.split("/", 1)[1],
                    code_file="worker.py",
                    kernel_type="script",
                    language="python",
                    control_class=ControlClass.MCP_MANAGED,
                    disposable=True,
                    dataset_sources=(exact_ref,),
                    enable_internet=False,
                    timeout_seconds=max(60, min(600, int(self._remaining()))),
                )
            )
        try:
            self._provider_call(lambda: self.adapter.poll_run(launched.run, self._poll_policy()))
        except KaggleTerminalFailure:
            destination = self.binding.working_directory / f"fm15-failure-{run_id}"
            if destination.exists():
                shutil.rmtree(destination)
            failed_identity = self._provider_call(
                lambda: self.adapter.download_exact_failed_run_output_file(
                    launched.run,
                    destination=destination,
                    file_name=_FM15_RECEIPT_NAME,
                    max_bytes=64 * 1024,
                )
            )
            failure_receipt = self._load_fm15_failure_receipt(
                destination / _FM15_RECEIPT_NAME,
                intent=intent,
                run_id=run_id,
                exact_ref=exact_ref,
                manifest_sha256=manifest.manifest_sha256,
                expected_content_sha256=expected_content_sha256,
            )
            self.registry.reject(intent.candidate_checkpoint_id, "FM15_FORCED_RESTORE_SMOKE_FAILURE")
        else:
            raise CheckpointAcceptanceError("FM15 fixed failing verifier unexpectedly completed")
        provider = {
            "provider_run_ref": launched.run.provider_run_ref,
            "source_version": launched.run.source_version,
            "source_sha256": launched.run.source_sha256,
            "effect_receipt": launched.effect.model_dump(mode="json"),
            "failure_receipt_sha256": _metadata_sha256(failure_receipt),
            "failed_output_identity": failed_identity.model_dump(mode="json"),
        }
        return self._stage(
            intent,
            "forced_restore_rejection",
            "FORCED_DISPOSABLE_RESTORE_FAILURE_REJECTED",
            disposable=True,
            exact_ref=exact_ref,
            outcome="rejected_expected",
            provider=provider,
        )

    def _assert_intent(self, intent: CheckpointAcceptanceIntent, scenario: Scenario) -> None:
        if (
            scenario != self.binding.scenario
            or intent.scenario != scenario
            or intent.operation_id != self.binding.operation_id
            or intent.task_run_id != self.binding.task_run_id
            or intent.source_revision != self.binding.source_revision
            or (intent.initial_head != self.head() and scenario != "FM05")
        ):
            raise CheckpointAcceptanceError("acceptance intent is outside the fixed runtime binding")
        self._remaining()

    def _assert_fm15_capability(self, intent: CheckpointAcceptanceIntent) -> None:
        self._assert_intent(intent, "FM15")
        if not callable(getattr(self.adapter, "download_exact_failed_run_output_file", None)):
            raise CheckpointAcceptanceCapabilityError(
                "KAGGLE_FAILED_RUN_EXACT_OUTPUT_UNAVAILABLE: KaggleProviderAdapter needs a bounded exact-run "
                "failed-output read seam before FM15 can mutate a disposable candidate"
            )

    def _ensure_candidate(
        self, intent: CheckpointAcceptanceIntent, *, corrupt: bool
    ) -> tuple[CheckpointManifest, Path]:
        self._assert_intent(intent, intent.scenario)
        destination = self.binding.working_directory / f"{intent.scenario.lower()}-{intent.candidate_checkpoint_id}"
        manifest_path = destination / CHECKPOINT_MANIFEST_NAME
        if destination.exists():
            manifest = CheckpointManifest.from_payload(json.loads(manifest_path.read_text()))
            if manifest.checkpoint_id != intent.candidate_checkpoint_id:
                raise CheckpointAcceptanceError("existing task package has a different candidate")
            if not corrupt:
                load_and_verify(manifest_path, destination)
            return manifest, destination
        try:
            source_manifest = load_and_verify(
                self.binding.template_directory / CHECKPOINT_MANIFEST_NAME,
                self.binding.template_directory,
            )
        except Exception as exc:
            raise CheckpointAcceptanceCapabilityError(
                "CHECKPOINT_ACCEPTANCE_TEMPLATE_INVALID: owner-configured verified package is unavailable"
            ) from exc
        if source_manifest.canonical_revision != 0 or sum(source_manifest.restore_probe.row_counts.values()) != 0:
            raise CheckpointAcceptanceCapabilityError(
                "CHECKPOINT_ACCEPTANCE_EMPTY_TEMPLATE_REQUIRED: owner must provide "
                "a verified empty PostgreSQL 18 package"
            )
        destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        temporary = destination.with_name(f".{destination.name}.building")
        shutil.rmtree(temporary, ignore_errors=True)
        temporary.mkdir(mode=0o700)
        for item in source_manifest.files:
            source = self.binding.template_directory / item.path
            target = temporary / item.path
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, target)
        manifest = build_manifest(
            package_directory=temporary,
            checkpoint_id=intent.candidate_checkpoint_id,
            master_instance_id=source_manifest.master_instance_id,
            epoch=source_manifest.epoch,
            parent_checkpoint_id=intent.initial_head.current_checkpoint_id,
            postgres_version=source_manifest.postgres_version,
            pgvector_version=source_manifest.pgvector_version,
            schema_version=source_manifest.schema_version,
            canonical_revision=0,
            source_run_id=str(intent.task_run_id),
            source_identity=f"checkpoint-acceptance:{intent.source_revision}",
            created_at=self.binding.started_at,
            checkpoint_lsn=source_manifest.checkpoint_lsn,
            file_kinds={item.path: item.kind for item in source_manifest.files},
            restore_probe=source_manifest.restore_probe,
        )
        write_manifest(temporary / CHECKPOINT_MANIFEST_NAME, manifest)
        if corrupt:
            target = temporary / _FIXED_CORRUPTION_PATH
            data = bytearray(target.read_bytes())
            if not data:
                raise CheckpointAcceptanceCapabilityError(
                    "CHECKPOINT_ACCEPTANCE_CORRUPTION_TARGET_EMPTY: fixed base archive has no byte to flip"
                )
            data[0] ^= 0x01
            target.write_bytes(data)
        temporary.replace(destination)
        return manifest, destination

    def _upload_disposable(
        self, intent: CheckpointAcceptanceIntent, package: Path
    ) -> tuple[str, DatasetMutationResult]:
        content_sha = directory_sha256(package)
        arguments = {
            "content_tree_sha256": content_sha,
            "control_class": ControlClass.MCP_EXCHANGE.value,
            "disposable": True,
        }
        effect = ProviderEffectIntent.create(
            operation_id=intent.operation_id,
            effect_id=uuid5(_EFFECT_NAMESPACE, f"{intent.scenario}:dataset:{intent.operation_id}"),
            idempotency_key=f"checkpoint-acceptance:{intent.scenario}:dataset:{intent.operation_id}",
            task_id=intent.task_run_id,
            action=MutationAction.CREATE_DATASET,
            provider_ref=self.binding.dataset_ref,
            arguments=arguments,
            requested_at=self.binding.started_at,
        )
        try:
            result = self._provider_call(
                lambda: self.adapter.reconcile_private_dataset_directory_mutation(
                    intent=effect,
                    source_directory=package,
                    expected_version=1,
                    arguments=arguments,
                    control_class=ControlClass.MCP_EXCHANGE,
                    disposable=True,
                )
            )
        except KaggleAmbiguousMutation:
            result = self._provider_call(
                lambda: self.adapter.create_private_dataset_from_directory(
                    intent=effect,
                    source_directory=package,
                    title=self.binding.dataset_ref.split("/", 1)[1],
                    control_class=ControlClass.MCP_EXCHANGE,
                    disposable=True,
                )
            )
        if result.effect.outcome not in {EffectOutcome.APPLIED, EffectOutcome.ALREADY_APPLIED}:
            raise CheckpointAcceptanceError("disposable Dataset mutation lacks an official applied receipt")
        return f"{result.identity.provider_ref}/{result.identity.version}", result

    def _exact_ref(self, intent: CheckpointAcceptanceIntent) -> str:
        self._assert_intent(intent, intent.scenario)
        version = self._provider_call(
            lambda: self.adapter.current_private_dataset_version(provider_ref=self.binding.dataset_ref)
        )
        if version is None:
            raise CheckpointAcceptanceError("task-owned Dataset has no exact numeric version")
        return f"{self.binding.dataset_ref}/{version}"

    def _readback(
        self, exact_ref: str, intent: CheckpointAcceptanceIntent, label: str
    ) -> tuple[KaggleDatasetIdentity, str]:
        version = int(exact_ref.rsplit("/", 1)[1])
        destination = self._readback_dir(intent, label)
        if destination.exists():
            shutil.rmtree(destination)
        identity = self._provider_call(
            lambda: self.adapter.download_private_dataset_exact(
                provider_ref=self.binding.dataset_ref, version=version, destination=destination
            )
        )
        # Content identity deliberately excludes the adapter-owned control
        # manifest.  ``identity.package_sha256`` separately proves the exact
        # provider package (content plus that persisted ownership manifest).
        return identity, directory_sha256(destination)

    def _readback_dir(self, intent: CheckpointAcceptanceIntent, label: str) -> Path:
        return self.binding.working_directory / f"readback-{label}-{intent.candidate_checkpoint_id}"

    @staticmethod
    def _corruption_hashes(package: Path, manifest: CheckpointManifest) -> tuple[str, str]:
        expected = next(item.sha256 for item in manifest.files if item.path == _FIXED_CORRUPTION_PATH)
        observed = sha256_file(package / _FIXED_CORRUPTION_PATH)
        return expected, observed

    def _fm15_source(
        self,
        intent: CheckpointAcceptanceIntent,
        run_id: UUID,
        exact_ref: str,
        *,
        manifest_sha256: str,
        expected_content_sha256: str,
    ) -> bytes:
        prefix = (
            f"# fixed FM15 isolated restore-smoke fixture\n"
            "import os as _mdh_os\n"
            "_mdh_os.environ.update({\n"
            f"    'MY_DATA_HUB_FM15_TASK_RUN_ID': {str(run_id)!r},\n"
            f"    'MY_DATA_HUB_FM15_CANDIDATE_ID': {str(intent.candidate_checkpoint_id)!r},\n"
            f"    'MY_DATA_HUB_FM15_EXACT_DATASET': {exact_ref!r},\n"
            f"    'MY_DATA_HUB_FM15_SOURCE_REVISION': {intent.source_revision!r},\n"
            f"    'MY_DATA_HUB_FM15_MANIFEST_SHA256': {manifest_sha256!r},\n"
            f"    'MY_DATA_HUB_FM15_EXPECTED_CONTENT_SHA256': {expected_content_sha256!r},\n"
            f"    'MY_DATA_HUB_FM15_RECEIPT_NAME': {_FM15_RECEIPT_NAME!r},\n"
            "})\n"
        ).encode()
        # The owner-reviewed verifier performs its normal package/restore setup;
        # the fixed terminal raise is appended here and cannot be caller-selected.
        return (
            prefix + self.binding.verifier_source + b"\nraise RuntimeError('MY_DATA_HUB_FIXED_FM15_RESTORE_FAILURE')\n"
        )

    @staticmethod
    def _load_fm15_failure_receipt(
        path: Path,
        *,
        intent: CheckpointAcceptanceIntent,
        run_id: UUID,
        exact_ref: str,
        manifest_sha256: str,
        expected_content_sha256: str,
    ) -> dict[str, object]:
        if not path.is_file() or path.is_symlink() or path.stat().st_size > 64 * 1024:
            raise CheckpointAcceptanceError("FM15 failed verifier lacks its bounded exact receipt")
        try:
            value = json.loads(path.read_bytes())
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CheckpointAcceptanceError("FM15 failed verifier receipt is invalid JSON") from exc
        expected = {
            "schema_version": "my-data-hub-checkpoint-acceptance-fm15-failure.v1",
            "task_run_id": str(run_id),
            "candidate_checkpoint_id": str(intent.candidate_checkpoint_id),
            "exact_version_ref": exact_ref,
            "source_revision": intent.source_revision,
            "manifest_sha256": manifest_sha256,
            "expected_content_sha256": expected_content_sha256,
            "detail_code": "FORCED_DISPOSABLE_RESTORE_FAILURE",
            "failure_code": "MY_DATA_HUB_FIXED_FM15_RESTORE_FAILURE",
            "restore_ok": False,
        }
        if not isinstance(value, dict) or value != expected:
            raise CheckpointAcceptanceError("FM15 failed verifier receipt differs from the fixed identity")
        return value

    def _stage(
        self,
        intent: CheckpointAcceptanceIntent,
        stage: str,
        detail: str,
        *,
        disposable: bool,
        provider: object | None = None,
        manifest: CheckpointManifest | None = None,
        package_sha: str | None = None,
        exact_ref: str | None = None,
        outcome: Literal["succeeded", "rejected_expected"] = "succeeded",
        expected: str | None = None,
        observed: str | None = None,
        canonical_revision: int | None = None,
        canonical_row_count: int | None = None,
    ) -> CheckpointAcceptanceStageReceipt:
        return CheckpointAcceptanceStageReceipt(
            stage=stage,
            candidate_checkpoint_id=intent.candidate_checkpoint_id,
            task_owned=True,
            disposable_candidate=disposable,
            outcome=outcome,
            detail_code=detail,
            provider_receipt_sha256=_metadata_sha256(
                provider
                if provider is not None
                else {
                    "stage": stage,
                    "candidate_checkpoint_id": str(intent.candidate_checkpoint_id),
                    "manifest_sha256": manifest.manifest_sha256 if manifest else None,
                    "package_sha256": package_sha,
                }
            ),
            manifest_sha256=manifest.manifest_sha256 if manifest else None,
            package_sha256=package_sha,
            expected_content_sha256=expected,
            observed_content_sha256=observed,
            exact_version_ref=exact_ref,
            canonical_revision=canonical_revision,
            canonical_row_count=canonical_row_count,
        )

    def _remaining(self) -> float:
        now = self.clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise CheckpointAcceptanceError("acceptance runtime clock must be timezone-aware")
        remaining = (self.binding.deadline_at - now.astimezone(UTC)).total_seconds()
        if remaining > ACCEPTANCE_TIMEOUT_SECONDS:
            raise CheckpointAcceptanceError("checkpoint acceptance start time is in the future")
        if remaining <= 0:
            raise CheckpointAcceptanceError("checkpoint acceptance absolute 900-second deadline expired")
        return remaining

    def _poll_policy(self) -> PollPolicy:
        remaining = self._remaining()
        return PollPolicy(
            interval_seconds=min(15.0, max(0.0, remaining / 30)),
            timeout_seconds=max(0.001, min(remaining, 600.0)),
            max_polls=120,
        )

    def _provider_call(self, call: Callable[[], Any]) -> Any:
        """Cap adapter retry sleeps to the remaining absolute operation budget."""
        remaining = self._remaining()
        with self._retry_budget(remaining):
            result = call()
        self._remaining()
        return result

    @contextmanager
    def _retry_budget(self, remaining: float) -> Iterator[None]:
        original = self.adapter.retry.policy
        self.adapter.retry.policy = original.model_copy(
            update={"max_elapsed_seconds": max(0.001, min(original.max_elapsed_seconds, remaining))}
        )
        try:
            yield
        finally:
            self.adapter.retry.policy = original
