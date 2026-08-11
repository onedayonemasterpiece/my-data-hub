#!/usr/bin/env python3
# ruff: noqa: E501
"""Disposable live proof for the brokered Kaggle Dataset upload path.

The process running this module is the credentialed *central* process.  It
constructs the repository's single :class:`KaggleProviderAdapter` and never
creates a second Kaggle client.  The producer Notebook receives only one
short-lived signed PUT capability; it has no Kaggle credential.  Payload bytes
flow Notebook -> Kaggle blob storage and never through the devstand.

This command is deliberately not a simulator.  Unit tests may inject a test
adapter into :class:`BrokerLiveCanary`, but such a run is stamped SIMULATED and
the receipt model forbids it from claiming a live PASS.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import subprocess
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, Protocol
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from pydantic import BaseModel, ConfigDict, Field, model_validator

from my_data_hub.control_plane.ledger import ControlLedger
from my_data_hub.hashing import canonical_json_bytes, sha256_value
from my_data_hub.providers.inventory import InventoryPage
from my_data_hub.providers.kaggle.adapter import KaggleProviderAdapter
from my_data_hub.providers.kaggle.contracts import (
    BrokeredBlobGrant,
    BrokeredDatasetFile,
    EffectOutcome,
    KaggleDatasetIdentity,
    MutationAction,
    PollPolicy,
    ProviderEffectIntent,
    ProviderEffectJournal,
    ProviderEffectReceipt,
    TaskResourceClaim,
)
from my_data_hub.providers.kaggle.control_journal import ControlLedgerKaggleJournal
from my_data_hub.providers.kaggle.credentials import kaggle_credentials_configured
from my_data_hub.providers.models import ControlClass, ProviderKind

EXTERNAL_BLOCKED = 78
PAYLOAD_FILE = "broker-canary.bin"
PRODUCER_RESULT = "broker-producer-result.json"
VERIFIER_RESULT = "broker-verifier-result.json"
RUN_RECEIPT = "my-data-hub-run-receipt.json"
DEFAULT_PAYLOAD_BYTES = 4096
_SHA256 = re.compile(r"^[a-f0-9]{64}$")


class BrokerCanaryError(RuntimeError):
    """Fail-closed live canary contract error."""


class NotebookEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    provider_ref: str = Field(pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
    provider_run_ref: str = Field(pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[1-9][0-9]*$")
    provider_kernel_id: int = Field(ge=1)
    source_version: int = Field(ge=1)
    source_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    output_tree_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    output_receipt_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    result_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    credential_free: Literal[True]


class DatasetEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    provider_ref: str = Field(pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
    exact_version_ref: str = Field(pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[1-9][0-9]*$")
    provider_version: int = Field(ge=1)
    privacy: Literal["private"]
    file_name: Literal["broker-canary.bin"]
    byte_size: int = Field(ge=1, le=1024 * 1024)
    file_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    manifest_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    metadata_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    claim_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")


class CleanupEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: Literal["dataset", "notebook"]
    provider_ref: str = Field(pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
    claim_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    outcome: Literal["applied", "already_applied"]
    detail_code: Literal["task_created_resource_absent", "task_created_resource_already_absent"]
    receipt_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")


class BrokerLiveCanaryReceipt(BaseModel):
    """Public receipt.  Signed URLs and opaque blob tokens have no fields here."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["my-data-hub-broker-live-canary-receipt.v1"]
    evidence_origin: Literal["observed", "test", "example"]
    execution_mode: Literal["live", "test", "not_run"]
    outcome: Literal["PASS", "SIMULATED", "NOT_RUN"]
    live_provider_mutations: bool
    canary_id: UUID
    task_id: UUID
    commit_sha: str = Field(pattern=r"^[a-f0-9]{40}$")
    broker_operation_id: UUID
    epoch: int = Field(ge=1)
    producer: NotebookEvidence
    dataset: DatasetEvidence
    verifier: NotebookEvidence
    cleanup: tuple[CleanupEvidence, CleanupEvidence, CleanupEvidence]
    inventory_absent: Literal[True]
    inventory_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    data_path: Literal["credential_free_notebook_to_signed_kaggle_blob"]
    devstand_checkpoint_bytes: Literal[0]
    central_kaggle_adapter_instances: Literal[1]
    secret_scan_passed: Literal[True]
    started_at: datetime
    completed_at: datetime

    @model_validator(mode="after")
    def truthfully_labels_live_evidence(self) -> BrokerLiveCanaryReceipt:
        live = self.evidence_origin == "observed" and self.execution_mode == "live"
        if (self.outcome == "PASS") != (live and self.live_provider_mutations):
            raise ValueError("only observed live provider mutations may claim PASS")
        if self.outcome == "SIMULATED" and not (
            self.evidence_origin == "test" and self.execution_mode == "test" and not self.live_provider_mutations
        ):
            raise ValueError("SIMULATED receipts must be non-live test evidence")
        if self.outcome == "NOT_RUN" and not (
            self.evidence_origin == "example" and self.execution_mode == "not_run" and not self.live_provider_mutations
        ):
            raise ValueError("NOT_RUN receipts must be example-only evidence")
        if self.completed_at < self.started_at:
            raise ValueError("canary completion precedes its start")
        refs = {item.provider_ref for item in self.cleanup}
        if refs != {self.producer.provider_ref, self.dataset.provider_ref, self.verifier.provider_ref}:
            raise ValueError("cleanup does not cover the Dataset and both Notebooks")
        if self.dataset.exact_version_ref != f"{self.dataset.provider_ref}/{self.dataset.provider_version}":
            raise ValueError("Dataset evidence is not exact-version bound")
        return self


class CanaryAdapter(Protocol):
    """The exact subset supplied by the one central KaggleProviderAdapter."""

    def provider_identity(self) -> Any: ...

    def start_brokered_dataset_blob(self, **kwargs: Any) -> BrokeredBlobGrant: ...

    def finalize_brokered_checkpoint_dataset(self, **kwargs: Any) -> int: ...

    def reconcile_brokered_checkpoint_dataset(self, **kwargs: Any) -> bool: ...

    def push_private_notebook(self, **kwargs: Any) -> Any: ...

    def poll_run(self, run: Any, policy: PollPolicy | None = None) -> Any: ...

    def read_exact_run_output(self, run: Any) -> Any: ...

    def download_exact_run_output_file(self, run: Any, **kwargs: Any) -> Any: ...

    def read_private_dataset(self, *, provider_ref: str, version: int) -> KaggleDatasetIdentity: ...

    def delete_task_created_resource(self, **kwargs: Any) -> ProviderEffectReceipt: ...

    def list_resources(self, *, kind: ProviderKind, cursor: str | None, limit: int) -> InventoryPage: ...


class AtomicCanaryState:
    """Small custom-state/status ledger modelled after proven Kaggle runners.

    Only resumable identities and hashes are stored.  The signed URL and blob
    token stay in process memory and are categorically rejected from state.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self.events: list[dict[str, Any]] = []

    def append(self, phase: str, **evidence: object) -> None:
        encoded_probe = canonical_json_bytes(evidence).decode("utf-8", errors="replace").casefold()
        if "create_url" in encoded_probe or "blob_token" in encoded_probe or "?x-goog-" in encoded_probe:
            raise BrokerCanaryError("secret-bearing capability cannot enter canary state")
        self.events.append(
            {
                "sequence": len(self.events) + 1,
                "phase": phase,
                "evidence": evidence,
                "observed_at": datetime.now(UTC).isoformat(),
            }
        )
        payload = canonical_json_bytes(
            {"schema_version": "my-data-hub-broker-live-canary-state.v1", "events": self.events}
        )
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_name(f".{self.path.name}.{uuid4()}.tmp")
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.path)
            os.chmod(self.path, 0o600)
        finally:
            if temporary.exists():
                temporary.unlink()


def _payload(canary_id: UUID, byte_size: int) -> bytes:
    seed = hashlib.sha256(f"my-data-hub:broker-live-canary:{canary_id}".encode()).digest()
    blocks = (byte_size + 31) // 32
    return b"".join(hashlib.sha256(seed + index.to_bytes(8, "big")).digest() for index in range(blocks))[:byte_size]


def _effect(
    *,
    canary_id: UUID,
    identity: str,
    action: MutationAction,
    ref: str,
    task_id: UUID,
    arguments: dict[str, object],
    expected: Any = None,
    requested_at: datetime,
) -> ProviderEffectIntent:
    effect_id = uuid5(NAMESPACE_URL, f"broker-live-canary:{canary_id}:{identity}:{action.value}:effect")
    return ProviderEffectIntent.create(
        operation_id=uuid5(NAMESPACE_URL, f"broker-live-canary:{canary_id}:{identity}:operation"),
        effect_id=effect_id,
        idempotency_key=f"broker-live-canary:{canary_id}:{identity}:{action.value}",
        task_id=task_id,
        action=action,
        provider_ref=ref,
        arguments=arguments,
        expected_fingerprint=expected,
        requested_at=requested_at,
    )


def build_producer_source(
    *, canary_id: UUID, task_run_id: UUID, provider_ref: str, create_url: str, byte_size: int
) -> bytes:
    """Build a credential-free Notebook that performs the direct signed PUT."""

    return f"""from __future__ import annotations
import hashlib, http.client, json, os
from pathlib import Path
from urllib.parse import urlsplit

TASK_RUN_ID = {str(task_run_id)!r}
CANARY_ID = {str(canary_id)!r}
PROVIDER_REF = {provider_ref!r}
SOURCE_VERSION = 1
CREATE_URL = {create_url!r}
BYTE_SIZE = {byte_size}

credential_env = sorted(key for key in ("KAGGLE_USERNAME", "KAGGLE_KEY", "KAGGLE_API_TOKEN") if os.environ.get(key))
credential_files = [str(path) for path in (Path.home()/".kaggle"/"kaggle.json", Path.home()/".kaggle"/"access_token") if path.exists()]
if credential_env or credential_files:
    raise RuntimeError("credential-free broker canary found a Kaggle credential")
seed = hashlib.sha256(f"my-data-hub:broker-live-canary:{{CANARY_ID}}".encode()).digest()
blocks = (BYTE_SIZE + 31) // 32
payload = b"".join(hashlib.sha256(seed + i.to_bytes(8, "big")).digest() for i in range(blocks))[:BYTE_SIZE]
payload_sha256 = hashlib.sha256(payload).hexdigest()
parsed = urlsplit(CREATE_URL)
if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password or parsed.fragment or parsed.port not in (None, 443):
    raise RuntimeError("signed PUT capability is invalid")
target = parsed.path or "/"
if parsed.query:
    target += "?" + parsed.query
connection = http.client.HTTPSConnection(parsed.hostname, parsed.port or 443, timeout=60)
try:
    connection.request("PUT", target, body=payload, headers={{"Content-Length": str(BYTE_SIZE), "Content-Type": "application/octet-stream"}})
    response = connection.getresponse()
    response.read(4097)
    if response.status < 200 or response.status >= 300:
        raise RuntimeError("direct provider blob upload was rejected")
finally:
    connection.close()
source_sha256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
run_receipt = {{"schema_version":"my-data-hub-run-receipt.v1","task_run_id":TASK_RUN_ID,"provider_ref":PROVIDER_REF,"source_version":SOURCE_VERSION,"source_sha256":source_sha256,"terminal_state":"complete"}}
result = {{"schema_version":"my-data-hub-broker-producer-result.v1","canary_id":CANARY_ID,"task_run_id":TASK_RUN_ID,"byte_size":BYTE_SIZE,"file_sha256":payload_sha256,"direct_put":True,"credential_env_present":credential_env,"credential_files_present":credential_files}}
Path("/kaggle/working/{RUN_RECEIPT}").write_text(json.dumps(run_receipt, sort_keys=True, separators=(",",":")), encoding="utf-8")
Path("/kaggle/working/{PRODUCER_RESULT}").write_text(json.dumps(result, sort_keys=True, separators=(",",":")), encoding="utf-8")
""".encode()


def build_verifier_source(
    *,
    canary_id: UUID,
    task_run_id: UUID,
    provider_ref: str,
    exact_version_ref: str,
    byte_size: int,
    file_sha256: str,
) -> bytes:
    """Build an independent verifier bound to one numeric Dataset version."""

    return f'''from __future__ import annotations
import hashlib, json, os
from pathlib import Path

TASK_RUN_ID = {str(task_run_id)!r}
CANARY_ID = {str(canary_id)!r}
PROVIDER_REF = {provider_ref!r}
SOURCE_VERSION = 1
EXACT_VERSION_REF = {exact_version_ref!r}
EXPECTED_BYTES = {byte_size}
EXPECTED_SHA256 = {file_sha256!r}
credential_env = sorted(key for key in ("KAGGLE_USERNAME", "KAGGLE_KEY", "KAGGLE_API_TOKEN") if os.environ.get(key))
credential_files = [str(path) for path in (Path.home()/".kaggle"/"kaggle.json", Path.home()/".kaggle"/"access_token") if path.exists()]
if credential_env or credential_files:
    raise RuntimeError("credential-free verifier found a Kaggle credential")
matches = sorted(Path("/kaggle/input").rglob("{PAYLOAD_FILE}"))
if len(matches) != 1 or matches[0].is_symlink() or not matches[0].is_file():
    raise RuntimeError("exact Dataset input did not expose one regular canary file")
payload = matches[0].read_bytes()
observed_sha256 = hashlib.sha256(payload).hexdigest()
if len(payload) != EXPECTED_BYTES or observed_sha256 != EXPECTED_SHA256:
    raise RuntimeError("exact-version canary payload differs from broker metadata")
source_sha256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
run_receipt = {{"schema_version":"my-data-hub-run-receipt.v1","task_run_id":TASK_RUN_ID,"provider_ref":PROVIDER_REF,"source_version":SOURCE_VERSION,"source_sha256":source_sha256,"terminal_state":"complete"}}
result = {{"schema_version":"my-data-hub-broker-verifier-result.v1","canary_id":CANARY_ID,"task_run_id":TASK_RUN_ID,"exact_version_ref":EXACT_VERSION_REF,"byte_size":len(payload),"file_sha256":observed_sha256,"credential_env_present":credential_env,"credential_files_present":credential_files}}
Path("/kaggle/working/{RUN_RECEIPT}").write_text(json.dumps(run_receipt, sort_keys=True, separators=(",",":")), encoding="utf-8")
Path("/kaggle/working/{VERIFIER_RESULT}").write_text(json.dumps(result, sort_keys=True, separators=(",",":")), encoding="utf-8")
'''.encode()


class BrokerLiveCanary:
    def __init__(
        self,
        *,
        adapter: CanaryAdapter,
        journal: ProviderEffectJournal,
        state: AtomicCanaryState,
        commit_sha: str,
        canary_id: UUID,
        payload_bytes: int = DEFAULT_PAYLOAD_BYTES,
        live_provider: bool,
        clock: Any = lambda: datetime.now(UTC),
    ) -> None:
        if not _SHA256.fullmatch("0" * 64) or not re.fullmatch(r"^[a-f0-9]{40}$", commit_sha):
            raise ValueError("canary requires an exact source commit")
        if not 1024 <= payload_bytes <= 1024 * 1024:
            raise ValueError("canary payload size is outside its disposable bound")
        self.adapter = adapter
        self.journal = journal
        self.state = state
        self.commit_sha = commit_sha
        self.canary_id = canary_id
        self.task_id = uuid5(NAMESPACE_URL, f"broker-live-canary:{canary_id}:task")
        self.payload_bytes = payload_bytes
        self.live_provider = live_provider
        self.clock = clock

    def _read_result(self, result: Any, file_name: str) -> tuple[dict[str, Any], str, Any]:
        output = self.adapter.read_exact_run_output(result.run)
        with tempfile.TemporaryDirectory(prefix="mdh-broker-canary-output-") as temporary:
            destination = Path(temporary)
            self.adapter.download_exact_run_output_file(
                result.run, destination=destination, file_name=file_name, max_bytes=64 * 1024
            )
            body = (destination / file_name).read_bytes()
        try:
            value = json.loads(body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise BrokerCanaryError("canary Notebook result is not bounded JSON") from exc
        if not isinstance(value, dict):
            raise BrokerCanaryError("canary Notebook result must be an object")
        return value, hashlib.sha256(body).hexdigest(), output

    def _push_notebook(
        self,
        *,
        identity: str,
        ref: str,
        task_run_id: UUID,
        source: bytes,
        dataset_sources: tuple[str, ...],
        internet: bool,
        requested_at: datetime,
    ) -> Any:
        source_sha = hashlib.sha256(source).hexdigest()
        intent = _effect(
            canary_id=self.canary_id,
            identity=identity,
            action=MutationAction.PUSH_NOTEBOOK,
            ref=ref,
            task_id=self.task_id,
            arguments={
                "task_run_id": str(task_run_id),
                "source_sha256": source_sha,
                "dataset_sources": dataset_sources,
                "control_class": ControlClass.MCP_MANAGED.value,
                "disposable": True,
            },
            requested_at=requested_at,
        )
        return self.adapter.push_private_notebook(
            intent=intent,
            task_run_id=task_run_id,
            source=source,
            title=ref.split("/", 1)[1],
            code_file="run.py",
            kernel_type="script",
            language="python",
            control_class=ControlClass.MCP_MANAGED,
            disposable=True,
            dataset_sources=dataset_sources,
            enable_internet=internet,
            timeout_seconds=900,
        )

    def _register_dataset_claim(
        self, *, ref: str, version: int, requested_at: datetime
    ) -> tuple[KaggleDatasetIdentity, TaskResourceClaim]:
        # This downloads only the bounded disposable canary file in the central
        # process to obtain the adapter's exact cleanup fingerprint. Production
        # checkpoint archives never take this path.
        identity = self.adapter.read_private_dataset(provider_ref=ref, version=version)
        intent = _effect(
            canary_id=self.canary_id,
            identity="broker-dataset",
            action=MutationAction.CREATE_DATASET,
            ref=ref,
            task_id=self.task_id,
            arguments={"brokered_canary_exact_version": version},
            requested_at=requested_at,
        )
        receipt = ProviderEffectReceipt(
            operation_id=intent.operation_id,
            effect_id=intent.effect_id,
            action=MutationAction.CREATE_DATASET,
            provider_ref=ref,
            outcome=EffectOutcome.APPLIED,
            attempts=1,
            observed_fingerprint=identity.fingerprint,
            provider_version=version,
            observed_at=self.clock(),
            detail_code="brokered_private_dataset_exact_readback",
        )
        claim = TaskResourceClaim.create(
            task_id=self.task_id,
            effect_id=intent.effect_id,
            provider_ref=ref,
            kind=ProviderKind.DATASET,
            control_class=ControlClass.MCP_MANAGED,
            disposable=True,
            fingerprint=identity.fingerprint,
            provider_version=version,
            registered_at=requested_at,
        )
        self.journal.persist_intent(intent)
        self.journal.persist_receipt(receipt)
        self.journal.persist_resource_claim(claim)
        return identity, claim

    def _cleanup(self, *, identity: str, claim: TaskResourceClaim, requested_at: datetime) -> CleanupEvidence:
        action = MutationAction.DELETE_DATASET if claim.kind is ProviderKind.DATASET else MutationAction.DELETE_NOTEBOOK
        intent = _effect(
            canary_id=self.canary_id,
            identity=f"cleanup:{identity}",
            action=action,
            ref=claim.provider_ref,
            task_id=self.task_id,
            arguments={"claim_sha256": claim.claim_sha256, "provider_version": claim.provider_version},
            expected=claim.fingerprint,
            requested_at=requested_at,
        )
        receipt = self.adapter.delete_task_created_resource(intent=intent, claim=claim)
        return CleanupEvidence(
            kind=claim.kind.value,
            provider_ref=claim.provider_ref,
            claim_sha256=claim.claim_sha256,
            outcome=receipt.outcome.value,
            detail_code=receipt.detail_code,
            receipt_sha256=hashlib.sha256(canonical_json_bytes(receipt.model_dump(mode="json"))).hexdigest(),
        )

    def _prove_inventory_absent(self, expected: dict[ProviderKind, set[str]]) -> str:
        observed: list[dict[str, str]] = []
        for kind in (ProviderKind.DATASET, ProviderKind.NOTEBOOK):
            cursor: str | None = None
            seen: set[str] = set()
            for _ in range(20):
                page = self.adapter.list_resources(kind=kind, cursor=cursor, limit=100)
                for resource in page.resources:
                    if resource.provider_ref in expected[kind]:
                        raise BrokerCanaryError("cleaned canary resource remains in owned inventory")
                    observed.append({"kind": kind.value, "provider_ref": resource.provider_ref})
                if page.next_cursor is None:
                    break
                if page.next_cursor in seen:
                    raise BrokerCanaryError("Kaggle inventory repeated a cursor")
                seen.add(page.next_cursor)
                cursor = page.next_cursor
            else:
                raise BrokerCanaryError("Kaggle inventory exceeded its bounded page count")
        return sha256_value(
            {"absent": {key.value: sorted(value) for key, value in expected.items()}, "inventory": observed}
        )

    def run(self) -> BrokerLiveCanaryReceipt:
        started_at = self.clock()
        requested_at = started_at
        owner = str(self.adapter.provider_identity().username)
        suffix = self.canary_id.hex[:12]
        dataset_ref = f"{owner}/mdh-broker-canary-{suffix}"
        producer_ref = f"{owner}/mdh-broker-producer-{suffix}"
        verifier_ref = f"{owner}/mdh-broker-verifier-{suffix}"
        producer_run_id = uuid5(NAMESPACE_URL, f"broker-live-canary:{self.canary_id}:producer-run")
        verifier_run_id = uuid5(NAMESPACE_URL, f"broker-live-canary:{self.canary_id}:verifier-run")
        broker_operation_id = uuid5(NAMESPACE_URL, f"broker-live-canary:{self.canary_id}:broker-operation")
        epoch = 1
        payload = _payload(self.canary_id, self.payload_bytes)
        file_sha = hashlib.sha256(payload).hexdigest()
        manifest_sha = sha256_value(
            {
                "schema_version": "my-data-hub-broker-live-canary-manifest.v1",
                "canary_id": str(self.canary_id),
                "file_name": PAYLOAD_FILE,
                "byte_size": len(payload),
                "file_sha256": file_sha,
            }
        )
        self.state.append(
            "PLANNED", canary_id=str(self.canary_id), dataset_ref=dataset_ref, manifest_sha256=manifest_sha
        )

        claims: list[tuple[str, TaskResourceClaim]] = []
        cleanup: list[CleanupEvidence] = []
        producer: Any = None
        verifier: Any = None
        dataset_identity: KaggleDatasetIdentity | None = None
        dataset_claim: TaskResourceClaim | None = None
        finalized_version: int | None = None
        expected_files: tuple[tuple[str, int, str], ...] | None = None
        producer_evidence: NotebookEvidence | None = None
        verifier_evidence: NotebookEvidence | None = None
        metadata_sha = ""
        primary_error: BaseException | None = None
        try:
            grant = self.adapter.start_brokered_dataset_blob(
                file_name=PAYLOAD_FILE,
                content_length=len(payload),
                content_type="application/octet-stream",
                last_modified_epoch_seconds=int(started_at.timestamp()),
            )
            self.state.append("BLOB_GRANTED", file_name=PAYLOAD_FILE, byte_size=len(payload), file_sha256=file_sha)
            producer_source = build_producer_source(
                canary_id=self.canary_id,
                task_run_id=producer_run_id,
                provider_ref=producer_ref,
                create_url=grant.create_url,
                byte_size=len(payload),
            )
            producer = self._push_notebook(
                identity="producer",
                ref=producer_ref,
                task_run_id=producer_run_id,
                source=producer_source,
                dataset_sources=(),
                internet=True,
                requested_at=requested_at,
            )
            claims.append(("producer", producer.claim))
            self.state.append("PRODUCER_LAUNCHED", provider_run_ref=producer.run.provider_run_ref)
            self.adapter.poll_run(producer.run, PollPolicy(interval_seconds=15, timeout_seconds=900, max_polls=60))
            producer_result, producer_result_sha, producer_output = self._read_result(producer, PRODUCER_RESULT)
            if producer_result != {
                "schema_version": "my-data-hub-broker-producer-result.v1",
                "canary_id": str(self.canary_id),
                "task_run_id": str(producer_run_id),
                "byte_size": len(payload),
                "file_sha256": file_sha,
                "direct_put": True,
                "credential_env_present": [],
                "credential_files_present": [],
            }:
                raise BrokerCanaryError("producer output differs from the direct credential-free PUT contract")
            producer_evidence = NotebookEvidence(
                provider_ref=producer_ref,
                provider_run_ref=producer.run.provider_run_ref,
                provider_kernel_id=producer.run.provider_kernel_id,
                source_version=producer.run.source_version,
                source_sha256=producer.run.source_sha256,
                output_tree_sha256=producer_output.output_tree_sha256,
                output_receipt_sha256=producer_output.receipt_sha256,
                result_sha256=producer_result_sha,
                credential_free=True,
            )
            self.state.append("BLOB_UPLOADED", producer_result_sha256=producer_result_sha)

            description = canonical_json_bytes(
                {
                    "operation_id": str(broker_operation_id),
                    "master_run_ref": producer.run.provider_run_ref,
                    "epoch": epoch,
                    "manifest_sha256": manifest_sha,
                    "file_sha256": file_sha,
                    "total_bytes": len(payload),
                }
            ).decode("utf-8")
            metadata_sha = hashlib.sha256(description.encode()).hexdigest()
            brokered_file = BrokeredDatasetFile(
                name=PAYLOAD_FILE,
                total_bytes=len(payload),
                description=description,
                blob_token=grant.blob_token,
            )
            version = self.adapter.finalize_brokered_checkpoint_dataset(
                provider_ref=dataset_ref,
                title=dataset_ref.split("/", 1)[1],
                files=(brokered_file,),
                version_notes=f"disposable broker canary {self.canary_id}",
                expected_previous_version=None,
            )
            finalized_version = version
            expected_files = ((PAYLOAD_FILE, len(payload), description),)
            if version != 1 or not self.adapter.reconcile_brokered_checkpoint_dataset(
                provider_ref=dataset_ref, version=version, expected_files=expected_files
            ):
                raise BrokerCanaryError("central finalizer did not resolve exact private Dataset version 1")
            dataset_identity, dataset_claim = self._register_dataset_claim(
                ref=dataset_ref, version=version, requested_at=requested_at
            )
            claims.append(("dataset", dataset_claim))
            exact_version_ref = f"{dataset_ref}/{version}"
            self.state.append("DATASET_FINALIZED", exact_version_ref=exact_version_ref, metadata_sha256=metadata_sha)

            verifier_source = build_verifier_source(
                canary_id=self.canary_id,
                task_run_id=verifier_run_id,
                provider_ref=verifier_ref,
                exact_version_ref=exact_version_ref,
                byte_size=len(payload),
                file_sha256=file_sha,
            )
            verifier = self._push_notebook(
                identity="verifier",
                ref=verifier_ref,
                task_run_id=verifier_run_id,
                source=verifier_source,
                dataset_sources=(exact_version_ref,),
                internet=False,
                requested_at=requested_at,
            )
            claims.append(("verifier", verifier.claim))
            self.state.append(
                "VERIFIER_LAUNCHED", provider_run_ref=verifier.run.provider_run_ref, exact_version_ref=exact_version_ref
            )
            self.adapter.poll_run(verifier.run, PollPolicy(interval_seconds=15, timeout_seconds=900, max_polls=60))
            verifier_result, verifier_result_sha, verifier_output = self._read_result(verifier, VERIFIER_RESULT)
            if verifier_result != {
                "schema_version": "my-data-hub-broker-verifier-result.v1",
                "canary_id": str(self.canary_id),
                "task_run_id": str(verifier_run_id),
                "exact_version_ref": exact_version_ref,
                "byte_size": len(payload),
                "file_sha256": file_sha,
                "credential_env_present": [],
                "credential_files_present": [],
            }:
                raise BrokerCanaryError("independent exact-version verifier output differs from broker metadata")
            verifier_evidence = NotebookEvidence(
                provider_ref=verifier_ref,
                provider_run_ref=verifier.run.provider_run_ref,
                provider_kernel_id=verifier.run.provider_kernel_id,
                source_version=verifier.run.source_version,
                source_sha256=verifier.run.source_sha256,
                output_tree_sha256=verifier_output.output_tree_sha256,
                output_receipt_sha256=verifier_output.receipt_sha256,
                result_sha256=verifier_result_sha,
                credential_free=True,
            )
            self.state.append("VERIFIED", verifier_result_sha256=verifier_result_sha)
        except BaseException as exc:
            primary_error = exc
        finally:
            # If finalization committed but the first bounded readback failed,
            # retry the exact numeric read once before cleanup.  Never guess a
            # fingerprint and never fall back to an unclaimed slug delete.
            if finalized_version is not None and dataset_claim is None:
                try:
                    dataset_identity, dataset_claim = self._register_dataset_claim(
                        ref=dataset_ref, version=finalized_version, requested_at=requested_at
                    )
                    claims.append(("dataset", dataset_claim))
                except BaseException as claim_error:
                    if primary_error is None:
                        primary_error = claim_error
            try:
                self.state.append("CLEANUP_STARTED", resource_count=len(claims))
            except BaseException as state_error:
                if primary_error is None:
                    primary_error = state_error
            for identity, claim in reversed(claims):
                try:
                    cleanup.append(self._cleanup(identity=identity, claim=claim, requested_at=requested_at))
                except BaseException as cleanup_error:
                    if primary_error is None:
                        primary_error = cleanup_error
            try:
                self.state.append("CLEANUP_FINISHED", cleaned_refs=sorted(item.provider_ref for item in cleanup))
            except BaseException as state_error:
                if primary_error is None:
                    primary_error = state_error
        if primary_error is not None:
            raise BrokerCanaryError(
                "broker live canary failed; inspect secret-free state and provider journal"
            ) from primary_error
        if not all(
            (producer, verifier, dataset_identity, dataset_claim, producer_evidence, verifier_evidence, expected_files)
        ):
            raise BrokerCanaryError("broker live canary lacks exact terminal evidence")
        if len(cleanup) != 3:
            raise BrokerCanaryError("broker live canary did not clean all three task-created resources")
        inventory_sha = self._prove_inventory_absent(
            {
                ProviderKind.DATASET: {dataset_ref},
                ProviderKind.NOTEBOOK: {producer_ref, verifier_ref},
            }
        )
        dataset_evidence = DatasetEvidence(
            provider_ref=dataset_ref,
            exact_version_ref=f"{dataset_ref}/{dataset_identity.version}",
            provider_version=dataset_identity.version,
            privacy=dataset_identity.privacy,
            file_name=PAYLOAD_FILE,
            byte_size=len(payload),
            file_sha256=file_sha,
            manifest_sha256=manifest_sha,
            metadata_sha256=metadata_sha,
            claim_sha256=dataset_claim.claim_sha256,
        )
        receipt = BrokerLiveCanaryReceipt(
            schema_version="my-data-hub-broker-live-canary-receipt.v1",
            evidence_origin="observed" if self.live_provider else "test",
            execution_mode="live" if self.live_provider else "test",
            outcome="PASS" if self.live_provider else "SIMULATED",
            live_provider_mutations=self.live_provider,
            canary_id=self.canary_id,
            task_id=self.task_id,
            commit_sha=self.commit_sha,
            broker_operation_id=broker_operation_id,
            epoch=epoch,
            producer=producer_evidence,
            dataset=dataset_evidence,
            verifier=verifier_evidence,
            cleanup=tuple(cleanup),
            inventory_absent=True,
            inventory_sha256=inventory_sha,
            data_path="credential_free_notebook_to_signed_kaggle_blob",
            devstand_checkpoint_bytes=0,
            central_kaggle_adapter_instances=1,
            secret_scan_passed=True,
            started_at=started_at,
            completed_at=self.clock(),
        )
        encoded = canonical_json_bytes(receipt.model_dump(mode="json"))
        secret_values = (grant.create_url, grant.blob_token)
        if (
            any(value.encode() in encoded for value in secret_values)
            or b"create_url" in encoded
            or b"blob_token" in encoded
        ):
            raise BrokerCanaryError("public canary receipt retained a signed URL or blob token")
        self.state.append(
            "COMPLETE", receipt_sha256=hashlib.sha256(encoded).hexdigest(), inventory_sha256=inventory_sha
        )
        return receipt


def _clean_commit(root: Path) -> str:
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    dirty = subprocess.check_output(["git", "status", "--porcelain=v1", "-z"], cwd=root, text=False)
    if not re.fullmatch(r"^[a-f0-9]{40}$", commit) or dirty:
        raise BrokerCanaryError("live provider mutation requires one clean exact source commit")
    return commit


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the disposable live brokered-upload Kaggle canary")
    parser.add_argument("--ledger", type=Path, required=True, help="private local control-ledger SQLite path")
    parser.add_argument("--state", type=Path, required=True, help="secret-free atomic custom-state JSON path")
    parser.add_argument("--receipt", type=Path, required=True, help="terminal public receipt path")
    parser.add_argument("--canary-id", type=UUID, default=None)
    parser.add_argument("--payload-bytes", type=int, default=DEFAULT_PAYLOAD_BYTES)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not kaggle_credentials_configured():
        print(json.dumps({"outcome": "BLOCKED", "blocker_code": "KAGGLE_CREDENTIAL_REQUIRED", "mutations_started": 0}))
        return EXTERNAL_BLOCKED
    root = Path(__file__).resolve().parents[2]
    commit = _clean_commit(root)
    ledger = ControlLedger(args.ledger.expanduser().resolve())
    journal = ControlLedgerKaggleJournal(ledger)
    # The sole construction site for the sole credentialed Kaggle client.
    adapter = KaggleProviderAdapter.from_environment(journal=journal)
    receipt = BrokerLiveCanary(
        adapter=adapter,
        journal=journal,
        state=AtomicCanaryState(args.state.expanduser().resolve()),
        commit_sha=commit,
        canary_id=args.canary_id or uuid4(),
        payload_bytes=args.payload_bytes,
        live_provider=True,
    ).run()
    encoded = canonical_json_bytes(receipt.model_dump(mode="json"))
    target = args.receipt.expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(encoded)
    os.chmod(target, stat.S_IRUSR | stat.S_IWUSR)
    print(json.dumps({"outcome": receipt.outcome, "receipt": str(target), "canary_id": str(receipt.canary_id)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
