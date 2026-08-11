from __future__ import annotations

import hashlib
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Final, Literal, Protocol
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from my_data_hub.control_plane.ledger import ControlLedger
from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.mcp.oauth import AccessIdentity
from my_data_hub.providers.kaggle.contracts import KaggleKernelRunIdentity, PollPolicy

ACCEPTANCE_EVIDENCE_SCENARIOS: Final = frozenset(
    {"FM01", "FM02", "FM03", "FM06", "FM22", "FM23"}
)


class _ProviderGateway(Protocol):
    adapter: Any

    def invoke(
        self, tool: str, arguments: Mapping[str, Any], principal: AccessIdentity
    ) -> dict[str, Any]: ...


class AcceptanceEvidenceError(RuntimeError):
    """A claimed acceptance mutation ended in a durable FAIL result."""


class DatasetLifecycleRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    scenario_id: Literal["FM01", "FM22"]
    task_id: UUID
    idempotency_key: str = Field(min_length=8, max_length=260)
    resource_ref: str = Field(pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", max_length=300)
    title: str = Field(min_length=6, max_length=50)
    file_name: str = Field(min_length=1, max_length=200)
    file_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    file_utf8: str = Field(max_length=65536)
    version_file_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    version_file_utf8: str = Field(max_length=65536)

    @field_validator("file_name")
    @classmethod
    def top_level_file(cls, value: str) -> str:
        if Path(value).name != value or value in {".", ".."} or "\\" in value:
            raise ValueError("acceptance dataset file must be one top-level path")
        return value

    @model_validator(mode="after")
    def exact_content_hashes(self) -> DatasetLifecycleRequest:
        if not 6 <= len(self.resource_ref.split("/", 1)[1]) <= 50:
            raise ValueError("acceptance dataset slug must contain 6 to 50 characters")
        if hashlib.sha256(self.file_utf8.encode()).hexdigest() != self.file_sha256:
            raise ValueError("acceptance dataset create content hash differs")
        if hashlib.sha256(self.version_file_utf8.encode()).hexdigest() != self.version_file_sha256:
            raise ValueError("acceptance dataset version content hash differs")
        return self


class NotebookLifecycleRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    scenario_id: Literal["FM01", "FM02", "FM03", "FM06", "FM22", "FM23"]
    task_id: UUID
    task_run_id: UUID
    idempotency_key: str = Field(min_length=8, max_length=260)
    resource_ref: str = Field(pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", max_length=300)
    title: str = Field(min_length=5, max_length=80)
    code_file: str = Field(min_length=1, max_length=200)
    source_utf8: str = Field(max_length=262144)
    dataset_sources: tuple[str, ...] = Field(default=(), max_length=20)
    output_file_name: str = Field(min_length=1, max_length=200)
    expected_output_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    max_output_bytes: int = Field(ge=1, le=1048576)

    @field_validator("code_file", "output_file_name")
    @classmethod
    def bounded_path(cls, value: str) -> str:
        if Path(value).name != value or value in {".", ".."} or "\\" in value:
            raise ValueError("acceptance notebook paths must be top-level basenames")
        return value

    @model_validator(mode="after")
    def exact_notebook_identity(self) -> NotebookLifecycleRequest:
        if self.title != self.resource_ref.split("/", 1)[1]:
            raise ValueError("acceptance notebook title must equal its exact provider slug")
        if str(self.task_run_id) not in self.source_utf8:
            raise ValueError("acceptance notebook source must embed its exact task_run_id")
        return self


class AcceptanceCleanupRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    scenario_id: Literal["FM01", "FM02", "FM03", "FM06", "FM22", "FM23"]
    task_id: UUID
    claim_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    provider_run_ref: str = Field(min_length=3, max_length=500)
    output_receipt_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    idempotency_key: str = Field(min_length=8, max_length=300)


class AcceptanceEvidenceController:
    """Scenario-specific provider acceptance with durable metadata-only evidence.

    One injected gateway owns the one Kaggle adapter. Task claims are committed
    before any provider call. Every stage uses deterministic effect identities,
    so a response-loss retry either reads a committed claim/receipt or asks the
    provider adapter to perform its exact reconciliation; it never issues a new
    logical mutation under a new identity.
    """

    def __init__(self, ledger: ControlLedger, provider_gateway: _ProviderGateway) -> None:
        self.ledger = ledger
        self.provider_gateway = provider_gateway

    def dataset_lifecycle(
        self, arguments: Mapping[str, Any], principal: AccessIdentity
    ) -> dict[str, Any]:
        request = DatasetLifecycleRequest.model_validate(self._without_permit(arguments))
        task = self._claim(request, principal)
        if task["state"] in {"SUCCEEDED", "FAILED"}:
            return self.claim_get(request.scenario_id, str(request.task_id))
        self.ledger.begin_acceptance_evidence_task(
            scenario_id=request.scenario_id, task_id=str(request.task_id)
        )
        try:
            recovered_cleanup = self._completed_cleanup_receipt(
                request.scenario_id, str(request.task_id)
            )
            if recovered_cleanup is not None:
                self.ledger.append_acceptance_evidence(
                    scenario_id=request.scenario_id,
                    task_id=str(request.task_id),
                    event_type="CLEANUP",
                    evidence=recovered_cleanup,
                )
                self.ledger.terminalize_acceptance_evidence_task(
                    scenario_id=request.scenario_id,
                    task_id=str(request.task_id),
                    state="SUCCEEDED",
                    evidence={"cleanup_state": "COMPLETE", "reconciled": True},
                )
                return self.claim_get(request.scenario_id, str(request.task_id))
            created = self._dataset_stage(
                request=request,
                principal=principal,
                stage="create",
                files={request.file_name: request.file_utf8},
                prior_claim=None,
            )
            versioned = self._dataset_stage(
                request=request,
                principal=principal,
                stage="version",
                files={request.file_name: request.version_file_utf8},
                prior_claim=str(created["claim_sha256"]),
            )
            readback = self.provider_gateway.invoke(
                "provider.resources.read",
                self._provider_arguments(
                    request.resource_ref,
                    {
                        "kind": "dataset",
                        "claim_sha256": versioned["claim_sha256"],
                    },
                ),
                principal,
            )
            if int(readback["provider_version"]) != int(versioned["provider_version"]):
                raise ValueError("dataset exact readback version differs from its durable claim")
            dataset_evidence = {
                "provider_ref": request.resource_ref,
                "provider_version": int(readback["provider_version"]),
                "package_sha256": str(readback["package_sha256"]),
                "fingerprint": readback["fingerprint"],
                "claim_sha256": str(versioned["claim_sha256"]),
                "create_effect_id": self._effect_id(request, "create"),
                "version_effect_id": self._effect_id(request, "version"),
            }
            self.ledger.append_acceptance_evidence(
                scenario_id=request.scenario_id,
                task_id=str(request.task_id),
                event_type="PROVIDER_DATASET",
                evidence=dataset_evidence,
            )
            cleanup = self._delete(
                scenario_id=request.scenario_id,
                task_id=str(request.task_id),
                provider_ref=request.resource_ref,
                kind="dataset",
                claim_sha256=str(versioned["claim_sha256"]),
                idempotency_key=f"{request.idempotency_key}:cleanup",
                principal=principal,
            )
            self.ledger.append_acceptance_evidence(
                scenario_id=request.scenario_id,
                task_id=str(request.task_id),
                event_type="CLEANUP",
                evidence=cleanup,
            )
            self.ledger.terminalize_acceptance_evidence_task(
                scenario_id=request.scenario_id,
                task_id=str(request.task_id),
                state="SUCCEEDED",
                evidence={"cleanup_state": "COMPLETE", "provider_version": int(readback["provider_version"])},
            )
        except Exception as exc:
            self._fail(request.scenario_id, str(request.task_id), exc)
            raise AcceptanceEvidenceError("acceptance dataset lifecycle terminalized as FAIL") from exc
        return self.claim_get(request.scenario_id, str(request.task_id))

    def _completed_cleanup_receipt(self, scenario_id: str, task_id: str) -> dict[str, Any] | None:
        effect_id = str(uuid5(NAMESPACE_URL, f"acceptance:{scenario_id}:{task_id}:cleanup"))
        receipt = self.ledger.latest_provider_effect_receipt(effect_id)
        if receipt is None or receipt.get("outcome") not in {"applied", "already_applied", "not_found"}:
            return None
        claim = self.ledger.acceptance_evidence_task(scenario_id=scenario_id, task_id=task_id)
        if claim is None:
            return None
        dataset_events = [item for item in claim["evidence"] if item["event_type"] == "PROVIDER_DATASET"]
        if len(dataset_events) != 1:
            return None
        evidence = dataset_events[0]["evidence"]
        return {
            "provider_ref": evidence["provider_ref"],
            "claim_sha256": evidence["claim_sha256"],
            "cleanup_effect_id": effect_id,
            "cleanup_outcome": str(receipt["outcome"]),
        }

    def notebook_lifecycle(
        self, arguments: Mapping[str, Any], principal: AccessIdentity
    ) -> dict[str, Any]:
        request = NotebookLifecycleRequest.model_validate(self._without_permit(arguments))
        task = self._claim(request, principal)
        if task["state"] in {"SUCCEEDED", "FAILED"}:
            return self.claim_get(request.scenario_id, str(request.task_id))
        self.ledger.begin_acceptance_evidence_task(
            scenario_id=request.scenario_id, task_id=str(request.task_id)
        )
        try:
            effect_id = self._effect_id(request, "run")
            existing = self.ledger.provider_resource_claim_for_effect(effect_id)
            if existing is None:
                run_result = self.provider_gateway.invoke(
                    "provider.resources.run",
                    self._provider_arguments(
                        request.resource_ref,
                        {
                            "kind": "notebook",
                            "task_id": str(request.task_id),
                            "effect_id": effect_id,
                            "idempotency_key": f"{request.idempotency_key}:run",
                            "task_run_id": str(request.task_run_id),
                            "title": request.title,
                            "code_file": request.code_file,
                            "kernel_type": "script",
                            "language": "python",
                            "source_utf8": request.source_utf8,
                            "dataset_sources": list(request.dataset_sources),
                            "disposable": True,
                        },
                    ),
                    principal,
                )
            else:
                run_result = self.provider_gateway.invoke(
                    "provider.resources.read",
                    self._provider_arguments(
                        request.resource_ref,
                        {"kind": "notebook", "claim_sha256": existing["claim_sha256"]},
                    ),
                    principal,
                )
            run = KaggleKernelRunIdentity(
                task_run_id=request.task_run_id,
                provider_ref=request.resource_ref,
                source_version=int(run_result["source_version"]),
                source_sha256=str(run_result["source_sha256"]),
                provider_kernel_id=int(run_result["provider_kernel_id"]),
                provider_run_ref=str(run_result["provider_run_ref"]),
                started_at=self.ledger.clock.now(),
            )
            terminal = self.provider_gateway.adapter.poll_run(run, PollPolicy())
            notebook_evidence = {
                "provider_ref": request.resource_ref,
                "provider_version": run.source_version,
                "source_version": run.source_version,
                "source_sha256": run.source_sha256,
                "fingerprint": run_result["fingerprint"],
                "provider_kernel_id": run.provider_kernel_id,
                "provider_run_ref": run.provider_run_ref,
                "task_run_id": str(run.task_run_id),
                "claim_sha256": str(run_result["claim_sha256"]),
                "run_effect_id": effect_id,
                "terminal_state": terminal.state.value,
            }
            self.ledger.append_acceptance_evidence(
                scenario_id=request.scenario_id,
                task_id=str(request.task_id),
                event_type="PROVIDER_NOTEBOOK",
                evidence=notebook_evidence,
            )
            with tempfile.TemporaryDirectory(prefix="my-data-hub-acceptance-output-") as temporary:
                destination = Path(temporary)
                output_tree = self.provider_gateway.adapter.download_exact_run_output_file(
                    run,
                    destination=destination,
                    file_name=request.output_file_name,
                    max_bytes=request.max_output_bytes,
                )
                output_path = destination / request.output_file_name
                output_file_sha256 = hashlib.sha256(output_path.read_bytes()).hexdigest()
            if output_file_sha256 != request.expected_output_sha256:
                raise ValueError("notebook selective output differs from its expected fingerprint")
            output_evidence = {
                "provider_run_ref": run.provider_run_ref,
                "output_file_name": request.output_file_name,
                "output_file_sha256": output_file_sha256,
                "output_tree_sha256": output_tree.output_tree_sha256,
                "file_count": output_tree.file_count,
            }
            output_evidence["output_receipt_sha256"] = hashlib.sha256(
                canonical_json_bytes(output_evidence)
            ).hexdigest()
            self.ledger.append_acceptance_evidence(
                scenario_id=request.scenario_id,
                task_id=str(request.task_id),
                event_type="OUTPUT_READ",
                evidence=output_evidence,
            )
            self.ledger.terminalize_acceptance_evidence_task(
                scenario_id=request.scenario_id,
                task_id=str(request.task_id),
                state="SUCCEEDED",
                evidence={
                    "cleanup_state": "PENDING",
                    "claim_sha256": notebook_evidence["claim_sha256"],
                    "provider_run_ref": run.provider_run_ref,
                    "output_receipt_sha256": output_evidence["output_receipt_sha256"],
                },
            )
        except Exception as exc:
            self._fail(request.scenario_id, str(request.task_id), exc)
            raise AcceptanceEvidenceError("acceptance notebook lifecycle terminalized as FAIL") from exc
        return self.claim_get(request.scenario_id, str(request.task_id))

    def cleanup(self, arguments: Mapping[str, Any], principal: AccessIdentity) -> dict[str, Any]:
        request = AcceptanceCleanupRequest.model_validate(self._without_permit(arguments))
        claim = self.claim_get(request.scenario_id, str(request.task_id))
        if not claim.get("found") or claim.get("state") != "SUCCEEDED":
            raise PermissionError("acceptance cleanup requires an exact successful evidence task")
        notebook = self._one_evidence(claim, "PROVIDER_NOTEBOOK")
        output = self._one_evidence(claim, "OUTPUT_READ")
        if (
            notebook.get("claim_sha256") != request.claim_sha256
            or notebook.get("provider_run_ref") != request.provider_run_ref
            or output.get("provider_run_ref") != request.provider_run_ref
            or output.get("output_receipt_sha256") != request.output_receipt_sha256
        ):
            raise PermissionError("acceptance cleanup differs from the exact claim/run/output-read receipt")
        cleanup_events = [item for item in claim["evidence"] if item["event_type"] == "CLEANUP"]
        if cleanup_events:
            return self.claim_get(request.scenario_id, str(request.task_id))
        try:
            cleanup = self._delete(
                scenario_id=request.scenario_id,
                task_id=str(request.task_id),
                provider_ref=str(notebook["provider_ref"]),
                kind="notebook",
                claim_sha256=request.claim_sha256,
                idempotency_key=request.idempotency_key,
                principal=principal,
            )
            self.ledger.append_acceptance_evidence(
                scenario_id=request.scenario_id,
                task_id=str(request.task_id),
                event_type="CLEANUP",
                evidence=cleanup,
            )
        except Exception as exc:
            # The evidence run remains successful, but cleanup itself is a
            # failed mutation request and is never represented as BLOCKED.
            raise AcceptanceEvidenceError("acceptance cleanup failed exact reconciliation") from exc
        return self.claim_get(request.scenario_id, str(request.task_id))

    def claim_get(self, scenario_id: str, task_id: str) -> dict[str, Any]:
        if scenario_id not in ACCEPTANCE_EVIDENCE_SCENARIOS:
            raise ValueError("acceptance evidence scenario is invalid")
        UUID(task_id)
        task = self.ledger.acceptance_evidence_task(scenario_id=scenario_id, task_id=task_id)
        if task is None:
            return {"found": False, "scenario_id": scenario_id, "task_id": task_id}
        evidence = [
            item
            for item in task["evidence"]
            if item["event_type"] not in {"CLAIMED", "RUNNING"}
        ]
        cleanup_state = "COMPLETE" if any(item["event_type"] == "CLEANUP" for item in evidence) else (
            "PENDING" if any(item["event_type"] == "PROVIDER_NOTEBOOK" for item in evidence) else "NOT_REQUIRED"
        )
        return {
            "found": True,
            "scenario_id": scenario_id,
            "task_id": task_id,
            "state": task["state"],
            "failure_code": task["failure_code"],
            "mutation_started": task["mutation_started"],
            "cleanup_state": cleanup_state,
            "evidence": evidence,
            "bounded": True,
        }

    def _dataset_stage(
        self,
        *,
        request: DatasetLifecycleRequest,
        principal: AccessIdentity,
        stage: Literal["create", "version"],
        files: Mapping[str, str],
        prior_claim: str | None,
    ) -> dict[str, Any]:
        effect_id = self._effect_id(request, stage)
        existing = self.ledger.provider_resource_claim_for_effect(effect_id)
        if existing is not None:
            return self.provider_gateway.invoke(
                "provider.resources.read",
                self._provider_arguments(
                    request.resource_ref,
                    {"kind": "dataset", "claim_sha256": existing["claim_sha256"]},
                ),
                principal,
            )
        payload: dict[str, Any] = {
            "kind": "dataset",
            "task_id": str(request.task_id),
            "effect_id": effect_id,
            "idempotency_key": f"{request.idempotency_key}:{stage}",
            "files": dict(files),
        }
        if stage == "create":
            payload.update({"title": request.title, "disposable": True})
            tool = "provider.resources.create"
        else:
            payload.update({"claim_sha256": prior_claim, "version_notes": "acceptance exact version"})
            tool = "provider.resources.version"
        return self.provider_gateway.invoke(
            tool, self._provider_arguments(request.resource_ref, payload), principal
        )

    def _delete(
        self,
        *,
        scenario_id: str,
        task_id: str,
        provider_ref: str,
        kind: Literal["dataset", "notebook"],
        claim_sha256: str,
        idempotency_key: str,
        principal: AccessIdentity,
    ) -> dict[str, Any]:
        effect_id = str(uuid5(NAMESPACE_URL, f"acceptance:{scenario_id}:{task_id}:cleanup"))
        existing = self.ledger.latest_provider_effect_receipt(effect_id)
        if existing is not None and existing.get("outcome") in {"applied", "already_applied", "not_found"}:
            return {
                "provider_ref": provider_ref,
                "claim_sha256": claim_sha256,
                "cleanup_effect_id": effect_id,
                "cleanup_outcome": str(existing["outcome"]),
            }
        result = self.provider_gateway.invoke(
            "provider.resources.delete",
            self._provider_arguments(
                provider_ref,
                {
                    "kind": kind,
                    "task_id": task_id,
                    "effect_id": effect_id,
                    "idempotency_key": idempotency_key,
                    "claim_sha256": claim_sha256,
                },
            ),
            principal,
        )
        return {
            "provider_ref": provider_ref,
            "claim_sha256": claim_sha256,
            "cleanup_effect_id": effect_id,
            "cleanup_outcome": str(result["outcome"]),
        }

    def _claim(self, request: BaseModel, principal: AccessIdentity) -> dict[str, Any]:
        payload = request.model_dump(mode="json")
        # Raw synthetic source/file content participates in the request digest,
        # but is categorically not stored in the devstand ledger.
        request_sha256 = hashlib.sha256(canonical_json_bytes(payload)).hexdigest()
        task, _created = self.ledger.ensure_acceptance_evidence_task(
            scenario_id=str(payload["scenario_id"]),
            task_id=str(payload["task_id"]),
            idempotency_key=str(payload["idempotency_key"]),
            principal_id=principal.subject,
            client_id=principal.client_id,
            request_sha256=request_sha256,
        )
        return task

    def _fail(self, scenario_id: str, task_id: str, exc: Exception) -> None:
        current = self.ledger.acceptance_evidence_task(scenario_id=scenario_id, task_id=task_id)
        if current is not None and current["state"] not in {"SUCCEEDED", "FAILED"}:
            self.ledger.terminalize_acceptance_evidence_task(
                scenario_id=scenario_id,
                task_id=task_id,
                state="FAILED",
                failure_code=f"PROVIDER_{type(exc).__name__.upper()}",
                evidence={"failure_class": type(exc).__name__, "mutation_started": True},
            )

    @staticmethod
    def _provider_arguments(resource_ref: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "resource_ref": resource_ref,
            "control_class": "mcp_managed",
            "private": True,
            "payload": dict(payload),
        }

    @staticmethod
    def _without_permit(arguments: Mapping[str, Any]) -> dict[str, Any]:
        return {key: value for key, value in arguments.items() if key != "_write_permit"}

    @staticmethod
    def _effect_id(request: BaseModel, stage: str) -> str:
        payload = request.model_dump(mode="json")
        return str(uuid5(NAMESPACE_URL, f"acceptance:{payload['scenario_id']}:{payload['task_id']}:{stage}"))

    @staticmethod
    def _one_evidence(claim: Mapping[str, Any], event_type: str) -> dict[str, Any]:
        matches = [item["evidence"] for item in claim["evidence"] if item["event_type"] == event_type]
        if len(matches) != 1:
            raise PermissionError(f"acceptance claim lacks one exact {event_type} receipt")
        return dict(matches[0])
