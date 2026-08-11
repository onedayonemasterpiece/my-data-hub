"""Production metadata adapters for the H6 operational data-workload core.

Only bounded control/MCP metadata crosses this boundary.  The module has no
PostgreSQL client, YDB reader, provider API, raw-row or vector transport.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import stat
import time
import urllib.error
import urllib.request
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Literal, Protocol
from urllib.parse import urlsplit
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from my_data_hub.embeddings.production import WORKER_ASSETS, EmbeddingProductionRequest
from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.workloads.bloggers.master_stage import (
    BLOGGER_REPLAY_STAGE_SCHEMA,
    BloggerDuplicateResolutionEnvelope,
    BloggerImportStageReceipt,
    BloggerMigrationRequest,
)

from .data_workloads import (
    BGE_EXACT_ID,
    E5_EXACT_ID,
    SHA256_PATTERN,
    BloggerAccountingEvidence,
    BloggerQuarantineEvidence,
    BloggerRequestObservation,
    BloggerTerminalEvidence,
    ChangeApplyEvidence,
    ChangePreviewEvidence,
    ChangeStatusEvidence,
    CheckpointEvidence,
    DataWorkloadEvidenceBundle,
    DataWorkloadExecutionResult,
    DataWorkloadGateway,
    DataWorkloadPlan,
    DataWorkloadState,
    DataWorkloadStateMachine,
    DataWorkloadStateStore,
    DuplicateReviewEvidence,
    EmbeddingModelEvidence,
    EmbeddingRequestObservation,
    EmbeddingTerminalEvidence,
    FixedChangeIntent,
    MasterEvidence,
    MutationAcceptance,
    OwnerDuplicateAuthorization,
    RestoreObservation,
)

MAX_METADATA_BYTES = 256 * 1024
INSERT_PROJECT_SQL = (
    "INSERT INTO hub.project(project_id,slug,name,description,status,metadata) VALUES ($1,$2,$3,$4,$5,$6)"
)
DELETE_PROJECT_SQL = "DELETE FROM hub.project WHERE project_id=$1"
_TERMINAL_OPERATIONS = {"DURABLE_COMPLETE", "FAILED", "FENCED", "ORPHANED"}


class ProductionCapabilityBlocker(RuntimeError):
    """Stable fail-closed blocker safe to include in metadata evidence."""

    def __init__(self, code: str, detail: str) -> None:
        if not code or any(char not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_" for char in code):
            raise ValueError("production blocker code is invalid")
        super().__init__(detail)
        self.code = code
        self.detail = detail


class AmbiguousMutation(RuntimeError):
    """Transport lost certainty after a mutation may have reached the server."""


class ControlMetadataClient(Protocol):
    async def get(self, path: str) -> dict[str, Any]: ...
    async def post(self, path: str, payload: Mapping[str, Any]) -> dict[str, Any]: ...


class McpMetadataClient(Protocol):
    async def call(self, profile: str, tool: str, arguments: Mapping[str, Any]) -> dict[str, Any]: ...


class ProductionDataWorkloadConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["my-data-hub-data-workload-production-config.v1"] = (
        "my-data-hub-data-workload-production-config.v1"
    )
    control_base_url: str
    mcp_endpoint: str
    blogger_v1_operation_id: UUID
    blogger_v2_operation_id: UUID
    probe_query: str = Field(min_length=1, max_length=500)
    timeout_seconds: int = Field(default=3600, ge=60, le=43_000)
    poll_seconds: float = Field(default=10.0, ge=1.0, le=60.0)

    @model_validator(mode="after")
    def safe_endpoints(self) -> ProductionDataWorkloadConfig:
        control = urlsplit(self.control_base_url)
        loopback = (
            control.scheme == "http"
            and control.hostname == "127.0.0.1"
            and control.port == 8080
            and control.path in {"", "/"}
        )
        remote = control.scheme == "https" and bool(control.hostname) and control.path in {"", "/"}
        if not (loopback or remote) or control.username or control.password or control.query or control.fragment:
            raise ValueError("control URL must be credential-free HTTPS or exact master loopback")
        mcp = urlsplit(self.mcp_endpoint)
        if (
            mcp.scheme != "https"
            or not mcp.hostname
            or mcp.username
            or mcp.password
            or mcp.query
            or mcp.fragment
            or mcp.path.rstrip("/") != "/mcp"
        ):
            raise ValueError("MCP endpoint must be a credential-free HTTPS /mcp URL")
        return self


class ProductionDataWorkloadReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["my-data-hub-data-workload-production-receipt.v1"] = (
        "my-data-hub-data-workload-production-receipt.v1"
    )
    matrix_id: UUID
    outcome: Literal["PROGRESS", "AWAITING_OWNER_AUTHORIZATION", "FAIL", "BLOCKED", "EVIDENCE_READY"]
    live_evidence: Literal[False] = False
    outer_reconciliation_required: Literal[True] = True
    state_sha256: str = Field(pattern=SHA256_PATTERN)
    blocker_code: str | None = Field(default=None, pattern=r"^[A-Z0-9_]+$")
    failure_code: str | None = Field(default=None, pattern=r"^[A-Z0-9_]+$")
    evidence: DataWorkloadEvidenceBundle | None = None

    @model_validator(mode="after")
    def exact_terminal_shape(self) -> ProductionDataWorkloadReceipt:
        if (self.outcome == "BLOCKED") != (self.blocker_code is not None):
            raise ValueError("only BLOCKED carries a capability blocker")
        if (self.outcome == "FAIL") != (self.failure_code is not None):
            raise ValueError("only FAIL carries a failure code")
        if (self.outcome == "EVIDENCE_READY") != (self.evidence is not None):
            raise ValueError("only EVIDENCE_READY carries the inner evidence bundle")
        return self


class AtomicJsonStateStore(DataWorkloadStateStore):
    """Crash-safe mode-0600 state persistence; contains metadata only."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def persist(self, state: DataWorkloadState) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_name(f".{self.path.name}.tmp-{os.getpid()}")
        payload = canonical_json_bytes(state.model_dump(mode="json")) + b"\n"
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.path)
            os.chmod(self.path, 0o600)
            directory = os.open(self.path.parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            if temporary.exists():
                temporary.unlink()

    def load(self, plan: DataWorkloadPlan) -> DataWorkloadState:
        if not self.path.exists():
            return DataWorkloadState.initial(plan)
        if self.path.is_symlink() or not stat.S_ISREG(self.path.stat(follow_symlinks=False).st_mode):
            raise ValueError("data-workload state path must be a regular file")
        raw = self.path.read_bytes()
        if len(raw) > MAX_METADATA_BYTES:
            raise ValueError("data-workload state exceeds 256 KiB")
        return DataWorkloadState.model_validate_json(raw)


def load_owner_authorization(
    path: Path,
) -> tuple[BloggerDuplicateResolutionEnvelope, OwnerDuplicateAuthorization]:
    """Load the exact owner-authorized envelope from a non-symlink mode-0600 file."""

    try:
        info = path.stat(follow_symlinks=False)
    except OSError as exc:
        raise ProductionCapabilityBlocker(
            "FM16_OWNER_ENVELOPE_UNAVAILABLE",
            "duplicate-resolution envelope is unavailable",
        ) from exc
    if (
        path.is_symlink()
        or not stat.S_ISREG(info.st_mode)
        or stat.S_IMODE(info.st_mode) != 0o600
        or info.st_uid != os.getuid()
    ):
        raise ProductionCapabilityBlocker(
            "FM16_OWNER_ENVELOPE_PERMISSIONS_INVALID",
            "duplicate-resolution envelope must be a regular mode-0600 file",
        )
    raw = path.read_bytes()
    if not raw or len(raw) > MAX_METADATA_BYTES:
        raise ProductionCapabilityBlocker(
            "FM16_OWNER_ENVELOPE_SIZE_INVALID", "duplicate-resolution envelope is empty or oversized"
        )
    try:
        envelope = BloggerDuplicateResolutionEnvelope.model_validate_json(raw)
    except Exception as exc:
        raise ProductionCapabilityBlocker(
            "FM16_OWNER_ENVELOPE_INVALID", "duplicate-resolution envelope failed exact validation"
        ) from exc
    identity_hashes = tuple(item.identity_sha256 for item in envelope.decisions)
    member_ids = tuple(sorted({value for item in envelope.decisions for value in item.member_record_ids}))
    authorization = OwnerDuplicateAuthorization(
        authorization_id=envelope.authorization_id,
        authorized_by_sha256=_sha_text(envelope.authorized_by),
        authorized_at=envelope.authorized_at,
        source_request_id=envelope.source_request_id,
        source_operation_id=envelope.source_operation_id,
        source_request_sha256=envelope.source_request_sha256,
        export_batch_id=envelope.export_batch_id,
        decision_count=len(envelope.decisions),
        identity_set_sha256=_sha_json(identity_hashes),
        member_record_id_set_sha256=_sha_json(member_ids),
        envelope_sha256=envelope.envelope_sha256,
    )
    return envelope, authorization


class UrllibControlMetadataClient(ControlMetadataClient):
    def __init__(self, base_url: str, bearer_token: str | None, *, timeout_seconds: float = 20.0) -> None:
        validated = ProductionDataWorkloadConfig(
            control_base_url=base_url,
            mcp_endpoint="https://invalid.example/mcp",
            blogger_v1_operation_id=UUID(int=1),
            blogger_v2_operation_id=UUID(int=2),
            probe_query="validation",
        )
        self.base_url = validated.control_base_url.rstrip("/")
        if bearer_token is not None and (
            not 24 <= len(bearer_token) <= 4096 or any(char.isspace() for char in bearer_token)
        ):
            raise ValueError("control bearer token is invalid")
        self._token = bearer_token
        self.timeout_seconds = timeout_seconds

    async def get(self, path: str) -> dict[str, Any]:
        return await asyncio.to_thread(self._request, "GET", path, None)

    async def post(self, path: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        return await asyncio.to_thread(self._request, "POST", path, canonical_json_bytes(dict(payload)))

    def _request(self, method: str, path: str, body: bytes | None) -> dict[str, Any]:
        if not path.startswith("/") or "?" in path or "#" in path or ".." in path.split("/"):
            raise ValueError("control metadata path is invalid")
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        request = urllib.request.Request(f"{self.base_url}{path}", data=body, headers=headers, method=method)
        opener = urllib.request.build_opener(_RejectRedirects())
        try:
            with opener.open(request, timeout=self.timeout_seconds) as response:
                raw = response.read(MAX_METADATA_BYTES + 1)
        except (OSError, urllib.error.URLError, urllib.error.HTTPError) as exc:
            if method == "POST":
                raise AmbiguousMutation("control mutation response was not observed") from exc
            raise ProductionCapabilityBlocker(
                "CONTROL_METADATA_UNAVAILABLE", "bounded control metadata request failed"
            ) from exc
        return _decode_metadata(raw, "control")


class _RejectRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args: object, **kwargs: object) -> None:
        return None


class StreamableHttpMcpMetadataClient(McpMetadataClient):
    def __init__(self, endpoint: str, tokens: Mapping[str, str]) -> None:
        parsed = urlsplit(endpoint)
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
            or parsed.path.rstrip("/") != "/mcp"
        ):
            raise ValueError("MCP endpoint must be a credential-free HTTPS /mcp URL")
        self.endpoint = endpoint
        self._tokens = dict(tokens)

    async def call(self, profile: str, tool: str, arguments: Mapping[str, Any]) -> dict[str, Any]:
        import httpx2
        from mcp import ClientSession
        from mcp.client.streamable_http import streamable_http_client

        token = self._tokens.get(profile, "")
        if not 24 <= len(token) <= 4096 or any(char.isspace() for char in token):
            raise ProductionCapabilityBlocker(
                f"MCP_{profile.upper()}_CREDENTIAL_ABSENT", "required MCP profile credential is absent"
            )
        try:
            async with (
                httpx2.AsyncClient(
                    headers={"Authorization": f"Bearer {token}"},
                    follow_redirects=False,
                    timeout=httpx2.Timeout(25.0, connect=5.0),
                ) as client,
                streamable_http_client(self.endpoint, http_client=client) as streams,
            ):
                read_stream, write_stream = streams
                async with ClientSession(read_stream, write_stream, read_timeout_seconds=25) as session:
                    await session.initialize()
                    result = await session.call_tool(tool, dict(arguments))
        except ProductionCapabilityBlocker:
            raise
        except Exception as exc:
            if tool in {
                "master.rotation.request",
                "checkpoint.restore.request",
                "data.change.preview",
                "data.change.apply",
            }:
                raise AmbiguousMutation(f"MCP {tool} response was not observed") from exc
            raise ProductionCapabilityBlocker("MCP_METADATA_UNAVAILABLE", f"bounded MCP {tool} call failed") from exc
        if bool(getattr(result, "is_error", getattr(result, "isError", False))):
            raise ProductionCapabilityBlocker("MCP_TOOL_REJECTED", f"bounded MCP {tool} returned a typed error")
        structured = getattr(result, "structured_content", getattr(result, "structuredContent", None))
        if isinstance(structured, Mapping):
            return _bounded_mapping(structured, "MCP")
        for block in getattr(result, "content", ()):
            text = getattr(block, "text", None)
            if isinstance(text, str) and len(text.encode()) <= MAX_METADATA_BYTES:
                try:
                    value = json.loads(text)
                except json.JSONDecodeError:
                    continue
                if isinstance(value, Mapping):
                    return _bounded_mapping(value, "MCP")
        raise ProductionCapabilityBlocker("MCP_RESULT_INVALID", f"MCP {tool} returned no object")


class ControlPlaneDataWorkloadGateway(DataWorkloadGateway):
    """Exact H1/H3/H5 production adapter; no live-PASS authority."""

    def __init__(
        self,
        *,
        plan: DataWorkloadPlan,
        config: ProductionDataWorkloadConfig,
        control: ControlMetadataClient,
        mcp: McpMetadataClient,
        owner_envelope: BloggerDuplicateResolutionEnvelope | None = None,
    ) -> None:
        if _sha_text(config.probe_query.strip()) != plan.embedding_probe_query_sha256:
            raise ValueError("production probe text differs from the plan hash")
        self.plan = plan
        self.config = config
        self.control = control
        self.mcp = mcp
        self.owner_envelope = owner_envelope
        self._reviews: dict[UUID, DuplicateReviewEvidence] = {}
        self._preview_receipts: dict[str, str] = {}
        self._blogger_prerequisite: BloggerTerminalEvidence | None = None

    def bind_state(self, state: DataWorkloadState) -> None:
        """Restore non-secret prerequisite context after a CLI restart."""

        self._blogger_prerequisite = state.blogger_terminal

    async def start_blogger_v1(
        self, *, request_id: UUID, intent_sha256: str, plan: DataWorkloadPlan
    ) -> MutationAcceptance:
        del intent_sha256
        if plan != self.plan:
            raise ValueError("blogger v1 plan differs from production gateway plan")
        request = BloggerMigrationRequest(
            request_id=request_id,
            operation_id=self.config.blogger_v1_operation_id,
            project_id=plan.blogger_project_id,
            snapshot_at=plan.blogger_snapshot_at,
            source_revision=plan.blogger_source_revision,
        )
        return await self._create_blogger_request(request)

    async def observe_blogger(self, request_id: UUID) -> BloggerRequestObservation:
        status = await self.control.get(f"/control/v1/blogger-closure/requests/{request_id}")
        exact_request = _uuid(status.get("request_id"), "FM16_H5_STATUS_IDENTITY_INVALID")
        request_sha = _sha_value(status.get("request_sha256"), "FM16_H5_STATUS_HASH_INVALID")
        if exact_request != request_id:
            raise ProductionCapabilityBlocker(
                "FM16_H5_STATUS_IDENTITY_MISMATCH", "H5 status returned another request identity"
            )
        state = str(status.get("state", ""))
        if state == "FAILED" and status.get("failure_code") == "BloggerMigrationQuarantined":
            quarantine_raw = status.get("quarantine_evidence")
            review_raw = status.get("duplicate_review")
            if not isinstance(quarantine_raw, Mapping) or not isinstance(review_raw, Mapping):
                raise ProductionCapabilityBlocker(
                    "FM16_H5_QUARANTINE_PROJECTION_UNAVAILABLE",
                    "H5 status lacks typed quarantine accounting and duplicate-review hashes",
                )
            try:
                quarantine = BloggerQuarantineEvidence.model_validate(quarantine_raw)
                review = DuplicateReviewEvidence.model_validate(review_raw)
            except Exception as exc:
                raise ProductionCapabilityBlocker(
                    "FM16_H5_QUARANTINE_PROJECTION_INVALID",
                    "H5 quarantine projection differs from the required metadata contract",
                ) from exc
            self._reviews[request_id] = review
            return BloggerRequestObservation(
                request_id=request_id,
                request_sha256=request_sha,
                state="FAILED",
                quarantine=quarantine,
            )
        if state == "CHECKPOINT_VERIFIED":
            terminal = await self._blogger_terminal(status)
            return BloggerRequestObservation(
                request_id=request_id,
                request_sha256=request_sha,
                state="CHECKPOINT_VERIFIED",
                terminal=terminal,
            )
        if state in {"REQUESTED", "CLAIMED", "IMPORT_COMMITTED"}:
            return BloggerRequestObservation(request_id=request_id, request_sha256=request_sha, state=state)
        return BloggerRequestObservation(request_id=request_id, request_sha256=request_sha, state="FAILED")

    async def duplicate_review(self, request_id: UUID) -> DuplicateReviewEvidence:
        review = self._reviews.get(request_id)
        if review is None:
            await self.observe_blogger(request_id)
            review = self._reviews.get(request_id)
        if review is None:
            raise ProductionCapabilityBlocker(
                "FM16_H5_DUPLICATE_REVIEW_UNAVAILABLE", "H5 duplicate review projection is absent"
            )
        return review

    async def start_blogger_v2(
        self,
        *,
        request_id: UUID,
        intent_sha256: str,
        authorization: OwnerDuplicateAuthorization,
    ) -> MutationAcceptance:
        del intent_sha256
        envelope = self.owner_envelope
        if envelope is None or _authorization_from_envelope(envelope) != authorization:
            raise ProductionCapabilityBlocker(
                "FM16_OWNER_ENVELOPE_BINDING_MISMATCH",
                "mode-0600 owner envelope does not bind the persisted duplicate review",
            )
        request = BloggerMigrationRequest(
            schema_version=BLOGGER_REPLAY_STAGE_SCHEMA,
            request_id=request_id,
            operation_id=self.config.blogger_v2_operation_id,
            project_id=self.plan.blogger_project_id,
            snapshot_at=self.plan.blogger_snapshot_at,
            source_revision=self.plan.blogger_source_revision,
            replay_of_request_id=envelope.source_request_id,
            duplicate_resolution=envelope,
        )
        return await self._create_blogger_request(request)

    async def migration_accounting(self, export_batch_id: UUID) -> BloggerAccountingEvidence:
        value = await self.mcp.call(
            "reader", "bloggers.migration.accounting", {"export_batch_id": str(export_batch_id)}
        )
        accounting = value.get("accounting")
        if value.get("found") is not True or not isinstance(accounting, Mapping):
            raise ProductionCapabilityBlocker("FM17_BLOGGER_ACCOUNTING_ABSENT", "exact migration accounting is absent")
        payload = dict(accounting)
        payload["canonical_revision"] = payload.pop("imported_canonical_revision", None)
        try:
            return BloggerAccountingEvidence.model_validate(payload)
        except Exception as exc:
            raise ProductionCapabilityBlocker(
                "FM17_BLOGGER_ACCOUNTING_INVALID", "migration accounting differs from the exact contract"
            ) from exc

    async def start_restore(
        self,
        *,
        idempotency_key_sha256: str,
        checkpoint: CheckpointEvidence,
        expected_epoch: int,
    ) -> MutationAcceptance:
        arguments = {
            "checkpoint_id": str(checkpoint.checkpoint_id),
            "exact_version_ref": checkpoint.exact_version_ref,
            "expected_active_epoch": expected_epoch,
            "expected_canonical_revision": checkpoint.canonical_revision,
            "timeout_seconds": min(1800, self.config.timeout_seconds),
            "idempotency_key": f"h6-fm17:{idempotency_key_sha256}",
        }
        try:
            value = await self.mcp.call("operator", "master.rotation.request", arguments)
        except AmbiguousMutation:
            # The core retained the exact idempotency identity and will replay this
            # request on the next invocation to recover the server operation ID.
            raise ProductionCapabilityBlocker(
                "FM17_ROTATION_RESPONSE_AMBIGUOUS",
                "rotation response was lost; resume replays the persisted idempotency identity",
            ) from None
        if value.get("accepted") is not True or value.get("execution_supported") is not True:
            blocker_code = str(value.get("blocker_code") or "")
            if not blocker_code or any(char not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_" for char in blocker_code):
                blocker_code = "FM17_ROTATION_CAPABILITY_UNAVAILABLE"
            raise ProductionCapabilityBlocker(
                blocker_code,
                "cold-restore rotation was not accepted by the production consumer",
            )
        operation_id = str(value.get("operation_id") or "")
        request_sha = _sha_value(value.get("request_sha256"), "FM17_ROTATION_REQUEST_HASH_INVALID")
        if not operation_id:
            raise ProductionCapabilityBlocker(
                "FM17_ROTATION_OPERATION_ID_ABSENT", "rotation did not return its server operation ID"
            )
        return MutationAcceptance(
            operation_id=operation_id,
            request_sha256=request_sha,
            outcome="replayed" if value.get("duplicate") is True else "accepted",
            state=str(value.get("state") or "REQUESTED"),
            response_sha256=_sha_json(value),
        )

    async def observe_restore(self, operation_id: str) -> RestoreObservation:
        value = await self.mcp.call("reader", "operation.get", {"operation_id": operation_id})
        if value.get("found") is not True or value.get("operation_id") != operation_id:
            raise ProductionCapabilityBlocker("FM17_ROTATION_OPERATION_ABSENT", "server rotation operation is absent")
        state = str(value.get("state") or "")
        normalized = state if state in _TERMINAL_OPERATIONS else "RUNNING"
        return RestoreObservation(operation_id=operation_id, state=normalized)

    async def active_master(self) -> MasterEvidence:
        value = await self.mcp.call("reader", "master.status", {})
        if value.get("master_state") != "ACTIVE":
            raise ProductionCapabilityBlocker("FM17_RESTORED_MASTER_NOT_ACTIVE", "restored master is not ACTIVE")
        try:
            return MasterEvidence(
                master_instance_id=value.get("instance_id"),
                epoch=value.get("master_epoch"),
                canonical_revision=value.get("canonical_revision"),
            )
        except Exception as exc:
            raise ProductionCapabilityBlocker(
                "FM17_RESTORED_MASTER_IDENTITY_INVALID", "restored master metadata is incomplete"
            ) from exc

    async def start_embedding(
        self,
        *,
        request_id: UUID,
        intent_sha256: str,
        blogger: BloggerTerminalEvidence,
        probe_query_sha256: str,
    ) -> MutationAcceptance:
        del intent_sha256
        request = EmbeddingProductionRequest(
            request_id=request_id,
            idempotency_key_sha256=self.plan.key_sha256("fm18-19:embedding"),
            blogger_receipt_id=blogger.request_id,
            blogger_receipt_sha256=blogger.receipt_sha256,
            blogger_canonical_revision=blogger.canonical_revision,
            blogger_checkpoint_id=blogger.checkpoint.checkpoint_id,
            source_revision=self.plan.source_commit,
            probe_query=self.config.probe_query,
            probe_query_sha256=probe_query_sha256,
        )
        self._blogger_prerequisite = blogger
        try:
            value = await self.control.post(
                "/control/v1/embedding-production/requests", request.model_dump(mode="json")
            )
        except AmbiguousMutation:
            return MutationAcceptance(
                operation_id=str(request_id),
                request_sha256=request.request_sha256,
                outcome="ambiguous",
                state="UNKNOWN",
                response_sha256=_sha_json({"ambiguous": True, "request_id": str(request_id)}),
            )
        return self._request_acceptance(value, request_id, request.request_sha256)

    async def observe_embedding(self, request_id: UUID) -> EmbeddingRequestObservation:
        value = await self.control.get(f"/control/v1/embedding-production/requests/{request_id}")
        exact_id = _uuid(value.get("request_id"), "FM18_19_STATUS_IDENTITY_INVALID")
        request_sha = _sha_value(value.get("request_sha256"), "FM18_19_STATUS_HASH_INVALID")
        if exact_id != request_id:
            raise ProductionCapabilityBlocker(
                "FM18_19_STATUS_IDENTITY_MISMATCH", "embedding status returned another request"
            )
        state = str(value.get("state") or "")
        if state == "CHECKPOINT_VERIFIED":
            terminal = await self._embedding_terminal(value)
            return EmbeddingRequestObservation(
                request_id=request_id,
                request_sha256=request_sha,
                state="CHECKPOINT_VERIFIED",
                terminal=terminal,
            )
        if state in {"REQUESTED", "CLAIMED", "STAGE_COMMITTED"}:
            return EmbeddingRequestObservation(request_id=request_id, request_sha256=request_sha, state=state)
        return EmbeddingRequestObservation(request_id=request_id, request_sha256=request_sha, state="FAILED")

    async def preview_fixed_change(self, intent: FixedChangeIntent) -> ChangePreviewEvidence:
        arguments = self._change_arguments(intent)
        try:
            value = await self.mcp.call("operator", "data.change.preview", arguments)
        except AmbiguousMutation as exc:
            raise ProductionCapabilityBlocker(
                "FM21_PREVIEW_RESPONSE_AMBIGUOUS",
                "preview response was lost; no apply is permitted without its exact receipt",
            ) from exc
        operation_id = _sha_value(value.get("operation_id"), "FM21_PREVIEW_OPERATION_INVALID")
        receipt = value.get("preview_receipt")
        if not isinstance(receipt, str) or not receipt:
            raise ProductionCapabilityBlocker(
                "FM21_PREVIEW_RECEIPT_ABSENT", "H1 preview did not return its signed receipt"
            )
        self._preview_receipts[operation_id] = receipt
        try:
            return ChangePreviewEvidence(
                operation_id=operation_id,
                request_sha256=intent.request_sha256,
                action=intent.action,
                affected_rows=value.get("affected_rows"),
                expected_revision=intent.expected_revision,
                pre_change_checkpoint_id=value.get("pre_change_checkpoint_id"),
                preview_receipt_sha256=_sha_text(receipt),
            )
        except Exception as exc:
            raise ProductionCapabilityBlocker(
                "FM21_PREVIEW_EVIDENCE_INVALID", "H1 preview metadata differs from the fixed contract"
            ) from exc

    async def apply_fixed_change(self, preview: ChangePreviewEvidence) -> ChangeApplyEvidence:
        receipt = self._preview_receipts.get(preview.operation_id)
        intent = self._intent_for_preview(preview)
        arguments = self._change_arguments(intent)
        if receipt is None:
            replay = await self.mcp.call("operator", "data.change.preview", arguments)
            if (
                replay.get("operation_id") != preview.operation_id
                or replay.get("affected_rows") != preview.affected_rows
                or _sha_text(str(replay.get("preview_receipt") or "")) != preview.preview_receipt_sha256
            ):
                raise ProductionCapabilityBlocker(
                    "FM21_PREVIEW_REPLAY_MISMATCH", "replayed preview differs from persisted evidence"
                )
            receipt = str(replay["preview_receipt"])
        arguments["preview_receipt"] = receipt
        try:
            value = await self.mcp.call("operator", "data.change.apply", arguments)
        except AmbiguousMutation:
            return ChangeApplyEvidence(
                operation_id=preview.operation_id,
                outcome="ambiguous",
                response_sha256=_sha_json({"ambiguous": True, "operation_id": preview.operation_id}),
            )
        if value.get("operation_id") != preview.operation_id:
            raise ProductionCapabilityBlocker(
                "FM21_APPLY_OPERATION_MISMATCH", "H1 apply returned another operation identity"
            )
        return ChangeApplyEvidence(
            operation_id=preview.operation_id,
            outcome="accepted",
            affected_rows=value.get("affected_rows"),
            committed_revision=value.get("canonical_revision"),
            response_sha256=_sha_json(value),
        )

    async def fixed_change_status(self, operation_id: str) -> ChangeStatusEvidence:
        value = await self.mcp.call("reader", "data.change.status", {"operation_id": operation_id})
        if value.get("found") is not True or value.get("operation_id") != operation_id:
            raise ProductionCapabilityBlocker("FM21_CHANGE_OPERATION_ABSENT", "H1 change status is absent")
        post_checkpoint = None
        post_id = value.get("post_change_checkpoint_id")
        if post_id is not None:
            checkpoint_status = await self.mcp.call("reader", "checkpoint.status", {})
            post_checkpoint = _checkpoint_from_status(checkpoint_status, expected_id=str(post_id))
        try:
            return ChangeStatusEvidence(
                operation_id=operation_id,
                state=value.get("state"),
                expected_revision=value.get("expected_revision"),
                committed_revision=value.get("committed_revision"),
                pre_change_checkpoint_id=value.get("pre_change_checkpoint_id"),
                post_change_checkpoint=post_checkpoint,
            )
        except Exception as exc:
            raise ProductionCapabilityBlocker(
                "FM21_CHANGE_STATUS_INVALID", "H1 change status differs from the durable contract"
            ) from exc

    async def _create_blogger_request(self, request: BloggerMigrationRequest) -> MutationAcceptance:
        try:
            value = await self.control.post("/control/v1/blogger-closure/requests", request.metadata_payload)
        except AmbiguousMutation:
            return MutationAcceptance(
                operation_id=str(request.request_id),
                request_sha256=request.request_sha256,
                outcome="ambiguous",
                state="UNKNOWN",
                response_sha256=_sha_json({"ambiguous": True, "request_id": str(request.request_id)}),
            )
        return self._request_acceptance(value, request.request_id, request.request_sha256)

    @staticmethod
    def _request_acceptance(value: Mapping[str, Any], request_id: UUID, request_sha256: str) -> MutationAcceptance:
        if value.get("request_id") != str(request_id) or value.get("request_sha256") != request_sha256:
            raise ProductionCapabilityBlocker(
                "CONTROL_REQUEST_REPLAY_MISMATCH", "control plane stored a different request identity"
            )
        return MutationAcceptance(
            operation_id=str(request_id),
            request_sha256=request_sha256,
            outcome="accepted" if value.get("created") is True else "replayed",
            state=str(value.get("state") or "REQUESTED"),
            response_sha256=_sha_json(value),
        )

    async def _blogger_terminal(self, status: Mapping[str, Any]) -> BloggerTerminalEvidence:
        try:
            imported = BloggerImportStageReceipt.model_validate(status.get("import_receipt"))
        except Exception as exc:
            raise ProductionCapabilityBlocker(
                "FM16_H5_TERMINAL_RECEIPT_INVALID", "H5 terminal import receipt is absent or invalid"
            ) from exc
        checkpoint_status = await self.mcp.call("reader", "checkpoint.status", {})
        checkpoint_receipt = status.get("checkpoint_receipt")
        if not isinstance(checkpoint_receipt, Mapping):
            raise ProductionCapabilityBlocker(
                "FM16_H5_CHECKPOINT_RECEIPT_ABSENT", "H5 terminal checkpoint receipt is absent"
            )
        checkpoint = _checkpoint_from_status(
            checkpoint_status, expected_id=str(checkpoint_receipt.get("checkpoint_id") or "")
        )
        try:
            return BloggerTerminalEvidence(
                request_id=imported.request_id,
                request_sha256=imported.request_sha256,
                receipt_sha256=imported.receipt_sha256,
                operation_id=imported.operation_id,
                import_schema=imported.schema_version,
                export_batch_id=imported.export_batch_id,
                dispositions=imported.dispositions,
                duplicate_group_count=imported.duplicate_group_count,
                replayed_count=imported.replayed_count,
                actor_count=imported.actor_count,
                account_count=imported.account_count,
                logical_sha256=imported.logical_sha256,
                record_id_set_sha256=imported.record_id_set_sha256,
                canonical_outcome_sha256=imported.canonical_outcome_sha256,
                source_master_instance_id=imported.master_instance_id,
                source_run_id=UUID(imported.run_id),
                source_epoch=imported.epoch,
                canonical_revision=imported.canonical_revision,
                checkpoint=checkpoint,
            )
        except Exception as exc:
            raise ProductionCapabilityBlocker(
                "FM16_H5_TERMINAL_EVIDENCE_INVALID",
                "H5 terminal receipt and checkpoint do not cross-bind",
            ) from exc

    async def _embedding_terminal(self, value: Mapping[str, Any]) -> EmbeddingTerminalEvidence:
        workers = _indexed_models(value.get("workers"), "FM18_19_WORKERS_INVALID")
        imports = _indexed_models(value.get("imports"), "FM18_19_IMPORTS_INVALID")
        coverage = _indexed_models(value.get("coverage"), "FM18_19_COVERAGE_INVALID")
        models: list[EmbeddingModelEvidence] = []
        for asset in WORKER_ASSETS:
            model_id = asset.model.exact_id
            worker, imported, covered = workers[model_id], imports[model_id], coverage[model_id]
            try:
                models.append(
                    EmbeddingModelEvidence(
                        model_exact_id=model_id,
                        task_run_id=worker.get("task_run_id"),
                        provider_ref=worker.get("provider_ref"),
                        provider_run_ref=worker.get("provider_run_ref"),
                        provider_kernel_id=worker.get("provider_kernel_id"),
                        source_sha256=worker.get("source_sha256"),
                        primary_source_sha256=worker.get("primary_source_sha256"),
                        artifact_id=imported.get("artifact_id"),
                        artifact_sha256=imported.get("artifact_sha256"),
                        inserted_count=imported.get("inserted_count"),
                        stale_count=imported.get("stale_count"),
                        failed_count=imported.get("failed_count"),
                        expected_documents=covered.get("expected_documents"),
                        completed_documents=covered.get("completed_documents"),
                        coverage=covered.get("coverage"),
                    )
                )
            except Exception as exc:
                raise ProductionCapabilityBlocker(
                    "FM18_19_MODEL_EVIDENCE_INVALID", "embedding model evidence is incomplete"
                ) from exc
        checkpoint_raw = value.get("checkpoint_receipt")
        if not isinstance(checkpoint_raw, Mapping):
            raise ProductionCapabilityBlocker(
                "FM18_19_CHECKPOINT_RECEIPT_ABSENT", "embedding checkpoint receipt is absent"
            )
        checkpoint_status = await self.mcp.call("reader", "checkpoint.status", {})
        checkpoint = _checkpoint_from_status(
            checkpoint_status, expected_id=str(checkpoint_raw.get("checkpoint_id") or "")
        )
        blogger = self._blogger_prerequisite
        if blogger is None:
            raise ProductionCapabilityBlocker(
                "FM18_19_BLOGGER_PREREQUISITE_ABSENT",
                "persisted H5 prerequisite is unavailable after resume",
            )
        try:
            return EmbeddingTerminalEvidence(
                request_id=value.get("request_id"),
                request_sha256=value.get("request_sha256"),
                blogger_export_batch_id=blogger.export_batch_id,
                blogger_canonical_revision=blogger.canonical_revision,
                canonical_revision=value.get("canonical_revision"),
                models=tuple(models),
                checkpoint=checkpoint,
            )
        except Exception as exc:
            raise ProductionCapabilityBlocker(
                "FM18_19_TERMINAL_EVIDENCE_INVALID", "embedding terminal evidence differs"
            ) from exc

    def _change_arguments(self, intent: FixedChangeIntent) -> dict[str, Any]:
        if intent.action == "insert":
            sql = INSERT_PROJECT_SQL
            parameters: list[Any] = [
                str(intent.fixture_project_id),
                f"fm21-{intent.fixture_project_id.hex}",
                "FM21 disposable acceptance fixture",
                "Deterministic disposable acceptance row; delete is mandatory.",
                "paused",
                {"acceptance_fixture_sha256": intent.fixture_sha256, "disposable": True},
            ]
        else:
            sql = DELETE_PROJECT_SQL
            parameters = [str(intent.fixture_project_id)]
        return {
            "sql": sql,
            "parameters": parameters,
            "expected_revision": intent.expected_revision,
            "max_affected_rows": 1,
            "idempotency_key": f"h6-fm21:{intent.idempotency_key_sha256}",
        }

    def _intent_for_preview(self, preview: ChangePreviewEvidence) -> FixedChangeIntent:
        fixture_id = self.plan.identity("fm21:project")
        fixture_sha = _sha_json(
            {
                "contract": "fm21_hub_project_fixture.v1",
                "matrix_id": str(self.plan.matrix_id),
                "project_id": str(fixture_id),
            }
        )
        suffix = "fm21:insert" if preview.action == "insert" else "fm21:delete"
        intent = FixedChangeIntent(
            action=preview.action,
            fixture_project_id=fixture_id,
            fixture_sha256=fixture_sha,
            expected_revision=preview.expected_revision,
            idempotency_key_sha256=self.plan.key_sha256(suffix),
        )
        if intent.request_sha256 != preview.request_sha256:
            raise ProductionCapabilityBlocker(
                "FM21_PERSISTED_PREVIEW_BINDING_MISMATCH", "persisted preview differs from fixture intent"
            )
        return intent


async def run_production_data_workload(
    *,
    plan: DataWorkloadPlan,
    store: AtomicJsonStateStore,
    gateway: ControlPlaneDataWorkloadGateway,
    owner_authorization: OwnerDuplicateAuthorization | None,
    timeout_seconds: int,
    poll_seconds: float,
) -> ProductionDataWorkloadReceipt:
    state = store.load(plan)
    gateway.bind_state(state)
    machine = DataWorkloadStateMachine(store)
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            result = await machine.advance(plan, state, gateway, owner_authorization=owner_authorization)
        except ProductionCapabilityBlocker as blocker:
            state = store.load(plan)
            return _production_receipt(plan, state, "BLOCKED", blocker_code=blocker.code)
        state = result.state
        if result.outcome != "PROGRESS":
            return _receipt_from_result(plan, result)
        await asyncio.sleep(min(poll_seconds, max(0.0, deadline - time.monotonic())))
    return _production_receipt(plan, state, "BLOCKED", blocker_code="DATA_WORKLOAD_DEADLINE_EXCEEDED")


def _receipt_from_result(plan: DataWorkloadPlan, result: DataWorkloadExecutionResult) -> ProductionDataWorkloadReceipt:
    return _production_receipt(
        plan,
        result.state,
        result.outcome,
        failure_code=result.failure_code,
        evidence=result.evidence,
    )


def _production_receipt(
    plan: DataWorkloadPlan,
    state: DataWorkloadState,
    outcome: Literal["PROGRESS", "AWAITING_OWNER_AUTHORIZATION", "FAIL", "BLOCKED", "EVIDENCE_READY"],
    *,
    blocker_code: str | None = None,
    failure_code: str | None = None,
    evidence: DataWorkloadEvidenceBundle | None = None,
) -> ProductionDataWorkloadReceipt:
    return ProductionDataWorkloadReceipt(
        matrix_id=plan.matrix_id,
        outcome=outcome,
        state_sha256=_sha_json(state.model_dump(mode="json")),
        blocker_code=blocker_code,
        failure_code=failure_code,
        evidence=evidence,
    )


def _authorization_from_envelope(
    envelope: BloggerDuplicateResolutionEnvelope,
) -> OwnerDuplicateAuthorization:
    identities = tuple(item.identity_sha256 for item in envelope.decisions)
    members = tuple(sorted({value for item in envelope.decisions for value in item.member_record_ids}))
    return OwnerDuplicateAuthorization(
        authorization_id=envelope.authorization_id,
        authorized_by_sha256=_sha_text(envelope.authorized_by),
        authorized_at=envelope.authorized_at,
        source_request_id=envelope.source_request_id,
        source_operation_id=envelope.source_operation_id,
        source_request_sha256=envelope.source_request_sha256,
        export_batch_id=envelope.export_batch_id,
        decision_count=len(envelope.decisions),
        identity_set_sha256=_sha_json(identities),
        member_record_id_set_sha256=_sha_json(members),
        envelope_sha256=envelope.envelope_sha256,
    )


def _checkpoint_from_status(value: Mapping[str, Any], *, expected_id: str) -> CheckpointEvidence:
    current = value.get("current")
    if (
        not isinstance(current, Mapping)
        or value.get("current_checkpoint_id") != expected_id
        or current.get("checkpoint_id") != expected_id
    ):
        raise ProductionCapabilityBlocker(
            "CHECKPOINT_HEAD_MISMATCH", "checkpoint HEAD differs from the exact operation receipt"
        )
    try:
        return CheckpointEvidence(
            checkpoint_id=current.get("checkpoint_id"),
            generation=value.get("generation"),
            exact_version_ref=current.get("exact_version_ref"),
            manifest_sha256=current.get("manifest_sha256"),
            canonical_revision=current.get("canonical_revision"),
            status=current.get("status"),
        )
    except Exception as exc:
        raise ProductionCapabilityBlocker(
            "CHECKPOINT_EVIDENCE_INVALID", "verified checkpoint metadata is incomplete"
        ) from exc


def _indexed_models(value: Any, code: str) -> dict[str, Mapping[str, Any]]:
    if not isinstance(value, list) or len(value) != 2 or any(not isinstance(item, Mapping) for item in value):
        raise ProductionCapabilityBlocker(code, "two-model metadata is absent")
    indexed = {str(item.get("model_exact_id")): item for item in value}
    if set(indexed) != {E5_EXACT_ID, BGE_EXACT_ID}:
        raise ProductionCapabilityBlocker(code, "two-model metadata differs from pinned model IDs")
    return indexed


def _decode_metadata(raw: bytes, source: str) -> dict[str, Any]:
    if len(raw) > MAX_METADATA_BYTES:
        raise ProductionCapabilityBlocker(f"{source.upper()}_METADATA_OVERSIZED", f"{source} response exceeds 256 KiB")
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProductionCapabilityBlocker(
            f"{source.upper()}_METADATA_INVALID", f"{source} response is not JSON"
        ) from exc
    if not isinstance(value, Mapping):
        raise ProductionCapabilityBlocker(f"{source.upper()}_METADATA_INVALID", f"{source} response is not an object")
    return _bounded_mapping(value, source)


def _bounded_mapping(value: Mapping[str, Any], source: str) -> dict[str, Any]:
    encoded = canonical_json_bytes(dict(value))
    if len(encoded) > MAX_METADATA_BYTES:
        raise ProductionCapabilityBlocker(f"{source.upper()}_METADATA_OVERSIZED", f"{source} response exceeds 256 KiB")
    lowered = encoded.lower()
    for forbidden in (b"postgresql://", b"postgres://", b"-----begin private key", b'"vector":'):
        if forbidden in lowered:
            raise ProductionCapabilityBlocker(
                f"{source.upper()}_DATA_PLANE_LEAK", f"{source} response contains forbidden data-plane material"
            )
    return dict(value)


def _sha_json(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _sha_text(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _sha_value(value: Any, code: str) -> str:
    text = str(value or "")
    if len(text) != 64 or any(char not in "0123456789abcdef" for char in text):
        raise ProductionCapabilityBlocker(code, "required SHA-256 metadata is absent")
    return text


def _uuid(value: Any, code: str) -> UUID:
    try:
        return UUID(str(value))
    except ValueError as exc:
        raise ProductionCapabilityBlocker(code, "required UUID metadata is absent") from exc
