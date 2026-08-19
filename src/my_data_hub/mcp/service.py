from __future__ import annotations

import inspect
import json
import time
from collections.abc import Callable, Mapping
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from my_data_hub.auth.context import current_identity
from my_data_hub.auth.control import OAuthAuditEvent
from my_data_hub.domain.commands import SemanticCommand
from my_data_hub.mcp.catalog import TOOL_CONTRACTS
from my_data_hub.mcp.contracts import (
    ControlPlaneReader,
    EnsureMasterReceipt,
    ExecutionLimits,
    MasterResolver,
    MasterSession,
    MasterSessionBroker,
    MasterSnapshot,
    MasterState,
    MCPAuditSink,
    SessionRequest,
    WriteGate,
    WritePermit,
)
from my_data_hub.mcp.oauth import AccessIdentity
from my_data_hub.mcp.postgres_broker import SessionBrokerError
from my_data_hub.mcp.region_talk_schemas import (
    RegionTalkPipelineController,
    RegionTalkPipelineRunRequest,
    validate_region_talk_arguments,
)
from my_data_hub.mcp.sql_policy import BoundedSQLPolicy
from my_data_hub.workloads.bloggers.discovery import validate_submit_discovery_batch


class SemanticCommandError(RuntimeError):
    pass


class HubPermissionError(PermissionError):
    pass


class MasterUnavailableError(RuntimeError):
    pass


_CONTROL_TOOLS = frozenset(
    {
        "operation.get",
        "checkpoint.status",
        "connector.coverage",
        "runtime.stale_epoch.probe",
        "provider.protected_resource.probe",
        "checkpoint.restore.request",
        "master.rotation.request",
        "embedding.production.capabilities",
        "provider.resources.status",
        "provider.resources.read",
        "provider.resources.list",
        "provider.resources.download",
        "provider.inventory.live",
        "provider.upload.status",
        "provider.acceptance.claim.get",
        "runtime.events.history",
        "acceptance.scenario.request",
        "acceptance.scenario.status",
        "data.change.status",
        "bloggers.import.status",
        "region_talk.pipeline.status",
    }
)
_PROVIDER_WRITES = frozenset(
    {
        "provider.resources.create",
        "provider.resources.version",
        "provider.resources.run",
        "provider.resources.delete",
        "provider.upload.start",
        "provider.upload.put_chunk",
        "provider.upload.finalize",
        "provider.upload.abort",
        "provider.acceptance.dataset.lifecycle",
        "provider.acceptance.notebook.lifecycle",
        "provider.acceptance.claim.cleanup",
    }
)
_DURABLE_WRITE_STATES = frozenset(
    {
        "COMMITTED_PENDING_CHECKPOINT",
        "CHECKPOINTING",
        "CHECKPOINT_VERIFIED",
        "DURABLE_COMPLETE",
    }
)


async def _await(value: Any) -> Any:
    return await value if inspect.isawaitable(value) else value


class HubService:
    """Identity-bound MCP application service with no static database URL.

    The resolver and broker are injected control/data-plane boundaries.  A
    request first resolves an ACTIVE epoch, then obtains a short-lived role
    session for that exact epoch.  Status remains available with no resolver,
    broker or master runtime.
    """

    def __init__(
        self,
        resolver: MasterResolver | object | None = None,
        *,
        broker: MasterSessionBroker | None = None,
        control: ControlPlaneReader | None = None,
        write_gate: WriteGate | None = None,
        audit: MCPAuditSink | None = None,
        sql_policy: BoundedSQLPolicy | None = None,
        region_talk_controller: RegionTalkPipelineController | None = None,
        identity_provider: Callable[[], AccessIdentity | None] = current_identity,
        fallback_identity: AccessIdentity | None = None,
        scopes: frozenset[str] | None = None,
        write_enabled: bool | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        # ``object`` keeps old helper-only construction source-compatible; it
        # is never treated as a resolver unless it implements the protocol.
        self.resolver = resolver if isinstance(resolver, MasterResolver) else None
        self.broker = broker
        self.control = control
        self.write_gate = write_gate
        self.audit = audit
        self.sql_policy = sql_policy or BoundedSQLPolicy()
        self.region_talk_controller = region_talk_controller
        self.identity_provider = identity_provider
        self.fallback_identity = fallback_identity
        self.scopes = scopes or frozenset()
        self.write_enabled = bool(write_enabled)
        self.clock = clock

    def identity(self) -> AccessIdentity:
        identity = self.identity_provider() or self.fallback_identity
        if identity is None:
            raise HubPermissionError("OAuth authentication is required")
        return identity

    def _require(self, scope: str) -> AccessIdentity | None:
        identity = self.identity_provider() or self.fallback_identity
        granted = identity.scopes if identity is not None else self.scopes
        if scope not in granted:
            raise HubPermissionError(f"MCP scope required: {scope}")
        return identity

    def _require_write(self, scope: str) -> AccessIdentity | None:
        identity = self._require(scope)
        # Legacy helper-only tests use this flag. Operational write tools do
        # not consult it: they require an injected WriteGate instead.
        if identity is None and not self.write_enabled:
            raise HubPermissionError("MCP writes are disabled by configuration")
        return identity

    async def invoke(self, tool: str, arguments: Mapping[str, Any]) -> dict[str, Any]:
        contract = TOOL_CONTRACTS.get(tool)
        if contract is None:
            raise ValueError("unknown MCP tool")
        identity = self.identity()
        if contract.scope not in identity.scopes:
            raise HubPermissionError(f"MCP scope required: {contract.scope}")
        bounded_arguments = self._bounded_arguments(
            arguments,
            max_bytes=(
                1_000_000
                if tool == "submit_discovery_batch"
                else 512 * 1024
                if tool in {"provider.resources.create", "provider.resources.version", "provider.upload.start"}
                else 256 * 1024
            ),
        )
        if tool.startswith("region_talk."):
            bounded_arguments = validate_region_talk_arguments(tool, bounded_arguments)
        try:
            if tool == "platform.status":
                result = await self._platform_status(identity)
            elif tool == "master.status":
                result = (await self._resolve(identity)).public()
            elif tool == "master.ensure":
                result = (await self._ensure(identity, intent="explicit-mcp-request")).public()
            elif tool == "runtime.stale_epoch.probe":
                result = await self._stale_epoch_probe(bounded_arguments, identity)
            elif tool in _CONTROL_TOOLS:
                result = await self._control(tool, bounded_arguments, identity)
            elif tool == "region_talk.pipeline.run":
                result = await self._region_talk_pipeline_run(bounded_arguments, identity)
            elif tool in _PROVIDER_WRITES:
                result = await self._provider_write(tool, bounded_arguments, identity)
            elif tool == "submit_discovery_batch":
                result = await self._submit_discovery_batch(bounded_arguments, identity)
            elif not contract.read_only:
                result = await self._write(tool, bounded_arguments, identity)
            else:
                result = await self._read(tool, bounded_arguments, identity, role=contract.role)
        except Exception:
            await self._audit(identity, tool, {}, outcome="denied_or_failed")
            raise
        await self._audit(identity, tool, result, outcome="accepted")
        return self._bounded_result(result)

    async def _platform_status(self, identity: AccessIdentity) -> dict[str, Any]:
        snapshot = await self._resolve(identity)
        base: dict[str, Any] = {
            "control_plane_ready": True,
            **snapshot.public(),
            "canonical_database_location": "kaggle-master-only",
        }
        if self.control is not None:
            observed = await _await(self.control.invoke_control("platform.status", {}, identity))
            base.update(dict(observed))
            # Control implementations may enrich status but cannot claim a
            # different master state than the authoritative resolver.
            base["master_state"] = snapshot.state.value
        return base

    async def _resolve(self, identity: AccessIdentity) -> MasterSnapshot:
        if self.resolver is None:
            return MasterSnapshot(MasterState.ABSENT)
        snapshot = await _await(self.resolver.resolve_master(identity))
        if not isinstance(snapshot, MasterSnapshot):
            raise MasterUnavailableError("master resolver returned an invalid snapshot")
        return snapshot

    async def _ensure(self, identity: AccessIdentity, *, intent: str) -> EnsureMasterReceipt:
        if self.resolver is None:
            raise MasterUnavailableError("master resolver is not configured")
        receipt = await _await(self.resolver.ensure_master(identity, intent=intent))
        if not isinstance(receipt, EnsureMasterReceipt):
            raise MasterUnavailableError("ensure_master returned an invalid receipt")
        return receipt

    async def _active_or_operation(
        self, identity: AccessIdentity, *, intent: str
    ) -> MasterSnapshot | dict[str, Any]:
        snapshot = await self._resolve(identity)
        if snapshot.state is MasterState.ACTIVE:
            return snapshot
        if snapshot.state is MasterState.ABSENT:
            return (await self._ensure(identity, intent=intent)).public()
        if snapshot.operation_id:
            result = {
                **snapshot.public(),
                "terminal": snapshot.state
                in {
                    MasterState.FAILED,
                    MasterState.FENCED,
                    MasterState.CHECKPOINT_FAILED,
                    MasterState.ORPHANED,
                },
            }
            if not result["terminal"]:
                result.update(
                    outcome="WAITING_FOR_MASTER",
                    retryable=True,
                    continuation={
                        "operation_id": snapshot.operation_id,
                        "status_tool": "operation.get",
                        "retry_original_request_when": "state=ACTIVE",
                    },
                )
            return result
        raise MasterUnavailableError("non-ACTIVE master state has no durable operation_id")

    async def _control(
        self, tool: str, arguments: Mapping[str, Any], identity: AccessIdentity
    ) -> dict[str, Any]:
        if self.control is None:
            raise MasterUnavailableError("control-ledger reader is not configured")
        result = await _await(self.control.invoke_control(tool, arguments, identity))
        if tool == "data.change.status" and result.get("state") == "APPLYING":
            snapshot = await self._resolve(identity)
            if snapshot.state is MasterState.ACTIVE:
                reconciled = await self._reconcile_pending_write(
                    identity=identity,
                    master=snapshot,
                    operation_id=str(arguments.get("operation_id", "")),
                    deny_if_absent=False,
                )
                if reconciled is not None and reconciled.get("found") is not False:
                    result = await _await(self.control.invoke_control(tool, arguments, identity))
                elif reconciled is not None:
                    result = {
                        **result,
                        "retry_allowed": False,
                        "reconciliation_required": True,
                        "canonical_receipt_found": False,
                    }
        if tool == "bloggers.import.status" and result.get("state") == "APPLYING":
            snapshot = await self._resolve(identity)
            if snapshot.state is MasterState.ACTIVE:
                reconciled = await self._reconcile_pending_blogger_import(
                    identity=identity,
                    master=snapshot,
                    operation_id=str(arguments.get("operation_id", "")),
                    deny_if_absent=False,
                )
                if reconciled is not None and reconciled.get("found") is not False:
                    result = await _await(self.control.invoke_control(tool, arguments, identity))
                elif reconciled is not None:
                    result = {
                        **result,
                        "retry_allowed": False,
                        "reconciliation_required": True,
                        "canonical_receipt_found": False,
                    }
        return dict(result)

    async def _region_talk_pipeline_run(
        self,
        arguments: Mapping[str, Any],
        identity: AccessIdentity,
    ) -> dict[str, Any]:
        """Request one metadata-only supervised run; never dispatch publication."""

        if self.region_talk_controller is None:
            raise HubPermissionError("Region Talk pipeline operation is not enabled")
        request = RegionTalkPipelineRunRequest.model_validate(arguments)
        result = await _await(
            self.region_talk_controller.request_supervised_run(
                request=request,
                principal=identity,
            )
        )
        if not isinstance(result, Mapping):
            raise RuntimeError("Region Talk pipeline controller returned an invalid receipt")
        operation_id = result.get("operation_id")
        duplicate = result.get("duplicate")
        if not isinstance(operation_id, str) or not operation_id or not isinstance(duplicate, bool):
            raise RuntimeError("Region Talk pipeline receipt lacks idempotent operation metadata")
        if result.get("idempotency_key") not in {None, request.idempotency_key}:
            raise RuntimeError("Region Talk pipeline receipt changed the idempotency key")
        if result.get("publication_dispatch") not in {None, False}:
            raise RuntimeError("Region Talk pipeline controller attempted publication dispatch")
        return {
            **dict(result),
            "idempotency_key": request.idempotency_key,
            "project_slug": request.project_slug,
            "mode": request.mode,
            "source_revision": request.source_revision,
            "publication_dispatch": False,
        }

    async def _submit_discovery_batch(
        self, arguments: Mapping[str, Any], identity: AccessIdentity
    ) -> dict[str, Any]:
        raw = arguments.get("payload")
        if not isinstance(raw, Mapping):
            raise ValueError("submit_discovery_batch requires one closed payload object")
        request = validate_submit_discovery_batch(raw)
        active = await self._active_or_operation(
            identity, intent=f"mcp-blogger-intake:{request.batch_id}"
        )
        if isinstance(active, dict):
            return {
                **active,
                "batch_id": str(request.batch_id),
                "request_sha256": request.request_sha256,
                "continuation": {
                    **dict(active.get("continuation", {})),
                    "retry_tool": "submit_discovery_batch",
                },
            }
        return await self._execute_session(
            "submit_discovery_batch",
            {"payload": request.model_dump(mode="json", exclude_none=True)},
            identity,
            active,
            role="connector",
        )

    async def _stale_epoch_probe(
        self, arguments: Mapping[str, Any], identity: AccessIdentity
    ) -> dict[str, Any]:
        """Exercise the real credential/session/fencing path without a write."""

        snapshot = await self._resolve(identity)
        supplied = arguments.get("submitted_epoch")
        expected = arguments.get("expected_active_epoch")
        if (
            snapshot.state is not MasterState.ACTIVE
            or snapshot.epoch is None
            or snapshot.instance_id is None
            or expected != snapshot.epoch
            or not isinstance(supplied, int)
            or isinstance(supplied, bool)
            or supplied >= snapshot.epoch
            or self.broker is None
        ):
            return {
                "evaluated": False,
                "denied": False,
                "mutation_attempted": False,
                "blocker_code": "STALE_EPOCH_ADMISSION_PATH_UNAVAILABLE",
            }
        limits = ExecutionLimits(timeout_ms=3_000, max_rows=1, max_bytes=4_096)
        current_request = SessionRequest(
            principal=identity,
            master_instance_id=snapshot.instance_id,
            epoch=snapshot.epoch,
            role="operator",
            tool="runtime.stale_epoch.probe",
            limits=limits,
        )
        current_session = None
        try:
            current_session = await _await(self.broker.issue_session(current_request))
            await _await(current_session.execute({"expected_active_epoch": snapshot.epoch}))
        except Exception:
            return {
                "evaluated": False,
                "denied": False,
                "mutation_attempted": False,
                "blocker_code": "STALE_EPOCH_ADMISSION_PATH_UNAVAILABLE",
            }
        finally:
            if current_session is not None:
                await _await(current_session.close())
        request = SessionRequest(
            principal=identity,
            master_instance_id=snapshot.instance_id,
            epoch=supplied,
            role="operator",
            tool="runtime.stale_epoch.probe",
            limits=limits,
        )
        session = None
        try:
            session = await _await(self.broker.issue_session(request))
            await _await(session.execute({"expected_active_epoch": snapshot.epoch}))
        except SessionBrokerError:
            return {
                "evaluated": True,
                "denied": True,
                "mutation_attempted": False,
                "reason_code": "STALE_EPOCH",
            }
        except Exception:
            return {
                "evaluated": False,
                "denied": False,
                "mutation_attempted": False,
                "blocker_code": "STALE_EPOCH_ADMISSION_PATH_UNAVAILABLE",
            }
        finally:
            if session is not None:
                await _await(session.close())
        return {
            "evaluated": True,
            "denied": False,
            "mutation_attempted": False,
            "reason_code": "STALE_EPOCH_WAS_ADMITTED",
        }

    async def _read(
        self,
        tool: str,
        arguments: Mapping[str, Any],
        identity: AccessIdentity,
        *,
        role: str,
    ) -> dict[str, Any]:
        if tool == "data.query":
            self.sql_policy.classify_read(
                str(arguments.get("sql", "")), self._parameters(arguments)
            )
        active = await self._active_or_operation(identity, intent=f"mcp-read:{tool}")
        if isinstance(active, dict):
            return active
        return await self._execute_session(tool, arguments, identity, active, role=role)

    async def _write(
        self, tool: str, arguments: Mapping[str, Any], identity: AccessIdentity
    ) -> dict[str, Any]:
        if tool in {"data.change.preview", "data.change.apply"}:
            self.sql_policy.classify_change(
                str(arguments.get("sql", "")), self._parameters(arguments)
            )
            self._require_write_contract(arguments, apply=tool.endswith(".apply"))
        if tool == "bloggers.import.apply":
            replay_reader = getattr(self.write_gate, "blogger_apply_replay", None)
            if replay_reader is None:
                raise HubPermissionError("blogger apply replay gate is not configured")
            replay = await _await(replay_reader(principal=identity, arguments=arguments))
            if replay is not None:
                self._require_durable_operation(replay)
                return dict(replay)
        prepared: Mapping[str, Any] | None = None
        if tool == "bloggers.import.preview":
            prepare = getattr(self.write_gate, "prepare_blogger_import", None)
            if prepare is None:
                raise HubPermissionError("blogger import lifecycle gate is not configured")
            prepared = await _await(prepare(principal=identity, arguments=arguments))
        active = await self._active_or_operation(identity, intent=f"mcp-write:{tool}")
        if isinstance(active, dict):
            if prepared is not None:
                marker = getattr(self.write_gate, "mark_blogger_import_waiting_master", None)
                if marker is None:
                    raise HubPermissionError("blogger cold-start continuation gate is not configured")
                record = await _await(marker(operation_id=str(prepared["operation_id"])))
                return {
                    **active,
                    "operation_id": str(record["operation_id"]),
                    "master_operation_id": active.get("operation_id"),
                    "status": str(record["state"]),
                    "request_sha256": str(record["request_sha256"]),
                    "continuation": {
                        "operation_id": str(record["operation_id"]),
                        "status_tool": "bloggers.import.status",
                        "retry_original_request_when": "master_state=ACTIVE",
                    },
                }
            return active
        if tool == "bloggers.import.apply":
            reconciled = await self._reconcile_pending_blogger_import(
                identity=identity,
                master=active,
                arguments=arguments,
                deny_if_absent=True,
            )
            if reconciled is not None:
                self._require_durable_operation(reconciled)
                return reconciled
        if tool == "data.change.apply":
            reconciled = await self._reconcile_pending_write(
                identity=identity,
                master=active,
                arguments=arguments,
                deny_if_absent=True,
            )
            if reconciled is not None:
                self._require_durable_operation(reconciled)
                return reconciled
        permit = await self._permit(tool, arguments, identity, active)
        if tool.startswith("bloggers.import."):
            builder = getattr(self.write_gate, "blogger_broker_arguments", None)
            if builder is None:
                raise HubPermissionError("blogger import broker binding is not configured")
            enriched = dict(await _await(builder(permit=permit, arguments=arguments)))
        else:
            enriched = {**arguments, "_write_permit": self._permit_public(permit)}
        result = await self._execute_session(
            tool,
            enriched,
            identity,
            active,
            role=TOOL_CONTRACTS[tool].role,
        )
        recorder = getattr(self.write_gate, "record_write_result", None)
        if recorder is None:
            raise HubPermissionError("write gate cannot durably record the write result")
        result = dict(await _await(recorder(permit=permit, result=result)))
        if tool.endswith(".apply"):
            self._require_durable_operation(result)
        return result

    async def _reconcile_pending_blogger_import(
        self,
        *,
        identity: AccessIdentity,
        master: MasterSnapshot,
        operation_id: str | None = None,
        arguments: Mapping[str, Any] | None = None,
        deny_if_absent: bool,
    ) -> dict[str, Any] | None:
        request_builder = getattr(self.write_gate, "blogger_reconciliation_request", None)
        recorder = getattr(self.write_gate, "record_reconciled_blogger_import", None)
        if request_builder is None or recorder is None:
            return None
        request = await _await(
            request_builder(
                principal=identity,
                master=master,
                operation_id=operation_id,
                arguments=arguments,
            )
        )
        if request is None:
            return None
        receipt = await self._execute_session(
            "bloggers.import.reconcile",
            request,
            identity,
            master,
            role="canonical_committer",
        )
        if receipt.get("found") is not True:
            if deny_if_absent:
                raise HubPermissionError(
                    "blogger apply retry remains denied until the exact canonical receipt is reconciled"
                )
            return {"found": False}
        return dict(
            await _await(
                recorder(operation_id=str(request["operation_id"]), receipt=receipt)
            )
        )

    async def _reconcile_pending_write(
        self,
        *,
        identity: AccessIdentity,
        master: MasterSnapshot,
        operation_id: str | None = None,
        arguments: Mapping[str, Any] | None = None,
        deny_if_absent: bool,
    ) -> dict[str, Any] | None:
        request_builder = getattr(self.write_gate, "reconciliation_request", None)
        recorder = getattr(self.write_gate, "record_reconciled_write", None)
        if request_builder is None or recorder is None:
            return None
        request = await _await(
            request_builder(
                principal=identity,
                master=master,
                operation_id=operation_id,
                arguments=arguments,
            )
        )
        if request is None:
            return None
        receipt = await self._execute_session(
            "data.change.reconcile",
            request,
            identity,
            master,
            role="operator",
        )
        if receipt.get("found") is not True:
            if deny_if_absent:
                raise HubPermissionError(
                    "apply retry remains denied until the exact canonical receipt is reconciled"
                )
            return {"found": False}
        return dict(
            await _await(
                recorder(operation_id=str(request["operation_id"]), receipt=receipt)
            )
        )

    async def _provider_write(
        self, tool: str, arguments: Mapping[str, Any], identity: AccessIdentity
    ) -> dict[str, Any]:
        if self.control is None:
            raise MasterUnavailableError("provider control gateway is not configured")
        acceptance_tool = tool.startswith("provider.acceptance.")
        resource_class = "mcp_managed" if acceptance_tool else str(arguments.get("control_class", ""))
        if resource_class not in {"mcp_managed", "mcp_exchange"}:
            raise HubPermissionError("provider mutation is limited to MCP-owned control classes")
        if not acceptance_tool and arguments.get("private") is not True:
            raise HubPermissionError("public provider resources are forbidden")
        if tool == "provider.resources.run" and resource_class != "mcp_managed":
            raise HubPermissionError("only mcp_managed notebooks may run")
        snapshot = await self._resolve(identity)
        permit = await self._permit(tool, arguments, identity, snapshot)
        if permit.allowed_resource_class != resource_class or not permit.private_resource_only:
            raise HubPermissionError("write gate did not bind the provider resource policy")
        result = await _await(
            self.control.invoke_control(
                tool, {**arguments, "_write_permit": self._permit_public(permit)}, identity
            )
        )
        return dict(result)

    async def _permit(
        self,
        tool: str,
        arguments: Mapping[str, Any],
        identity: AccessIdentity,
        master: MasterSnapshot,
    ) -> WritePermit:
        if self.write_gate is None:
            raise HubPermissionError("remote writes are fail-closed: write gate is not configured")
        permit = await _await(
            self.write_gate.authorize_write(
                principal=identity, tool=tool, arguments=arguments, master=master
            )
        )
        if not isinstance(permit, WritePermit):
            raise HubPermissionError("write gate returned no valid permit")
        provider_only = (
            permit.canonical_data_independent
            and tool in _PROVIDER_WRITES
            and permit.master_epoch == 0
            and permit.canonical_revision == 0
            and not permit.checkpoint_lifecycle_bound
            and not permit.pre_change_checkpoint_verified
        )
        if (
            permit.tool != tool
            or permit.principal != identity.subject
            or permit.client_id != identity.client_id
            or (not provider_only and permit.master_epoch != (master.epoch or 0))
            or permit.expires_at <= int(self.clock())
            or (not provider_only and not permit.checkpoint_lifecycle_bound)
        ):
            raise HubPermissionError("write permit binding is invalid or expired")
        if tool.endswith(".apply") and not permit.preview_bound:
            raise HubPermissionError("apply requires an exact bound preview receipt")
        if (
            TOOL_CONTRACTS[tool].destructive
            and not permit.pre_change_checkpoint_verified
            and not provider_only
        ):
            raise HubPermissionError("destructive write requires a verified pre-change checkpoint")
        return permit

    async def _execute_session(
        self,
        tool: str,
        arguments: Mapping[str, Any],
        identity: AccessIdentity,
        master: MasterSnapshot,
        *,
        role: str,
    ) -> dict[str, Any]:
        if self.broker is None or master.instance_id is None or master.epoch is None:
            raise MasterUnavailableError("master session broker is not configured")
        limits = ExecutionLimits(
            timeout_ms=min(int(arguments.get("timeout_ms", 5_000)), 30_000),
            max_rows=min(int(arguments.get("limit", arguments.get("max_rows", 200))), 1_000),
            max_bytes=min(int(arguments.get("max_bytes", 262_144)), 2_097_152),
        )
        session = await _await(
            self.broker.issue_session(
                SessionRequest(
                    principal=identity,
                    master_instance_id=master.instance_id,
                    epoch=master.epoch,
                    role=role,
                    tool=tool,
                    limits=limits,
                    canonical_revision=master.canonical_revision,
                )
            )
        )
        if not isinstance(session, MasterSession):
            raise MasterUnavailableError("session broker returned an invalid session")
        try:
            result = await session.execute(arguments)
            response = dict(result)
        finally:
            await session.close()
        if response.get("master_epoch") != master.epoch:
            raise MasterUnavailableError("master response epoch did not match the resolved epoch")
        if master.canonical_revision is not None and response.get("canonical_revision") is None:
            raise MasterUnavailableError("master response omitted canonical revision")
        return response

    async def _audit(
        self,
        identity: AccessIdentity,
        tool: str,
        result: Mapping[str, Any],
        *,
        outcome: str,
    ) -> None:
        if self.audit is None:
            return
        await _await(
            self.audit.record_mcp_audit(
                OAuthAuditEvent(
                    event="mcp_tool",
                    outcome=outcome,
                    issuer=identity.issuer,
                    client_id=identity.client_id,
                    subject=identity.subject,
                    token_id=identity.token_id,
                    tool=tool,
                    operation_id=(str(result["operation_id"]) if result.get("operation_id") else None),
                    master_epoch=(int(result["master_epoch"]) if result.get("master_epoch") else None),
                    canonical_revision=(
                        int(result["canonical_revision"])
                        if result.get("canonical_revision") is not None
                        else None
                    ),
                )
            )
        )

    @staticmethod
    def _bounded_arguments(
        arguments: Mapping[str, Any], *, max_bytes: int = 262_144
    ) -> dict[str, Any]:
        if not isinstance(arguments, Mapping):
            raise ValueError("tool arguments must be an object")
        encoded = json.dumps(arguments, ensure_ascii=False, separators=(",", ":"), default=str).encode()
        if len(encoded) > max_bytes:
            raise ValueError("tool arguments exceed the semantic body limit")
        return dict(arguments)

    @staticmethod
    def _bounded_result(result: Mapping[str, Any]) -> dict[str, Any]:
        encoded = json.dumps(result, ensure_ascii=False, separators=(",", ":"), default=str).encode()
        if len(encoded) > 2_097_152:
            raise RuntimeError("tool result exceeds the bounded response limit")
        return dict(result)

    @staticmethod
    def _parameters(arguments: Mapping[str, Any]) -> list[Any]:
        raw = arguments.get("parameters", [])
        if not isinstance(raw, list):
            raise ValueError("parameters must be a JSON array")
        return raw

    @staticmethod
    def _require_write_contract(arguments: Mapping[str, Any], *, apply: bool) -> None:
        expected = arguments.get("expected_revision")
        if isinstance(expected, bool) or not isinstance(expected, int) or expected < 0:
            raise ValueError("expected_revision is required")
        key = arguments.get("idempotency_key")
        if not isinstance(key, str) or not 8 <= len(key) <= 300:
            raise ValueError("a bounded idempotency_key is required")
        impact = arguments.get("max_affected_rows")
        if isinstance(impact, bool) or not isinstance(impact, int) or not 1 <= impact <= 1_000:
            raise ValueError("max_affected_rows must be between 1 and 1000")
        if apply and not isinstance(arguments.get("preview_receipt"), str):
            raise ValueError("apply requires a preview_receipt")

    @staticmethod
    def _permit_public(permit: WritePermit) -> dict[str, Any]:
        return {
            "permit_id": permit.permit_id,
            "tool": permit.tool,
            "master_epoch": permit.master_epoch,
            "canonical_revision": permit.canonical_revision,
            "expires_at": permit.expires_at,
        }

    @staticmethod
    def _require_durable_operation(result: Mapping[str, Any]) -> None:
        if not result.get("operation_id") or result.get("status") not in _DURABLE_WRITE_STATES:
            raise RuntimeError("write result is not bound to the checkpoint durability lifecycle")

    # Retained pure compatibility helpers; they perform no database access.
    @staticmethod
    def _normalize_url(raw: str) -> str:
        raw = raw.strip()
        if not raw or len(raw) > 4000:
            raise ValueError("url is empty or too long")
        parsed = urlsplit(raw)
        if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
            raise ValueError("url must be absolute HTTP(S)")
        host = parsed.hostname.lower() if parsed.hostname else ""
        port = parsed.port
        default = (parsed.scheme.lower() == "http" and port == 80) or (
            parsed.scheme.lower() == "https" and port == 443
        )
        netloc = host if port is None or default else f"{host}:{port}"
        return urlunsplit((parsed.scheme.lower(), netloc, parsed.path or "/", parsed.query, ""))

    @staticmethod
    def _validate_revision(command: SemanticCommand, canonical_revision: int) -> None:
        if command.base_revision > canonical_revision:
            raise SemanticCommandError("command base revision is ahead of canonical state")
        if command.expected_revision is not None and command.expected_revision != canonical_revision:
            raise SemanticCommandError(
                f"expected revision {command.expected_revision} does not match canonical revision {canonical_revision}"
            )

    def _apply_supported_command(
        self, cursor: Any, command: SemanticCommand, revision: int, *, preview_only: bool
    ) -> dict[str, Any]:
        if command.command_type != "region_talk.work.enqueue":
            raise SemanticCommandError(f"unsupported bootstrap command type: {command.command_type}")
        stage = str(command.payload.get("stage", "")).strip()
        cursor.execute(
            "SELECT pipeline_id, stage_id, project_id, status, enabled, compute_lane FROM bounded_stage WHERE stage=%s",
            (stage,),
        )
        row = cursor.fetchone()
        if row is None:
            raise SemanticCommandError(f"Region Talk stage not found: {stage}")
        pipeline_status, enabled, lane = str(row[3]), bool(row[4]), str(row[5])
        blocked = lane == "local-side-effect"
        return {
            "would_enqueue": pipeline_status == "active" and enabled and not blocked,
            "stage": stage,
            "subject_id": str(command.payload.get("subject_id")),
            "dedupe_key": str(command.payload.get("dedupe_key") or command.input_fingerprint),
            "pipeline_status": pipeline_status,
            "stage_enabled": enabled,
            "compute_lane": lane,
            "side_effect_stage_blocked": blocked,
            "canonical_change": False,
        }
