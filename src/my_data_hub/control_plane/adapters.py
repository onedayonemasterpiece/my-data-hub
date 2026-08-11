from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from typing import Any

from my_data_hub.auth.control import (
    OAuthAuditEvent,
    OAuthClientRecord,
    OAuthRevocationQuery,
)
from my_data_hub.control_plane.ledger import ControlLedger
from my_data_hub.mcp.contracts import (
    ControlPlaneReader,
    EnsureMasterReceipt,
    MasterResolver,
    MasterSnapshot,
    MasterState,
)
from my_data_hub.mcp.oauth import AccessIdentity
from my_data_hub.orchestrator.master import MasterCoordinator


def _revocation_reference(query: OAuthRevocationQuery) -> str:
    return json.dumps(asdict(query), sort_keys=True, separators=(",", ":"))


class ControlLedgerOAuthAuthority:
    """OAuth revocation/client/audit authority that remains available with no master."""

    def __init__(self, ledger: ControlLedger) -> None:
        self.ledger = ledger

    def is_revoked(self, query: OAuthRevocationQuery) -> bool:
        return self.ledger.is_oauth_reference_revoked(_revocation_reference(query))

    def get_client(self, issuer: str, client_id: str) -> OAuthClientRecord | None:
        row = self.ledger.oauth_client(issuer, client_id)
        if row is None:
            return None
        return OAuthClientRecord(
            issuer=issuer,
            client_id=client_id,
            enabled=bool(row["enabled"]),
            allowed_scopes=frozenset(row["allowed_scopes"]),
        )

    def record_oauth_audit(self, event: OAuthAuditEvent) -> None:
        self.ledger.append_audit(
            action=f"oauth:{event.event}:{event.outcome}",
            audit_ref=hashlib.sha256(repr(event).encode()).hexdigest(),
            principal_id=event.subject,
            client_id=event.client_id,
            operation_id=event.operation_id,
            epoch=event.master_epoch,
            revision=event.canonical_revision,
            metadata={"issuer": event.issuer, "tool": event.tool},
        )

    def record_mcp_audit(self, event: OAuthAuditEvent) -> None:
        self.record_oauth_audit(event)


class LedgerControlReader(ControlPlaneReader):
    def __init__(self, ledger: ControlLedger, *, deployed_commit: str | None = None) -> None:
        if deployed_commit is not None and (
            len(deployed_commit) != 40
            or any(character not in "0123456789abcdef" for character in deployed_commit)
        ):
            raise ValueError("deployed commit must be an exact lowercase Git SHA")
        self.ledger = ledger
        self.deployed_commit = deployed_commit

    def invoke_control(
        self, tool: str, arguments: dict[str, Any], principal: AccessIdentity
    ) -> dict[str, Any]:
        if tool == "platform.status":
            return {
                "control_plane_ready": True,
                "control_ledger": "sqlite-wal",
                **({"deployed_commit": self.deployed_commit} if self.deployed_commit else {}),
            }
        if tool in {"operation.get", "data.change.status"}:
            record = self.ledger.get_operation(str(arguments.get("operation_id", "")))
            return {"found": False} if record is None else {
                "found": True,
                "operation_id": record.operation_id,
                "operation_kind": record.operation_kind,
                "state": record.state,
                "updated_at": record.updated_at.isoformat(),
            }
        if tool == "checkpoint.status":
            head = self.ledger.checkpoint_head("postgres-master")
            return {
                "current_checkpoint_id": head.current_checkpoint_id if head else None,
                "previous_checkpoint_id": head.previous_checkpoint_id if head else None,
                "freshness": "absent" if head is None else "recorded",
            }
        if tool == "provider.resources.status":
            limit = min(int(arguments.get("limit", 100)), 100)
            resources = self.ledger.list_provider_resources(limit=limit)
            return {"resources": resources, "count": len(resources), "bounded": True}
        if tool == "embedding.coverage":
            return {"e5": {"coverage": 0.0}, "bge_m3": {"coverage": 0.0}, "master_state": "ABSENT"}
        raise ValueError(f"unsupported bounded control tool: {tool}")


class LedgerMasterResolver(MasterResolver):
    def __init__(self, ledger: ControlLedger) -> None:
        self.ledger = ledger

    def resolve_master(self, principal: AccessIdentity) -> MasterSnapshot:
        service = self.ledger.resolve_service("postgres-master")
        if service is not None:
            return MasterSnapshot(
                state=MasterState.ACTIVE,
                instance_id=service.master_instance_id,
                epoch=service.epoch,
                canonical_revision=service.canonical_revision,
                lease_expires_at=service.lease_until.isoformat(),
                capabilities=frozenset(service.capabilities),
            )
        operations = self.ledger.incomplete_operations("ensure_master")
        if operations:
            latest = operations[-1]
            try:
                state = MasterState(latest.state)
            except ValueError:
                state = MasterState.REQUESTED
            return MasterSnapshot(state=state, operation_id=latest.operation_id)
        request = self.ledger.latest_master_request()
        if request is not None:
            return MasterSnapshot(
                state=MasterState.REQUESTED,
                operation_id=str(request["operation_id"]),
            )
        return MasterSnapshot(state=MasterState.ABSENT)

    def ensure_master(self, principal: AccessIdentity, *, intent: str) -> EnsureMasterReceipt:
        key = f"mcp:{principal.subject}:{intent}"
        identity = MasterCoordinator.identity_for(key)
        request, created = self.ledger.request_master(
            request_id=hashlib.sha256(f"request:{key}".encode()).hexdigest(),
            idempotency_key=key,
            requested_by=principal.subject,
            intent=intent,
            operation_id=identity["operation_id"],
        )
        return EnsureMasterReceipt(
            operation_id=str(request["operation_id"]),
            state=MasterState.REQUESTED,
            duplicate=not created,
            intent=intent,
        )
