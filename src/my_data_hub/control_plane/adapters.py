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
            current = (
                self.ledger.checkpoint_candidate(head.current_checkpoint_id)
                if head and head.current_checkpoint_id
                else None
            )
            previous = (
                self.ledger.checkpoint_candidate(head.previous_checkpoint_id)
                if head and head.previous_checkpoint_id
                else None
            )
            return {
                "current_checkpoint_id": head.current_checkpoint_id if head else None,
                "previous_checkpoint_id": head.previous_checkpoint_id if head else None,
                "current_exact_version_ref": current.get("version_ref") if current else None,
                "previous_exact_version_ref": previous.get("version_ref") if previous else None,
                "verified_at": current.get("verified_at") if current else None,
                "current": self._checkpoint_projection(current),
                "previous": self._checkpoint_projection(previous),
                "generation": head.generation if head else 0,
                "freshness": "absent" if head is None else "recorded",
            }
        if tool == "connector.coverage":
            rows = self.ledger.connector_coverage_metadata(limit=100)
            if rows:
                complete = sum(row["state"] == "COMPLETE" for row in rows)
                observed = sorted(str(row["observed_at"]) for row in rows)
                return {
                    "available": True,
                    "bounded": True,
                    "source": "control-ledger-metadata",
                    "connector_count": len(rows),
                    "complete_count": complete,
                    "oldest_observed_at": observed[0],
                    "newest_observed_at": observed[-1],
                }
            # Connector business state belongs to the Kaggle PostgreSQL primary;
            # absence of its metadata heartbeat must not trigger a row query.
            return {
                "available": False,
                "bounded": True,
                "source": "control-ledger-metadata",
                "blocker_code": "CONNECTOR_METADATA_HEARTBEAT_ABSENT",
                "connector_count": 0,
                "complete_count": 0,
            }
        if tool == "runtime.stale_epoch.probe":
            service = self.ledger.resolve_service("postgres-master")
            supplied = arguments.get("submitted_epoch")
            expected = arguments.get("expected_active_epoch")
            binding_valid = (
                service is not None
                and isinstance(supplied, int)
                and not isinstance(supplied, bool)
                and isinstance(expected, int)
                and not isinstance(expected, bool)
                and expected == service.epoch
                and supplied < service.epoch
            )
            return {
                "evaluated": False,
                "denied": False,
                "binding_valid": binding_valid,
                "reason_code": (
                    "STALE_EPOCH_ADMISSION_PATH_UNAVAILABLE"
                    if binding_valid
                    else "PROBE_BINDING_INVALID"
                ),
                "blocker_code": "STALE_EPOCH_ADMISSION_PATH_UNAVAILABLE",
                "active_epoch": service.epoch if service else None,
                "submitted_epoch": supplied if isinstance(supplied, int) else None,
                "mutation_attempted": False,
            }
        if tool == "provider.protected_resource.probe":
            resource_ref = str(arguments.get("resource_ref", ""))
            rows = self.ledger.list_provider_resources(limit=500)
            resource = next((row for row in rows if row["resource_ref"] == resource_ref), None)
            protected = bool(resource and resource.get("control_class") == "orchestrator_protected")
            return {
                "evaluated": False,
                "protected": protected,
                "denied": False,
                "binding_valid": protected,
                "reason_code": (
                    "PROTECTED_RESOURCE_ADMISSION_PATH_UNAVAILABLE"
                    if protected
                    else "PROBE_BINDING_INVALID"
                ),
                "blocker_code": "PROTECTED_RESOURCE_ADMISSION_PATH_UNAVAILABLE",
                "mutation_attempted": False,
            }
        if tool in {"checkpoint.restore.request", "master.rotation.request"}:
            return self._acceptance_action_request(tool, arguments, principal)
        if tool == "provider.resources.status":
            limit = min(int(arguments.get("limit", 100)), 100)
            resources = self.ledger.list_provider_resources(limit=limit)
            return {"resources": resources, "count": len(resources), "bounded": True}
        if tool == "embedding.coverage":
            return {"e5": {"coverage": 0.0}, "bge_m3": {"coverage": 0.0}, "master_state": "ABSENT"}
        raise ValueError(f"unsupported bounded control tool: {tool}")

    @staticmethod
    def _checkpoint_projection(candidate: dict[str, Any] | None) -> dict[str, Any] | None:
        if candidate is None:
            return None
        return {
            "checkpoint_id": candidate["checkpoint_id"],
            "exact_version_ref": candidate["version_ref"],
            "manifest_sha256": candidate["manifest_sha256"],
            "verified_at": candidate["verified_at"],
            "status": candidate["status"],
        }

    def _acceptance_action_request(
        self,
        tool: str,
        arguments: dict[str, Any],
        principal: AccessIdentity,
    ) -> dict[str, Any]:
        """Persist an exact request without pretending an executor exists.

        These operations are intentionally metadata-only.  A production
        consumer must atomically claim the operation before it may drain a
        master or launch an isolated verifier; none is configured in the
        control plane today.
        """

        request_key = str(arguments.get("idempotency_key", ""))
        if not 8 <= len(request_key) <= 200 or any(ord(char) < 32 for char in request_key):
            raise ValueError("acceptance request idempotency_key is invalid")
        head = self.ledger.checkpoint_head("postgres-master")
        if head is None or head.current_checkpoint_id is None:
            raise ValueError("checkpoint HEAD is absent")
        target = "current"
        if tool == "checkpoint.restore.request":
            target = str(arguments.get("target", ""))
            if target not in {"current", "previous"}:
                raise ValueError("restore target must be current or previous")
        checkpoint_id = (
            head.previous_checkpoint_id if target == "previous" else head.current_checkpoint_id
        )
        if checkpoint_id is None:
            raise ValueError("requested checkpoint generation is absent")
        candidate = self.ledger.checkpoint_candidate(checkpoint_id)
        if candidate is None or candidate.get("status") != "VERIFIED":
            raise ValueError("requested checkpoint is not verified")
        exact_version = str(candidate.get("version_ref") or "")
        if (
            arguments.get("checkpoint_id") != checkpoint_id
            or arguments.get("exact_version_ref") != exact_version
        ):
            raise ValueError("request does not bind the exact checkpoint HEAD generation")
        if tool == "master.rotation.request":
            service = self.ledger.resolve_service("postgres-master")
            expected_epoch = arguments.get("expected_active_epoch")
            if service is None or expected_epoch != service.epoch:
                raise ValueError("rotation request does not bind the active epoch")
        timeout = arguments.get("timeout_seconds")
        if not isinstance(timeout, int) or isinstance(timeout, bool) or not 60 <= timeout <= 3600:
            raise ValueError("acceptance request timeout_seconds must be between 60 and 3600")
        intent = {
            "tool": tool,
            "target": target,
            "checkpoint_id": checkpoint_id,
            "exact_version_ref": exact_version,
            "head_generation": head.generation,
            "timeout_seconds": timeout,
        }
        if tool == "master.rotation.request":
            intent["expected_active_epoch"] = arguments["expected_active_epoch"]
        digest = hashlib.sha256(
            json.dumps(intent, sort_keys=True, separators=(",", ":")).encode()
            + b":"
            + request_key.encode()
        ).hexdigest()
        operation, created = self.ledger.ensure_operation(
            operation_id=digest,
            idempotency_key=f"scheduled-acceptance:{tool}:{request_key}",
            operation_kind=(
                "checkpoint_restore_smoke" if tool == "checkpoint.restore.request" else "forced_master_rotation"
            ),
            intent=intent,
            initial_state="REQUESTED",
            identity={"principal": principal.subject, "request_sha256": digest},
        )
        return {
            "accepted": True,
            "duplicate": not created,
            "operation_id": operation.operation_id,
            "state": operation.state,
            "target": target,
            "checkpoint_id": checkpoint_id,
            "exact_version_ref": exact_version,
            "head_generation": head.generation,
            "execution_supported": False,
            "blocker_code": (
                "ISOLATED_RESTORE_OPERATION_CONSUMER_MISSING"
                if tool == "checkpoint.restore.request"
                else "MASTER_ROTATION_OPERATION_CONSUMER_MISSING"
            ),
        }


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
