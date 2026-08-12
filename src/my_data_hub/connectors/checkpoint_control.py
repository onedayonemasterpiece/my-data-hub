"""Connector checkpoint demand projected through the real master checkpoint path."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from my_data_hub.control_plane.ledger import ControlLedger


@dataclass(slots=True)
class ControlLedgerVerifiedCheckpointCoordinator:
    """Request an ACTIVE master drain and observe its broker-verified checkpoint.

    This adapter does not upload, verify, or promote checkpoints. The task-bound
    master runtime claims the durable request, exits its ACTIVE loop, and uses its
    existing ``RuntimeCheckpointCoordinator`` / central upload broker. This class
    returns terminal success only from the resulting current VERIFIED ledger head.
    """

    ledger: ControlLedger

    def request_verified_checkpoint(
        self, *, operation_id: str, canonical_revision: int, idempotency_key: str
    ) -> dict[str, Any]:
        row = self.ledger.ensure_connector_checkpoint_request(
            operation_id=operation_id,
            canonical_revision=canonical_revision,
            idempotency_key=idempotency_key,
        )
        return self._status(row)

    def checkpoint_status(self, operation_id: str) -> dict[str, Any]:
        row = self.ledger.connector_checkpoint_request(operation_id)
        if row is None:
            raise LookupError("connector checkpoint operation was not found")
        if row["state"] not in {"DURABLE_COMPLETE", "FAILED"}:
            candidate = self.ledger.verified_checkpoint_for_operation(
                str(row["master_operation_id"])
            )
            if candidate is not None:
                details = self.ledger.checkpoint_candidate(str(candidate["checkpoint_id"]))
                head = self.ledger.checkpoint_head("postgres-master")
                manifest = details.get("manifest") if details is not None else None
                protected_revision = (
                    manifest.get("canonical_revision") if isinstance(manifest, dict) else None
                )
                if (
                    details is not None
                    and head is not None
                    and head.current_checkpoint_id == candidate["checkpoint_id"]
                    and isinstance(protected_revision, int)
                    and not isinstance(protected_revision, bool)
                    and protected_revision >= int(row["canonical_revision"])
                    and isinstance(details.get("verified_at"), str)
                ):
                    row = self.ledger.complete_connector_checkpoint_request(
                        operation_id,
                        checkpoint_id=str(candidate["checkpoint_id"]),
                        manifest_sha256=str(candidate["manifest_sha256"]),
                        verified_at=str(details["verified_at"]),
                    )
            if row["state"] not in {"DURABLE_COMPLETE", "FAILED"}:
                source = self.ledger.get_operation(str(row["master_operation_id"]))
                if source is not None and source.state in {"FAILED", "FENCED", "ORPHANED"}:
                    row = self.ledger.fail_connector_checkpoint_request(
                        operation_id,
                        failure_code=f"MASTER_{source.state}_WITHOUT_VERIFIED_CHECKPOINT",
                    )
        elif row["state"] == "DURABLE_COMPLETE":
            head = self.ledger.checkpoint_head("postgres-master")
            candidate = self.ledger.checkpoint_candidate(str(row["checkpoint_id"]))
            if (
                head is None
                or head.current_checkpoint_id != row["checkpoint_id"]
                or candidate is None
                or candidate.get("status") != "VERIFIED"
                or candidate.get("manifest_sha256") != row["manifest_sha256"]
            ):
                raise RuntimeError(
                    "recorded connector durability checkpoint is no longer exact current VERIFIED HEAD"
                )
        return self._status(row)

    @staticmethod
    def _status(row: dict[str, Any]) -> dict[str, Any]:
        state = str(row["state"])
        public_state = "CHECKPOINTING" if state == "CLAIMED" else state
        result: dict[str, Any] = {
            "operation_id": str(row["operation_id"]),
            "idempotency_key": str(row["idempotency_key"]),
            "canonical_revision": int(row["canonical_revision"]),
            "state": public_state,
        }
        if state == "DURABLE_COMPLETE":
            result.update(
                {
                    "checkpoint_status": "VERIFIED",
                    "checkpoint_id": str(row["checkpoint_id"]),
                    "current_checkpoint_id": str(row["checkpoint_id"]),
                    "manifest_sha256": str(row["manifest_sha256"]),
                    "verified_at": str(row["verified_at"]),
                }
            )
        elif state == "FAILED":
            result["failure_code"] = str(row["failure_code"])
        return result
