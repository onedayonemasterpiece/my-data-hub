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
    returns terminal success only from a VERIFIED checkpoint that is still in
    the current ledger HEAD ancestry.
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
            candidate = self.ledger.checkpoint_candidate(str(row["checkpoint_id"]))
            if (
                candidate is None
                or candidate.get("status") != "VERIFIED"
                or candidate.get("manifest_sha256") != row["manifest_sha256"]
                or not self._is_current_or_verified_ancestor(str(row["checkpoint_id"]))
            ):
                raise RuntimeError(
                    "recorded connector durability checkpoint is not in the current verified checkpoint ancestry"
                )
        return self._status(row)

    def _is_current_or_verified_ancestor(self, checkpoint_id: str) -> bool:
        """Prove that ``checkpoint_id`` remains protected by the current HEAD.

        Advancing HEAD must not invalidate an already durable operation.  The
        proof follows only VERIFIED, generation-consistent parent links and is
        bounded by the durable HEAD generation; a missing link or cycle fails
        closed.
        """

        head = self.ledger.checkpoint_head("postgres-master")
        if head is None or head.current_checkpoint_id is None:
            return False
        current_checkpoint_id: str | None = head.current_checkpoint_id
        expected_source_generation = head.generation - 1
        seen: set[str] = set()
        for _ in range(head.generation):
            if current_checkpoint_id is None or current_checkpoint_id in seen:
                return False
            seen.add(current_checkpoint_id)
            candidate = self.ledger.checkpoint_candidate(current_checkpoint_id)
            if candidate is None or candidate.get("status") != "VERIFIED":
                return False
            if current_checkpoint_id == checkpoint_id:
                return True
            if candidate.get("source_head_generation") != expected_source_generation:
                return False
            source_checkpoint_id = candidate.get("source_checkpoint_id")
            current_checkpoint_id = (
                str(source_checkpoint_id) if isinstance(source_checkpoint_id, str) else None
            )
            expected_source_generation -= 1
        return False

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
