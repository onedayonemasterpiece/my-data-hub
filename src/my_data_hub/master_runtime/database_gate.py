"""psycopg adapter for migration 0011's authoritative database write gate."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from .contracts import MasterIdentity, require_utc


class DatabaseGate:
    """Invoke bounded SECURITY DEFINER transitions; never interpolate identities."""

    def __init__(self, connection: Any) -> None:
        self.connection = connection

    def acquire(self, identity: MasterIdentity, lease_until: datetime) -> None:
        self._call(
            "SELECT master_control.begin_epoch(%s, %s, %s, %s)",
            (
                identity.master_instance_id,
                identity.run_id,
                identity.epoch,
                require_utc(lease_until, "lease_until"),
            ),
        )

    def activate(self, identity: MasterIdentity) -> None:
        self._call(
            "SELECT master_control.open_write_gate(%s, %s)",
            (identity.master_instance_id, identity.epoch),
        )

    def renew(self, identity: MasterIdentity, lease_until: datetime) -> None:
        self._call(
            "SELECT master_control.renew_epoch(%s, %s, %s)",
            (
                identity.master_instance_id,
                identity.epoch,
                require_utc(lease_until, "lease_until"),
            ),
        )

    def drain(self, identity: MasterIdentity, reason: str = "drain") -> None:
        self._call(
            "SELECT master_control.close_write_gate(%s, %s, 'draining', %s)",
            (identity.master_instance_id, identity.epoch, reason),
        )

    def fence(self, identity: MasterIdentity, reason: str) -> None:
        self._call(
            "SELECT master_control.close_write_gate(%s, %s, 'fenced', %s)",
            (identity.master_instance_id, identity.epoch, reason),
        )

    def bind_credential(
        self,
        principal: str,
        identity: MasterIdentity,
        expires_at: datetime,
        credential_id: UUID,
    ) -> None:
        self._call(
            "SELECT master_control.bind_epoch_credential(%s, %s, %s, %s, %s)",
            (
                credential_id,
                principal,
                identity.master_instance_id,
                identity.epoch,
                require_utc(expires_at, "expires_at"),
            ),
        )

    def revoke_credential(self, credential_id: UUID, reason: str) -> None:
        self._call(
            "SELECT master_control.revoke_epoch_credential(%s, %s)",
            (credential_id, reason),
        )

    def _call(self, statement: str, parameters: tuple[object, ...]) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(statement, parameters)
            cursor.fetchone()
        self.connection.commit()
