"""Short-lived, role-bound PostgreSQL login provisioning for one master epoch."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

from .contracts import MasterIdentity, require_utc
from .database_gate import DatabaseGate

ALLOWED_GROUPS = frozenset(
    {
        "mdh_application",
        "mdh_orchestrator",
        "mdh_connector_intake",
        "mdh_mcp_reader",
        "mdh_mcp_editor",
        "mdh_migration_operator",
        "mdh_canonical_committer",
        "mdh_monitoring",
        "mdh_checkpoint",
        "mdh_embedding_worker",
    }
)
_PRINCIPAL = re.compile(r"^mdh_e[1-9][0-9]*_[a-z][a-z0-9_]{0,40}_[a-f0-9]{8}$")


@dataclass(frozen=True, slots=True)
class LoginPolicy:
    statement_timeout_ms: int = 30_000
    lock_timeout_ms: int = 5_000
    idle_transaction_timeout_ms: int = 30_000
    connection_limit: int = 2

    def validate(self) -> None:
        if not 1 <= self.connection_limit <= 8:
            raise ValueError("connection_limit is outside the bounded contract")
        for timeout in (
            self.statement_timeout_ms,
            self.lock_timeout_ms,
            self.idle_transaction_timeout_ms,
        ):
            if not 100 <= timeout <= 300_000:
                raise ValueError("session timeout is outside the bounded contract")


class CredentialProvisioner:
    """Provision a unique LOGIN role and bind session_user to one epoch.

    The caller supplies the generated password.  It is passed only as a database
    protocol parameter/literal and is never returned, printed, or persisted by this
    object.  The returned value is the non-secret principal name.
    """

    def __init__(self, connection: Any, gate: DatabaseGate | None = None) -> None:
        self.connection = connection
        self.gate = gate or DatabaseGate(connection)

    def create(
        self,
        *,
        principal: str,
        password: str,
        group: str,
        identity: MasterIdentity,
        credential_id: UUID,
        expires_at: datetime,
        now: datetime,
        policy: LoginPolicy | None = None,
    ) -> str:
        from psycopg import sql

        policy = policy or LoginPolicy()
        policy.validate()
        expiry = require_utc(expires_at, "expires_at")
        observed = require_utc(now, "now")
        if expiry <= observed or expiry - observed > timedelta(minutes=15):
            raise ValueError("credential lifetime must be positive and no more than 15 minutes")
        if group not in ALLOWED_GROUPS:
            raise ValueError("role group is not broker-issuable")
        if not _PRINCIPAL.fullmatch(principal) or not principal.startswith(f"mdh_e{identity.epoch}_"):
            raise ValueError("principal name does not bind visibly to the requested epoch")
        if len(password) < 24 or len(password) > 1024:
            raise ValueError("credential secret length is outside the contract")

        with self.connection.cursor() as cursor:
            cursor.execute(
                sql.SQL(
                    "CREATE ROLE {} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT "
                    "NOREPLICATION NOBYPASSRLS CONNECTION LIMIT {} PASSWORD {} VALID UNTIL {}"
                ).format(
                    sql.Identifier(principal),
                    sql.Literal(policy.connection_limit),
                    sql.Literal(password),
                    # CREATE ROLE's VALID UNTIL grammar accepts a string
                    # constant, not psycopg's typed timestamptz literal.
                    sql.Literal(expiry.isoformat()),
                )
            )
            cursor.execute(
                sql.SQL("GRANT {} TO {}").format(sql.Identifier(group), sql.Identifier(principal))
            )
            database_name = cursor.execute("SELECT current_database()").fetchone()[0]
            # NOINHERIT is deliberate, but CONNECT is checked before the client
            # can issue SET ROLE.  Grant only this unavoidable database-level
            # capability directly to the short-lived LOGIN.
            cursor.execute(
                sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                    sql.Identifier(database_name), sql.Identifier(principal)
                )
            )
            settings = {
                "statement_timeout": f"{policy.statement_timeout_ms}ms",
                "lock_timeout": f"{policy.lock_timeout_ms}ms",
                "idle_in_transaction_session_timeout": f"{policy.idle_transaction_timeout_ms}ms",
                "default_transaction_read_only": "on" if group in {"mdh_mcp_reader", "mdh_monitoring"} else "off",
            }
            for name, value in settings.items():
                cursor.execute(
                    sql.SQL("ALTER ROLE {} SET {} TO {}").format(
                        sql.Identifier(principal), sql.Identifier(name), sql.Literal(value)
                    )
                )
            # PostgreSQL role DDL is transactional.  Persist the authoritative
            # epoch binding before committing so a crash can never leave a
            # usable but unbound LOGIN role behind.
            cursor.execute(
                "SELECT master_control.bind_epoch_credential(%s, %s, %s, %s, %s)",
                (
                    credential_id,
                    principal,
                    identity.master_instance_id,
                    identity.epoch,
                    expiry,
                ),
            )
            cursor.fetchone()
        self.connection.commit()
        return principal

    def drop(self, principal: str) -> None:
        from psycopg import sql

        if not _PRINCIPAL.fullmatch(principal):
            raise ValueError("invalid broker principal")
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = %s", (principal,))
            cursor.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(principal)))
        self.connection.commit()
