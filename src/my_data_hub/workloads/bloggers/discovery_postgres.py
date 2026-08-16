"""Fixed PostgreSQL procedure facade for blogger discovery preview/apply.

This module intentionally accepts no SQL text.  The short-lived brokered
``mdh_canonical_committer`` session can call only the fixed functions installed
by migration 0020.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from uuid import UUID


def _hash(value: str, name: str) -> str:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{name} must be lowercase SHA-256")
    return value


def _identity(value: str, name: str) -> str:
    if not 1 <= len(value) <= 300:
        raise ValueError(f"{name} must contain 1..300 characters")
    return value


@dataclass(frozen=True, slots=True)
class BloggerImportIdentity:
    batch_id: UUID
    operation_id: str
    request_sha256: str
    expected_revision: int
    principal_id: str
    client_id: str

    def __post_init__(self) -> None:
        _hash(self.operation_id, "operation_id")
        _hash(self.request_sha256, "request_sha256")
        if self.expected_revision < 0:
            raise ValueError("expected_revision must be non-negative")
        _identity(self.principal_id, "principal_id")
        _identity(self.client_id, "client_id")


@dataclass(frozen=True, slots=True)
class BloggerImportPreview:
    batch_id: UUID
    operation_id: str
    request_sha256: str
    plan_sha256: str
    expected_revision: int
    create_actor_count: int
    link_existing_count: int
    quarantine_count: int
    account_count: int

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> BloggerImportPreview:
        result = cls(
            batch_id=UUID(str(row["batch_id"])),
            operation_id=_hash(str(row["operation_id"]), "operation_id"),
            request_sha256=_hash(str(row["request_sha256"]), "request_sha256"),
            plan_sha256=_hash(str(row["plan_sha256"]), "plan_sha256"),
            expected_revision=int(row["expected_revision"]),
            create_actor_count=int(row["create_actor_count"]),
            link_existing_count=int(row["link_existing_count"]),
            quarantine_count=int(row["quarantine_count"]),
            account_count=int(row["account_count"]),
        )
        if min(
            result.expected_revision,
            result.create_actor_count,
            result.link_existing_count,
            result.quarantine_count,
            result.account_count,
        ) < 0:
            raise ValueError("preview counts/revision must be non-negative")
        return result


@dataclass(frozen=True, slots=True)
class BloggerImportApplyReceipt:
    operation_id: str
    batch_id: UUID
    plan_sha256: str
    affected_rows: int
    revision_after: int
    duplicate: bool

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> BloggerImportApplyReceipt:
        result = cls(
            operation_id=_hash(str(row["operation_id"]), "operation_id"),
            batch_id=UUID(str(row["batch_id"])),
            plan_sha256=_hash(str(row["plan_sha256"]), "plan_sha256"),
            affected_rows=int(row["affected_rows"]),
            revision_after=int(row["revision_after"]),
            duplicate=bool(row["duplicate"]),
        )
        if result.affected_rows < 1 or result.revision_after < 1:
            raise ValueError("apply receipt must describe a positive canonical commit")
        return result


class BloggerDiscoveryPostgres:
    """Execute only migration-owned fixed functions through an existing cursor."""

    @staticmethod
    def preview(cursor: Any, identity: BloggerImportIdentity) -> BloggerImportPreview:
        row = cursor.execute(
            "SELECT * FROM integration.preview_blogger_discovery(%s::uuid,%s,%s,%s,%s,%s)",
            (
                identity.batch_id,
                identity.operation_id,
                identity.request_sha256,
                identity.expected_revision,
                identity.principal_id,
                identity.client_id,
            ),
        ).fetchone()
        if row is None:
            raise RuntimeError("blogger discovery preview returned no receipt")
        return BloggerImportPreview.from_row(row)

    @staticmethod
    def apply(
        cursor: Any, identity: BloggerImportIdentity, *, plan_sha256: str
    ) -> BloggerImportApplyReceipt:
        row = cursor.execute(
            "SELECT * FROM integration.apply_blogger_discovery(%s::uuid,%s,%s,%s,%s,%s,%s)",
            (
                identity.batch_id,
                identity.operation_id,
                identity.request_sha256,
                _hash(plan_sha256, "plan_sha256"),
                identity.expected_revision,
                identity.principal_id,
                identity.client_id,
            ),
        ).fetchone()
        if row is None:
            raise RuntimeError("blogger discovery apply returned no receipt")
        return BloggerImportApplyReceipt.from_row(row)

    @staticmethod
    def reconcile(
        cursor: Any,
        identity: BloggerImportIdentity,
        *,
        plan_sha256: str,
        master_instance_id: UUID,
        master_epoch: int,
    ) -> BloggerImportApplyReceipt | None:
        if master_epoch < 1:
            raise ValueError("master_epoch must be positive")
        row = cursor.execute(
            "SELECT * FROM integration.reconcile_blogger_discovery(%s,%s,%s,%s::uuid,%s,%s,%s,%s::text)",
            (
                identity.operation_id,
                identity.request_sha256,
                _hash(plan_sha256, "plan_sha256"),
                master_instance_id,
                master_epoch,
                identity.expected_revision,
                identity.principal_id,
                identity.client_id,
            ),
        ).fetchone()
        return BloggerImportApplyReceipt.from_row(row) if row is not None else None
