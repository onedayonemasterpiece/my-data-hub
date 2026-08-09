"""Explicit rollout policy and backup freshness gates for database operators."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from types import MappingProxyType

from my_data_hub.hashing import sha256_value

from .errors import GateClosed

_PRODUCTION_ENVIRONMENTS = frozenset({"prod", "production"})


@dataclass(frozen=True, slots=True, order=True)
class Relation:
    schema: str
    name: str

    def __post_init__(self) -> None:
        if not self.schema or not self.name:
            raise ValueError("relation schema and name must not be empty")

    @property
    def qualified_name(self) -> str:
        return f"{self.schema}.{self.name}"


@dataclass(frozen=True, slots=True, order=True)
class Function:
    schema: str
    name: str

    def __post_init__(self) -> None:
        if not self.schema or not self.name:
            raise ValueError("function schema and name must not be empty")


@dataclass(frozen=True, slots=True)
class DatabaseAllowlist:
    """Exact grants used in addition to PostgreSQL roles.

    An empty column set means that a relation is not writable. Inserts must name
    their columns so the allowlist is always checked rather than inferred from the
    current database shape.
    """

    readable_relations: frozenset[Relation] = frozenset()
    writable_columns: Mapping[Relation, frozenset[str]] = field(default_factory=dict)
    readable_functions: frozenset[Function] = frozenset()
    rollout_disposable_schema: str | None = None

    def __post_init__(self) -> None:
        normalized = {
            relation: frozenset(columns)
            for relation, columns in self.writable_columns.items()
        }
        for relation, columns in normalized.items():
            if not columns:
                raise ValueError(f"writable relation {relation.qualified_name} needs explicit columns")
            if not all(columns):
                raise ValueError("writable column names must not be empty")
        object.__setattr__(self, "writable_columns", MappingProxyType(normalized))

    @classmethod
    def rollout_r1(
        cls,
        *,
        environment: str,
        disposable_schema: str,
        readable_tables: Iterable[str] = (),
        writable_tables: Mapping[str, Iterable[str]] | None = None,
        readable_functions: Iterable[Function] = (),
    ) -> DatabaseAllowlist:
        """Build the first-rollout allowlist.

        R1 is deliberately limited to one disposable schema. Production is always
        empty, even if table names are accidentally supplied by configuration.
        """

        if environment.lower() in _PRODUCTION_ENVIRONMENTS:
            return cls()
        if not disposable_schema:
            raise ValueError("disposable_schema must not be empty")
        readable = frozenset(Relation(disposable_schema, name) for name in readable_tables)
        writable = {
            Relation(disposable_schema, name): frozenset(columns)
            for name, columns in (writable_tables or {}).items()
        }
        # A write target must also be readable for preview and predicates.
        readable |= frozenset(writable)
        allowed_functions = frozenset(
            function
            for function in readable_functions
            if function.schema in {"pg_catalog", disposable_schema}
        )
        return cls(readable, writable, allowed_functions, disposable_schema)

    def can_read(self, relation: Relation) -> bool:
        return relation in self.readable_relations

    def writable_for(self, relation: Relation) -> frozenset[str] | None:
        return self.writable_columns.get(relation)


@dataclass(frozen=True, slots=True)
class OperatorLimits:
    statement_timeout_ms: int = 10_000
    transaction_timeout_ms: int = 15_000
    lock_timeout_ms: int = 2_000
    idle_transaction_timeout_ms: int = 15_000
    max_rows: int = 1_000
    max_bytes: int = 2 * 1024 * 1024
    max_write_rows: int = 100

    def __post_init__(self) -> None:
        for name in (
            "statement_timeout_ms",
            "transaction_timeout_ms",
            "lock_timeout_ms",
            "idle_transaction_timeout_ms",
            "max_rows",
            "max_bytes",
            "max_write_rows",
        ):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")
        if self.statement_timeout_ms > self.transaction_timeout_ms:
            raise ValueError("statement timeout must not exceed transaction timeout")
        if self.max_bytes < 2:
            raise ValueError("max_bytes must be at least 2 so an empty JSON array fits")


@dataclass(frozen=True, slots=True)
class BackupState:
    evidence_revision: str
    completed_at: datetime
    readback_verified: bool
    offsite_available: bool
    schema_revision: int
    restore_drill_at: datetime
    restore_drill_succeeded: bool
    checkpoint_revision: str | None = None
    unprotected_high_impact_change: bool = False

    def __post_init__(self) -> None:
        if not self.evidence_revision:
            raise ValueError("backup evidence revision must not be empty")
        if self.schema_revision < 0:
            raise ValueError("schema revision must not be negative")
        for value in (self.completed_at, self.restore_drill_at):
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError("backup timestamps must be timezone-aware")

    @property
    def fingerprint(self) -> str:
        return sha256_value(
            {
                "evidence_revision": self.evidence_revision,
                "completed_at": self.completed_at.astimezone(UTC).isoformat(),
                "readback_verified": self.readback_verified,
                "offsite_available": self.offsite_available,
                "schema_revision": self.schema_revision,
                "restore_drill_at": self.restore_drill_at.astimezone(UTC).isoformat(),
                "restore_drill_succeeded": self.restore_drill_succeeded,
                "checkpoint_revision": self.checkpoint_revision,
                "unprotected_high_impact_change": self.unprotected_high_impact_change,
            }
        )


@dataclass(frozen=True, slots=True)
class BackupFreshnessPolicy:
    max_backup_age: timedelta = timedelta(hours=24)
    max_restore_drill_age: timedelta = timedelta(days=7)
    future_clock_skew: timedelta = timedelta(minutes=5)

    def __post_init__(self) -> None:
        if self.max_backup_age <= timedelta(0) or self.max_restore_drill_age <= timedelta(0):
            raise ValueError("freshness ages must be positive")
        if self.future_clock_skew < timedelta(0):
            raise ValueError("future_clock_skew must not be negative")

    def require_open(
        self,
        state: BackupState,
        *,
        now: datetime,
        expected_schema_revision: int,
        require_checkpoint: bool = False,
    ) -> None:
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("now must be timezone-aware")
        now = now.astimezone(UTC)
        backup_at = state.completed_at.astimezone(UTC)
        restore_at = state.restore_drill_at.astimezone(UTC)
        findings: list[str] = []
        if not state.readback_verified:
            findings.append("backup readback/hash is not verified")
        if not state.offsite_available:
            findings.append("off-host backup generation is unavailable")
        if not state.restore_drill_succeeded:
            findings.append("last restore drill failed")
        if state.schema_revision != expected_schema_revision:
            findings.append("backup schema revision is incompatible")
        if backup_at > now + self.future_clock_skew or restore_at > now + self.future_clock_skew:
            findings.append("backup evidence timestamp is in the future")
        if now - backup_at > self.max_backup_age:
            findings.append("backup is stale")
        if now - restore_at > self.max_restore_drill_age:
            findings.append("restore drill is stale")
        if state.unprotected_high_impact_change:
            findings.append("a newer high-impact change is not protected")
        if require_checkpoint and not state.checkpoint_revision:
            findings.append("a pre-change checkpoint is required")
        if findings:
            raise GateClosed("; ".join(findings))
