"""Typed master runtime contracts with no credential-bearing fields."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from uuid import UUID


class GateState(StrEnum):
    CLOSED = "closed"
    OPEN = "open"
    DRAINING = "draining"
    FENCED = "fenced"


class BootSource(StrEnum):
    EMPTY_BASELINE = "empty_baseline"
    VERIFIED_CHECKPOINT = "verified_checkpoint"


class BootPhase(StrEnum):
    PLANNED = "planned"
    VERIFYING_SOURCE = "verifying_source"
    INITIALIZING = "initializing"
    RESTORING = "restoring"
    STARTING_POSTGRES = "starting_postgres"
    MIGRATING = "migrating"
    RECONCILING_ROLES = "reconciling_roles"
    ACQUIRING_EPOCH = "acquiring_epoch"
    STARTING_TUNNEL = "starting_tunnel"
    ANNOUNCING_READY = "announcing_ready"
    WAITING_FOR_ACTIVATION = "waiting_for_activation"
    ACTIVE = "active"
    FAILED = "failed"


def require_utc(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


@dataclass(frozen=True, slots=True)
class MasterIdentity:
    master_instance_id: UUID
    run_id: str
    epoch: int

    def __post_init__(self) -> None:
        if self.epoch < 1:
            raise ValueError("epoch must be positive")
        if not self.run_id or len(self.run_id) > 256:
            raise ValueError("run_id must contain 1..256 characters")


@dataclass(frozen=True, slots=True)
class MasterPaths:
    """All mutable paths are rooted below the Kaggle working directory."""

    working: Path
    pgdata: Path
    socket: Path
    logs: Path
    runtime_events: Path
    checkpoints: Path

    @classmethod
    def under(cls, working: Path) -> MasterPaths:
        root = working.resolve()
        return cls(
            working=root,
            pgdata=root / "postgres" / "data",
            socket=root / "postgres" / "socket",
            logs=root / "postgres" / "logs",
            runtime_events=root / "runtime" / "events.jsonl",
            checkpoints=root / "checkpoints",
        )

    def validate(self) -> None:
        root = self.working.resolve()
        for field in (self.pgdata, self.socket, self.logs, self.runtime_events, self.checkpoints):
            try:
                field.resolve().relative_to(root)
            except ValueError as exc:
                raise ValueError(f"mutable master path escapes working directory: {field}") from exc


@dataclass(frozen=True, slots=True)
class ServiceReady:
    identity: MasterIdentity
    endpoint: str
    tls_fingerprint_sha256: str
    canonical_revision: int
    schema_version: int
    lease_until: datetime
    capabilities: tuple[str, ...] = ("sql", "fts", "pgvector")
    service_kind: str = "postgres-master"
    protocol: str = "postgresql+tls"

    def __post_init__(self) -> None:
        require_utc(self.lease_until, "lease_until")
        if not self.endpoint or len(self.endpoint) > 512:
            raise ValueError("endpoint must contain 1..512 characters")
        if len(self.tls_fingerprint_sha256) != 64 or any(
            char not in "0123456789abcdef" for char in self.tls_fingerprint_sha256
        ):
            raise ValueError("tls fingerprint must be lowercase SHA-256")
        if self.canonical_revision < 0 or self.schema_version < 1:
            raise ValueError("revision values are outside the contract")
        if self.capabilities != ("sql", "fts", "pgvector"):
            raise ValueError("master capabilities must be exact and ordered")

    def event_payload(self) -> dict[str, object]:
        """Return the bounded callback payload; credentials cannot be represented."""

        return {
            "service_kind": self.service_kind,
            "endpoint": self.endpoint,
            "protocol": self.protocol,
            "tls_fingerprint": self.tls_fingerprint_sha256,
            "capabilities": list(self.capabilities),
            "canonical_revision": self.canonical_revision,
            "schema_version": self.schema_version,
            "lease_until": require_utc(self.lease_until, "lease_until").isoformat().replace("+00:00", "Z"),
            "master_instance_id": str(self.identity.master_instance_id),
            "epoch": self.identity.epoch,
            "run_id": self.identity.run_id,
        }
