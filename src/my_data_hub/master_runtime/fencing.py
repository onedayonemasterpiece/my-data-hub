"""Fail-closed epoch and lease model used by runtime tests and watchdogs.

PostgreSQL is authoritative for the live write gate (migration 0011).  This pure
model mirrors that contract so the control/runtime boundary can be tested without
turning the devstand ledger into a canonical database.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from threading import RLock

from .contracts import GateState, MasterIdentity, require_utc


class FencingError(RuntimeError):
    """An epoch, lease, gate, or credential invariant was rejected."""


@dataclass(frozen=True, slots=True)
class Lease:
    identity: MasterIdentity
    lease_until: datetime
    gate: GateState
    reason: str


@dataclass(frozen=True, slots=True)
class CredentialBinding:
    principal: str
    identity: MasterIdentity
    expires_at: datetime


class EpochFence:
    """Thread-safe monotonic epoch model with epoch-bound session principals."""

    def __init__(self) -> None:
        self._lock = RLock()
        self._highest_epoch = 0
        self._lease: Lease | None = None
        self._bindings: dict[str, CredentialBinding] = {}

    @property
    def highest_epoch(self) -> int:
        with self._lock:
            return self._highest_epoch

    @property
    def lease(self) -> Lease | None:
        with self._lock:
            return self._lease

    def acquire(self, identity: MasterIdentity, *, lease_until: datetime, now: datetime) -> Lease:
        deadline = require_utc(lease_until, "lease_until")
        observed = require_utc(now, "now")
        if deadline <= observed:
            raise FencingError("lease deadline must be in the future")
        with self._lock:
            current = self._lease
            if (
                current
                and current.gate not in {GateState.FENCED, GateState.DRAINING}
                and current.lease_until > observed
            ):
                raise FencingError("another master has an unexpired lease")
            expected = self._highest_epoch + 1
            if identity.epoch != expected:
                raise FencingError(f"epoch must advance exactly once: expected={expected}")
            self._highest_epoch = identity.epoch
            self._lease = Lease(identity, deadline, GateState.CLOSED, "registered")
            return self._lease

    def open(self, identity: MasterIdentity, *, now: datetime) -> Lease:
        with self._lock:
            lease = self._require_current(identity, now=now)
            if lease.gate is not GateState.CLOSED:
                raise FencingError("only a closed current gate can be opened")
            self._lease = replace(lease, gate=GateState.OPEN, reason="activated")
            return self._lease

    def renew(self, identity: MasterIdentity, *, lease_until: datetime, now: datetime) -> Lease:
        deadline = require_utc(lease_until, "lease_until")
        with self._lock:
            lease = self._require_current(identity, now=now)
            if lease.gate not in {GateState.CLOSED, GateState.OPEN}:
                raise FencingError("draining or fenced epoch cannot renew")
            if deadline <= lease.lease_until:
                raise FencingError("lease renewal must extend the deadline")
            self._lease = replace(lease, lease_until=deadline, reason="renewed")
            return self._lease

    def bind(
        self,
        principal: str,
        identity: MasterIdentity,
        *,
        expires_at: datetime,
        now: datetime,
    ) -> CredentialBinding:
        if not principal or len(principal) > 63:
            raise FencingError("principal must contain 1..63 characters")
        expiry = require_utc(expires_at, "expires_at")
        with self._lock:
            lease = self._require_current(identity, now=now)
            if lease.gate is not GateState.OPEN:
                raise FencingError("credentials are issued only for an open gate")
            if expiry > lease.lease_until or expiry <= require_utc(now, "now"):
                raise FencingError("credential expiry must be in the remaining lease")
            binding = CredentialBinding(principal, identity, expiry)
            self._bindings[principal] = binding
            return binding

    def assert_write(self, principal: str, *, now: datetime) -> None:
        observed = require_utc(now, "now")
        with self._lock:
            lease = self._lease
            binding = self._bindings.get(principal)
            if lease is None or binding is None:
                raise FencingError("principal has no current epoch binding")
            if lease.gate is not GateState.OPEN:
                raise FencingError(f"write gate is {lease.gate.value}")
            if lease.lease_until <= observed:
                raise FencingError("master lease expired")
            if binding.expires_at <= observed:
                raise FencingError("credential expired")
            if binding.identity != lease.identity:
                raise FencingError("credential belongs to a fenced epoch")

    def drain(self, identity: MasterIdentity, *, now: datetime) -> Lease:
        with self._lock:
            lease = self._require_identity(identity)
            self._lease = replace(lease, gate=GateState.DRAINING, reason="drain")
            return self._lease

    def fence(self, identity: MasterIdentity, *, reason: str) -> Lease:
        if not reason or len(reason) > 256:
            raise FencingError("fence reason must contain 1..256 characters")
        with self._lock:
            lease = self._require_identity(identity)
            self._lease = replace(lease, gate=GateState.FENCED, reason=reason)
            return self._lease

    def expire(self, *, now: datetime) -> bool:
        observed = require_utc(now, "now")
        with self._lock:
            lease = self._lease
            if lease is None or lease.gate in {GateState.DRAINING, GateState.FENCED}:
                return False
            if lease.lease_until > observed:
                return False
            self._lease = replace(lease, gate=GateState.FENCED, reason="lease_expired")
            return True

    def _require_identity(self, identity: MasterIdentity) -> Lease:
        if self._lease is None or self._lease.identity != identity:
            raise FencingError("stale master identity")
        return self._lease

    def _require_current(self, identity: MasterIdentity, *, now: datetime) -> Lease:
        lease = self._require_identity(identity)
        if lease.lease_until <= require_utc(now, "now"):
            raise FencingError("master lease expired")
        return lease


class LeaseWatchdog:
    """Closes the database gate before lease/control reachability becomes unsafe."""

    def __init__(
        self,
        *,
        close_gate: Callable[[str], None],
        safety_margin: timedelta,
        control_timeout: timedelta,
    ) -> None:
        if safety_margin <= timedelta(0) or control_timeout <= timedelta(0):
            raise ValueError("watchdog timeouts must be positive")
        self._close_gate = close_gate
        self._safety_margin = safety_margin
        self._control_timeout = control_timeout
        self._lease_until: datetime | None = None
        self._last_control: datetime | None = None
        self._closed_reason: str | None = None

    @property
    def closed_reason(self) -> str | None:
        return self._closed_reason

    def observe_lease(self, lease_until: datetime) -> None:
        self._lease_until = require_utc(lease_until, "lease_until")

    def observe_control(self, now: datetime) -> None:
        self._last_control = require_utc(now, "now")

    def poll(self, now: datetime) -> str | None:
        observed = require_utc(now, "now")
        reason: str | None = None
        if self._lease_until is None or self._lease_until - observed <= self._safety_margin:
            reason = "lease_safety_margin"
        elif self._last_control is None or observed - self._last_control >= self._control_timeout:
            reason = "control_heartbeat_lost"
        if reason is not None and self._closed_reason is None:
            self._close_gate(reason)
            self._closed_reason = reason
        return self._closed_reason
