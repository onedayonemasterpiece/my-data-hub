"""Ordered, fail-closed master bootstrap coordinator."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Protocol

from .contracts import BootPhase, BootSource, MasterIdentity, ServiceReady


class BootstrapError(RuntimeError):
    """Master bootstrap failed and the write gate remains closed."""


class BootstrapPostgres(Protocol):
    def initialize_empty(self) -> None: ...
    def start(self) -> None: ...
    def stop(self, *, immediate: bool = False) -> None: ...


class BootstrapGate(Protocol):
    def acquire(self, identity: MasterIdentity, lease_until: datetime) -> None: ...
    def fence(self, identity: MasterIdentity, reason: str) -> None: ...


class BootstrapTunnel(Protocol):
    def start(self, *, now: datetime) -> None: ...
    def stop(self) -> None: ...


@dataclass(frozen=True, slots=True)
class BootstrapRequest:
    identity: MasterIdentity
    source: BootSource
    lease_until: datetime
    now: datetime
    checkpoint_directory: Path | None = None

    def __post_init__(self) -> None:
        if self.source is BootSource.EMPTY_BASELINE and self.checkpoint_directory is not None:
            raise ValueError("empty bootstrap cannot include a checkpoint")
        if self.source is BootSource.VERIFIED_CHECKPOINT and self.checkpoint_directory is None:
            raise ValueError("restore bootstrap requires an exact checkpoint directory")


class MasterBootstrap:
    """Execute boot gates in deterministic order and never open the gate itself.

    Activation belongs to the control plane after it has verified the service.ready
    identity, endpoint, and TLS fingerprint.  This prevents readiness from becoming
    implicit write authority.
    """

    def __init__(
        self,
        *,
        postgres: BootstrapPostgres,
        gate: BootstrapGate,
        tunnel: BootstrapTunnel,
        restore: Callable[[Path], None],
        migrate: Callable[[], None],
        reconcile_roles: Callable[[], None],
        verify_database: Callable[[], tuple[int, int, str]],
        announce_ready: Callable[[ServiceReady], None],
        endpoint: Callable[[], str],
    ) -> None:
        self.postgres = postgres
        self.gate = gate
        self.tunnel = tunnel
        self.restore = restore
        self.migrate = migrate
        self.reconcile_roles = reconcile_roles
        self.verify_database = verify_database
        self.announce_ready = announce_ready
        self.endpoint = endpoint
        self.phases: list[BootPhase] = [BootPhase.PLANNED]

    def run(self, request: BootstrapRequest) -> ServiceReady:
        postgres_started = False
        tunnel_started = False
        epoch_acquired = False
        try:
            self._phase(BootPhase.VERIFYING_SOURCE)
            if request.source is BootSource.EMPTY_BASELINE:
                self._phase(BootPhase.INITIALIZING)
                self.postgres.initialize_empty()
            else:
                self._phase(BootPhase.RESTORING)
                assert request.checkpoint_directory is not None
                self.restore(request.checkpoint_directory)
            self._phase(BootPhase.STARTING_POSTGRES)
            self.postgres.start()
            postgres_started = True
            self._phase(BootPhase.MIGRATING)
            self.migrate()
            self._phase(BootPhase.RECONCILING_ROLES)
            self.reconcile_roles()
            schema_version, canonical_revision, fingerprint = self.verify_database()
            self._phase(BootPhase.ACQUIRING_EPOCH)
            self.gate.acquire(request.identity, request.lease_until)
            epoch_acquired = True
            self._phase(BootPhase.STARTING_TUNNEL)
            self.tunnel.start(now=request.now)
            tunnel_started = True
            ready = ServiceReady(
                identity=request.identity,
                endpoint=self.endpoint(),
                tls_fingerprint_sha256=fingerprint,
                canonical_revision=canonical_revision,
                schema_version=schema_version,
                lease_until=request.lease_until,
            )
            self._phase(BootPhase.ANNOUNCING_READY)
            self.announce_ready(ready)
            self._phase(BootPhase.WAITING_FOR_ACTIVATION)
            return ready
        except Exception as exc:
            self._phase(BootPhase.FAILED)
            if epoch_acquired:
                with suppress(Exception):
                    self.gate.fence(request.identity, "bootstrap_failed")
            if tunnel_started:
                self.tunnel.stop()
            if postgres_started:
                with suppress(Exception):
                    self.postgres.stop(immediate=True)
            raise BootstrapError(f"master bootstrap failed in phase {self.phases[-2].value}") from exc

    def _phase(self, phase: BootPhase) -> None:
        self.phases.append(phase)
