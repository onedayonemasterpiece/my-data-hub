from __future__ import annotations

import threading
import time
from collections.abc import Awaitable
from dataclasses import dataclass, replace
from enum import StrEnum
from typing import Protocol, runtime_checkable


@dataclass(frozen=True, slots=True)
class AuthorizationGrant:
    code_digest: str
    code_challenge: str
    client_id: str
    redirect_uri: str
    resource: str
    scopes: tuple[str, ...]
    subject: str
    nonce: str | None
    authenticated_at: int
    expires_at: int


@dataclass(frozen=True, slots=True)
class RefreshGrant:
    credential_digest: str
    family_id: str
    client_id: str
    resource: str
    scopes: tuple[str, ...]
    subject: str
    authenticated_at: int
    expires_at: int
    consumed_at: int | None = None
    revoked_at: int | None = None


@dataclass(frozen=True, slots=True)
class RefreshRotationRequest:
    presented_digest: str
    successor_digest: str
    client_id: str
    resource: str
    requested_scopes: tuple[str, ...] | None
    successor_expires_at: int
    now: int


class RefreshRotationStatus(StrEnum):
    ROTATED = "rotated"
    INVALID = "invalid"
    REPLAYED = "replayed"


@dataclass(frozen=True, slots=True)
class RefreshRotationResult:
    status: RefreshRotationStatus
    grant: RefreshGrant | None = None


@runtime_checkable
class OAuthGrantStore(Protocol):
    """Durable, atomic authorization-code and refresh-token storage boundary.

    Production adapters belong in the control plane, never canonical PostgreSQL.
    They store only digests of bearer credentials.  Code consumption and refresh
    rotation must be atomic.  Refresh replay must revoke the complete family.
    """

    def create_authorization_grant(self, grant: AuthorizationGrant) -> bool | Awaitable[bool]: ...

    def consume_authorization_grant(
        self, code_digest: str, *, now: int
    ) -> AuthorizationGrant | Awaitable[AuthorizationGrant | None] | None: ...

    def create_refresh_grant(self, grant: RefreshGrant) -> bool | Awaitable[bool]: ...

    def rotate_refresh_grant(
        self, request: RefreshRotationRequest
    ) -> RefreshRotationResult | Awaitable[RefreshRotationResult]: ...

    def revoke_refresh_grant(
        self, credential_digest: str, *, client_id: str, now: int
    ) -> Awaitable[None] | None: ...


class MemoryOAuthGrantStore:
    """Thread-safe conformance store for tests/local evaluation, not deployment."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._codes: dict[str, AuthorizationGrant] = {}
        self._refresh: dict[str, RefreshGrant] = {}

    def create_authorization_grant(self, grant: AuthorizationGrant) -> bool:
        with self._lock:
            if grant.code_digest in self._codes:
                return False
            self._codes[grant.code_digest] = grant
            return True

    def consume_authorization_grant(self, code_digest: str, *, now: int) -> AuthorizationGrant | None:
        with self._lock:
            grant = self._codes.pop(code_digest, None)
            return grant if grant is not None and grant.expires_at > now else None

    def create_refresh_grant(self, grant: RefreshGrant) -> bool:
        with self._lock:
            if grant.credential_digest in self._refresh:
                return False
            self._refresh[grant.credential_digest] = grant
            return True

    def rotate_refresh_grant(self, request: RefreshRotationRequest) -> RefreshRotationResult:
        with self._lock:
            current = self._refresh.get(request.presented_digest)
            if current is None:
                return RefreshRotationResult(RefreshRotationStatus.INVALID)
            if current.consumed_at is not None:
                self._revoke_family(current.family_id, request.now)
                return RefreshRotationResult(RefreshRotationStatus.REPLAYED)
            if current.revoked_at is not None or current.expires_at <= request.now:
                return RefreshRotationResult(RefreshRotationStatus.INVALID)
            requested = current.scopes if request.requested_scopes is None else request.requested_scopes
            if (
                current.client_id != request.client_id
                or current.resource != request.resource
                or not set(requested).issubset(current.scopes)
                or request.successor_digest in self._refresh
            ):
                return RefreshRotationResult(RefreshRotationStatus.INVALID)
            self._refresh[current.credential_digest] = replace(current, consumed_at=request.now)
            successor = RefreshGrant(
                credential_digest=request.successor_digest,
                family_id=current.family_id,
                client_id=current.client_id,
                resource=current.resource,
                scopes=requested,
                subject=current.subject,
                authenticated_at=current.authenticated_at,
                expires_at=min(current.expires_at, request.successor_expires_at),
            )
            self._refresh[successor.credential_digest] = successor
            return RefreshRotationResult(RefreshRotationStatus.ROTATED, successor)

    def revoke_refresh_grant(self, credential_digest: str, *, client_id: str, now: int) -> None:
        with self._lock:
            current = self._refresh.get(credential_digest)
            if current is not None and current.client_id == client_id:
                self._revoke_family(current.family_id, now)

    def _revoke_family(self, family_id: str, now: int) -> None:
        for digest, grant in tuple(self._refresh.items()):
            if grant.family_id == family_id and grant.revoked_at is None:
                self._refresh[digest] = replace(grant, revoked_at=now)

    def family_is_revoked(self, family_id: str) -> bool:
        """Testing/adapter-conformance observation without exposing credentials."""

        with self._lock:
            family = [grant for grant in self._refresh.values() if grant.family_id == family_id]
            return bool(family) and all(grant.revoked_at is not None for grant in family)


def unix_time() -> int:
    return int(time.time())
