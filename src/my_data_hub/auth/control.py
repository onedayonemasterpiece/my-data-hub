from __future__ import annotations

from collections.abc import Awaitable, Mapping
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from my_data_hub.mcp.oauth import RevocationKey


@dataclass(frozen=True, slots=True)
class OAuthRevocationQuery:
    issuer: str
    token_id: str
    client_id: str
    subject: str
    issued_at: int


@dataclass(frozen=True, slots=True)
class OAuthClientRecord:
    issuer: str
    client_id: str
    enabled: bool
    allowed_scopes: frozenset[str]


@dataclass(frozen=True, slots=True)
class OAuthAuditEvent:
    event: str
    outcome: str
    issuer: str
    client_id: str
    subject: str
    token_id: str | None = None
    tool: str | None = None
    operation_id: str | None = None
    master_epoch: int | None = None
    canonical_revision: int | None = None


@dataclass(frozen=True, slots=True)
class AuthorizationCodeExchange:
    """OAuth 2.1 code exchange inputs passed to an established IdP/library."""

    code: str
    code_verifier: str
    redirect_uri: str
    client_id: str
    resource: str


@dataclass(frozen=True, slots=True)
class RefreshCredentialRotation:
    refresh_credential: str
    client_id: str
    resource: str


@runtime_checkable
class OAuthControlLedger(Protocol):
    """Durable OAuth authority on the devstand control ledger.

    Implementations must be bounded and fail closed.  This protocol is
    deliberately independent of canonical PostgreSQL: revocation, client and
    access-audit state must remain available while the master is ABSENT.
    """

    def is_revoked(self, query: OAuthRevocationQuery) -> bool | Awaitable[bool]: ...

    def get_client(
        self, issuer: str, client_id: str
    ) -> OAuthClientRecord | Awaitable[OAuthClientRecord | None] | None: ...

    def register_resolved_client(
        self,
        record: OAuthClientRecord,
        *,
        principal_id: str,
    ) -> OAuthClientRecord | Awaitable[OAuthClientRecord]: ...

    def record_oauth_audit(self, event: OAuthAuditEvent) -> Awaitable[None] | None: ...


@runtime_checkable
class OAuthAuthorizationService(Protocol):
    """Narrow IdP/library boundary; crypto primitives live behind it."""

    def exchange_authorization_code(
        self, request: AuthorizationCodeExchange
    ) -> Mapping[str, object] | Awaitable[Mapping[str, object]]: ...

    def rotate_refresh_credential(
        self, request: RefreshCredentialRotation
    ) -> Mapping[str, object] | Awaitable[Mapping[str, object]]: ...

    def revoke_refresh_credential(
        self, refresh_credential: str, *, client_id: str
    ) -> Awaitable[None] | None: ...


@dataclass(frozen=True, slots=True)
class ControlLedgerRevocationStore:
    """Adapt the control-ledger authority to OAuth bearer validation."""

    ledger: OAuthControlLedger

    def is_revoked(self, key: RevocationKey) -> bool | Awaitable[bool]:
        return self.ledger.is_revoked(
            OAuthRevocationQuery(
                issuer=key.issuer,
                token_id=key.token_id,
                client_id=key.client_id,
                subject=key.subject,
                issued_at=key.issued_at,
            )
        )
