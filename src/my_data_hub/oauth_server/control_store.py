"""Durable OAuth grant adapter backed only by the lightweight control ledger."""

from __future__ import annotations

from dataclasses import asdict

from my_data_hub.control_plane.ledger import ControlLedger

from .stores import (
    AuthorizationGrant,
    RefreshGrant,
    RefreshRotationRequest,
    RefreshRotationResult,
    RefreshRotationStatus,
)


class ControlLedgerOAuthGrantStore:
    """Atomic production store; only credential digests enter SQLite."""

    def __init__(self, ledger: ControlLedger) -> None:
        self.ledger = ledger

    def create_authorization_grant(self, grant: AuthorizationGrant) -> bool:
        return self.ledger.create_oauth_authorization_grant(asdict(grant))

    def consume_authorization_grant(
        self, code_digest: str, *, now: int
    ) -> AuthorizationGrant | None:
        payload = self.ledger.consume_oauth_authorization_grant(code_digest, now=now)
        return AuthorizationGrant(**payload) if payload is not None else None

    def create_refresh_grant(self, grant: RefreshGrant) -> bool:
        return self.ledger.create_oauth_refresh_grant(asdict(grant))

    def rotate_refresh_grant(self, request: RefreshRotationRequest) -> RefreshRotationResult:
        status, payload = self.ledger.rotate_oauth_refresh_grant(**asdict(request))
        return RefreshRotationResult(
            RefreshRotationStatus(status),
            RefreshGrant(**payload) if payload is not None else None,
        )

    def revoke_refresh_grant(
        self, credential_digest: str, *, client_id: str, now: int
    ) -> None:
        self.ledger.revoke_oauth_refresh_grant(
            credential_digest, client_id=client_id, now=now
        )
