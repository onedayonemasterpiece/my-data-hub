from __future__ import annotations

from pathlib import Path

from my_data_hub.control_plane.ledger import ControlLedger
from my_data_hub.oauth_server import ControlLedgerOAuthGrantStore
from my_data_hub.oauth_server.stores import (
    AuthorizationGrant,
    RefreshGrant,
    RefreshRotationRequest,
    RefreshRotationStatus,
)


def test_control_ledger_grants_are_atomic_durable_and_replay_revokes_family(
    tmp_path: Path,
) -> None:
    ledger_path = tmp_path / "control.sqlite3"
    store = ControlLedgerOAuthGrantStore(ControlLedger(ledger_path))
    authorization = AuthorizationGrant(
        code_digest="a" * 64,
        code_challenge="challenge",
        client_id="chatgpt-reader",
        redirect_uri="https://chatgpt.example/callback",
        resource="https://mcp.example/mcp",
        scopes=("bloggers:read",),
        subject="owner",
        nonce="nonce",
        authenticated_at=100,
        expires_at=200,
    )
    assert store.create_authorization_grant(authorization)
    assert not store.create_authorization_grant(authorization)
    restarted = ControlLedgerOAuthGrantStore(ControlLedger(ledger_path))
    assert restarted.consume_authorization_grant("a" * 64, now=150) == authorization
    assert restarted.consume_authorization_grant("a" * 64, now=150) is None

    refresh = RefreshGrant(
        credential_digest="b" * 64,
        family_id="family-1",
        client_id="chatgpt-reader",
        resource="https://mcp.example/mcp",
        scopes=("bloggers:read",),
        subject="owner",
        authenticated_at=100,
        expires_at=1_000,
    )
    assert restarted.create_refresh_grant(refresh)
    request = RefreshRotationRequest(
        presented_digest="b" * 64,
        successor_digest="c" * 64,
        client_id="chatgpt-reader",
        resource="https://mcp.example/mcp",
        requested_scopes=("bloggers:read",),
        successor_expires_at=900,
        now=200,
    )
    rotated = restarted.rotate_refresh_grant(request)
    assert rotated.status is RefreshRotationStatus.ROTATED
    assert rotated.grant is not None and rotated.grant.credential_digest == "c" * 64
    replayed = restarted.rotate_refresh_grant(request)
    assert replayed.status is RefreshRotationStatus.REPLAYED
    assert restarted.rotate_refresh_grant(
        RefreshRotationRequest(
            presented_digest="c" * 64,
            successor_digest="d" * 64,
            client_id="chatgpt-reader",
            resource="https://mcp.example/mcp",
            requested_scopes=None,
            successor_expires_at=900,
            now=201,
        )
    ).status is RefreshRotationStatus.INVALID


def test_expired_authorization_code_is_consumed_once(tmp_path: Path) -> None:
    store = ControlLedgerOAuthGrantStore(ControlLedger(tmp_path / "control.sqlite3"))
    grant = AuthorizationGrant(
        code_digest="e" * 64,
        code_challenge="challenge",
        client_id="client",
        redirect_uri="https://client.example/callback",
        resource="https://mcp.example/mcp",
        scopes=("bloggers:read",),
        subject="owner",
        nonce=None,
        authenticated_at=1,
        expires_at=2,
    )
    assert store.create_authorization_grant(grant)
    assert store.consume_authorization_grant(grant.code_digest, now=2) is None
    assert store.consume_authorization_grant(grant.code_digest, now=1) is None
