from __future__ import annotations

from my_data_hub.auth.control import OAuthAuditEvent, OAuthClientRecord, OAuthRevocationQuery
from my_data_hub.control_plane.adapters import ControlLedgerOAuthAuthority
from my_data_hub.control_plane.ledger import ControlLedger


def test_oauth_clients_revocation_and_audit_live_in_control_ledger(tmp_path) -> None:  # type: ignore[no-untyped-def]
    ledger = ControlLedger(tmp_path / "control.sqlite3")
    authority = ControlLedgerOAuthAuthority(ledger)
    ledger.register_oauth_client(
        issuer="https://issuer.example",
        client_id="chatgpt-reader",
        principal_id="datahub-owner",
        allowed_scopes=frozenset({"hub:read"}),
        profile_kind="reader",
    )
    client = authority.get_client("https://issuer.example", "chatgpt-reader")
    assert client is not None and client.enabled and client.allowed_scopes == frozenset({"hub:read"})
    query = OAuthRevocationQuery(
        issuer="https://issuer.example", token_id="jti-1", client_id="chatgpt-reader",
        subject="datahub-owner", issued_at=1,
    )
    assert authority.is_revoked(query) is False
    from my_data_hub.control_plane.adapters import _revocation_reference
    ledger.revoke_oauth_reference(
        token_reference=_revocation_reference(query), client_id=query.client_id,
        principal_id=query.subject, reason_code="owner_rotation", audit_ref="audit-1",
    )
    assert authority.is_revoked(query) is True
    authority.record_oauth_audit(
        OAuthAuditEvent(
            event="token", outcome="denied", issuer=query.issuer,
            client_id=query.client_id, subject=query.subject, token_id=query.token_id,
        )
    )


def test_configured_oauth_client_refresh_preserves_atomic_disabled_state(tmp_path) -> None:  # type: ignore[no-untyped-def]
    ledger = ControlLedger(tmp_path / "control.sqlite3")
    ledger.register_oauth_client(
        issuer="https://issuer.example",
        client_id="chatgpt-reader",
        principal_id="datahub-owner",
        allowed_scopes=frozenset({"hub:read"}),
        profile_kind="reader",
        enabled=False,
    )

    ledger.register_configured_oauth_client(
        issuer="https://issuer.example",
        client_id="chatgpt-reader",
        principal_id="new-owner",
        allowed_scopes=frozenset({"hub:read", "data:read"}),
        profile_kind="owner_operator",
    )

    configured = ledger.oauth_client("https://issuer.example", "chatgpt-reader")
    assert configured == {
        "issuer": "https://issuer.example",
        "client_id": "chatgpt-reader",
        "enabled": False,
        "allowed_scopes": frozenset({"hub:read", "data:read"}),
        "principal_id": "new-owner",
        "profile_kind": "owner_operator",
    }


def test_resolved_cimd_client_is_persisted_without_reenabling_disabled_client(tmp_path) -> None:  # type: ignore[no-untyped-def]
    ledger = ControlLedger(tmp_path / "control.sqlite3")
    authority = ControlLedgerOAuthAuthority(ledger)
    record = OAuthClientRecord(
        issuer="https://identity.example",
        client_id="https://chatgpt.com/oauth/exact/client.json",
        enabled=True,
        allowed_scopes=frozenset({"openid", "provider:read", "provider:write"}),
    )

    first = authority.register_resolved_client(record, principal_id="datahub-owner")
    ledger.register_oauth_client(
        issuer=record.issuer,
        client_id=record.client_id,
        principal_id="datahub-owner",
        allowed_scopes=record.allowed_scopes,
        profile_kind="owner_operator",
        enabled=False,
    )
    disabled = authority.register_resolved_client(record, principal_id="datahub-owner")

    assert first == record
    assert disabled == OAuthClientRecord(
        issuer=record.issuer,
        client_id=record.client_id,
        enabled=False,
        allowed_scopes=record.allowed_scopes,
    )
