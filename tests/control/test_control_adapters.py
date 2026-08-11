from __future__ import annotations

from my_data_hub.auth.control import OAuthAuditEvent, OAuthRevocationQuery
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
