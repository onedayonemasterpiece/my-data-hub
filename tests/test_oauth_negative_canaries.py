from __future__ import annotations

import json
from pathlib import Path

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from my_data_hub.control_plane.adapters import ControlLedgerOAuthAuthority
from my_data_hub.control_plane.ledger import ControlLedger
from my_data_hub.mcp.catalog import READER_PROFILE_SCOPES
from my_data_hub.mcp.oauth import RevocationKey
from scripts.prepare_oauth_negative_canaries import prepare_bundle


def test_prepares_independent_private_canaries_and_control_ledger_revocation(tmp_path: Path) -> None:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    key_path = tmp_path / "signing.pem"
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    key_path.chmod(0o600)
    ledger_path = tmp_path / "control.sqlite3"
    ledger = ControlLedger(ledger_path)
    ledger.register_oauth_client(
        issuer="https://identity.kenigevents.ru",
        client_id="chatgpt-reader",
        principal_id="datahub-owner",
        allowed_scopes=READER_PROFILE_SCOPES | {"openid", "offline_access"},
        profile_kind="reader",
    )
    output = tmp_path / "negative.json"

    receipt = prepare_bundle(
        signing_key_file=key_path,
        control_ledger_path=ledger_path,
        output=output,
        issuer="https://identity.kenigevents.ru",
        audience="https://mcp-datahub.kenigevents.ru/mcp",
        resource="https://mcp-datahub.kenigevents.ru/mcp",
        subject="datahub-owner",
        client_id="chatgpt-reader",
        key_id="canary-key",
        now=1_800_000_000,
    )

    assert receipt["credential_count"] == 7
    assert output.stat().st_mode & 0o777 == 0o600
    bundle = json.loads(output.read_text())
    assert set(bundle) == {
        "invalid",
        "expired",
        "revoked",
        "wrong_issuer",
        "wrong_audience",
        "wrong_resource",
        "wrong_scope",
    }
    assert len(set(bundle.values())) == 7
    revoked = jwt.decode(
        bundle["revoked"],
        key.public_key(),
        algorithms=["RS256"],
        audience="https://mcp-datahub.kenigevents.ru/mcp",
        issuer="https://identity.kenigevents.ru",
        options={"verify_exp": False, "verify_iat": False, "verify_nbf": False},
    )
    assert ControlLedgerOAuthAuthority(ledger).is_revoked(
        RevocationKey(
            issuer=revoked["iss"],
            token_id=revoked["jti"],
            client_id=revoked["client_id"],
            subject=revoked["sub"],
            issued_at=revoked["iat"],
        )
    )
    wrong_scope = jwt.decode(
        bundle["wrong_scope"],
        key.public_key(),
        algorithms=["RS256"],
        options={
            "verify_signature": True,
            "verify_aud": False,
            "verify_exp": False,
            "verify_iat": False,
            "verify_nbf": False,
        },
    )
    assert wrong_scope["scope"] == "post-deploy:forbidden"
