from __future__ import annotations

from pathlib import Path

import pytest

from scripts.verify_owner_oidc_bootstrap import verify_bootstrap_session


def test_owner_bootstrap_verifier_maps_sanitized_fixed_principal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    token = tmp_path / "owner.jwt"
    token.write_text("header.payload.signature")
    token.chmod(0o600)

    class Client:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        def get_signing_key_from_jwt(self, value: str) -> object:
            assert value == "header.payload.signature"
            return type("SigningKey", (), {"key": object()})()

    monkeypatch.setattr("jwt.PyJWKClient", Client)
    monkeypatch.setattr(
        "jwt.decode",
        lambda *_args, **_kwargs: {
            "iss": "https://idp.example",
            "sub": "opaque-subject",
            "aud": "owner-client",
            "exp": 2_000_000_000,
            "iat": 1_900_000_000,
            "nbf": 1_900_000_000,
            "auth_time": 1_900_000_000,
        },
    )
    result = verify_bootstrap_session(
        token_file=token,
        issuer="https://idp.example",
        audience="owner-client",
        jwks_url="https://idp.example/jwks",
        provider_subject="opaque-subject",
    )
    assert result["local_principal"] == "datahub-owner"
    assert result["credential_emitted"] is False
    assert "opaque-subject" not in result.values()


def test_owner_bootstrap_verifier_rejects_nonprivate_input(tmp_path: Path) -> None:
    token = tmp_path / "owner.jwt"
    token.write_text("header.payload.signature")
    token.chmod(0o644)
    with pytest.raises(ValueError, match="mode-0600"):
        verify_bootstrap_session(
            token_file=token,
            issuer="https://idp.example",
            audience="owner-client",
            jwks_url="https://idp.example/jwks",
            provider_subject="opaque-subject",
        )
