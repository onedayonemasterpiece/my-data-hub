#!/usr/bin/env python3
"""Verify one provider-issued owner session without printing the credential."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from urllib.parse import urlsplit

import jwt


def verify_bootstrap_session(
    *,
    token_file: Path,
    issuer: str,
    audience: str,
    jwks_url: str,
    provider_subject: str,
) -> dict[str, object]:
    for name, value in (("issuer", issuer), ("JWKS", jwks_url)):
        parsed = urlsplit(value)
        if (
            parsed.scheme != "https"
            or not parsed.netloc
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError(f"owner bootstrap {name} must be an exact HTTPS URL")
    if not audience or len(audience) > 512 or not provider_subject or len(provider_subject) > 512:
        raise ValueError("owner bootstrap audience and provider subject must be bounded")
    if (
        not token_file.is_absolute()
        or token_file.is_symlink()
        or not token_file.is_file()
        or token_file.stat().st_mode & 0o077
        or not 1 <= token_file.stat().st_size <= 16_384
    ):
        raise ValueError("owner bootstrap session must be an absolute mode-0600 regular file")
    token = token_file.read_text(encoding="utf-8").strip()
    if not token or any(character.isspace() or ord(character) < 0x21 for character in token):
        raise ValueError("owner bootstrap session is not one bounded JWT")
    client = jwt.PyJWKClient(jwks_url, cache_keys=False, timeout=5)
    signing_key = client.get_signing_key_from_jwt(token)
    claims = jwt.decode(
        token,
        signing_key.key,
        algorithms=["RS256", "ES256"],
        audience=audience,
        issuer=issuer,
        options={"require": ["iss", "sub", "aud", "exp", "iat", "nbf", "auth_time"]},
    )
    if claims.get("sub") != provider_subject:
        raise ValueError("owner bootstrap session provider subject differs from policy")
    return {
        "ok": True,
        "local_principal": "datahub-owner",
        "provider_subject_verified": True,
        "issuer": issuer,
        "audience": audience,
        "expires_at": int(claims["exp"]),
        "credential_emitted": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--token-file", required=True)
    parser.add_argument("--issuer", required=True)
    parser.add_argument("--audience", required=True)
    parser.add_argument("--jwks-url", required=True)
    parser.add_argument("--provider-subject", required=True)
    args = parser.parse_args()
    result = verify_bootstrap_session(
        token_file=Path(args.token_file).expanduser(),
        issuer=args.issuer,
        audience=args.audience,
        jwks_url=args.jwks_url,
        provider_subject=args.provider_subject,
    )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
