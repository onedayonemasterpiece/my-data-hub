#!/usr/bin/env python3
"""Create a private, short-lived post-deploy OAuth negative-token bundle."""

from __future__ import annotations

import argparse
import json
import os
import secrets
import time
from dataclasses import asdict
from pathlib import Path

import jwt
from cryptography.hazmat.primitives.serialization import load_pem_private_key

from my_data_hub.auth.control import OAuthRevocationQuery
from my_data_hub.control_plane.ledger import ControlLedger
from my_data_hub.mcp.catalog import READER_PROFILE_SCOPES


def _private_regular_file(value: str, label: str) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute() or path.is_symlink() or not path.is_file() or path.stat().st_mode & 0o077:
        raise ValueError(f"{label} must be an absolute mode-0600 regular file")
    return path


def _write_private_json(path: Path, payload: dict[str, str]) -> None:
    if not path.is_absolute() or path.is_symlink() or (path.exists() and not path.is_file()):
        raise ValueError("output must be an absolute non-symbolic file path")
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{secrets.token_hex(8)}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, separators=(",", ":"), sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        path.chmod(0o600)
    finally:
        temporary.unlink(missing_ok=True)


def prepare_bundle(
    *,
    signing_key_file: Path,
    control_ledger_path: Path,
    output: Path,
    issuer: str,
    audience: str,
    resource: str,
    subject: str,
    client_id: str,
    key_id: str,
    now: int | None = None,
) -> dict[str, object]:
    current = int(time.time()) if now is None else now
    private_key = load_pem_private_key(signing_key_file.read_bytes(), password=None)
    if getattr(private_key, "key_size", 0) < 2048:
        raise ValueError("OAuth canary signing key must be RSA with at least 2048 bits")
    ledger = ControlLedger(control_ledger_path)
    client = ledger.oauth_client(issuer, client_id)
    if (
        client is None
        or client["enabled"] is not True
        or client["principal_id"] != subject
        or client["profile_kind"] != "reader"
        or frozenset(client["allowed_scopes"]) - {"openid", "offline_access"} != READER_PROFILE_SCOPES
    ):
        raise RuntimeError("enabled exact reader client is absent from the control ledger")

    def issue(
        name: str,
        *,
        issued_at: int = current,
        expires_at: int = current + 600,
        claim_issuer: str = issuer,
        claim_audience: str = audience,
        claim_resource: str = resource,
        scopes: frozenset[str] = READER_PROFILE_SCOPES,
    ) -> tuple[str, OAuthRevocationQuery]:
        token_id = f"post-deploy-{name}-{secrets.token_urlsafe(12)}"
        token = jwt.encode(
            {
                "iss": claim_issuer,
                "sub": subject,
                "aud": claim_audience,
                "resource": claim_resource,
                "client_id": client_id,
                "scope": " ".join(sorted(scopes)),
                "jti": token_id,
                "iat": issued_at,
                "nbf": issued_at,
                "exp": expires_at,
            },
            private_key,
            algorithm="RS256",
            headers={"kid": key_id, "typ": "at+jwt"},
        )
        return token, OAuthRevocationQuery(issuer, token_id, client_id, subject, issued_at)

    expired, _ = issue("expired", issued_at=current - 600, expires_at=current - 300)
    revoked, revoked_query = issue("revoked")
    wrong_issuer, _ = issue("wrong-issuer", claim_issuer=f"{issuer}/wrong")
    wrong_audience, _ = issue("wrong-audience", claim_audience=f"{audience}/wrong")
    wrong_resource, _ = issue("wrong-resource", claim_resource=f"{resource}/wrong")
    wrong_scope, _ = issue("wrong-scope", scopes=frozenset({"post-deploy:forbidden"}))
    reference = json.dumps(asdict(revoked_query), sort_keys=True, separators=(",", ":"))
    ledger.revoke_oauth_reference(
        token_reference=reference,
        client_id=client_id,
        principal_id=subject,
        reason_code="POST_DEPLOY_CANARY",
        audit_ref=f"post-deploy://oauth-canary/{current}",
    )
    _write_private_json(
        output,
        {
            "invalid": secrets.token_urlsafe(32),
            "expired": expired,
            "revoked": revoked,
            "wrong_issuer": wrong_issuer,
            "wrong_audience": wrong_audience,
            "wrong_resource": wrong_resource,
            "wrong_scope": wrong_scope,
        },
    )
    return {
        "ok": True,
        "output": str(output),
        "credential_count": 7,
        "expires_at": current + 600,
        "revocation_authority": "control-ledger",
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--signing-key-file", required=True)
    parser.add_argument("--control-ledger", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--issuer", default="https://identity.kenigevents.ru")
    parser.add_argument("--audience", default="https://mcp-datahub.kenigevents.ru/mcp")
    parser.add_argument("--resource", default="https://mcp-datahub.kenigevents.ru/mcp")
    parser.add_argument("--subject", default="datahub-owner")
    parser.add_argument("--client-id", default="chatgpt-reader")
    parser.add_argument("--key-id", required=True)
    args = parser.parse_args()
    result = prepare_bundle(
        signing_key_file=_private_regular_file(args.signing_key_file, "OAuth signing key"),
        control_ledger_path=Path(args.control_ledger).expanduser(),
        output=Path(args.output).expanduser(),
        issuer=args.issuer,
        audience=args.audience,
        resource=args.resource,
        subject=args.subject,
        client_id=args.client_id,
        key_id=args.key_id,
    )
    # This receipt contains paths and counts only, never bearer values.
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
