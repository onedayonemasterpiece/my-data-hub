"""Restart-safe upstream OIDC login portal for the single owner principal."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import secrets
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlencode, urlsplit

from cryptography.fernet import Fernet, InvalidToken
from fastapi import Request
from fastapi.responses import JSONResponse, RedirectResponse, Response

from .owner_oidc import OIDCSessionOwnerAuthenticator


class OwnerPortalError(RuntimeError):
    """The bounded upstream owner ceremony could not be completed."""


def _exact_https(value: str, *, label: str) -> str:
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
    ):
        raise ValueError(f"{label} must use exact HTTPS")
    return value


def _no_store() -> dict[str, str]:
    return {"Cache-Control": "no-store", "Pragma": "no-cache"}


@dataclass(slots=True)
class OIDCLoginPortal:
    """Perform code+PKCE upstream login and persist no bearer server-side.

    The only in-flight state is an encrypted, short-lived HttpOnly cookie.  Its
    key is a durable owner-only file, so a control-process restart does not lose
    an already-started login.  The completed session cookie contains the
    provider-signed ID token; this service never mints an owner identity or
    stores an owner password.
    """

    authorization_endpoint: str
    token_endpoint: str
    client_id: str
    client_secret: str
    redirect_uri: str
    state_key: bytes
    authenticator: OIDCSessionOwnerAuthenticator
    state_cookie_name: str = "mdh_owner_login_state"
    state_ttl_seconds: int = 300
    token_timeout_seconds: float = 8.0
    clock: Any = time.time
    _fernet: Fernet = field(init=False, repr=False)

    def __post_init__(self) -> None:
        _exact_https(self.authorization_endpoint, label="owner authorization endpoint")
        _exact_https(self.token_endpoint, label="owner token endpoint")
        _exact_https(self.redirect_uri, label="owner redirect URI")
        if urlsplit(self.authorization_endpoint).query or urlsplit(self.token_endpoint).query:
            raise ValueError("owner OIDC endpoints may not contain a fixed query")
        redirect = urlsplit(self.redirect_uri)
        if redirect.query or not redirect.path.endswith("/owner/callback"):
            raise ValueError("owner redirect URI must be the exact callback endpoint")
        if not self.client_id or len(self.client_id) > 256:
            raise ValueError("owner OIDC client ID is invalid")
        if not self.client_secret or len(self.client_secret.encode("utf-8")) > 8192:
            raise ValueError("owner OIDC client secret is invalid")
        if len(self.state_key) != 32:
            raise ValueError("owner portal state key must be exactly 32 bytes")
        if not self.state_cookie_name.replace("_", "").isalnum():
            raise ValueError("owner portal state cookie name is invalid")
        if not 60 <= self.state_ttl_seconds <= 600:
            raise ValueError("owner portal state lifetime must be 60..600 seconds")
        if not 1 <= self.token_timeout_seconds <= 15:
            raise ValueError("owner token timeout must be 1..15 seconds")
        self._fernet = Fernet(base64.urlsafe_b64encode(self.state_key))

    def begin(self, request: Request, *, return_to: str) -> Response:
        self.authenticator.validate_return_to(return_to)
        state = secrets.token_urlsafe(32)
        nonce = secrets.token_urlsafe(32)
        verifier = secrets.token_urlsafe(48)
        issued_at = int(self.clock())
        payload = json.dumps(
            {
                "state": state,
                "nonce": nonce,
                "verifier": verifier,
                "return_to": return_to,
                "issued_at": issued_at,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        sealed = self._fernet.encrypt(payload).decode("ascii")
        challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode().rstrip("=")
        location = f"{self.authorization_endpoint}?{urlencode({
            'response_type': 'code',
            'client_id': self.client_id,
            'redirect_uri': self.redirect_uri,
            'scope': 'openid profile email',
            'state': state,
            'nonce': nonce,
            'code_challenge': challenge,
            'code_challenge_method': 'S256',
        })}"
        response = RedirectResponse(location, status_code=303, headers=_no_store())
        response.set_cookie(
            self.state_cookie_name,
            sealed,
            max_age=self.state_ttl_seconds,
            secure=True,
            httponly=True,
            samesite="lax",
            path=urlsplit(self.redirect_uri).path,
        )
        return response

    async def callback(self, request: Request) -> Response:
        try:
            query = list(request.query_params.multi_items())
            if len(query) > 8 or len(request.scope.get("query_string", b"")) > 8192:
                raise OwnerPortalError("owner callback query is outside its bound")
            values: dict[str, str] = {}
            for key, value in query:
                if key in values:
                    raise OwnerPortalError("owner callback contains a duplicate field")
                values[key] = value
            if values.get("error") or set(values) - {"code", "state", "error", "error_description"}:
                raise OwnerPortalError("upstream owner authorization was denied")
            code = values.get("code", "")
            state = values.get("state", "")
            if not code or len(code) > 8192 or not state or len(state) > 256:
                raise OwnerPortalError("owner callback identity is invalid")
            session = self._open_state(request.cookies.get(self.state_cookie_name, ""))
            if not secrets.compare_digest(state, session["state"]):
                raise OwnerPortalError("owner callback state differs from its ceremony")
            token = await asyncio.to_thread(self._exchange, code, session["verifier"])
            claims = await asyncio.to_thread(
                self.authenticator.verified_claims,
                token,
                expected_nonce=session["nonce"],
            )
            now = int(self.clock())
            expires_at = claims.get("exp")
            if isinstance(expires_at, bool) or not isinstance(expires_at, int) or expires_at <= now:
                raise OwnerPortalError("owner session is already expired")
            max_age = min(3600, expires_at - now)
            response = RedirectResponse(session["return_to"], status_code=303, headers=_no_store())
            response.delete_cookie(
                self.state_cookie_name,
                path=urlsplit(self.redirect_uri).path,
                secure=True,
                httponly=True,
                samesite="lax",
            )
            response.set_cookie(
                self.authenticator.cookie_name,
                token,
                max_age=max_age,
                secure=True,
                httponly=True,
                samesite="lax",
                path="/",
            )
            return response
        except Exception:
            response = JSONResponse({"error": "access_denied"}, status_code=401, headers=_no_store())
            response.delete_cookie(
                self.state_cookie_name,
                path=urlsplit(self.redirect_uri).path,
                secure=True,
                httponly=True,
                samesite="lax",
            )
            return response

    def _open_state(self, sealed: str) -> dict[str, Any]:
        if not sealed or len(sealed) > 32_768:
            raise OwnerPortalError("owner login state is absent")
        try:
            raw = self._fernet.decrypt(sealed.encode("ascii"), ttl=self.state_ttl_seconds)
            value = json.loads(raw)
        except (InvalidToken, UnicodeError, json.JSONDecodeError) as exc:
            raise OwnerPortalError("owner login state is invalid or expired") from exc
        if not isinstance(value, dict) or set(value) != {
            "state",
            "nonce",
            "verifier",
            "return_to",
            "issued_at",
        }:
            raise OwnerPortalError("owner login state shape is invalid")
        for key in ("state", "nonce", "verifier", "return_to"):
            if not isinstance(value[key], str) or not value[key]:
                raise OwnerPortalError("owner login state value is invalid")
        issued_at = value["issued_at"]
        if isinstance(issued_at, bool) or not isinstance(issued_at, int):
            raise OwnerPortalError("owner login issue time is invalid")
        now = int(self.clock())
        if issued_at > now + 5 or now - issued_at > self.state_ttl_seconds:
            raise OwnerPortalError("owner login state is outside its lifetime")
        self.authenticator.validate_return_to(value["return_to"])
        return value

    def _exchange(self, code: str, verifier: str) -> str:
        encoded = urllib.parse.urlencode(
            {
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": self.redirect_uri,
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "code_verifier": verifier,
            }
        ).encode("ascii")
        request = urllib.request.Request(
            self.token_endpoint,
            data=encoded,
            method="POST",
            headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=self.token_timeout_seconds) as response:
                body = response.read(65_537)
                content_type = response.headers.get_content_type()
        except (OSError, urllib.error.HTTPError) as exc:
            raise OwnerPortalError("upstream owner token exchange failed") from exc
        if len(body) > 65_536 or content_type != "application/json":
            raise OwnerPortalError("upstream owner token response is invalid")
        try:
            value = json.loads(body)
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise OwnerPortalError("upstream owner token response is not JSON") from exc
        if not isinstance(value, dict) or set(value) - {
            "access_token",
            "expires_in",
            "id_token",
            "scope",
            "token_type",
        }:
            raise OwnerPortalError("upstream owner token response fields are invalid")
        token = value.get("id_token")
        if not isinstance(token, str) or not 64 <= len(token) <= 16_384:
            raise OwnerPortalError("upstream owner ID token is invalid")
        return token
