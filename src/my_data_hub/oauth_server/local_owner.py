"""Local browser-token owner ceremony for the devstand authorization server.

This deliberately mirrors the proven eventsBot MCP login topology: the browser
receives a bounded form on the authorization-server origin and the owner enters
one high-entropy operator token.  The token never appears in a URL, cookie,
OAuth grant, repository configuration, or response body.
"""

from __future__ import annotations

import base64
import html
import json
import secrets
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from cryptography.fernet import Fernet, InvalidToken
from fastapi import Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response

from .models import OwnerAuthenticationChallenge, OwnerIdentity


class LocalOwnerError(RuntimeError):
    """The local owner ceremony was malformed, expired, or denied."""


def _form_action_callback_origin(return_to: str, *, issuer_origin: str) -> str:
    """Return the one validated browser callback origin for form redirects.

    Chromium applies ``form-action`` to the full redirect chain initiated by a
    form submission.  The owner form posts to the issuer, then the OAuth
    authorization endpoint redirects to the client callback.  Permit only the
    two callback origin classes admitted by this service: the exact native
    IPv4 loopback origin and ChatGPT's exact HTTPS origin.  Same-origin
    callbacks need no duplicate source.
    """

    pairs = parse_qsl(urlsplit(return_to).query, keep_blank_values=True)
    redirects = [value for name, value in pairs if name == "redirect_uri"]
    if len(redirects) != 1:
        raise LocalOwnerError("owner request has no unique redirect URI")
    parsed = urlsplit(redirects[0])
    if parsed.username is not None or parsed.password is not None or parsed.fragment:
        raise LocalOwnerError("owner callback origin is invalid")
    try:
        callback_port = parsed.port
    except ValueError as exc:
        raise LocalOwnerError("owner callback port is invalid") from exc
    callback_origin = urlunsplit((parsed.scheme, parsed.netloc, "", "", ""))
    if callback_origin == issuer_origin:
        return issuer_origin
    if (
        parsed.scheme == "http"
        and parsed.hostname == "127.0.0.1"
        and callback_port is not None
        and parsed.netloc == f"127.0.0.1:{callback_port}"
    ):
        return callback_origin
    if parsed.scheme == "https" and parsed.netloc == "chatgpt.com":
        return callback_origin
    raise LocalOwnerError("owner callback origin is outside the browser allowlist")


def _no_store() -> dict[str, str]:
    return {
        "Cache-Control": "no-store",
        "Pragma": "no-cache",
        "Referrer-Policy": "no-referrer",
        "X-Content-Type-Options": "nosniff",
        "X-Frame-Options": "DENY",
    }


@dataclass(slots=True)
class LocalOwnerTokenAuthenticator:
    """Authenticate the one owner with a short-lived encrypted session cookie."""

    issuer: str
    authorization_url: str
    login_url: str
    owner_subject: str
    operator_token: str = field(repr=False)
    state_key: bytes = field(repr=False)
    cookie_name: str = "mdh_owner_session"
    request_ttl_seconds: int = 300
    session_ttl_seconds: int = 3600
    clock: Any = time.time
    _fernet: Fernet = field(init=False, repr=False)

    def __post_init__(self) -> None:
        for value in (self.issuer, self.authorization_url, self.login_url):
            parsed = urlsplit(value)
            if (
                parsed.scheme != "https"
                or not parsed.netloc
                or parsed.username is not None
                or parsed.password is not None
                or parsed.fragment
            ):
                raise ValueError("local owner URLs must use exact HTTPS")
        issuer = urlsplit(self.issuer)
        authorization = urlsplit(self.authorization_url)
        login = urlsplit(self.login_url)
        if (
            authorization.scheme != issuer.scheme
            or authorization.netloc != issuer.netloc
            or authorization.path != "/authorize"
            or authorization.query
            or login.scheme != issuer.scheme
            or login.netloc != issuer.netloc
            or login.path != "/owner/login"
            or login.query
        ):
            raise ValueError("local owner endpoints must stay on the exact issuer origin")
        if not self.owner_subject or len(self.owner_subject) > 255:
            raise ValueError("local owner subject is invalid")
        if not 32 <= len(self.operator_token) <= 1024:
            raise ValueError("local owner operator token is outside its bound")
        if len(self.state_key) != 32:
            raise ValueError("local owner state key must be exactly 32 bytes")
        if not self.cookie_name.replace("_", "").isalnum():
            raise ValueError("local owner session cookie name is invalid")
        if not 60 <= self.request_ttl_seconds <= 600:
            raise ValueError("local owner request lifetime must be 60..600 seconds")
        if not 300 <= self.session_ttl_seconds <= 43_200:
            raise ValueError("local owner session lifetime must be 5 minutes..12 hours")
        self._fernet = Fernet(base64.urlsafe_b64encode(self.state_key))

    def authenticate_owner(self, request: Request, *, return_to: str) -> OwnerIdentity | OwnerAuthenticationChallenge:
        self.validate_return_to(return_to)
        sealed = request.cookies.get(self.cookie_name, "")
        if sealed:
            try:
                payload = self._open(sealed, ttl=self.session_ttl_seconds)
                if set(payload) != {"subject", "authenticated_at", "issued_at", "nonce"}:
                    raise LocalOwnerError("owner session shape is invalid")
                subject = payload["subject"]
                authenticated_at = payload["authenticated_at"]
                if (
                    subject != self.owner_subject
                    or isinstance(authenticated_at, bool)
                    or not isinstance(authenticated_at, int)
                    or authenticated_at < 0
                ):
                    raise LocalOwnerError("owner session identity is invalid")
                return OwnerIdentity(subject=subject, authenticated_at=authenticated_at)
            except Exception:
                # Invalid/expired cookies never reach OAuth code issuance.  A
                # successful fresh form submission replaces the cookie.
                pass
        return OwnerAuthenticationChallenge(self._challenge(return_to))

    def validate_return_to(self, return_to: str) -> None:
        target = urlsplit(return_to)
        expected = urlsplit(self.authorization_url)
        if (
            target.scheme != expected.scheme
            or target.netloc != expected.netloc
            or target.path != expected.path
            or not target.query
            or target.fragment
            or len(return_to) > 16_384
        ):
            raise ValueError("owner login return URL differs from the authorize endpoint")

    def seal_request(self, return_to: str) -> str:
        self.validate_return_to(return_to)
        return self._seal(
            {
                "return_to": return_to,
                "issued_at": int(self.clock()),
                "nonce": secrets.token_urlsafe(24),
            }
        )

    def open_request(self, sealed: str) -> str:
        payload = self._open(sealed, ttl=self.request_ttl_seconds)
        if set(payload) != {"return_to", "issued_at", "nonce"}:
            raise LocalOwnerError("owner request shape is invalid")
        return_to = payload["return_to"]
        if not isinstance(return_to, str):
            raise LocalOwnerError("owner request return URL is invalid")
        self.validate_return_to(return_to)
        return return_to

    def verify_operator_token(self, supplied: str) -> bool:
        return 1 <= len(supplied) <= 1024 and secrets.compare_digest(supplied, self.operator_token)

    def session_cookie(self) -> str:
        now = int(self.clock())
        return self._seal(
            {
                "subject": self.owner_subject,
                "authenticated_at": now,
                "issued_at": now,
                "nonce": secrets.token_urlsafe(24),
            }
        )

    def _challenge(self, return_to: str) -> str:
        self.validate_return_to(return_to)
        parsed = urlsplit(self.login_url)
        return urlunsplit(
            (
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                urlencode({"return_to": return_to}),
                "",
            )
        )

    def _seal(self, payload: Mapping[str, object]) -> str:
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        sealed: bytes = self._fernet.encrypt(raw)
        return sealed.decode("ascii")

    def _open(self, sealed: str, *, ttl: int) -> dict[str, object]:
        if not sealed or len(sealed) > 32_768:
            raise LocalOwnerError("owner state is absent")
        try:
            decoded = base64.urlsafe_b64decode(sealed.encode("ascii"))
            if base64.urlsafe_b64encode(decoded).decode("ascii") != sealed:
                raise LocalOwnerError("owner state encoding is not canonical")
            raw = self._fernet.decrypt(sealed.encode("ascii"), ttl=ttl)
            payload = json.loads(raw)
        except (InvalidToken, UnicodeError, ValueError, json.JSONDecodeError) as exc:
            raise LocalOwnerError("owner state is invalid or expired") from exc
        if not isinstance(payload, dict):
            raise LocalOwnerError("owner state is not an object")
        issued_at = payload.get("issued_at")
        if isinstance(issued_at, bool) or not isinstance(issued_at, int):
            raise LocalOwnerError("owner state issue time is invalid")
        now = int(self.clock())
        if issued_at > now + 5 or now - issued_at > ttl:
            raise LocalOwnerError("owner state is outside its lifetime")
        return dict(payload)


@dataclass(slots=True)
class LocalOwnerTokenPortal:
    """Render and accept the owner-token form on the issuer origin."""

    authenticator: LocalOwnerTokenAuthenticator

    def begin(self, request: Request, *, return_to: str) -> Response:
        sealed = self.authenticator.seal_request(return_to)
        issuer = urlsplit(self.authenticator.issuer)
        issuer_origin = urlunsplit((issuer.scheme, issuer.netloc, "", "", ""))
        callback_origin = _form_action_callback_origin(
            return_to,
            issuer_origin=issuer_origin,
        )
        form_action_sources = issuer_origin
        if callback_origin != issuer_origin:
            form_action_sources += f" {callback_origin}"
        # Use the already-validated absolute issuer endpoint both in the form
        # and CSP.  Some Chrome launch contexts reject ``'self'`` for an OAuth
        # popup even though the visible top-level URL is same-origin.
        login_url = html.escape(self.authenticator.login_url, quote=True)
        body = f"""<!doctype html><html lang=\"ru\"><head><meta charset=\"utf-8\">
<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">
<title>my-data-hub — авторизация</title>
<style>body{{font:16px system-ui;max-width:42rem;margin:8vh auto;padding:1.5rem;line-height:1.5}}
main{{border:1px solid #ccc;border-radius:12px;padding:1.5rem}}
input{{width:100%;box-sizing:border-box;padding:.7rem;margin:.5rem 0 1rem}}
button{{padding:.7rem 1rem}}.muted{{color:#666}}</style></head><body><main>
<h1>my-data-hub</h1><p>Подтвердите подключение OpenCode или ChatGPT к MCP.</p>
<p class=\"muted\">Токен используется только этой формой и не передаётся MCP или Kaggle.</p>
<form method=\"post\" action=\"{login_url}\" autocomplete=\"off\">
<input type=\"hidden\" name=\"owner_request\" value=\"{html.escape(sealed, quote=True)}\">
<label>Операторский токен<br>
<input type=\"password\" name=\"operator_token\" required autofocus autocomplete=\"off\"></label>
<button type=\"submit\">Предоставить доступ</button></form></main></body></html>"""
        return HTMLResponse(
            body,
            status_code=200,
            headers={
                **_no_store(),
                # ``no-referrer`` can serialize the Origin of a basic form
                # POST as ``null``.  Preserve only the issuer origin so the
                # admission boundary can enforce the exact same-origin POST
                # without disclosing the sealed return URL.
                "Referrer-Policy": "origin",
                "Content-Security-Policy": (
                    "default-src 'none'; style-src 'unsafe-inline'; "
                    f"form-action {form_action_sources}; base-uri 'none'; "
                    "frame-ancestors 'none'"
                ),
            },
        )

    def submit(self, request: Request, parameters: Mapping[str, str]) -> Response:
        try:
            if set(parameters) != {"owner_request", "operator_token"}:
                raise LocalOwnerError("owner form fields are invalid")
            return_to = self.authenticator.open_request(parameters["owner_request"])
            if not self.authenticator.verify_operator_token(parameters["operator_token"]):
                raise LocalOwnerError("owner token was denied")
            response = RedirectResponse(return_to, status_code=303, headers=_no_store())
            response.set_cookie(
                self.authenticator.cookie_name,
                self.authenticator.session_cookie(),
                max_age=self.authenticator.session_ttl_seconds,
                secure=True,
                httponly=True,
                samesite="lax",
                path="/",
            )
            return response
        except Exception:
            return JSONResponse({"error": "access_denied"}, status_code=403, headers=_no_store())

    def callback(self, request: Request) -> Response:
        # Local-token mode has no upstream callback endpoint.
        return JSONResponse({"error": "invalid_request"}, status_code=404, headers=_no_store())
