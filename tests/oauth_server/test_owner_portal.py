from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from urllib.parse import parse_qs, urlsplit

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from my_data_hub.oauth_server.owner_portal import OIDCLoginPortal

RETURN_TO = (
    "https://identity.example.test/authorize?response_type=code&client_id=chatgpt-reader"
)


@dataclass
class FakeAuthenticator:
    cookie_name: str = "mdh_owner_session"
    observed_nonce: str | None = None
    accepted_returns: list[str] = field(default_factory=list)

    def validate_return_to(self, return_to: str) -> None:
        if not return_to.startswith("https://identity.example.test/authorize?"):
            raise ValueError("wrong return URL")
        self.accepted_returns.append(return_to)

    def verified_claims(self, token: str, *, expected_nonce: str | None = None) -> dict[str, Any]:
        assert token == "provider-signed." + "x" * 80
        assert expected_nonce
        self.observed_nonce = expected_nonce
        return {
            "sub": "provider-owner",
            "auth_time": 1_999_999_900,
            "exp": 2_000_000_600,
            "nonce": expected_nonce,
        }


def _portal(authenticator: FakeAuthenticator, key: bytes = b"s" * 32) -> OIDCLoginPortal:
    return OIDCLoginPortal(
        authorization_endpoint="https://idp.example.test/authorize",
        token_endpoint="https://idp.example.test/token",
        client_id="owner-client",
        client_secret="owner-secret",
        redirect_uri="https://identity.example.test/owner/callback",
        state_key=key,
        authenticator=authenticator,  # type: ignore[arg-type]
        clock=lambda: 2_000_000_000,
    )


def _app(portal: OIDCLoginPortal) -> FastAPI:
    app = FastAPI()

    @app.get("/owner/login")
    def login(request: Request, return_to: str):  # type: ignore[no-untyped-def]
        return portal.begin(request, return_to=return_to)

    @app.get("/owner/callback")
    async def callback(request: Request):  # type: ignore[no-untyped-def]
        return await portal.callback(request)

    return app


def test_owner_code_portal_is_pkce_bound_restart_safe_and_sets_provider_session(
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    authenticator = FakeAuthenticator()
    first = _portal(authenticator)
    client = TestClient(_app(first), base_url="https://identity.example.test")
    started = client.get("/owner/login", params={"return_to": RETURN_TO}, follow_redirects=False)
    assert started.status_code == 303
    query = parse_qs(urlsplit(started.headers["location"]).query)
    assert query["client_id"] == ["owner-client"]
    assert query["redirect_uri"] == ["https://identity.example.test/owner/callback"]
    assert query["code_challenge_method"] == ["S256"]
    assert "owner-secret" not in started.headers["location"]
    cookie_header = started.headers["set-cookie"]
    assert "HttpOnly" in cookie_header and "Secure" in cookie_header and "SameSite=lax" in cookie_header

    sealed = client.cookies.get(first.state_cookie_name)
    state = first._open_state(sealed)
    assert state["state"] == query["state"][0]
    assert state["nonce"] == query["nonce"][0]
    assert state["return_to"] == RETURN_TO

    # A new process can finish the same in-flight ceremony because only the
    # durable state-key file and encrypted client cookie are required.
    restarted = _portal(authenticator)
    observed: dict[str, str] = {}

    def exchange(_self: OIDCLoginPortal, code: str, verifier: str) -> str:
        observed.update(code=code, verifier=verifier)
        return "provider-signed." + "x" * 80

    monkeypatch.setattr(OIDCLoginPortal, "_exchange", exchange)
    client = TestClient(_app(restarted), base_url="https://identity.example.test", cookies=client.cookies)
    completed = client.get(
        "/owner/callback",
        params={"code": "one-time-code", "state": state["state"]},
        follow_redirects=False,
    )
    assert completed.status_code == 303
    assert completed.headers["location"] == RETURN_TO
    assert observed == {"code": "one-time-code", "verifier": state["verifier"]}
    assert authenticator.observed_nonce == state["nonce"]
    cookies = completed.headers.get_list("set-cookie")
    assert any("mdh_owner_session=" in value and "HttpOnly" in value for value in cookies)
    assert all("owner-secret" not in value and "one-time-code" not in value for value in cookies)


def test_owner_portal_rejects_state_tampering_without_token_exchange(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    portal = _portal(FakeAuthenticator())
    client = TestClient(_app(portal), base_url="https://identity.example.test")
    started = client.get("/owner/login", params={"return_to": RETURN_TO}, follow_redirects=False)
    assert started.status_code == 303
    monkeypatch.setattr(
        OIDCLoginPortal,
        "_exchange",
        lambda *_args: (_ for _ in ()).throw(AssertionError("exchange must not run")),
    )
    denied = client.get(
        "/owner/callback", params={"code": "code", "state": "changed"}, follow_redirects=False
    )
    assert denied.status_code == 401
    assert denied.json() == {"error": "access_denied"}
    assert denied.headers["cache-control"] == "no-store"


def test_owner_portal_rejects_open_return_url() -> None:
    portal = _portal(FakeAuthenticator())
    with pytest.raises(ValueError, match="wrong return URL"):
        portal.authenticator.validate_return_to(
            "https://attacker.example/authorize?client_id=x"
        )
