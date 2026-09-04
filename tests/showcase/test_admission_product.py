"""No global timeout relaxation: only authorized bounded Showcase calls."""
from __future__ import annotations

import asyncio
import dataclasses
import json

import pytest

from my_data_hub.mcp.admission import AdmissionLimits, HTTPAdmissionSecurity
from tests.showcase.test_main_mcp_integration import identity
from tests.test_http_security import echo_app, run_asgi, status_of


@pytest.mark.parametrize("name,scopes,expected", [
    ("showcase.create_view", {"showcase:write"}, 200),
    ("showcase.get_source", {"showcase:read"}, 200),
    ("showcase.create_view", {"showcase:read"}, 504),
    ("showcase.get_source", {"other:read"}, 504),
    ("other.tool", {"showcase:write"}, 504),
])
def test_only_authorized_showcase_gets_build_budget(name, scopes, expected):
    async def slow(scope, receive, send):
        await asyncio.sleep(0.2)
        await echo_app(scope, receive, send)

    principal = dataclasses.replace(identity(), scopes=frozenset(scopes))
    app = HTTPAdmissionSecurity(
        slow, allowed_origins=(), allowed_hosts=("localhost",),
        authenticator=lambda _header: principal,
        limits=AdmissionLimits(request_timeout_seconds=0.1),
    )
    messages = asyncio.run(run_asgi(
        app, headers=[(b"host", b"localhost"), (b"authorization", b"Bearer test")],
        chunks=[json.dumps({"method": "tools/call", "params": {"name": name}}).encode()],
    ))
    assert status_of(messages) == expected
