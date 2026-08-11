from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest

from my_data_hub.mcp.control_gateway import (
    AuthenticatedProviderControlClient,
    SplitControlPlaneReader,
)
from my_data_hub.mcp.oauth import AccessIdentity


def identity() -> AccessIdentity:
    return AccessIdentity(
        subject="owner",
        client_id="owner-operator",
        scopes=frozenset({"provider:read", "provider:write"}),
        audience="mcp",
        token_id="oauth-token-id-must-not-cross",
        expires_at=2_100_000_000,
        issuer="https://issuer.example",
        issued_at=2_000_000_000,
        resource="https://mcp.example/mcp",
    )


@pytest.mark.asyncio
async def test_authenticated_control_client_forwards_metadata_without_oauth_or_kaggle_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed = {}

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):  # type: ignore[no-untyped-def]
            return None

        def read(self, _limit):  # type: ignore[no-untyped-def]
            return b'{"provider_ref":"owner/data","provider_version":1}'

    def open_request(request, timeout):  # type: ignore[no-untyped-def]
        observed["request"] = request
        observed["timeout"] = timeout
        return Response()

    monkeypatch.setattr("my_data_hub.mcp.control_gateway.urlopen", open_request)
    client = AuthenticatedProviderControlClient(
        "http://control-plane:8080/internal/mcp-provider/invoke",
        b"g" * 32,
    )
    result = await client.invoke_control(
        "provider.resources.read",
        {
            "resource_ref": "owner/data",
            "control_class": "mcp_managed",
            "private": True,
            "payload": {"kind": "dataset", "claim_sha256": "a" * 64},
        },
        identity(),
    )
    assert result["provider_version"] == 1
    request = observed["request"]
    assert request.get_header("Authorization") == "Bearer " + "g" * 32
    body = json.loads(request.data)
    assert observed["timeout"] == 3660
    assert body["principal"]["subject"] == "owner"
    assert "token_id" not in body["principal"]
    assert b"oauth-token-id-must-not-cross" not in request.data
    assert b"KAGGLE" not in request.data


@pytest.mark.asyncio
async def test_split_reader_routes_only_exact_provider_actions_to_control_authority() -> None:
    class Target:
        def __init__(self, name: str) -> None:
            self.name = name
            self.calls = []

        async def invoke_control(self, tool, arguments, principal):  # type: ignore[no-untyped-def]
            self.calls.append((tool, dict(arguments), principal.subject))
            return {"target": self.name}

    local = Target("local-ledger")
    provider = Target("single-provider-authority")
    split = SplitControlPlaneReader(local, provider)  # type: ignore[arg-type]
    assert (await split.invoke_control("provider.resources.delete", {}, identity()))["target"] == (
        "single-provider-authority"
    )
    assert (await split.invoke_control("data.change.status", {}, identity()))["target"] == "local-ledger"
    acceptance_identity = replace(identity(), scopes=frozenset({"acceptance:operate"}))
    assert (
        await split.invoke_control(
            "acceptance.scenario.status", {"task_id": "00000000-0000-0000-0000-000000000001"}, acceptance_identity
        )
    )["target"] == "single-provider-authority"
    assert len(provider.calls) == 2 and len(local.calls) == 1


def test_control_gateway_token_file_must_be_private(tmp_path: Path) -> None:
    token = tmp_path / "gateway.token"
    token.write_text("g" * 32)
    token.chmod(0o644)
    with pytest.raises(ValueError, match="private"):
        AuthenticatedProviderControlClient.from_token_file(
            "http://control-plane:8080/internal/mcp-provider/invoke", token
        )
    token.chmod(0o600)
    assert AuthenticatedProviderControlClient.from_token_file(
        "http://control-plane:8080/internal/mcp-provider/invoke", token
    )
