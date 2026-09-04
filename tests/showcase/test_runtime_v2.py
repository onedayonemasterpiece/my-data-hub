from __future__ import annotations

import json
import time
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from my_data_hub.mcp.oauth import AccessIdentity
from my_data_hub.showcase.gateway import (
    ShowcaseGatewayClient,
    ShowcaseGatewayError,
    ShowcaseGatewaySettings,
)
from my_data_hub.showcase.models import ShowcaseView
from my_data_hub.showcase.runtime import (
    PrincipalDocument,
    ShowcaseOperationController,
    ShowcaseOperationJournal,
    ShowcaseRuntimeSettings,
    create_app,
    prepare_site_runtime,
    read_runtime_token,
)

TOKEN = "showcase-runtime-test-token-0123456789abcdef"
OLD_SLUG = "old_showcase_slug_0123456789abcdef"
NEW_SLUG = "new_showcase_slug_0123456789abcdef"


def test_prepare_site_runtime_accepts_read_only_image_template(tmp_path: Path) -> None:
    template = tmp_path / "template"
    (template / "node_modules").mkdir(parents=True)
    (template / "package.json").write_text("{}", encoding="utf-8")
    template.chmod(0o555)
    runtime = tmp_path / "work" / "site"
    settings = ShowcaseRuntimeSettings(
        token_file=tmp_path / "gateway.key",
        operation_journal=tmp_path / "operations.json",
        site_template_dir=template,
        site_runtime_dir=runtime,
    )

    prepare_site_runtime(settings)

    assert (runtime / "node_modules").is_symlink()
    assert (runtime / "package.json").read_text(encoding="utf-8") == "{}"


def token_file(tmp_path: Path) -> Path:
    path = tmp_path / "showcase.key"
    path.write_text(TOKEN, encoding="utf-8")
    path.chmod(0o600)
    return path


def identity(*scopes: str) -> AccessIdentity:
    return AccessIdentity(
        subject="owner",
        client_id="chatgpt",
        scopes=frozenset(scopes),
        audience="my-data-hub",
        token_id="token-id",
        expires_at=int(time.time()) + 300,
        issuer="https://issuer.example",
        issued_at=int(time.time()),
        resource="https://mcp.example/mcp",
    )


def principal(*scopes: str) -> PrincipalDocument:
    return PrincipalDocument(
        subject="owner",
        client_id="chatgpt",
        scopes=list(scopes),
        audience="my-data-hub",
        token_id="token-id",
        expires_at=int(time.time()) + 300,
        issuer="https://issuer.example",
        issued_at=int(time.time()),
        resource="https://mcp.example/mcp",
    )


class FakePublisher:
    def __init__(self) -> None:
        self.revoked: list[str] = []

    def revoke(self, *, view_id: str, slug: str) -> None:
        assert view_id == "main"
        self.revoked.append(slug)


class FakeManager:
    def __init__(self) -> None:
        self._publisher = FakePublisher()
        self.links = {"main": f"https://ideas.example/v/{OLD_SLUG}/"}
        self.rebuild_calls = 0
        self.rotate_calls = 0

    def list_surfaces(self):  # type: ignore[no-untyped-def]
        return [
            {
                "view_id": "main",
                "url": self.links["main"],
                "slug": OLD_SLUG,
                "status": "active",
            }
        ]

    def get_link(self, view_id: str):  # type: ignore[no-untyped-def]
        return {"view_id": view_id, "url": self.links[view_id], "status": "active"}

    def rebuild(self, view_id: str, *, idempotency_key: str | None = None):  # type: ignore[no-untyped-def]
        self.rebuild_calls += 1
        return {"view_id": view_id, "url": self.links[view_id], "build": self.rebuild_calls}

    def create_view(self, view_id: str, *, idempotency_key: str | None = None):  # type: ignore[no-untyped-def]
        self.links.setdefault(view_id, f"https://ideas.example/v/{OLD_SLUG}/")
        return {"view_id": view_id, "url": self.links[view_id]}

    def revoke_link(self, view_id: str, *, idempotency_key: str | None = None):  # type: ignore[no-untyped-def]
        return {"view_id": view_id, "status": "revoked"}

    def rotate_link(
        self,
        view_id: str,
        *,
        slug: str | None = None,
        idempotency_key: str | None = None,
    ):
        self.rotate_calls += 1
        previous = self.links[view_id]
        next_slug = slug or NEW_SLUG
        self.links[view_id] = f"https://ideas.example/v/{next_slug}/"
        self._publisher.revoke(view_id=view_id, slug=previous.rstrip("/").split("/")[-1])
        return {"view_id": view_id, "url": self.links[view_id]}


def test_gateway_carries_only_service_token_and_principal(tmp_path: Path) -> None:
    path = token_file(tmp_path)
    observed: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        observed["authorization"] = request.headers["authorization"]
        observed["document"] = json.loads(request.content)
        return httpx.Response(200, json={"ok": True, "result": {"status": "active"}})

    client = ShowcaseGatewayClient(
        ShowcaseGatewaySettings(
            url="http://127.0.0.1:8790/internal/mcp-showcase/invoke",
            token_file=path,
        ),
        default_identity=identity("showcase:read"),
        transport=httpx.MockTransport(handler),
    )
    assert client.list_surfaces() == {"status": "active"}
    assert observed["authorization"] == f"Bearer {TOKEN}"
    document = observed["document"]
    assert isinstance(document, dict)
    assert document["tool"] == "showcase.list"
    assert document["arguments"] == {}
    assert document["principal"]["subject"] == "owner"
    serialized = json.dumps(document)
    assert "GITHUB" not in serialized
    assert "publisher" not in serialized.lower()


def test_gateway_serializes_fastmcp_pydantic_arguments(tmp_path: Path) -> None:
    path = token_file(tmp_path)
    observed: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        observed.update(json.loads(request.content))
        return httpx.Response(200, json={"ok": True, "result": {"status": "dry_run"}})

    client = ShowcaseGatewayClient(
        ShowcaseGatewaySettings(
            url="http://127.0.0.1:8790/internal/mcp-showcase/invoke",
            token_file=path,
        ),
        default_identity=identity("showcase:write"),
        transport=httpx.MockTransport(handler),
    )
    view = ShowcaseView.model_validate(
        {
            "id": "main",
            "title": "Main view",
            "subtitle": "Partner-facing ideas",
            "item_ids": ["one-item"],
            "contact": {
                "value": "@owner",
                "href": "https://t.me/owner",
            },
        }
    )

    assert client.apply(
        "main",
        expected_source_revision="a" * 40,
        view=view,  # type: ignore[arg-type]  # FastMCP supplies the validated model.
        items=[],
        dry_run=True,
        publish=False,
        idempotency_key="showcase-apply-test",
    ) == {"status": "dry_run"}
    arguments = observed["arguments"]
    assert isinstance(arguments, dict)
    assert arguments["view"]["id"] == "main"


def test_gateway_refuses_non_loopback_plain_http(tmp_path: Path, monkeypatch) -> None:
    path = token_file(tmp_path)
    monkeypatch.setenv(
        "MY_DATA_HUB_SHOWCASE_GATEWAY_URL",
        "http://showcase.internal/internal/mcp-showcase/invoke",
    )
    monkeypatch.setenv("MY_DATA_HUB_SHOWCASE_GATEWAY_TOKEN_FILE", str(path))
    with pytest.raises(ShowcaseGatewayError, match="loopback"):
        ShowcaseGatewaySettings.from_env()


def test_runtime_list_never_returns_full_secret_url(tmp_path: Path) -> None:
    manager = FakeManager()
    controller = ShowcaseOperationController(
        manager,
        ShowcaseOperationJournal(tmp_path / "operations.json"),
    )
    result = controller.invoke("showcase.list", {}, principal("showcase:read"))
    serialized = json.dumps(result, ensure_ascii=False)
    assert OLD_SLUG not in serialized
    assert "https://ideas.example/v/old_…cdef/" in serialized
    assert "slug_sha256" in serialized


def test_runtime_enforces_service_token_and_oauth_scope(tmp_path: Path) -> None:
    manager = FakeManager()
    app = create_app(
        controller=ShowcaseOperationController(
            manager,
            ShowcaseOperationJournal(tmp_path / "operations.json"),
        ),
        token=TOKEN,
    )
    client = TestClient(app)
    body = {
        "tool": "showcase.list",
        "arguments": {},
        "principal": principal("showcase:read").model_dump(),
    }
    assert client.post("/internal/mcp-showcase/invoke", json=body).status_code == 401
    forbidden = dict(body)
    forbidden["principal"] = principal("platform:read").model_dump()
    response = client.post(
        "/internal/mcp-showcase/invoke",
        headers={"Authorization": f"Bearer {TOKEN}"},
        json=forbidden,
    )
    assert response.status_code == 403


def test_write_idempotency_returns_cached_result(tmp_path: Path) -> None:
    manager = FakeManager()
    controller = ShowcaseOperationController(
        manager,
        ShowcaseOperationJournal(tmp_path / "operations.json"),
    )
    arguments = {"view_id": "main", "idempotency_key": "rebuild:main:0001"}
    first = controller.invoke("showcase.rebuild", arguments, principal("showcase:write"))
    second = controller.invoke("showcase.rebuild", arguments, principal("showcase:write"))
    assert first["duplicate"] is False
    assert second["duplicate"] is True
    assert manager.rebuild_calls == 1


def test_rotation_recovers_after_active_switch_before_old_revoke(tmp_path: Path) -> None:
    manager = FakeManager()
    manager.links["main"] = f"https://ideas.example/v/{NEW_SLUG}/"
    journal_path = tmp_path / "operations.json"
    operation_key = "a" * 64
    journal_path.write_text(
        json.dumps(
            {
                "version": 1,
                "completed": {},
                "rotations": {
                    "main": {
                        "idempotency_key": "rotate:main:0001",
                        "operation_key": operation_key,
                        "old_slug": OLD_SLUG,
                        "new_slug": NEW_SLUG,
                        "phase": "active_switched",
                        "started_at": int(time.time()),
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    journal_path.chmod(0o600)
    controller = ShowcaseOperationController(
        manager,
        ShowcaseOperationJournal(journal_path),
    )
    result = controller.invoke(
        "showcase.rotate_link",
        {"view_id": "main", "idempotency_key": "rotate:main:0001"},
        principal("showcase:write"),
    )
    assert result["duplicate"] is False
    assert manager.rotate_calls == 0
    assert manager._publisher.revoked == [OLD_SLUG]
    saved = json.loads(journal_path.read_text(encoding="utf-8"))
    assert saved["rotations"] == {}


def test_runtime_token_requires_owner_only_permissions(tmp_path: Path) -> None:
    path = token_file(tmp_path)
    assert read_runtime_token(path) == TOKEN
    path.chmod(0o644)
    with pytest.raises(Exception, match="group or others"):
        read_runtime_token(path)


def test_full_link_requires_owner_write_scope(tmp_path: Path) -> None:
    manager = FakeManager()
    app = create_app(
        controller=ShowcaseOperationController(
            manager,
            ShowcaseOperationJournal(tmp_path / "operations.json"),
        ),
        token=TOKEN,
    )
    client = TestClient(app)
    body = {
        "tool": "showcase.get_link",
        "arguments": {"view_id": "main"},
        "principal": principal("showcase:read").model_dump(),
    }
    headers = {"Authorization": f"Bearer {TOKEN}"}
    assert client.post("/internal/mcp-showcase/invoke", headers=headers, json=body).status_code == 403
    body["principal"] = principal("showcase:write").model_dump()
    response = client.post("/internal/mcp-showcase/invoke", headers=headers, json=body)
    assert response.status_code == 200
    assert response.json()["result"]["url"].endswith(f"/{OLD_SLUG}/")


def test_apply_is_a_write_tool() -> None:
    from my_data_hub.showcase.gateway import SHOWCASE_WRITE_TOOLS
    assert "showcase.apply" in SHOWCASE_WRITE_TOOLS
