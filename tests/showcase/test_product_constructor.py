"""Product contract tested through real source, controller, gateway and MCP schemas."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from my_data_hub.showcase.gateway import ShowcaseGatewayClient, ShowcaseGatewayError, ShowcaseGatewaySettings
from my_data_hub.showcase.manager import ShowcaseManager
from my_data_hub.showcase.models import Contact, ShowcaseViewInput, ShowcaseWriteItem
from my_data_hub.showcase.requests import MAX_ARGUMENT_BYTES, ShowcaseRequestError, resolve_mode
from my_data_hub.showcase.runtime import ShowcaseOperationController, ShowcaseOperationJournal, create_app
from my_data_hub.showcase.source import FilesystemShowcaseSource, GitHubShowcaseWriter, ShowcaseSourceError
from my_data_hub.showcase.state import ShowcaseStateStore
from tests.showcase.test_manager import FakeBuilder, FakePublisher
from tests.showcase.test_runtime_v2 import TOKEN, identity, principal, token_file

FIXTURES = Path(__file__).parent / "fixtures"


class LocalTestWriter:
    """Test-only CAS adapter: production uses the existing Git writers."""

    def __init__(self, root: Path):
        self.root = root
        self.source = FilesystemShowcaseSource(root)
        self.writes = 0

    def apply(self, *, expected_revision, files, message):
        with self.source.snapshot() as snapshot:
            assert snapshot.revision == expected_revision
        return self._write(files)

    def create(self, *, view_id, files, message, expected_revision=None):
        with self.source.snapshot() as snapshot:
            assert snapshot.revision == expected_revision
            assert not snapshot.view_exists(view_id)
        return self._write(files)

    def _write(self, files):
        self.writes += 1
        for name, text in files.items():
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text)
        with self.source.snapshot() as snapshot:
            return snapshot.revision


@pytest.fixture
def setup(tmp_path):
    root = tmp_path / "source"
    shutil.copytree(FIXTURES, root)
    writer = LocalTestWriter(root)
    manager = ShowcaseManager(
        source=writer.source,
        writer=writer,
        builder=FakeBuilder(),
        publisher=FakePublisher(),
        state=ShowcaseStateStore(tmp_path / "state.json"),
        origin="https://ideas.example",
    )
    journal = ShowcaseOperationJournal(tmp_path / "operations.json")
    controller = ShowcaseOperationController(manager, journal)
    original = manager.source.get_source("main")
    view = {"title": "Partner tasks", "subtitle": "A small approved selection", "item_ids": [original.items[0].id]}
    return manager, writer, controller, journal, view


def invoke(controller, tool, **args):
    return controller.invoke(f"showcase.{tool}", args, principal("showcase:read", "showcase:write"))


def test_create_from_existing_ids_preview_publish_retry_and_stable_update(setup):
    manager, writer, control, journal, view = setup
    before = {p.name: p.read_bytes() for p in (writer.root / "items").glob("*.yaml")}
    arguments = {"view_id": "partner-one", "view": view, "idempotency_key": "one-create-key"}
    preview = invoke(control, "create_view", mode="preview", **arguments)
    assert preview["validation"]["valid"]
    assert preview["validation"]["buildable"] is None
    assert preview["validation"]["build_checked"] is False
    assert writer.writes == 0 and not journal.path.exists()
    assert not manager.state.path.exists()
    first = invoke(control, "create_view", mode="publish", **arguments)
    assert first["status"] == "published" and first["duplicate"] is False
    again = invoke(control, "create_view", mode="publish", **arguments)
    assert again["duplicate"] is True and again["url"] == first["url"] and writer.writes == 1
    assert {p.name: p.read_bytes() for p in (writer.root / "items").glob("*.yaml")} == before
    current = manager.get_source("partner-one")
    updated_view = {**current["view"], "title": "Revised partner tasks"}
    result = invoke(
        control,
        "apply",
        view_id="partner-one",
        expected_source_revision=current["source_revision"],
        view=updated_view,
        mode="publish",
        idempotency_key="one-update-key",
    )
    assert result["status"] == "published" and result["url"] == first["url"]
    assert writer.writes == 2


def test_new_card_draft_roundtrip_and_publication_gate(setup):
    manager, writer, control, _journal, view = setup
    card = manager.source.get_source("main").items[0].model_dump()
    card.update(id="partner-new-card", capability_type="product", publish_state="draft")
    view["item_ids"] = [card["id"]]
    result = invoke(
        control,
        "create_view",
        view_id="partner-draft",
        view=view,
        items=[card],
        mode="save",
        idempotency_key="draft-save-key",
    )
    assert result["status"] == "applied" and not result["validation"]["publication_ready"]
    assert not manager.state.path.exists()
    current = manager.get_source("partner-draft")
    assert current["items"][0]["publish_state"] == "draft"
    with pytest.raises(ValueError, match="not ready"):
        manager.source.load_bundle("partner-draft")
    with pytest.raises(ValueError, match="not ready"):
        manager.source.get_source("partner-draft").published()
    with pytest.raises(ShowcaseRequestError, match="ITEM_NOT_READY"):
        invoke(
            control,
            "apply",
            view_id="partner-draft",
            expected_source_revision=current["source_revision"],
            mode="publish",
            idempotency_key="draft-reject-key",
        )
    assert writer.writes == 1
    card["publish_state"] = "ready"
    result = invoke(
        control,
        "apply",
        view_id="partner-draft",
        expected_source_revision=current["source_revision"],
        items=[card],
        mode="publish",
        idempotency_key="draft-ready-key",
    )
    assert result["status"] == "published"


def test_create_and_update_cannot_silently_change_another_showcase(setup):
    manager, _writer, control, _journal, view = setup
    card = manager.source.get_source("main").items[0].model_dump()
    card.update(title="Adapted for this audience", capability_type="product")
    with pytest.raises(ShowcaseRequestError, match="ITEM_ID_CONFLICT"):
        invoke(control, "create_view", view_id="partner-one", view=view, items=[card], mode="preview")
    invoke(control, "create_view", view_id="partner-one", view=view, mode="save", idempotency_key="shared-create-key")
    current = manager.get_source("partner-one")
    with pytest.raises(ShowcaseRequestError, match="SHARED_ITEM"):
        invoke(
            control,
            "apply",
            view_id="partner-one",
            expected_source_revision=current["source_revision"],
            items=[card],
            mode="save",
            idempotency_key="shared-modify-key",
        )
    card["id"] = "adapted-partner-card"
    view["item_ids"] = [card["id"]]
    result = invoke(
        control,
        "apply",
        view_id="partner-one",
        expected_source_revision=current["source_revision"],
        view=view,
        items=[card],
        mode="save",
        idempotency_key="shared-copy-key",
    )
    assert result["status"] == "applied"
    assert manager.source.get_source("main").items[0].title != card["title"]


@pytest.mark.parametrize(
    "code,patch",
    [
        (
            "ITEM_NOT_FOUND",
            {"view": {"title": "For partners", "subtitle": "Useful working tasks", "item_ids": ["missing-item"]}},
        ),
        (
            "VIEW_ID_MISMATCH",
            {
                "view": {
                    "id": "not-matching",
                    "title": "For partners",
                    "subtitle": "Useful working tasks",
                    "item_ids": ["missing-item"],
                }
            },
        ),
        ("INVALID_MODE", {"mode": "publish", "dry_run": True}),
    ],
)
def test_errors_are_actionable_without_source_writes(setup, code, patch):
    _manager, writer, control, _journal, view = setup
    args = {"view_id": "partner-one", "view": view, "mode": "preview", **patch}
    with pytest.raises(ShowcaseRequestError) as error:
        invoke(control, "create_view", **args)
    assert error.value.code == code
    assert error.value.payload()["field"] and error.value.payload()["next_action"]
    assert writer.writes == 0


def test_noop_avoids_commit_and_conflicts_are_explicit(setup):
    manager, writer, control, _journal, _view = setup
    current = manager.get_source("main")
    result = invoke(
        control,
        "apply",
        view_id="main",
        view=current["view"],
        expected_source_revision=current["source_revision"],
        mode="save",
        idempotency_key="noop-save-key",
    )
    assert result["status"] == "applied" and writer.writes == 0 and not result["changed_paths"]
    with pytest.raises(ShowcaseRequestError, match="REVISION_CONFLICT"):
        invoke(control, "apply", view_id="main", expected_source_revision="wrong-revision", mode="preview")
    with pytest.raises(ShowcaseRequestError, match="IDEMPOTENCY_CONFLICT"):
        invoke(
            control,
            "apply",
            view_id="main",
            expected_source_revision=current["source_revision"],
            mode="publish",
            idempotency_key="noop-save-key",
        )


def test_http_gateway_keeps_safe_error_and_does_not_leak_source_exceptions(setup, tmp_path):
    manager, _writer, control, _journal, view = setup
    app = TestClient(create_app(controller=control, token=TOKEN))

    def transport(request):
        response = app.post(request.url.path, content=request.content, headers=dict(request.headers))
        return httpx.Response(response.status_code, json=response.json())

    gateway = ShowcaseGatewayClient(
        ShowcaseGatewaySettings(
            url="http://127.0.0.1:8790/internal/mcp-showcase/invoke", token_file=token_file(tmp_path)
        ),
        default_identity=identity("showcase:write"),
        transport=httpx.MockTransport(transport),
    )
    view["item_ids"] = ["missing-item"]
    with pytest.raises(ShowcaseGatewayError) as exc:
        gateway.create_view("partner-one", view=view, mode="preview")
    error = json.loads(str(exc.value))
    assert error["code"] == "ITEM_NOT_FOUND" and error["field"] == "view.item_ids[0]" and error["next_action"]

    def unavailable(*args, **kwargs):
        raise ShowcaseSourceError("token=PRIVATE_PASSWORD source://private")

    manager.create_view = unavailable
    with pytest.raises(ShowcaseGatewayError) as exc:
        gateway.create_view("partner-one", view=view, mode="preview")
    assert "SOURCE_UNAVAILABLE" in str(exc.value)
    assert "PRIVATE_PASSWORD" not in str(exc.value)


def test_payload_limit_utf8_and_preview_no_idempotency(setup):
    _manager, writer, control, _journal, view = setup
    assert invoke(control, "create_view", view_id="partner-one", view=view)["status"] == "dry_run"
    with pytest.raises(ShowcaseRequestError, match="REQUEST_TOO_LARGE"):
        invoke(control, "create_view", view_id="partner-one", view=view, extra="я" * (MAX_ARGUMENT_BYTES // 2))
    with pytest.raises(ShowcaseRequestError, match="IDEMPOTENCY_REQUIRED"):
        invoke(control, "create_view", view_id="partner-one", view=view, mode="save")
    assert writer.writes == 0


@pytest.mark.parametrize(
    "href",
    [
        "javascript:alert(1)",
        "http://insecure.test",
        "https://user:pass@host.test",
        "https://a.test\\evil",
        "tel:call-me",
    ],
)
def test_contact_rejects_unsafe_targets(href):
    with pytest.raises(ValidationError):
        Contact(href=href)


def test_new_input_schema_requires_type_and_contacts_allow_tel():
    assert "capability_type" in ShowcaseWriteItem.model_json_schema()["required"]
    view = ShowcaseViewInput(
        title="For partners",
        subtitle="Useful tasks",
        item_ids=["existing-item"],
        contacts=[Contact(label="Phone", value="+70000000000", href="tel:+70000000000")],
    )
    assert view.id is None and view.contacts[0].href.startswith("tel:")


def test_legacy_flags_and_no_implicit_publish():
    assert resolve_mode(None, publish=True) == "preview"
    assert resolve_mode(None, dry_run=False, publish=True) == "publish"
    assert resolve_mode(None, dry_run=False) == "save"
    with pytest.raises(ShowcaseRequestError, match="INVALID_MODE"):
        resolve_mode("publish", publish=True)


def test_rest_create_rejects_existing_item_path_before_any_write():
    writer = GitHubShowcaseWriter(token="test", repository="owner/repo")
    calls = []

    def request(url, *, method="GET", payload=None):
        calls.append(method)
        if "/ref/" in url:
            return {"object": {"sha": "a" * 40}}
        if "/commits/" in url:
            return {"tree": {"sha": "b" * 40}}
        return {"tree": [{"path": "showcase/items/existing-item.yaml"}]}

    writer._request = request
    with pytest.raises(ShowcaseRequestError, match="ITEM_ID_CONFLICT"):
        writer.create(view_id="new-view", files={"items/existing-item.yaml": "overwrite"}, message="test")
    assert all(method == "GET" for method in calls)


@pytest.mark.asyncio
async def test_actual_mcp_schema_and_call_create_a_typed_card(setup):
    from my_data_hub.mcp.server import MCPDependencies, create_server
    from tests.showcase.test_main_mcp_integration import identity as mcp_identity
    from tests.showcase.test_main_mcp_integration import settings

    manager, writer, _control, _journal, view = setup
    server = create_server(
        settings(), dependencies=MCPDependencies(showcase_manager=manager), default_identity=mcp_identity()
    )
    tools = {tool.name: tool for tool in await server.list_tools()}
    create = tools["showcase.create_view"]
    assert create.input_schema["additionalProperties"] is False
    assert "expected_source_revision" not in create.input_schema["properties"]
    assert "mode" in create.input_schema["properties"]
    assert "no source revision" in create.description.lower()
    card = manager.source.get_source("main").items[0].model_dump()
    card.update(id="typed-new-card", capability_type="product")
    view["item_ids"] = [card["id"]]
    result = await server.call_tool(
        "showcase.create_view",
        {
            "view_id": "typed-partner",
            "view": view,
            "items": [card],
            "mode": "publish",
            "idempotency_key": "typed-create-key",
        },
    )
    assert result.is_error is False
    assert result.structured_content["result"]["status"] == "published"
    assert writer.writes == 1


def test_runtime_checks_actual_body_size_not_just_header(setup):
    _manager, writer, control, _journal, _view = setup
    app = TestClient(create_app(controller=control, token=TOKEN, max_request_bytes=4096))
    payload = json.dumps(
        {
            "tool": "showcase.list",
            "arguments": {"extra": "x" * 5000},
            "principal": principal("showcase:read").model_dump(),
        }
    ).encode()
    response = app.post(
        "/internal/mcp-showcase/invoke",
        content=iter([payload[:3000], payload[3000:]]),
        headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"},
    )
    assert response.status_code == 413
    assert writer.writes == 0
