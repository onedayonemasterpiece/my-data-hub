from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def replace_once(relative: str, old: str, new: str) -> None:
    path = ROOT / relative
    text = path.read_text(encoding="utf-8")
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"expected one marker in {relative}, found {count}: {old[:80]!r}")
    path.write_text(text.replace(old, new, 1), encoding="utf-8")


replace_once(
    "src/my_data_hub/mcp/catalog.py",
    '''    ("data.query", "data:read"),
    ("data.change.status", "operation:read"),
)''',
    '''    ("data.query", "data:read"),
    ("data.change.status", "operation:read"),
    ("showcase.list", "showcase:read"),
    ("showcase.get_link", "showcase:read"),
)''',
)
replace_once(
    "src/my_data_hub/mcp/catalog.py",
    '''    ToolContract("provider.resources.create", "provider:write", False, open_world=True, role="provider_operator"),''',
    '''    ToolContract(
        "showcase.rebuild",
        "showcase:write",
        False,
        idempotent=True,
        open_world=True,
        role="operator",
    ),
    ToolContract(
        "showcase.create_view",
        "showcase:write",
        False,
        idempotent=True,
        open_world=True,
        role="operator",
    ),
    ToolContract(
        "showcase.rotate_link",
        "showcase:write",
        False,
        destructive=True,
        idempotent=False,
        open_world=True,
        role="operator",
    ),
    ToolContract(
        "showcase.revoke_link",
        "showcase:write",
        False,
        destructive=True,
        idempotent=True,
        open_world=True,
        role="operator",
    ),
    ToolContract("provider.resources.create", "provider:write", False, open_world=True, role="provider_operator"),''',
)

replace_once(
    "src/my_data_hub/config.py",
    '''            "provider:read",
        }
        remote_write_scopes = {''',
    '''            "provider:read",
            "showcase:read",
        }
        remote_write_scopes = {''',
)
replace_once(
    "src/my_data_hub/config.py",
    '''            "provider:write",
        }
        if self.mcp_remote_enabled''',
    '''            "provider:write",
            "showcase:write",
        }
        if self.mcp_remote_enabled''',
)

replace_once(
    "src/my_data_hub/mcp/server.py",
    '''import inspect
from dataclasses import dataclass''',
    '''import asyncio
import inspect
import os
from dataclasses import dataclass, replace''',
)
replace_once(
    "src/my_data_hub/mcp/server.py",
    '''from my_data_hub.mcp.transport import ToolSecurityMetadataMiddleware
from my_data_hub.workloads.bloggers.discovery import (''',
    '''from my_data_hub.mcp.transport import ToolSecurityMetadataMiddleware
from my_data_hub.showcase.manager import ShowcaseManager
from my_data_hub.workloads.bloggers.discovery import (''',
)
replace_once(
    "src/my_data_hub/mcp/server.py",
    '''READER_PROFILE_TOOLS = frozenset(''',
    '''_SHOWCASE_TOOL_NAMES = frozenset(
    {
        "showcase.list",
        "showcase.get_link",
        "showcase.rebuild",
        "showcase.create_view",
        "showcase.rotate_link",
        "showcase.revoke_link",
    }
)

READER_PROFILE_TOOLS = frozenset(''',
)
replace_once(
    "src/my_data_hub/mcp/server.py",
    '''    region_talk_pipeline_run_enabled: bool = False


def _local_identity''',
    '''    region_talk_pipeline_run_enabled: bool = False
    showcase_manager: ShowcaseManager | None = None


def _showcase_enabled() -> bool:
    return os.getenv("MY_DATA_HUB_SHOWCASE_ENABLED", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _with_showcase_manager(dependencies: MCPDependencies) -> MCPDependencies:
    if dependencies.showcase_manager is not None or not _showcase_enabled():
        return dependencies
    return replace(dependencies, showcase_manager=ShowcaseManager.from_env())


def _local_identity''',
)
replace_once(
    "src/my_data_hub/mcp/server.py",
    '''def _profile_tool_names(dependencies: MCPDependencies) -> set[str]:
    names = set(TOOL_CONTRACTS)
    if not dependencies.acceptance_scenarios_enabled:''',
    '''def _profile_tool_names(dependencies: MCPDependencies) -> set[str]:
    names = set(TOOL_CONTRACTS)
    if dependencies.showcase_manager is None:
        names -= _SHOWCASE_TOOL_NAMES
    if not dependencies.acceptance_scenarios_enabled:''',
)
replace_once(
    "src/my_data_hub/mcp/server.py",
    '''    deps = dependencies or MCPDependencies()
    profile_tools = _profile_tool_names(deps)''',
    '''    deps = _with_showcase_manager(dependencies or MCPDependencies())
    profile_tools = _profile_tool_names(deps)''',
)
replace_once(
    "src/my_data_hub/mcp/server.py",
    '''    async def checkpoint_status() -> dict[str, Any]:
        return await service.invoke("checkpoint.status", {})

    async def acceptance_scenario_request(''',
    '''    async def checkpoint_status() -> dict[str, Any]:
        return await service.invoke("checkpoint.status", {})

    def showcase_manager() -> ShowcaseManager:
        if deps.showcase_manager is None:
            raise RuntimeError("IdeaHub Showcase is not enabled")
        return deps.showcase_manager

    async def showcase_list() -> dict[str, Any]:
        return await asyncio.to_thread(showcase_manager().list_surfaces)

    async def showcase_get_link(view_id: str) -> dict[str, Any]:
        return await asyncio.to_thread(showcase_manager().get_link, view_id)

    async def showcase_rebuild(view_id: str) -> dict[str, Any]:
        return await asyncio.to_thread(showcase_manager().rebuild, view_id)

    async def showcase_create_view(view_id: str, publish: bool = True) -> dict[str, Any]:
        return await asyncio.to_thread(
            showcase_manager().create_view,
            view_id,
            publish=publish,
        )

    async def showcase_rotate_link(view_id: str) -> dict[str, Any]:
        return await asyncio.to_thread(showcase_manager().rotate_link, view_id)

    async def showcase_revoke_link(view_id: str) -> dict[str, Any]:
        return await asyncio.to_thread(showcase_manager().revoke_link, view_id)

    async def acceptance_scenario_request(''',
)
replace_once(
    "src/my_data_hub/mcp/server.py",
    '''        "checkpoint.status": checkpoint_status,
        "acceptance.scenario.request": acceptance_scenario_request,''',
    '''        "checkpoint.status": checkpoint_status,
        "showcase.list": showcase_list,
        "showcase.get_link": showcase_get_link,
        "showcase.rebuild": showcase_rebuild,
        "showcase.create_view": showcase_create_view,
        "showcase.rotate_link": showcase_rotate_link,
        "showcase.revoke_link": showcase_revoke_link,
        "acceptance.scenario.request": acceptance_scenario_request,''',
)
replace_once(
    "src/my_data_hub/mcp/server.py",
    '''    from my_data_hub.mcp.admission import AdmissionLimits, OAuthAdmissionSecurity

    server = create_server(settings, dependencies=dependencies)''',
    '''    from my_data_hub.mcp.admission import AdmissionLimits, OAuthAdmissionSecurity

    dependencies = _with_showcase_manager(dependencies)
    server = create_server(settings, dependencies=dependencies)''',
)

replace_once(
    "docs/ideahub-showcase.md",
    '''`my-data-hub-showcase-mcp` is initially a stdio entry point for contract testing. The final
deployment mounts the same manager behind the existing my-data-hub OAuth boundary rather
than introducing a second authorization system.''',
    '''The standard `my-data-hub` MCP server exposes the same six tools when
`MY_DATA_HUB_SHOWCASE_ENABLED=true` and the authenticated owner/operator token carries
`showcase:read` and `showcase:write`. They therefore use the existing OAuth boundary,
security metadata and audit path. `my-data-hub-showcase-mcp` remains only a local stdio
entry point for focused contract testing.''',
)
replace_once(
    "docs/ideahub-showcase.md",
    '''| Variable | Purpose |
|---|---|
| `MY_DATA_HUB_SHOWCASE_GITHUB_TOKEN`''',
    '''| Variable | Purpose |
|---|---|
| `MY_DATA_HUB_SHOWCASE_ENABLED` | Enables the six tools in the standard my-data-hub MCP catalog. |
| `MY_DATA_HUB_SHOWCASE_GITHUB_TOKEN`''',
)

integration_test = ROOT / "tests/showcase/test_main_mcp_integration.py"
integration_test.write_text(
    '''from __future__ import annotations

from types import SimpleNamespace

import pytest

from my_data_hub.mcp.oauth import AccessIdentity
from my_data_hub.mcp.server import MCPDependencies, create_server

RESOURCE = "https://mcp.example.test/mcp"
SHOWCASE_TOOLS = {
    "showcase.list",
    "showcase.get_link",
    "showcase.rebuild",
    "showcase.create_view",
    "showcase.rotate_link",
    "showcase.revoke_link",
}


class FakeShowcaseManager:
    pass


def identity() -> AccessIdentity:
    return AccessIdentity(
        subject="owner",
        client_id="owner-client",
        scopes=frozenset({"showcase:read", "showcase:write"}),
        audience=RESOURCE,
        token_id="showcase-test",
        expires_at=2**63 - 1,
        issuer="https://issuer.example.test",
        issued_at=0,
        resource=RESOURCE,
    )


def settings() -> SimpleNamespace:
    return SimpleNamespace(
        mcp_remote_enabled=True,
        mcp_scopes=frozenset({"showcase:read", "showcase:write"}),
        mcp_oauth_resource=RESOURCE,
    )


@pytest.mark.asyncio
async def test_standard_mcp_lists_showcase_tools_when_manager_is_enabled(monkeypatch) -> None:
    monkeypatch.delenv("MY_DATA_HUB_SHOWCASE_ENABLED", raising=False)
    server = create_server(
        settings(),  # type: ignore[arg-type]
        dependencies=MCPDependencies(showcase_manager=FakeShowcaseManager()),  # type: ignore[arg-type]
        default_identity=identity(),
    )
    tools = {tool.name: tool for tool in await server.list_tools()}
    assert SHOWCASE_TOOLS <= set(tools)
    assert tools["showcase.get_link"].annotations.readOnlyHint is True
    assert tools["showcase.rebuild"].annotations.idempotentHint is True
    assert tools["showcase.rotate_link"].annotations.destructiveHint is True
    assert tools["showcase.rotate_link"].annotations.idempotentHint is False


@pytest.mark.asyncio
async def test_standard_mcp_hides_showcase_tools_when_not_configured(monkeypatch) -> None:
    monkeypatch.delenv("MY_DATA_HUB_SHOWCASE_ENABLED", raising=False)
    server = create_server(
        settings(),  # type: ignore[arg-type]
        dependencies=MCPDependencies(),
        default_identity=identity(),
    )
    names = {tool.name for tool in await server.list_tools()}
    assert names.isdisjoint(SHOWCASE_TOOLS)
''',
    encoding="utf-8",
)

for relative in (
    "scripts/apply_showcase_mcp_integration.py",
    ".github/workflows/apply-showcase-mcp-integration.yml",
):
    path = ROOT / relative
    if path.exists():
        path.unlink()

print("Showcase tools integrated into the standard my-data-hub MCP server")
