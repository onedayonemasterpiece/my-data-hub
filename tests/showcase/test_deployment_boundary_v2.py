from __future__ import annotations

import ast
import inspect
from pathlib import Path

from my_data_hub.showcase.manager import ShowcaseManager

ROOT = Path(__file__).resolve().parents[2]


def test_remote_mcp_receives_only_showcase_gateway_configuration() -> None:
    source = (ROOT / "compose.showcase.yaml").read_text(encoding="utf-8")
    remote_section, runtime_section = source.split("\n  showcase-runtime:\n", maxsplit=1)
    assert "MY_DATA_HUB_SHOWCASE_GATEWAY_URL" in remote_section
    assert "MY_DATA_HUB_SHOWCASE_GATEWAY_TOKEN_FILE" in remote_section
    assert "MY_DATA_HUB_SHOWCASE_EDGE_GATEWAY_TOKEN_FILE" in remote_section
    for forbidden in (
        "GITHUB_TOKEN",
        "PUBLISH_COMMAND",
        "REVOKE_COMMAND",
        "SITE_TEMPLATE_DIR",
        "ARTIFACT_ROOT",
        "RUNTIME_ENV_FILE",
    ):
        assert forbidden not in remote_section
    assert "MY_DATA_HUB_SHOWCASE_RUNTIME_ENV_FILE" in runtime_section
    assert "MY_DATA_HUB_SHOWCASE_RUNTIME_GATEWAY_TOKEN_FILE" in runtime_section
    assert "MY_DATA_HUB_SHOWCASE_GITHUB_SSH_KEY_FILE" in runtime_section
    assert "MY_DATA_HUB_SHOWCASE_SITE_TEMPLATE_DIR" in runtime_section
    assert "MY_DATA_HUB_SHOWCASE_SITE_ROOT" in runtime_section
    assert "MY_DATA_HUB_SHOWCASE_SITE_DIR" not in runtime_section
    assert "MY_DATA_HUB_SHOWCASE_STATE_DIR" in runtime_section
    assert "showcase-static" in runtime_section


def test_showcase_runtime_image_contains_renderer_but_mcp_image_stays_unchanged() -> None:
    showcase = (ROOT / "deploy/showcase-runtime/Dockerfile").read_text(encoding="utf-8")
    mcp = (ROOT / "deploy/control-plane/Dockerfile").read_text(encoding="utf-8")
    assert "FROM node:22" in showcase
    assert "showcase-site/node_modules" in showcase
    assert 'ENTRYPOINT ["my-data-hub-showcase-runtime"]' in showcase
    assert "USER 65532:65532" in showcase
    assert "showcase-site" not in mcp
    assert "node_modules" not in mcp


def test_standard_remote_mcp_selects_private_gateway() -> None:
    source = (ROOT / "src/my_data_hub/mcp/server.py").read_text(encoding="utf-8")
    assert "ShowcaseGatewayClient" in source
    assert "settings.mcp_remote_enabled" in source
    assert "_showcase_backend_from_env" in source


def test_rotation_accepts_prepared_slug_and_commits_state_before_revoke() -> None:
    signature = inspect.signature(ShowcaseManager.rotate_link)
    assert "slug" in signature.parameters
    assert "idempotency_key" in signature.parameters

    path = ROOT / "src/my_data_hub/showcase/manager.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    manager = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "ShowcaseManager")
    rotate = next(node for node in manager.body if isinstance(node, ast.FunctionDef) and node.name == "rotate_link")

    def calls(statement: ast.AST) -> set[str]:
        result: set[str] = set()
        for child in ast.walk(statement):
            if isinstance(child, ast.Call):
                function = child.func
                if isinstance(function, ast.Attribute):
                    result.add(function.attr)
                elif isinstance(function, ast.Name):
                    result.add(function.id)
        return result

    def inspect_statement_lists(statements: list[ast.stmt]) -> bool:
        revoke_index = None
        persist_index = None
        for index, statement in enumerate(statements):
            names = calls(statement)
            segment = ast.get_source_segment(path.read_text(encoding="utf-8"), statement) or ""
            if "revoke" in names:
                revoke_index = index
            if names & {"save", "write", "upsert", "set_surface", "replace"} and "state" in segment:
                persist_index = index
            nested_lists = []
            for attribute in ("body", "orelse", "finalbody"):
                nested = getattr(statement, attribute, None)
                if isinstance(nested, list):
                    nested_lists.append(nested)
            for nested in nested_lists:
                if inspect_statement_lists(nested):
                    return True
        return revoke_index is not None and persist_index is not None and persist_index < revoke_index

    assert inspect_statement_lists(rotate.body)
