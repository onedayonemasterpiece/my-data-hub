from __future__ import annotations

import ast
import re
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parents[1]


def read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def write(relative: str, source: str) -> None:
    path = ROOT / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(source, encoding="utf-8")


def offsets(source: str) -> list[int]:
    values = [0]
    for match in re.finditer("\n", source):
        values.append(match.end())
    return values


def node_span(source: str, node: ast.AST) -> tuple[int, int]:
    line_offsets = offsets(source)
    start = line_offsets[node.lineno - 1] + node.col_offset  # type: ignore[attr-defined]
    end = line_offsets[node.end_lineno - 1] + node.end_col_offset  # type: ignore[attr-defined]
    return start, end


def find_function(
    source: str,
    name: str,
    *,
    class_name: str | None = None,
) -> ast.FunctionDef | ast.AsyncFunctionDef:
    tree = ast.parse(source)
    candidates: Iterable[ast.AST]
    if class_name is None:
        candidates = tree.body
    else:
        owner = next(
            (
                node
                for node in tree.body
                if isinstance(node, ast.ClassDef) and node.name == class_name
            ),
            None,
        )
        if owner is None:
            raise RuntimeError(f"class {class_name} not found")
        candidates = owner.body
    function = next(
        (
            node
            for node in candidates
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == name
        ),
        None,
    )
    if function is None:
        raise RuntimeError(f"function {name} not found")
    return function


def add_parameter(
    source: str,
    name: str,
    parameter: str,
    parameter_name: str,
    *,
    class_name: str | None = None,
    keyword_only: bool = True,
) -> str:
    function = find_function(source, name, class_name=class_name)
    if any(argument.arg == parameter_name for argument in (
        *function.args.posonlyargs,
        *function.args.args,
        *function.args.kwonlyargs,
    )):
        return source
    start, _ = node_span(source, function)
    first_body = function.body[0]
    body_start, _ = node_span(source, first_body)
    header = source[start:body_start]
    close = header.rfind(")")
    open_paren = header.find("(")
    if open_paren < 0 or close < open_paren:
        raise RuntimeError(f"cannot parse header for {name}")
    arguments = header[open_paren + 1 : close]
    if keyword_only and "*" not in arguments:
        insertion = f", *, {parameter}"
    else:
        insertion = f", {parameter}"
    header = header[:close] + insertion + header[close:]
    return source[:start] + header + source[body_start:]


def replace_in_function(
    source: str,
    name: str,
    replacements: list[tuple[str, str]],
    *,
    class_name: str | None = None,
) -> str:
    function = find_function(source, name, class_name=class_name)
    start, end = node_span(source, function)
    block = source[start:end]
    original = block
    for pattern, replacement in replacements:
        block = re.sub(pattern, replacement, block, flags=re.MULTILINE | re.DOTALL)
    if block == original:
        raise RuntimeError(f"no call site changed in {name}")
    return source[:start] + block + source[end:]


def call_names(node: ast.AST) -> set[str]:
    result: set[str] = set()
    for child in ast.walk(node):
        if not isinstance(child, ast.Call):
            continue
        function = child.func
        if isinstance(function, ast.Attribute):
            result.add(function.attr)
        elif isinstance(function, ast.Name):
            result.add(function.id)
    return result


def patch_manager() -> None:
    relative = "src/my_data_hub/showcase/manager.py"
    source = read(relative)
    for method in ("rebuild", "create_view", "rotate_link", "revoke_link"):
        source = add_parameter(
            source,
            method,
            "idempotency_key: str | None = None",
            "idempotency_key",
            class_name="ShowcaseManager",
        )
    source = add_parameter(
        source,
        "rotate_link",
        "slug: str | None = None",
        "slug",
        class_name="ShowcaseManager",
    )

    rotate = find_function(source, "rotate_link", class_name="ShowcaseManager")
    slug_assignment = None
    for node in ast.walk(rotate):
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        names = {
            target.id
            for target in targets
            if isinstance(target, ast.Name)
        }
        if not names or all(name == "slug" or "old" in name or "current" in name for name in names):
            continue
        value = node.value
        if value is None:
            continue
        value_source = ast.get_source_segment(source, value) or ""
        lowered = value_source.lower()
        if "slug or" in lowered:
            slug_assignment = node
            break
        if "slug" in lowered or "token" in lowered or "secret" in lowered:
            slug_assignment = node
            value_start, value_end = node_span(source, value)
            source = source[:value_start] + f"slug or ({value_source})" + source[value_end:]
            break
    if slug_assignment is None:
        raise RuntimeError("rotate_link secret-slug assignment was not found")

    rotate = find_function(source, "rotate_link", class_name="ShowcaseManager")
    persist_names = {"save", "write", "upsert", "set_surface", "replace"}

    def locate(statements: list[ast.stmt]):
        revoke = None
        persist = None
        for index, statement in enumerate(statements):
            names = call_names(statement)
            segment = ast.get_source_segment(source, statement) or ""
            if "revoke" in names:
                revoke = (index, statement)
            if names & persist_names and "state" in segment.lower():
                persist = (index, statement)
            if revoke and persist:
                return statements, revoke, persist
            for attribute in ("body", "orelse", "finalbody"):
                nested = getattr(statement, attribute, None)
                if isinstance(nested, list):
                    found = locate(nested)
                    if found is not None:
                        return found
        return None

    located = locate(rotate.body)
    if located is None:
        raise RuntimeError("rotate_link revoke/state statements were not found")
    _, (revoke_index, revoke_node), (persist_index, persist_node) = located
    if revoke_index < persist_index:
        lines = source.splitlines(keepends=True)
        revoke_start = revoke_node.lineno - 1
        revoke_end = revoke_node.end_lineno
        block = lines[revoke_start:revoke_end]
        del lines[revoke_start:revoke_end]
        insertion = persist_node.end_lineno - len(block)
        lines[insertion:insertion] = block
        source = "".join(lines)

    write(relative, source)


def ensure_showcase_scopes() -> None:
    catalog = read("src/my_data_hub/mcp/catalog.py")
    for required in (
        '"showcase.list"',
        '"showcase.get_link"',
        '"showcase.rebuild"',
        '"showcase.rotate_link"',
        '"showcase.create_view"',
        '"showcase.revoke_link"',
        '"showcase:read"',
        '"showcase:write"',
    ):
        if required not in catalog:
            raise RuntimeError(f"existing showcase MCP catalog is incomplete: {required}")

    relative = "src/my_data_hub/config.py"
    source = read(relative)
    if '"showcase:read"' not in source:
        marker = "remote_read_scopes = {\n"
        if marker not in source:
            raise RuntimeError("remote_read_scopes marker not found")
        source = source.replace(marker, marker + '            "showcase:read",\n', 1)
    if '"showcase:write"' not in source:
        marker = "remote_write_scopes = {\n"
        if marker not in source:
            raise RuntimeError("remote_write_scopes marker not found")
        source = source.replace(marker, marker + '            "showcase:write",\n', 1)
    write(relative, source)


def patch_server() -> None:
    relative = "src/my_data_hub/mcp/server.py"
    source = read(relative)
    if "from my_data_hub.showcase.gateway import ShowcaseGatewayClient" not in source:
        marker = "\nREADER_PROFILE_TOOLS = frozenset("
        if marker not in source:
            raise RuntimeError("server import boundary not found")
        source = source.replace(
            marker,
            "\nfrom my_data_hub.showcase.gateway import ShowcaseGatewayClient\n" + marker,
            1,
        )
    if '"showcase.list"' not in source.split("PROVIDER_ONLY_TOOLS", maxsplit=1)[0]:
        marker = '        "region_talk.pipeline.status",\n'
        if marker not in source:
            raise RuntimeError("reader profile marker not found")
        source = source.replace(
            marker,
            marker + '        "showcase.list",\n        "showcase.get_link",\n',
            1,
        )

    if "_showcase_backend_from_env" not in source:
        if "ShowcaseManager.from_env()" not in source:
            raise RuntimeError("ShowcaseManager.from_env call not found")
        source = source.replace(
            "ShowcaseManager.from_env()",
            "_showcase_backend_from_env(settings, fallback)",
        )
        marker = "\ndef create_server(\n"
        helper = '''\n\ndef _showcase_backend_from_env(\n    settings: Settings, fallback: AccessIdentity | None\n):  # type: ignore[no-untyped-def]\n    if settings.mcp_remote_enabled:\n        return ShowcaseGatewayClient.from_env(default_identity=fallback)\n    return ShowcaseManager.from_env()\n'''
        if marker not in source:
            raise RuntimeError("create_server marker not found")
        source = source.replace(marker, helper + marker, 1)

    wrapper_calls = {
        "showcase_rebuild": "rebuild",
        "showcase_rotate_link": "rotate_link",
        "showcase_create_view": "create_view",
        "showcase_revoke_link": "revoke_link",
    }
    for function, method in wrapper_calls.items():
        source = add_parameter(
            source,
            function,
            "idempotency_key: str",
            "idempotency_key",
            keyword_only=False,
        )
        node = find_function(source, function)
        start, end = node_span(source, node)
        block = source[start:end]
        if "idempotency_key=idempotency_key" not in block:
            patterns = [
                (
                    rf"(to_thread\(\s*showcase_manager\(\)\.{method}\s*,\s*view_id\s*)\)",
                    rf"\1, idempotency_key=idempotency_key)",
                ),
                (
                    rf"(\.{method}\(\s*view_id\s*)\)",
                    rf"\1, idempotency_key=idempotency_key)",
                ),
            ]
            original = block
            for pattern, replacement in patterns:
                block = re.sub(pattern, replacement, block, flags=re.MULTILINE | re.DOTALL)
            if block == original:
                raise RuntimeError(f"showcase wrapper call not found: {function}")
            source = source[:start] + block + source[end:]

    audit_anchor = "            return await super().call_tool(name, arguments, context)"
    if "showcase_tool_audit" not in source:
        if audit_anchor not in source:
            raise RuntimeError("server call_tool return anchor not found")
        audit_block = '''            try:\n                result = await super().call_tool(name, arguments, context)\n            except Exception:\n                if (\n                    str(name).startswith("showcase.")\n                    and identity is not None\n                    and deps.audit is not None\n                ):\n                    showcase_tool_audit = deps.audit.record_mcp_audit(\n                        OAuthAuditEvent(\n                            event="mcp_tool",\n                            outcome="denied_or_failed",\n                            issuer=identity.issuer,\n                            client_id=identity.client_id,\n                            subject=identity.subject,\n                            token_id=identity.token_id,\n                            tool=str(name),\n                        )\n                    )\n                    if inspect.isawaitable(showcase_tool_audit):\n                        await showcase_tool_audit\n                raise\n            if (\n                str(name).startswith("showcase.")\n                and identity is not None\n                and deps.audit is not None\n            ):\n                showcase_tool_audit = deps.audit.record_mcp_audit(\n                    OAuthAuditEvent(\n                        event="mcp_tool",\n                        outcome="accepted",\n                        issuer=identity.issuer,\n                        client_id=identity.client_id,\n                        subject=identity.subject,\n                        token_id=identity.token_id,\n                        tool=str(name),\n                    )\n                )\n                if inspect.isawaitable(showcase_tool_audit):\n                    await showcase_tool_audit\n            return result'''
        source = source.replace(audit_anchor, audit_block, 1)

    write(relative, source)


def patch_standalone_server() -> None:
    relative = "src/my_data_hub/showcase/mcp_server.py"
    path = ROOT / relative
    if not path.exists():
        return
    source = read(relative)
    wrapper_calls = {
        "showcase_rebuild": "rebuild",
        "showcase_rotate_link": "rotate_link",
        "showcase_create_view": "create_view",
        "showcase_revoke_link": "revoke_link",
    }
    for function, method in wrapper_calls.items():
        try:
            source = add_parameter(
                source,
                function,
                "idempotency_key: str",
                "idempotency_key",
                keyword_only=False,
            )
        except RuntimeError:
            continue
        node = find_function(source, function)
        start, end = node_span(source, node)
        block = source[start:end]
        if "idempotency_key=idempotency_key" in block:
            continue
        original = block
        block = re.sub(
            rf"(to_thread\(\s*manager\(\)\.{method}\s*,\s*view_id\s*)\)",
            rf"\1, idempotency_key=idempotency_key)",
            block,
            flags=re.MULTILINE | re.DOTALL,
        )
        block = re.sub(
            rf"(\.{method}\(\s*view_id\s*)\)",
            rf"\1, idempotency_key=idempotency_key)",
            block,
            flags=re.MULTILINE | re.DOTALL,
        )
        if block != original:
            source = source[:start] + block + source[end:]
    write(relative, source)


def patch_pyproject() -> None:
    relative = "pyproject.toml"
    source = read(relative)
    entry = 'my-data-hub-showcase-runtime = "my_data_hub.showcase.runtime:main"\n'
    if entry not in source:
        marker = 'my-data-hub-mcp = "my_data_hub.mcp.server:main"\n'
        if marker not in source:
            raise RuntimeError("project scripts marker not found")
        source = source.replace(marker, marker + entry, 1)
    write(relative, source)


def main() -> None:
    patch_manager()
    ensure_showcase_scopes()
    patch_server()
    patch_standalone_server()
    patch_pyproject()
    print("showcase runtime v2 source integration applied")


if __name__ == "__main__":
    main()
