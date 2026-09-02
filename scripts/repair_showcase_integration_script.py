from __future__ import annotations

from pathlib import Path

path = Path(__file__).with_name("integrate_showcase_runtime.py")
source = path.read_text(encoding="utf-8")
start = source.index("    old_order = dedent(")
end = source.index("\n\n    source = source.replace(", start)
replacement = '''    dependency_pattern = (
        r"    deps = _with_showcase_manager\\(dependencies or MCPDependencies\\(\\)\\)\\n"
        r"    profile_tools = _profile_tool_names\\(deps\\)\\n"
        r"    server_security_schemes = _configured_security_schemes\\(settings, deps\\)\\n"
        r"    fallback = default_identity or _local_identity\\(settings\\)\\n"
    )
    dependency_replacement = (
        "    fallback = default_identity or _local_identity(settings)\\n"
        "    deps = _with_showcase_manager(\\n"
        "        dependencies or MCPDependencies(),\\n"
        "        settings=settings,\\n"
        "        fallback=fallback,\\n"
        "    )\\n"
        "    profile_tools = _profile_tool_names(deps)\\n"
        "    server_security_schemes = _configured_security_schemes(settings, deps)\\n"
    )
    source, changed = re.subn(
        dependency_pattern,
        dependency_replacement,
        source,
        count=1,
    )
    if changed != 1:
        raise RuntimeError("cannot locate create_server dependency order")'''
path.write_text(source[:start] + replacement + source[end:], encoding="utf-8")
