from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def write(path: str, source: str) -> None:
    (ROOT / path).write_text(source.rstrip() + "\n", encoding="utf-8")


def replace_once(source: str, old: str, new: str, label: str) -> str:
    if new in source:
        return source
    if source.count(old) != 1:
        raise RuntimeError(f"expected one {label}, found {source.count(old)}")
    return source.replace(old, new, 1)


def patch_server() -> None:
    path = "src/my_data_hub/mcp/server.py"
    source = read(path)

    start = source.index("READER_PROFILE_TOOLS = frozenset(")
    end = source.index("\n)\n\nPROVIDER_ONLY_TOOLS", start)
    profile = source[start:end]
    profile = profile.replace('        "showcase.list",\n', "")
    if '"showcase.list"' in profile or '"showcase.get_link"' in profile:
        raise RuntimeError("showcase tools must not enter the generic reader profile")
    source = source[:start] + profile + source[end:]

    source = replace_once(
        source,
        "    dependencies = _with_showcase_manager(dependencies)\n",
        "    dependencies = _with_showcase_manager(\n"
        "        dependencies,\n"
        "        settings=settings,\n"
        "        fallback=None,\n"
        "    )\n",
        "streamable HTTP showcase dependency wiring",
    )
    write(path, source)


def patch_catalog() -> None:
    path = "src/my_data_hub/mcp/catalog.py"
    source = read(path)
    source = source.replace('    ("showcase.list", "showcase:read"),\n', "")
    source = source.replace('    ("showcase.get_link", "showcase:write"),\n', "")
    marker = "_WRITES = (\n"
    explicit = (
        "_WRITES = (\n"
        "    ToolContract(\n"
        '        "showcase.list",\n'
        '        "showcase:read",\n'
        "        True,\n"
        "        idempotent=True,\n"
        '        role="operator",\n'
        "    ),\n"
        "    ToolContract(\n"
        '        "showcase.get_link",\n'
        '        "showcase:write",\n'
        "        True,\n"
        "        idempotent=True,\n"
        '        role="operator",\n'
        "    ),\n"
    )
    source = replace_once(source, marker, explicit, "explicit showcase read contracts")
    write(path, source)


def patch_architecture_test() -> None:
    path = "tests/test_architecture_invariants.py"
    source = read(path)
    source = replace_once(
        source,
        '        "compose.control-plane.yaml",\n    }',
        '        "compose.control-plane.yaml",\n'
        '        "compose.showcase.yaml",\n'
        "    }",
        "Compose inventory expectation",
    )
    source = replace_once(
        source,
        '    } == {"ci.yml", "nightly.yml", "post-deploy.yml", "provider-real.yml"}',
        '    } == {\n'
        '        "ci.yml",\n'
        '        "ideahub-showcase.yml",\n'
        '        "nightly.yml",\n'
        '        "post-deploy.yml",\n'
        '        "provider-real.yml",\n'
        "    }",
        "workflow inventory expectation",
    )
    source = replace_once(
        source,
        '                "compose.control-plane.yaml",\n            }',
        '                "compose.control-plane.yaml",\n'
        '                "compose.showcase.yaml",\n'
        "            }",
        "YAML service inventory expectation",
    )
    write(path, source)


def patch_runbook() -> None:
    path = "docs/operations/ideahub-showcase-runtime.md"
    source = read(path)
    source = source.replace(
        "The OAuth owner/operator client receives `showcase:read` and\n"
        "`showcase:write`. A generic reader may receive only `showcase:read`:\n"
        "it can call `showcase.list` and receives masked URLs.\n",
        "The OAuth owner/operator client receives `showcase:read` and\n"
        "`showcase:write`. Showcase tools are not added to the generic reader\n"
        "profile. `showcase.list` still returns only masked URLs.\n",
    )
    write(path, source)


def main() -> None:
    patch_server()
    patch_catalog()
    patch_architecture_test()
    patch_runbook()
    print("showcase full-CI compatibility patch applied")


if __name__ == "__main__":
    main()
