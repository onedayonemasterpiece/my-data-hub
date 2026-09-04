import inspect
from pathlib import Path


def test_mcp_module_declares_expected_tools() -> None:
    source = (Path(__file__).resolve().parents[2] / "src" / "my_data_hub" / "showcase" / "mcp_server.py").read_text(
        encoding="utf-8"
    )
    for name in (
        "showcase.list",
        "showcase.get_link",
        "showcase.get_source",
        "showcase.apply",
        "showcase.rebuild",
        "showcase.create_view",
        "showcase.rotate_link",
        "showcase.revoke_link",
    ):
        assert f'"{name}"' in source


def test_showcase_source_tools_are_catalogued_with_bounded_apply_defaults() -> None:
    from my_data_hub.mcp.catalog import TOOL_CONTRACTS
    from my_data_hub.showcase.mcp_server import create_server

    assert TOOL_CONTRACTS["showcase.get_source"].scope == "showcase:read"
    assert TOOL_CONTRACTS["showcase.apply"].scope == "showcase:write"
    assert "showcase.apply" in inspect.getsource(create_server)


async def _apply_schema(server: object) -> dict[str, object]:
    tools = await server.list_tools()  # type: ignore[attr-defined]
    return next(tool.input_schema for tool in tools if tool.name == "showcase.apply")


def test_standalone_apply_items_remains_optional() -> None:
    import asyncio

    from my_data_hub.showcase.mcp_server import create_server

    schema = asyncio.run(_apply_schema(create_server(manager=object())))
    assert "items" not in schema.get("required", [])
