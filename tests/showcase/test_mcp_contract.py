from pathlib import Path


def test_mcp_module_declares_expected_tools() -> None:
    source = (Path(__file__).resolve().parents[2] / "src" / "my_data_hub" / "showcase" / "mcp_server.py").read_text(
        encoding="utf-8"
    )
    for name in (
        "showcase.list",
        "showcase.get_link",
        "showcase.rebuild",
        "showcase.create_view",
        "showcase.rotate_link",
        "showcase.revoke_link",
    ):
        assert f'"{name}"' in source
