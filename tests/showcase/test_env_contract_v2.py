from __future__ import annotations

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]


def test_every_showcase_environment_name_is_documented() -> None:
    code = "\n".join(path.read_text(encoding="utf-8") for path in (ROOT / "src/my_data_hub/showcase").glob("*.py"))
    used = set(re.findall(r"MY_DATA_HUB_SHOWCASE_[A-Z0-9_]+", code))
    documented = "\n".join(
        [
            (ROOT / ".env.example").read_text(encoding="utf-8"),
            (ROOT / "deploy/showcase-runtime/runtime.env.example").read_text(encoding="utf-8"),
            (ROOT / "compose.showcase.yaml").read_text(encoding="utf-8"),
            (ROOT / "docs/operations/ideahub-showcase-runtime.md").read_text(encoding="utf-8"),
        ]
    )
    assert sorted(name for name in used if name not in documented) == []


def test_loopback_gateway_matches_existing_control_plane_network_mode() -> None:
    base = yaml.safe_load((ROOT / "compose.control-plane.yaml").read_text(encoding="utf-8"))
    overlay = yaml.safe_load((ROOT / "compose.showcase.yaml").read_text(encoding="utf-8"))
    assert base["services"]["remote-mcp"]["network_mode"] == "host"
    remote = overlay["services"]["remote-mcp"]
    assert remote["environment"]["MY_DATA_HUB_SHOWCASE_GATEWAY_URL"].startswith("http://127.0.0.1:")
    runtime = overlay["services"]["showcase-runtime"]
    assert "ports" not in runtime
    assert runtime["network_mode"] == "host"
    assert runtime["mem_limit"].endswith("512m}")
    assert runtime["cpus"].endswith("1.00}")


def test_full_link_tool_is_not_in_reader_profile() -> None:
    server = (ROOT / "src/my_data_hub/mcp/server.py").read_text(encoding="utf-8")
    start = server.index("READER_PROFILE_TOOLS = frozenset(")
    end = server.index("\n)\n\nPROVIDER_ONLY_TOOLS", start)
    profile = server[start:end]
    assert '"showcase.list"' in profile
    assert '"showcase.get_link"' not in profile
    catalog = (ROOT / "src/my_data_hub/mcp/catalog.py").read_text(encoding="utf-8")
    assert '("showcase.get_link", "showcase:write")' in catalog
