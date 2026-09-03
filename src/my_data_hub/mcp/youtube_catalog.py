from __future__ import annotations

from typing import Any

from my_data_hub.mcp.catalog import TOOL_CONTRACTS as BASE_TOOL_CONTRACTS
from my_data_hub.mcp.catalog import ToolContract
from my_data_hub.mcp.oauth import AccessIdentity

YOUTUBE_TOOL_NAME = "youtube.video.analyze"
YOUTUBE_SCOPE = "youtube:analyze"
YOUTUBE_TOOL_CONTRACT = ToolContract(
    name=YOUTUBE_TOOL_NAME,
    scope=YOUTUBE_SCOPE,
    read_only=True,
    destructive=False,
    idempotent=False,
    open_world=True,
    role="operator",
)

TOOL_CONTRACTS: dict[str, ToolContract] = {
    **BASE_TOOL_CONTRACTS,
    YOUTUBE_TOOL_NAME: YOUTUBE_TOOL_CONTRACT,
}
ALL_SCOPES = frozenset(contract.scope for contract in TOOL_CONTRACTS.values())
DEFAULT_SECURITY_SCHEMES = [{"type": "oauth2", "scopes": sorted(ALL_SCOPES)}]


def visible_tools(identity: AccessIdentity | None) -> frozenset[str]:
    if identity is None:
        return frozenset()
    return frozenset(
        name for name, contract in TOOL_CONTRACTS.items() if contract.scope in identity.scopes
    )


def security_catalog(identity: AccessIdentity | None) -> dict[str, Any]:
    names = visible_tools(identity)
    return {
        "protocolVersion": "2026-07-28",
        "securitySchemes": DEFAULT_SECURITY_SCHEMES,
        "tools": [
            {
                "name": name,
                "securitySchemes": TOOL_CONTRACTS[name].security_schemes(),
                "annotations": TOOL_CONTRACTS[name].annotations(),
                "_meta": {"securitySchemes": TOOL_CONTRACTS[name].security_schemes()},
            }
            for name in sorted(names)
        ],
    }
