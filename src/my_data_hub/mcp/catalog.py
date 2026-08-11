from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from my_data_hub.mcp.oauth import AccessIdentity


@dataclass(frozen=True, slots=True)
class ToolContract:
    name: str
    scope: str
    read_only: bool
    destructive: bool = False
    idempotent: bool = True
    open_world: bool = False
    role: str = "reader"

    def security_schemes(self) -> list[dict[str, Any]]:
        return [{"type": "oauth2", "scopes": [self.scope]}]

    def annotations(self) -> dict[str, bool]:
        return {
            "readOnlyHint": self.read_only,
            "destructiveHint": self.destructive,
            "idempotentHint": self.idempotent,
            "openWorldHint": self.open_world,
        }


_READ = (
    ("platform.status", "platform:read"),
    ("master.status", "master:read"),
    ("operation.get", "operation:read"),
    ("checkpoint.status", "checkpoint:read"),
    ("embedding.coverage", "embedding:read"),
    ("embedding.production.capabilities", "embedding:read"),
    ("provider.resources.status", "provider:read"),
    ("bloggers.list", "bloggers:read"),
    ("bloggers.get", "bloggers:read"),
    ("bloggers.search", "bloggers:read"),
    ("bloggers.provenance", "bloggers:read"),
    ("bloggers.statistics", "bloggers:read"),
    ("bloggers.migration.accounting", "bloggers:read"),
    ("data.query", "data:read"),
    ("data.change.status", "operation:read"),
)

_WRITES = (
    ToolContract("master.ensure", "master:ensure", False, idempotent=True, role="operator"),
    ToolContract("master.rotation.request", "master:rotate", False, idempotent=True, role="operator"),
    ToolContract("checkpoint.restore.request", "recovery:request", False, idempotent=True, role="operator"),
    ToolContract("connector.coverage", "acceptance:probe", True, idempotent=True, role="operator"),
    ToolContract("runtime.stale_epoch.probe", "acceptance:probe", True, idempotent=True, role="operator"),
    ToolContract(
        "provider.protected_resource.probe", "acceptance:probe", True, idempotent=True, role="operator"
    ),
    ToolContract("runtime.events.history", "acceptance:probe", True, idempotent=True, role="operator"),
    ToolContract("data.change.preview", "data:write", False, role="operator"),
    ToolContract("data.change.apply", "data:write", False, destructive=True, role="operator"),
    ToolContract("bloggers.import.preview", "migration:operate", False, role="migration_operator"),
    ToolContract(
        "bloggers.import.apply", "migration:operate", False, destructive=True, role="migration_operator"
    ),
    ToolContract("provider.resources.create", "provider:write", False, open_world=True, role="provider_operator"),
    ToolContract("provider.resources.version", "provider:write", False, open_world=True, role="provider_operator"),
    ToolContract("provider.resources.run", "provider:write", False, open_world=True, role="provider_operator"),
    ToolContract("provider.resources.read", "provider:write", True, open_world=True, role="provider_operator"),
    ToolContract(
        "provider.acceptance.dataset.lifecycle",
        "provider:write",
        False,
        open_world=True,
        role="provider_operator",
    ),
    ToolContract(
        "provider.acceptance.notebook.lifecycle",
        "provider:write",
        False,
        open_world=True,
        role="provider_operator",
    ),
    ToolContract(
        "provider.acceptance.claim.get",
        "provider:write",
        True,
        open_world=True,
        role="provider_operator",
    ),
    ToolContract(
        "provider.acceptance.claim.cleanup",
        "provider:write",
        False,
        destructive=True,
        open_world=True,
        role="provider_operator",
    ),
    ToolContract(
        "provider.resources.delete",
        "provider:write",
        False,
        destructive=True,
        open_world=True,
        role="provider_operator",
    ),
)

TOOL_CONTRACTS: dict[str, ToolContract] = {
    item.name: item
    for item in (
        *(ToolContract(name, scope, True) for name, scope in _READ),
        *_WRITES,
    )
}

ALL_SCOPES = frozenset(contract.scope for contract in TOOL_CONTRACTS.values())
DEFAULT_SECURITY_SCHEMES = [{"type": "oauth2", "scopes": sorted(ALL_SCOPES)}]
READER_PROFILE_SCOPES = frozenset(
    {
        "platform:read",
        "master:read",
        "operation:read",
        "checkpoint:read",
        "embedding:read",
        "provider:read",
        "bloggers:read",
        "data:read",
    }
)
OWNER_OPERATOR_PROFILE_SCOPES = ALL_SCOPES


def visible_tools(identity: AccessIdentity | None) -> frozenset[str]:
    if identity is None:
        return frozenset()
    return frozenset(
        name for name, contract in TOOL_CONTRACTS.items() if contract.scope in identity.scopes
    )


def security_catalog(identity: AccessIdentity | None) -> dict[str, Any]:
    """Return the security subset mirrored into discovery and tool metadata."""

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
