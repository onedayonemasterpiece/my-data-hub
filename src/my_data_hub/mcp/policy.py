from __future__ import annotations

from dataclasses import dataclass


class MCPAuthorizationError(PermissionError):
    """Raised when the configured principal lacks a tool scope."""


@dataclass(frozen=True, slots=True)
class ScopeAuthorizer:
    granted: frozenset[str]

    def require(self, *required: str) -> None:
        missing = [scope for scope in required if scope not in self.granted]
        if missing:
            raise MCPAuthorizationError("missing MCP scope(s): " + ", ".join(sorted(missing)))

    def require_any(self, *alternatives: str) -> None:
        if not self.granted.intersection(alternatives):
            raise MCPAuthorizationError(
                "one MCP scope is required: " + ", ".join(sorted(alternatives))
            )
