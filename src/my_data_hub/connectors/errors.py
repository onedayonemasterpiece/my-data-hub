from __future__ import annotations

from typing import Any


class ConnectorCapabilityBlocked(RuntimeError):
    """A precise, pre-mutation capability/availability denial."""

    def __init__(
        self,
        code: str,
        *,
        master_state: str | None = None,
        operation_id: str | None = None,
        retryable: bool,
    ) -> None:
        self.code = code
        self.master_state = master_state
        self.operation_id = operation_id
        self.retryable = retryable
        super().__init__(code)

    def public(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "master_state": self.master_state,
            "operation_id": self.operation_id,
            "retryable": self.retryable,
            "mutation_started": False,
        }
