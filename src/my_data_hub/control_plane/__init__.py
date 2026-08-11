"""Database-free devstand control-plane contract for architecture-reset PR-A.

The public application exports are lazy so importing a leaf ledger model from a
provider-side runtime does not eagerly assemble FastAPI and create an
acceptance-runtime import cycle.
"""
from __future__ import annotations

from typing import Any

__all__ = ["ControlPlaneSettings", "create_app"]


def __getattr__(name: str) -> Any:
    if name in __all__:
        from .app import ControlPlaneSettings, create_app

        return {"ControlPlaneSettings": ControlPlaneSettings, "create_app": create_app}[name]
    raise AttributeError(name)
