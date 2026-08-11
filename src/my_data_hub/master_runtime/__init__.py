"""Kaggle-hosted PostgreSQL master runtime primitives.

The package is deliberately independent from the devstand control ledger.  It owns
the fail-closed data-plane mechanisms which must continue to work when the control
plane is unreachable: epoch validation, the database write gate, process layout,
and tunnel lifetime.
"""

from .contracts import BootSource, GateState, MasterIdentity, ServiceReady
from .fencing import EpochFence, FencingError, LeaseWatchdog

__all__ = [
    "BootSource",
    "EpochFence",
    "FencingError",
    "GateState",
    "LeaseWatchdog",
    "MasterIdentity",
    "ServiceReady",
]
