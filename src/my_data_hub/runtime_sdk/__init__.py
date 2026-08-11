"""Generic, secret-safe runtime callback SDK for ephemeral notebooks."""

from .client import DeliveryReceipt, RetryPolicy, RuntimeClient
from .events import ArtifactRef, RuntimeEvent, RuntimeEventType
from .lifetime import (
    CHECKPOINT_ATTEMPT_BUDGET_SECONDS,
    CHECKPOINT_TRANSITION_GUARD_SECONDS,
    KAGGLE_HARD_CAP_SECONDS,
    KAGGLE_PROVIDER_TIMEOUT_SECONDS,
    MAX_NOTEBOOK_PROCESS_SECONDS,
    MIN_CHECKPOINT_RESERVE_SECONDS,
    MIN_PROCESS_EXIT_RESERVE_SECONDS,
    PROVIDER_HARD_CUTOFF_RESERVE_SECONDS,
)
from .spool import JsonlEventSpool
from .transport import CallbackTransport, TransportResponse, UrllibCallbackTransport

__all__ = [
    "CHECKPOINT_ATTEMPT_BUDGET_SECONDS",
    "CHECKPOINT_TRANSITION_GUARD_SECONDS",
    "KAGGLE_HARD_CAP_SECONDS",
    "KAGGLE_PROVIDER_TIMEOUT_SECONDS",
    "MAX_NOTEBOOK_PROCESS_SECONDS",
    "MIN_CHECKPOINT_RESERVE_SECONDS",
    "MIN_PROCESS_EXIT_RESERVE_SECONDS",
    "PROVIDER_HARD_CUTOFF_RESERVE_SECONDS",
    "ArtifactRef",
    "CallbackTransport",
    "DeliveryReceipt",
    "JsonlEventSpool",
    "RetryPolicy",
    "RuntimeClient",
    "RuntimeEvent",
    "RuntimeEventType",
    "TransportResponse",
    "UrllibCallbackTransport",
]
