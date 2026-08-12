"""Pure master lifecycle core and provider-neutral reconciliation."""

from .coordinator import MASTER_SERVICE_KIND, MasterCoordinator, MasterHandle, MasterIntent
from .evidence import ExactOutput, PlatformStatus, TerminalDecision, decide_terminal
from .fake_kaggle import FakeKaggleRuntime, SimulatedProcessCrash
from .provider import (
    EffectReconciliation,
    MasterRuntimeProvider,
    PlannedProviderEffect,
    ProviderEffectReceipt,
    ReconciliationStatus,
)
from .state_machine import (
    InvalidMasterTransition,
    MasterEffect,
    MasterSignal,
    MasterState,
    Transition,
    transition_master,
)

__all__ = [
    "MASTER_SERVICE_KIND",
    "EffectReconciliation",
    "ExactOutput",
    "FakeKaggleRuntime",
    "InvalidMasterTransition",
    "MasterCoordinator",
    "MasterEffect",
    "MasterHandle",
    "MasterIntent",
    "MasterRuntimeProvider",
    "MasterSignal",
    "MasterState",
    "PlannedProviderEffect",
    "PlatformStatus",
    "ProviderEffectReceipt",
    "ReconciliationStatus",
    "SimulatedProcessCrash",
    "TerminalDecision",
    "Transition",
    "decide_terminal",
    "transition_master",
]
