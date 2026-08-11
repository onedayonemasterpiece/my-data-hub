"""Single official Kaggle 2.2.4 provider boundary and private canary contracts."""

from typing import TYPE_CHECKING, Any

from .adapter import KaggleProviderAdapter, directory_sha256, mapping_sha256, tree_sha256
from .canary import (
    CanaryCleanupReceipt,
    DatasetCanaryReceipt,
    KagglePrivateCanaryAdapter,
    NotebookCanaryReceipt,
    PrivateDatasetSnapshot,
    PrivateNotebookSnapshot,
)
from .contracts import (
    CONTROL_MANIFEST_NAME,
    KAGGLE_API_PACKAGE,
    KAGGLE_API_VERSION,
    RUN_RECEIPT_NAME,
    DatasetMutationResult,
    EffectOutcome,
    KaggleAmbiguousMutation,
    KaggleApiProtocol,
    KaggleContractError,
    KaggleDatasetIdentity,
    KaggleDependencyError,
    KaggleIdentityError,
    KaggleKernelFailureOutputIdentity,
    KaggleKernelOutputIdentity,
    KaggleKernelOutputTreeIdentity,
    KaggleKernelRunIdentity,
    KaggleKernelSourceIdentity,
    KaggleKernelStatus,
    KaggleNotFound,
    KagglePolicyError,
    KagglePollingTimeout,
    KaggleProviderError,
    KaggleProviderIdentity,
    KaggleRetryExhausted,
    KaggleTerminalFailure,
    KernelState,
    MutationAction,
    NotebookMutationResult,
    PollPolicy,
    PrivateAccessProof,
    ProviderEffectIntent,
    ProviderEffectJournal,
    ProviderEffectReceipt,
    RetryClass,
    TaskResourceClaim,
    UnauthenticatedDatasetProbe,
)
from .control_journal import (
    AuthenticatedControlPlaneClient,
    ControlLedgerKaggleJournal,
    ControlPlaneMetadataError,
    ControlPlaneRuntimeIdentity,
    MetadataHttpResponse,
    MetadataHttpsTransport,
    RemoteControlLedgerKaggleJournal,
)
from .provenance import DonorCompatibilityPin, compatibility_inventory
from .retry import BoundedRetry, ClassifiedFailure, RetryPolicy, classify_failure, parse_retry_after

if TYPE_CHECKING:
    from .master_runtime import (
        KaggleMasterLaunchAssets,
        KaggleMasterRuntimeProvider,
        MasterLaunchContractError,
    )

__all__ = [
    "CONTROL_MANIFEST_NAME",
    "KAGGLE_API_PACKAGE",
    "KAGGLE_API_VERSION",
    "RUN_RECEIPT_NAME",
    "AuthenticatedControlPlaneClient",
    "BoundedRetry",
    "CanaryCleanupReceipt",
    "ClassifiedFailure",
    "ControlLedgerKaggleJournal",
    "ControlPlaneMetadataError",
    "ControlPlaneRuntimeIdentity",
    "DatasetCanaryReceipt",
    "DatasetMutationResult",
    "DonorCompatibilityPin",
    "EffectOutcome",
    "KaggleAmbiguousMutation",
    "KaggleApiProtocol",
    "KaggleContractError",
    "KaggleDatasetIdentity",
    "KaggleDependencyError",
    "KaggleIdentityError",
    "KaggleKernelFailureOutputIdentity",
    "KaggleKernelOutputIdentity",
    "KaggleKernelOutputTreeIdentity",
    "KaggleKernelRunIdentity",
    "KaggleKernelSourceIdentity",
    "KaggleKernelStatus",
    "KaggleMasterLaunchAssets",
    "KaggleMasterRuntimeProvider",
    "KaggleNotFound",
    "KagglePolicyError",
    "KagglePollingTimeout",
    "KagglePrivateCanaryAdapter",
    "KaggleProviderAdapter",
    "KaggleProviderError",
    "KaggleProviderIdentity",
    "KaggleRetryExhausted",
    "KaggleTerminalFailure",
    "KernelState",
    "MasterLaunchContractError",
    "MetadataHttpResponse",
    "MetadataHttpsTransport",
    "MutationAction",
    "NotebookCanaryReceipt",
    "NotebookMutationResult",
    "PollPolicy",
    "PrivateAccessProof",
    "PrivateDatasetSnapshot",
    "PrivateNotebookSnapshot",
    "ProviderEffectIntent",
    "ProviderEffectJournal",
    "ProviderEffectReceipt",
    "RemoteControlLedgerKaggleJournal",
    "RetryClass",
    "RetryPolicy",
    "TaskResourceClaim",
    "UnauthenticatedDatasetProbe",
    "classify_failure",
    "compatibility_inventory",
    "directory_sha256",
    "mapping_sha256",
    "parse_retry_after",
    "render_notebook_source",
    "tree_sha256",
]


def __getattr__(name: str) -> Any:
    if name in {
        "KaggleMasterLaunchAssets",
        "KaggleMasterRuntimeProvider",
        "MasterLaunchContractError",
        "render_notebook_source",
    }:
        from . import master_runtime

        return getattr(master_runtime, name)
    raise AttributeError(name)
