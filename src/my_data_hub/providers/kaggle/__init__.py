"""Kaggle-specific private canary interfaces.

No concrete credentialed adapter is included here. Provider mutations are enabled only
when an application supplies a separately reviewed implementation.
"""

from .canary import (
    CanaryCleanupReceipt,
    DatasetCanaryReceipt,
    KagglePrivateCanaryAdapter,
    NotebookCanaryReceipt,
    PrivateDatasetSnapshot,
    PrivateNotebookSnapshot,
)

__all__ = [
    "CanaryCleanupReceipt",
    "DatasetCanaryReceipt",
    "KagglePrivateCanaryAdapter",
    "NotebookCanaryReceipt",
    "PrivateDatasetSnapshot",
    "PrivateNotebookSnapshot",
]
