"""Compatibility import for the shared production Kaggle credential preflight."""

from my_data_hub.providers.kaggle.credentials import (
    kaggle_credentials_configured,
    kaggle_exact_kernel_read_credentials_configured,
)

__all__ = [
    "kaggle_credentials_configured",
    "kaggle_exact_kernel_read_credentials_configured",
]
