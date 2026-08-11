"""Deterministic embedding and hybrid-search contracts.

Workers in this package are typed producers.  They never connect to or mutate
the canonical database; the master imports their immutable artifacts.
"""

from my_data_hub.embeddings.importer import (
    EmbeddingImportConflict,
    EmbeddingImportReceipt,
    PostgresEmbeddingImporter,
)
from my_data_hub.embeddings.models import BGE_M3, E5_MULTILINGUAL_BASE, model_by_key

__all__ = [
    "BGE_M3",
    "E5_MULTILINGUAL_BASE",
    "EmbeddingImportConflict",
    "EmbeddingImportReceipt",
    "PostgresEmbeddingImporter",
    "model_by_key",
]
