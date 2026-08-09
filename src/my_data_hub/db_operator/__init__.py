"""Restricted PostgreSQL data-reader and data-editor primitives.

This package is intentionally not registered on the default MCP surface. It is the
core engine for a separately enabled operator profile under ADR-0012.
"""

from .engine import (
    ApplyResult,
    DatabaseOperator,
    InMemoryIdempotencyStore,
    PreviewResult,
    ReadResult,
)
from .errors import (
    DatabaseOperatorError,
    EffectBoundsError,
    GateClosed,
    IdempotencyConflict,
    ReceiptError,
    RevisionConflict,
    SqlRejected,
)
from .policy import (
    BackupFreshnessPolicy,
    BackupState,
    DatabaseAllowlist,
    Function,
    OperatorLimits,
    Relation,
)
from .receipts import ReceiptSigner, parameter_fingerprint
from .sql import (
    SqlAnalysis,
    analyze_editor_sql,
    analyze_reader_sql,
    compile_psycopg_parameters,
)

__all__ = [
    "ApplyResult",
    "BackupFreshnessPolicy",
    "BackupState",
    "DatabaseAllowlist",
    "DatabaseOperator",
    "DatabaseOperatorError",
    "EffectBoundsError",
    "Function",
    "GateClosed",
    "IdempotencyConflict",
    "InMemoryIdempotencyStore",
    "OperatorLimits",
    "PreviewResult",
    "ReadResult",
    "ReceiptError",
    "ReceiptSigner",
    "Relation",
    "RevisionConflict",
    "SqlAnalysis",
    "SqlRejected",
    "analyze_editor_sql",
    "analyze_reader_sql",
    "compile_psycopg_parameters",
    "parameter_fingerprint",
]
