"""Fail-closed errors raised by the restricted database operator."""

from __future__ import annotations


class DatabaseOperatorError(RuntimeError):
    """Base class for operator policy and execution failures."""


class SqlRejected(DatabaseOperatorError):
    """The submitted SQL is outside the supported AST policy."""


class GateClosed(DatabaseOperatorError):
    """Backup/restore evidence is not fresh enough for a write."""


class ReceiptError(DatabaseOperatorError):
    """A signed receipt is malformed, expired, forged, or mismatched."""


class RevisionConflict(DatabaseOperatorError):
    """The canonical revision no longer matches the preview."""


class EffectBoundsError(DatabaseOperatorError):
    """A preview or apply changed rows outside its approved bounds."""


class IdempotencyConflict(DatabaseOperatorError):
    """An idempotency key was already used for a different request."""
