from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar, Token
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from my_data_hub.mcp.oauth import AccessIdentity


_identity: ContextVar[AccessIdentity | None] = ContextVar("my_data_hub_oauth_identity", default=None)


def current_identity() -> AccessIdentity | None:
    """Return the cryptographically authenticated identity for this request."""

    return _identity.get()


def bind_identity(identity: AccessIdentity | None) -> Token[AccessIdentity | None]:
    """Bind an identity while an admitted request is dispatched."""

    return _identity.set(identity)


def reset_identity(token: Token[AccessIdentity | None]) -> None:
    _identity.reset(token)


@contextmanager
def identity_context(identity: AccessIdentity | None) -> Iterator[None]:
    token = bind_identity(identity)
    try:
        yield
    finally:
        reset_identity(token)
