from __future__ import annotations

import hmac
from typing import Any

from my_data_hub.mcp.admission import (
    AdmissionLimits,
    ASGIApp,
    HTTPAdmissionSecurity,
)
from my_data_hub.mcp.oauth import AccessIdentity, TokenValidationError


class DevelopmentBearerSecurity:
    """Fail-closed loopback/private guard; production uses OAuth admission."""

    def __init__(
        self,
        app: ASGIApp,
        *,
        token: str,
        allowed_origins: tuple[str, ...],
        allowed_hosts: tuple[str, ...],
        max_request_bytes: int = 1_048_576,
        max_response_bytes: int = 1_048_576,
        max_concurrency: int = 16,
        requests_per_minute: int = 120,
        request_timeout_seconds: float = 30.0,
    ) -> None:
        if not token:
            raise ValueError("development bearer token must not be empty")
        self.app = app
        self.token = token
        self.allowed_origins = set(allowed_origins)
        self.allowed_hosts = set(allowed_hosts)
        self.max_request_bytes = max_request_bytes

        def authenticate(header: str) -> AccessIdentity:
            expected = f"Bearer {self.token}"
            if not hmac.compare_digest(header, expected):
                raise TokenValidationError("invalid_token")
            # Development identity never crosses the production OAuth boundary.
            return AccessIdentity(
                subject="development",
                client_id="development",
                scopes=frozenset({"development"}),
                audience="loopback",
                token_id="development",
                expires_at=2**63 - 1,
                issuer="development",
                issued_at=0,
                resource="loopback",
            )

        self._admission = HTTPAdmissionSecurity(
            app,
            allowed_origins=allowed_origins,
            allowed_hosts=allowed_hosts,
            authenticator=authenticate,
            limits=AdmissionLimits(
                max_request_bytes=max_request_bytes,
                max_response_bytes=max_response_bytes,
                max_concurrency=max_concurrency,
                requests_per_window=requests_per_minute,
                rate_window_seconds=60,
                request_timeout_seconds=request_timeout_seconds,
            ),
        )

    async def __call__(self, scope: dict[str, Any], receive, send) -> None:  # type: ignore[no-untyped-def]
        await self._admission(scope, receive, send)
