from __future__ import annotations

import inspect
import json
from collections.abc import Callable, Mapping
from typing import Any

from pydantic import ValidationError

from my_data_hub.auth.context import current_identity
from my_data_hub.auth.control import OAuthAuditEvent
from my_data_hub.google_ai.contracts import YouTubeAnalyzeRequest, YouTubeVideoAnalyzer
from my_data_hub.google_ai.errors import GoogleAIError, GoogleAIErrorCode
from my_data_hub.mcp.contracts import MCPAuditSink
from my_data_hub.mcp.oauth import AccessIdentity
from my_data_hub.mcp.youtube_catalog import YOUTUBE_SCOPE, YOUTUBE_TOOL_NAME


class YouTubeHubService:
    """Isolated quota-consuming service; it never enters HubService._write."""

    def __init__(
        self,
        *,
        analyzer: YouTubeVideoAnalyzer | None,
        enabled: bool,
        audit: MCPAuditSink | None,
        identity_provider: Callable[[], AccessIdentity | None] = current_identity,
        fallback_identity: AccessIdentity | None = None,
        max_result_bytes: int = 1_048_576,
    ) -> None:
        self._analyzer = analyzer
        self._enabled = enabled
        self._audit_sink = audit
        self._identity_provider = identity_provider
        self._fallback_identity = fallback_identity
        self._max_result_bytes = max_result_bytes

    async def invoke(self, arguments: Mapping[str, Any]) -> dict[str, Any]:
        identity = self._identity_provider() or self._fallback_identity
        if identity is None or YOUTUBE_SCOPE not in identity.scopes:
            raise PermissionError(f"MCP scope required: {YOUTUBE_SCOPE}")
        try:
            bounded = self._bounded_arguments(arguments)
            if not self._enabled or self._analyzer is None:
                raise GoogleAIError(GoogleAIErrorCode.FEATURE_DISABLED)
            request = YouTubeAnalyzeRequest.model_validate(bounded)
            result = dict(await self._analyzer.analyze(request))
        except ValidationError:
            result = GoogleAIError(GoogleAIErrorCode.RESPONSE_SCHEMA_INVALID).public()
            await self._audit(identity, outcome="denied_or_failed")
            return self._bounded_result(result)
        except GoogleAIError as exc:
            await self._audit(identity, outcome="denied_or_failed")
            return self._bounded_result(exc.public())
        except Exception:
            result = GoogleAIError(GoogleAIErrorCode.RECONCILIATION_REQUIRED).public()
            await self._audit(identity, outcome="denied_or_failed")
            return self._bounded_result(result)
        await self._audit(identity, outcome="accepted")
        return self._bounded_result(result)

    @staticmethod
    def _bounded_arguments(arguments: Mapping[str, Any]) -> dict[str, Any]:
        try:
            encoded = json.dumps(arguments, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise GoogleAIError(GoogleAIErrorCode.RESPONSE_SCHEMA_INVALID) from exc
        if len(encoded) > 32_768:
            raise GoogleAIError(GoogleAIErrorCode.RESPONSE_SCHEMA_INVALID)
        return dict(arguments)

    def _bounded_result(self, result: Mapping[str, Any]) -> dict[str, Any]:
        try:
            encoded = json.dumps(result, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise GoogleAIError(GoogleAIErrorCode.RESPONSE_SCHEMA_INVALID) from exc
        if len(encoded) > self._max_result_bytes:
            return GoogleAIError(GoogleAIErrorCode.RESPONSE_TOO_LARGE).public()
        return dict(result)

    async def _audit(self, identity: AccessIdentity, *, outcome: str) -> None:
        if self._audit_sink is None:
            return
        recorded = self._audit_sink.record_mcp_audit(
            OAuthAuditEvent(
                event="mcp_tool",
                outcome=outcome,
                issuer=identity.issuer,
                client_id=identity.client_id,
                subject=identity.subject,
                token_id=identity.token_id,
                tool=YOUTUBE_TOOL_NAME,
            )
        )
        if inspect.isawaitable(recorded):
            await recorded
