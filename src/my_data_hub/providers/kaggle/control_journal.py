from __future__ import annotations

import json
import urllib.error
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol
from urllib.parse import urlsplit
from uuid import UUID

from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.providers.models import ControlClass, ProviderKind

from .contracts import (
    KaggleContractError,
    ProviderEffectIntent,
    ProviderEffectReceipt,
    TaskResourceClaim,
)

if TYPE_CHECKING:
    from my_data_hub.control_plane.ledger import ControlLedger

_MAX_METADATA_BYTES = 256 * 1024
_FORBIDDEN_METADATA_KEYS = {
    "bytes",
    "checkpoint_bytes",
    "content",
    "database_url",
    "dsn",
    "password",
    "pgdata",
}


class ControlPlaneMetadataError(KaggleContractError):
    """The authenticated metadata-only control-plane call failed closed."""


@dataclass(frozen=True, slots=True)
class MetadataHttpResponse:
    status: int
    body: bytes = b""


@dataclass(frozen=True, slots=True)
class ControlPlaneRuntimeIdentity:
    run_id: UUID
    attempt_id: UUID
    master_instance_id: UUID
    epoch: int

    def __post_init__(self) -> None:
        if self.epoch < 1:
            raise ValueError("control-plane runtime epoch must be positive")

    def headers(self) -> dict[str, str]:
        return {
            "X-MDH-Run-ID": str(self.run_id),
            "X-MDH-Attempt-ID": str(self.attempt_id),
            "X-MDH-Master-Instance-ID": str(self.master_instance_id),
            "X-MDH-Epoch": str(self.epoch),
        }


class MetadataHttpsTransport(Protocol):
    def request(
        self,
        *,
        url: str,
        method: str,
        headers: Mapping[str, str],
        body: bytes | None,
        timeout_seconds: float,
    ) -> MetadataHttpResponse: ...


class _UrllibMetadataHttpsTransport:
    def request(
        self,
        *,
        url: str,
        method: str,
        headers: Mapping[str, str],
        body: bytes | None,
        timeout_seconds: float,
    ) -> MetadataHttpResponse:
        request = urllib.request.Request(url, data=body, headers=dict(headers), method=method)
        opener = urllib.request.build_opener(_RejectRedirects())
        try:
            with opener.open(request, timeout=timeout_seconds) as response:
                payload = response.read(_MAX_METADATA_BYTES + 1)
                return MetadataHttpResponse(status=int(response.status), body=payload)
        except urllib.error.HTTPError as exc:
            # Never include response bodies: reverse proxies and frameworks may
            # echo an Authorization header or submitted metadata in an error.
            return MetadataHttpResponse(status=int(exc.code))
        except (OSError, urllib.error.URLError) as exc:
            raise ControlPlaneMetadataError("authenticated control-plane metadata request failed") from exc


class _RejectRedirects(urllib.request.HTTPRedirectHandler):
    """Never forward a runtime bearer credential across an HTTP redirect."""

    def redirect_request(self, *args: object, **kwargs: object) -> None:
        return None


class AuthenticatedControlPlaneClient:
    """Bounded HTTPS JSON client that categorically rejects data-plane payloads.

    The bearer credential is kept only in the request header.  Errors never
    include the URL query, token, request body, or response body.
    """

    def __init__(
        self,
        *,
        base_url: str,
        bearer_token: str,
        runtime_identity: ControlPlaneRuntimeIdentity,
        timeout_seconds: float = 15.0,
        transport: MetadataHttpsTransport | None = None,
    ) -> None:
        parsed = urlsplit(base_url)
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("control-plane base URL must be credential-free HTTPS")
        if not 24 <= len(bearer_token) <= 4096 or any(char.isspace() for char in bearer_token):
            raise ValueError("control-plane bearer token is invalid")
        if not 1 <= timeout_seconds <= 60:
            raise ValueError("control-plane metadata timeout is outside the bounded contract")
        self.base_url = base_url.rstrip("/")
        self._bearer_token = bearer_token
        self.runtime_identity = runtime_identity
        self.timeout_seconds = timeout_seconds
        self.transport = transport or _UrllibMetadataHttpsTransport()

    def get(self, path: str) -> dict[str, Any]:
        return self._request("GET", path, None)

    def post(self, path: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        _assert_metadata_only(payload)
        encoded = canonical_json_bytes(dict(payload))
        if len(encoded) > _MAX_METADATA_BYTES:
            raise ControlPlaneMetadataError("control-plane metadata payload exceeds 256 KiB")
        return self._request("POST", path, encoded)

    def _request(self, method: str, path: str, body: bytes | None) -> dict[str, Any]:
        if not path.startswith("/") or "?" in path or "#" in path or ".." in path.split("/"):
            raise ValueError("control-plane metadata path is invalid")
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self._bearer_token}",
            "Content-Type": "application/json",
            "User-Agent": "my-data-hub-kaggle-runtime/1",
            **self.runtime_identity.headers(),
        }
        response = self.transport.request(
            url=f"{self.base_url}{path}",
            method=method,
            headers=headers,
            body=body,
            timeout_seconds=self.timeout_seconds,
        )
        if response.status < 200 or response.status >= 300:
            raise ControlPlaneMetadataError(
                f"control-plane metadata request was rejected with HTTP {response.status}"
            )
        if len(response.body) > _MAX_METADATA_BYTES:
            raise ControlPlaneMetadataError("control-plane metadata response exceeds 256 KiB")
        if not response.body:
            return {}
        try:
            value = json.loads(response.body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ControlPlaneMetadataError("control-plane metadata response is not JSON") from exc
        if not isinstance(value, dict):
            raise ControlPlaneMetadataError("control-plane metadata response must be an object")
        _assert_metadata_only(value)
        return value


def _assert_metadata_only(value: object, *, path: str = "$") -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            normalized = str(key).strip().casefold()
            if normalized in _FORBIDDEN_METADATA_KEYS:
                raise ControlPlaneMetadataError(f"data-plane field is forbidden in metadata payload: {path}")
            _assert_metadata_only(item, path=f"{path}.{normalized}")
        return
    if isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _assert_metadata_only(item, path=f"{path}[{index}]")
        return
    if isinstance(value, (bytes, bytearray, memoryview)):
        raise ControlPlaneMetadataError(f"binary data is forbidden in metadata payload: {path}")
    if isinstance(value, str):
        lowered = value.casefold()
        if "postgresql://" in lowered or "postgres://" in lowered or "-----begin " in lowered:
            raise ControlPlaneMetadataError(f"credential/data-plane value is forbidden in metadata payload: {path}")
class ControlLedgerKaggleJournal:
    """Durable provider journal backed by the non-canonical control ledger."""

    def __init__(self, ledger: ControlLedger) -> None:
        self.ledger = ledger

    def persist_intent(self, intent: ProviderEffectIntent) -> None:
        self.ledger.persist_provider_effect_intent(intent.model_dump(mode="json"))

    def persist_receipt(self, receipt: ProviderEffectReceipt) -> None:
        self.ledger.persist_provider_effect_receipt(
            str(receipt.effect_id), receipt.model_dump(mode="json")
        )

    def persist_resource_claim(self, claim: TaskResourceClaim) -> None:
        self.ledger.persist_provider_resource_claim(claim.model_dump(mode="json"))

    def assert_resource_claim(self, claim: TaskResourceClaim) -> None:
        self.ledger.assert_provider_resource_claim(
            claim.claim_sha256, claim.model_dump(mode="json")
        )


class RemoteControlLedgerKaggleJournal:
    """Provider journal used inside Kaggle against devstand's HTTPS API.

    Only intent/receipt/claim metadata crosses this boundary.  Checkpoint
    archives, notebook outputs, PostgreSQL URLs and credentials are rejected
    before the transport is invoked.
    """

    def __init__(self, client: AuthenticatedControlPlaneClient) -> None:
        self.client = client

    def persist_intent(self, intent: ProviderEffectIntent) -> None:
        self.client.post(
            "/internal/provider-journal/intents",
            {"intent": intent.model_dump(mode="json")},
        )

    def persist_receipt(self, receipt: ProviderEffectReceipt) -> None:
        self.client.post(
            "/internal/provider-journal/receipts",
            {"receipt": receipt.model_dump(mode="json")},
        )

    def persist_resource_claim(self, claim: TaskResourceClaim) -> None:
        self.client.post(
            "/internal/provider-journal/resource-claims",
            {"claim": claim.model_dump(mode="json")},
        )

    def assert_resource_claim(self, claim: TaskResourceClaim) -> None:
        response = self.client.post(
            "/internal/provider-journal/resource-claims/assert",
            {"claim": claim.model_dump(mode="json")},
        )
        if response.get("authorized") is not True:
            raise PermissionError("provider resource claim is not authorized by the control ledger")

    def current_resource_claim(
        self,
        *,
        provider_ref: str,
        kind: ProviderKind,
        control_class: ControlClass,
    ) -> TaskResourceClaim:
        response = self.client.post(
            "/internal/provider-journal/resource-claims/current",
            {
                "provider_ref": provider_ref,
                "kind": kind.value,
                "control_class": control_class.value,
            },
        )
        try:
            claim = TaskResourceClaim.model_validate(response["claim"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ControlPlaneMetadataError("current resource claim response is invalid") from exc
        if (
            claim.provider_ref != provider_ref
            or claim.kind is not kind
            or claim.control_class is not control_class
            or claim.disposable
        ):
            raise ControlPlaneMetadataError("current resource claim differs from the authorized exact resource")
        return claim
