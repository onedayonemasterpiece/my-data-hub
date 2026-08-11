from __future__ import annotations

import base64
import hashlib
import inspect
import secrets
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from my_data_hub.auth.control import OAuthClientRecord, OAuthControlLedger
from my_data_hub.oauth_server.models import (
    AuthorizationServerSettings,
    OwnerIdentity,
    StaticClient,
    parse_scope,
    validate_pkce_value,
    validate_s256_challenge,
)
from my_data_hub.oauth_server.stores import (
    AuthorizationGrant,
    OAuthGrantStore,
    RefreshGrant,
    RefreshRotationRequest,
    RefreshRotationStatus,
    unix_time,
)
from my_data_hub.oauth_server.tokens import JwtIssuer


class OAuthProtocolError(ValueError):
    def __init__(self, error: str, *, status_code: int = 400) -> None:
        super().__init__(error)
        self.error = error
        self.status_code = status_code


@dataclass(frozen=True, slots=True)
class ValidatedAuthorizationRequest:
    client: StaticClient
    redirect_uri: str
    resource: str
    scopes: tuple[str, ...]
    state: str | None
    nonce: str | None
    code_challenge: str


async def _resolve(value: Any) -> Any:
    return await value if inspect.isawaitable(value) else value


def _digest(credential: str) -> str:
    return hashlib.sha256(credential.encode("ascii")).hexdigest()


def _credential() -> str:
    return secrets.token_urlsafe(32)


def _bounded_credential(value: str) -> bool:
    return bool(value) and len(value) <= 1024 and value.isascii() and not any(char.isspace() for char in value)


class AuthorizationService:
    """Single-owner OAuth 2.1/OIDC authorization-code service.

    The service is storage-agnostic.  Client enablement/audit use the existing
    control-ledger protocol; one-time grant state uses ``OAuthGrantStore``.
    """

    def __init__(
        self,
        *,
        settings: AuthorizationServerSettings,
        control_ledger: OAuthControlLedger,
        grant_store: OAuthGrantStore,
        clock: Callable[[], int] = unix_time,
    ) -> None:
        self.settings = settings
        self.control_ledger = control_ledger
        self.grant_store = grant_store
        self.clock = clock
        self.jwt = JwtIssuer(
            issuer=settings.issuer,
            audience=settings.audience,
            resource=settings.resource,
            private_key_pem=settings.signing_key_pem,
            key_id=settings.signing_key_id,
            access_token_ttl_seconds=settings.access_token_ttl_seconds,
        )

    def authorization_server_metadata(self) -> dict[str, object]:
        issuer = self.settings.issuer
        return {
            "issuer": issuer,
            "authorization_endpoint": f"{issuer}/authorize",
            "token_endpoint": f"{issuer}/token",
            "jwks_uri": f"{issuer}/.well-known/jwks.json",
            "revocation_endpoint": f"{issuer}/revoke",
            "response_types_supported": ["code"],
            "grant_types_supported": ["authorization_code", "refresh_token"],
            "code_challenge_methods_supported": ["S256"],
            "token_endpoint_auth_methods_supported": ["none"],
            "revocation_endpoint_auth_methods_supported": ["none"],
            "scopes_supported": sorted(self.settings.scopes_supported),
        }

    def openid_configuration(self) -> dict[str, object]:
        return {
            **self.authorization_server_metadata(),
            "subject_types_supported": ["public"],
            "id_token_signing_alg_values_supported": ["RS256"],
            "claims_supported": ["iss", "sub", "aud", "exp", "iat", "auth_time", "nonce"],
        }

    async def _enabled_client(self, client_id: str) -> tuple[StaticClient, OAuthClientRecord]:
        configured = self.settings.client(client_id)
        if configured is None:
            raise OAuthProtocolError("invalid_client", status_code=401)
        try:
            record = await _resolve(self.control_ledger.get_client(self.settings.issuer, client_id))
        except Exception as exc:
            raise OAuthProtocolError("temporarily_unavailable", status_code=503) from exc
        if (
            record is None
            or not record.enabled
            or record.issuer != self.settings.issuer
            or record.client_id != client_id
        ):
            raise OAuthProtocolError("invalid_client", status_code=401)
        return configured, record

    async def validate_authorization_request(
        self, parameters: Mapping[str, str]
    ) -> ValidatedAuthorizationRequest:
        required = ("response_type", "client_id", "redirect_uri", "resource", "scope", "code_challenge")
        if any(not parameters.get(name) for name in required):
            raise OAuthProtocolError("invalid_request")
        if parameters["response_type"] != "code":
            raise OAuthProtocolError("unsupported_response_type")
        if parameters.get("code_challenge_method") != "S256":
            raise OAuthProtocolError("invalid_request")
        challenge = parameters["code_challenge"]
        if not validate_s256_challenge(challenge):
            raise OAuthProtocolError("invalid_request")
        configured, ledger_client = await self._enabled_client(parameters["client_id"])
        redirect_uri = parameters["redirect_uri"]
        if redirect_uri not in configured.redirect_uris:
            raise OAuthProtocolError("invalid_request")
        if parameters["resource"] != self.settings.resource:
            raise OAuthProtocolError("invalid_target")
        if "audience" in parameters and parameters["audience"] != self.settings.audience:
            raise OAuthProtocolError("invalid_target")
        try:
            scopes = parse_scope(parameters["scope"])
        except ValueError as exc:
            raise OAuthProtocolError("invalid_scope") from exc
        allowed = configured.allowed_scopes.intersection(ledger_client.allowed_scopes)
        if not set(scopes).issubset(allowed):
            raise OAuthProtocolError("invalid_scope")
        nonce = parameters.get("nonce")
        if "openid" in scopes and (not nonce or len(nonce) > 255):
            raise OAuthProtocolError("invalid_request")
        state = parameters.get("state")
        if state is not None and (not state or len(state) > 1024):
            raise OAuthProtocolError("invalid_request")
        return ValidatedAuthorizationRequest(
            client=configured,
            redirect_uri=redirect_uri,
            resource=self.settings.resource,
            scopes=scopes,
            state=state,
            nonce=nonce,
            code_challenge=challenge,
        )

    async def complete_authorization(
        self, request: ValidatedAuthorizationRequest, owner: OwnerIdentity
    ) -> str:
        now = int(self.clock())
        for _ in range(3):
            code = _credential()
            created = await _resolve(
                self.grant_store.create_authorization_grant(
                    AuthorizationGrant(
                        code_digest=_digest(code),
                        code_challenge=request.code_challenge,
                        client_id=request.client.client_id,
                        redirect_uri=request.redirect_uri,
                        resource=request.resource,
                        scopes=request.scopes,
                        subject=owner.subject,
                        nonce=request.nonce,
                        authenticated_at=owner.authenticated_at,
                        expires_at=now + self.settings.authorization_code_ttl_seconds,
                    )
                )
            )
            if created:
                return code
        raise OAuthProtocolError("temporarily_unavailable", status_code=503)

    async def exchange_authorization_code(self, parameters: Mapping[str, str]) -> dict[str, object]:
        required = ("code", "code_verifier", "redirect_uri", "client_id", "resource")
        if any(not parameters.get(name) for name in required):
            raise OAuthProtocolError("invalid_request")
        code = parameters["code"]
        verifier = parameters["code_verifier"]
        if not _bounded_credential(code) or not validate_pkce_value(verifier):
            raise OAuthProtocolError("invalid_grant")
        configured, ledger_client = await self._enabled_client(parameters["client_id"])
        if parameters["resource"] != self.settings.resource:
            raise OAuthProtocolError("invalid_target")
        if "audience" in parameters and parameters["audience"] != self.settings.audience:
            raise OAuthProtocolError("invalid_target")
        now = int(self.clock())
        grant = await _resolve(self.grant_store.consume_authorization_grant(_digest(code), now=now))
        if grant is None:
            raise OAuthProtocolError("invalid_grant")
        computed = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode("ascii")).digest()).rstrip(b"=").decode()
        if not secrets.compare_digest(computed, grant.code_challenge):
            raise OAuthProtocolError("invalid_grant")
        if (
            grant.client_id != parameters["client_id"]
            or grant.redirect_uri != parameters["redirect_uri"]
            or grant.resource != parameters["resource"]
            or not set(grant.scopes).issubset(
                configured.allowed_scopes.intersection(ledger_client.allowed_scopes)
            )
        ):
            raise OAuthProtocolError("invalid_grant")
        return await self._tokens_for_authorization_grant(grant, now=now)

    async def _tokens_for_authorization_grant(
        self, grant: AuthorizationGrant, *, now: int
    ) -> dict[str, object]:
        refresh_token, _refresh_grant = await self._new_refresh_grant(grant, now=now)
        response = self._access_response(
            subject=grant.subject,
            client_id=grant.client_id,
            scopes=grant.scopes,
            now=now,
        )
        response["refresh_token"] = refresh_token
        if "openid" in grant.scopes:
            response["id_token"] = self.jwt.issue_id_token(
                subject=grant.subject,
                client_id=grant.client_id,
                nonce=grant.nonce,
                authenticated_at=grant.authenticated_at,
                now=now,
            )
        return response

    async def _new_refresh_grant(
        self, grant: AuthorizationGrant, *, now: int
    ) -> tuple[str, RefreshGrant]:
        family_id = secrets.token_urlsafe(18)
        for _ in range(3):
            raw = _credential()
            refresh = RefreshGrant(
                credential_digest=_digest(raw),
                family_id=family_id,
                client_id=grant.client_id,
                resource=grant.resource,
                scopes=grant.scopes,
                subject=grant.subject,
                authenticated_at=grant.authenticated_at,
                expires_at=now + self.settings.refresh_token_ttl_seconds,
            )
            if await _resolve(self.grant_store.create_refresh_grant(refresh)):
                return raw, refresh
        raise OAuthProtocolError("temporarily_unavailable", status_code=503)

    def _access_response(
        self, *, subject: str, client_id: str, scopes: tuple[str, ...], now: int
    ) -> dict[str, object]:
        token, _ = self.jwt.issue_access_token(
            subject=subject,
            client_id=client_id,
            scopes=scopes,
            token_id=secrets.token_urlsafe(18),
            now=now,
        )
        return {
            "access_token": token,
            "token_type": "Bearer",
            "expires_in": self.settings.access_token_ttl_seconds,
            "scope": " ".join(scopes),
        }

    async def rotate_refresh_token(self, parameters: Mapping[str, str]) -> dict[str, object]:
        required = ("refresh_token", "client_id", "resource")
        if any(not parameters.get(name) for name in required):
            raise OAuthProtocolError("invalid_request")
        presented = parameters["refresh_token"]
        if not _bounded_credential(presented):
            raise OAuthProtocolError("invalid_grant")
        configured, ledger_client = await self._enabled_client(parameters["client_id"])
        if parameters["resource"] != self.settings.resource:
            raise OAuthProtocolError("invalid_target")
        if "audience" in parameters and parameters["audience"] != self.settings.audience:
            raise OAuthProtocolError("invalid_target")
        requested_scopes: tuple[str, ...] | None = None
        if "scope" in parameters:
            try:
                requested_scopes = parse_scope(parameters["scope"])
            except ValueError as exc:
                raise OAuthProtocolError("invalid_scope") from exc
            if not set(requested_scopes).issubset(
                configured.allowed_scopes.intersection(ledger_client.allowed_scopes)
            ):
                raise OAuthProtocolError("invalid_scope")
        successor = _credential()
        now = int(self.clock())
        rotation = await _resolve(
            self.grant_store.rotate_refresh_grant(
                RefreshRotationRequest(
                    presented_digest=_digest(presented),
                    successor_digest=_digest(successor),
                    client_id=parameters["client_id"],
                    resource=parameters["resource"],
                    requested_scopes=requested_scopes,
                    successor_expires_at=now + self.settings.refresh_token_ttl_seconds,
                    now=now,
                )
            )
        )
        if rotation.status is not RefreshRotationStatus.ROTATED or rotation.grant is None:
            raise OAuthProtocolError("invalid_grant")
        response = self._access_response(
            subject=rotation.grant.subject,
            client_id=rotation.grant.client_id,
            scopes=rotation.grant.scopes,
            now=now,
        )
        response["refresh_token"] = successor
        return response

    async def revoke_refresh_token(self, parameters: Mapping[str, str]) -> None:
        token = parameters.get("token", "")
        client_id = parameters.get("client_id", "")
        if not _bounded_credential(token) or not client_id:
            return
        try:
            await self._enabled_client(client_id)
        except OAuthProtocolError:
            return
        await _resolve(
            self.grant_store.revoke_refresh_grant(_digest(token), client_id=client_id, now=int(self.clock()))
        )
