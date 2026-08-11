from __future__ import annotations

import inspect
from collections.abc import Awaitable, Mapping
from typing import Protocol, runtime_checkable
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, RedirectResponse, Response

from my_data_hub.oauth_server.models import OwnerAuthenticationChallenge, OwnerIdentity
from my_data_hub.oauth_server.service import AuthorizationService, OAuthProtocolError


@runtime_checkable
class OwnerAuthenticator(Protocol):
    """Bootstrap seam for an external owner authentication ceremony.

    Implementations validate an existing passkey, upstream OIDC, or hardened
    session and must not persist passwords in this authorization service.
    """

    def authenticate_owner(
        self, request: Request, *, return_to: str
    ) -> OwnerIdentity | OwnerAuthenticationChallenge | Awaitable[OwnerIdentity | OwnerAuthenticationChallenge]: ...


def _no_store(headers: Mapping[str, str] | None = None) -> dict[str, str]:
    return {"Cache-Control": "no-store", "Pragma": "no-cache", **dict(headers or {})}


def _error(error: OAuthProtocolError) -> JSONResponse:
    return JSONResponse(
        {"error": error.error},
        status_code=error.status_code,
        headers=_no_store(),
    )


def _unique_pairs(items: list[tuple[str, str]]) -> dict[str, str]:
    if len(items) > 32:
        raise OAuthProtocolError("invalid_request")
    result: dict[str, str] = {}
    for name, value in items:
        if name in result:
            raise OAuthProtocolError("invalid_request")
        result[name] = value
    return result


async def _form_parameters(request: Request) -> dict[str, str]:
    content_type = request.headers.get("content-type", "").partition(";")[0].strip().lower()
    if content_type != "application/x-www-form-urlencoded":
        raise OAuthProtocolError("invalid_request")
    try:
        content_length = int(request.headers.get("content-length", "0"))
    except ValueError as exc:
        raise OAuthProtocolError("invalid_request") from exc
    if content_length < 0 or content_length > 16_384:
        raise OAuthProtocolError("invalid_request")
    body = await request.body()
    if len(body) > 16_384:
        raise OAuthProtocolError("invalid_request")
    try:
        text = body.decode("ascii")
        return _unique_pairs(parse_qsl(text, keep_blank_values=True, strict_parsing=True, max_num_fields=32))
    except (UnicodeDecodeError, ValueError) as exc:
        raise OAuthProtocolError("invalid_request") from exc


def _authorization_redirect(redirect_uri: str, *, code: str, state: str | None) -> str:
    parsed = urlsplit(redirect_uri)
    response_query = [("code", code)]
    if state is not None:
        response_query.append(("state", state))
    query = f"{parsed.query}&{urlencode(response_query)}" if parsed.query else urlencode(response_query)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, query, ""))


def create_authorization_app(
    *, service: AuthorizationService, owner_authenticator: OwnerAuthenticator
) -> FastAPI:
    """Build the explicit authorization-server ASGI surface.

    No default/bootstrap password authenticator or in-memory store is installed;
    deployment wiring must provide both the owner-auth and durable store seams.
    """

    app = FastAPI(title="my-data-hub owner authorization server", docs_url=None, redoc_url=None)

    @app.get("/.well-known/oauth-authorization-server")
    async def authorization_server_metadata() -> JSONResponse:
        return JSONResponse(service.authorization_server_metadata())

    @app.get("/.well-known/openid-configuration")
    async def openid_configuration() -> JSONResponse:
        return JSONResponse(service.openid_configuration())

    @app.get("/.well-known/jwks.json")
    async def jwks() -> JSONResponse:
        return JSONResponse(service.jwt.jwks(), headers={"Cache-Control": "public, max-age=300"})

    @app.get("/authorize")
    async def authorize(request: Request) -> Response:
        try:
            parameters = _unique_pairs(list(request.query_params.multi_items()))
            validated = await service.validate_authorization_request(parameters)
        except OAuthProtocolError as exc:
            return _error(exc)
        try:
            result = owner_authenticator.authenticate_owner(request, return_to=str(request.url))
            owner = await result if inspect.isawaitable(result) else result
        except Exception:
            return _error(OAuthProtocolError("access_denied", status_code=401))
        if isinstance(owner, OwnerAuthenticationChallenge):
            return RedirectResponse(owner.location, status_code=303, headers=_no_store())
        if not isinstance(owner, OwnerIdentity):
            return _error(OAuthProtocolError("access_denied", status_code=401))
        try:
            code = await service.complete_authorization(validated, owner)
        except OAuthProtocolError as exc:
            return _error(exc)
        return RedirectResponse(
            _authorization_redirect(validated.redirect_uri, code=code, state=validated.state),
            status_code=303,
            headers=_no_store(),
        )

    @app.post("/token")
    async def token(request: Request) -> JSONResponse:
        try:
            if request.headers.get("authorization"):
                raise OAuthProtocolError("invalid_client", status_code=401)
            parameters = await _form_parameters(request)
            if "client_secret" in parameters:
                raise OAuthProtocolError("invalid_client", status_code=401)
            grant_type = parameters.get("grant_type")
            if not grant_type:
                raise OAuthProtocolError("invalid_request")
            if grant_type == "authorization_code":
                payload = await service.exchange_authorization_code(parameters)
            elif grant_type == "refresh_token":
                payload = await service.rotate_refresh_token(parameters)
            else:
                raise OAuthProtocolError("unsupported_grant_type")
            return JSONResponse(payload, headers=_no_store())
        except OAuthProtocolError as exc:
            return _error(exc)

    @app.post("/revoke")
    async def revoke(request: Request) -> Response:
        try:
            parameters = await _form_parameters(request)
            await service.revoke_refresh_token(parameters)
        except OAuthProtocolError:
            # RFC 7009 does not reveal whether the submitted token was valid.
            pass
        return Response(status_code=200, headers=_no_store())

    return app
