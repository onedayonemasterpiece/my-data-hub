"""Narrow OAuth 2.1/OIDC authorization service for the remote MCP surface."""

from my_data_hub.oauth_server.app import (
    OAuthHTTPPolicy,
    OwnerAuthenticator,
    create_authorization_app,
)
from my_data_hub.oauth_server.client_metadata import (
    ChatGPTClientMetadataResolver,
    ClientMetadataError,
    ClientMetadataResponse,
)
from my_data_hub.oauth_server.control_store import ControlLedgerOAuthGrantStore
from my_data_hub.oauth_server.models import (
    AuthorizationServerSettings,
    OwnerAuthenticationChallenge,
    OwnerIdentity,
    StaticClient,
)
from my_data_hub.oauth_server.owner_oidc import OIDCSessionOwnerAuthenticator
from my_data_hub.oauth_server.owner_portal import OIDCLoginPortal
from my_data_hub.oauth_server.service import AuthorizationService
from my_data_hub.oauth_server.stores import MemoryOAuthGrantStore, OAuthGrantStore

__all__ = [
    "AuthorizationServerSettings",
    "AuthorizationService",
    "ChatGPTClientMetadataResolver",
    "ClientMetadataError",
    "ClientMetadataResponse",
    "ControlLedgerOAuthGrantStore",
    "MemoryOAuthGrantStore",
    "OAuthGrantStore",
    "OAuthHTTPPolicy",
    "OIDCLoginPortal",
    "OIDCSessionOwnerAuthenticator",
    "OwnerAuthenticationChallenge",
    "OwnerAuthenticator",
    "OwnerIdentity",
    "StaticClient",
    "create_authorization_app",
]
