"""Narrow OAuth 2.1/OIDC authorization service for the remote MCP surface."""

from my_data_hub.oauth_server.app import OwnerAuthenticator, create_authorization_app
from my_data_hub.oauth_server.models import (
    AuthorizationServerSettings,
    OwnerAuthenticationChallenge,
    OwnerIdentity,
    StaticClient,
)
from my_data_hub.oauth_server.service import AuthorizationService
from my_data_hub.oauth_server.stores import MemoryOAuthGrantStore, OAuthGrantStore

__all__ = [
    "AuthorizationServerSettings",
    "AuthorizationService",
    "MemoryOAuthGrantStore",
    "OAuthGrantStore",
    "OwnerAuthenticationChallenge",
    "OwnerAuthenticator",
    "OwnerIdentity",
    "StaticClient",
    "create_authorization_app",
]
