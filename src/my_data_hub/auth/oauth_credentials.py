"""Restart-safe OAuth bearer rotation for long-running operator clients.

The credential file is owner-only state.  It is deliberately outside durable
controller artifacts: only the local runner that performs the acceptance work
may read or replace it.
"""

from __future__ import annotations

import asyncio
import fcntl
import json
import os
import stat
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping
from contextlib import suppress
from pathlib import Path
from typing import Protocol, cast
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, model_validator

from my_data_hub.hashing import canonical_json_bytes

MAX_CREDENTIAL_BYTES = 64 * 1024
MIN_CREDENTIAL_LENGTH = 24
MAX_CREDENTIAL_LENGTH = 4096
REFRESH_SKEW_SECONDS = 60
PROFILES = frozenset({"reader", "operator", "migration", "provider"})


class OAuthCredentialError(RuntimeError):
    """The bounded OAuth credential source cannot return a usable bearer."""


class BearerSource(Protocol):
    async def token(self, profile: str) -> str: ...


class OAuthProfileState(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    client_id: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9._-]+$")
    refresh_token: str = Field(min_length=MIN_CREDENTIAL_LENGTH, max_length=MAX_CREDENTIAL_LENGTH)
    access_token: str | None = Field(
        default=None, min_length=MIN_CREDENTIAL_LENGTH, max_length=MAX_CREDENTIAL_LENGTH
    )
    access_expires_at: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def exact_token_pair(self) -> OAuthProfileState:
        for value in (self.refresh_token, self.access_token):
            if value is not None and any(character.isspace() for character in value):
                raise ValueError("OAuth credentials must not contain whitespace")
        if (self.access_token is None) != (self.access_expires_at is None):
            raise ValueError("cached OAuth access token and expiry must be stored together")
        return self


class OAuthCredentialState(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: str = Field(pattern=r"^my-data-hub-mcp-oauth-credentials\.v1$")
    token_endpoint: str
    resource: str
    profiles: dict[str, OAuthProfileState] = Field(min_length=1, max_length=4)

    @model_validator(mode="after")
    def exact_contract(self) -> OAuthCredentialState:
        for label, value, expected_path in (
            ("token endpoint", self.token_endpoint, "/token"),
            ("resource", self.resource, "/mcp"),
        ):
            parsed = urlsplit(value)
            if (
                parsed.scheme != "https"
                or not parsed.hostname
                or parsed.username
                or parsed.password
                or parsed.query
                or parsed.fragment
                or parsed.path.rstrip("/") != expected_path
            ):
                raise ValueError(f"OAuth {label} must be an exact credential-free HTTPS URL")
        if not set(self.profiles).issubset(PROFILES):
            raise ValueError("OAuth credential file has an unknown profile")
        return self


class StaticBearerSource:
    def __init__(self, tokens: Mapping[str, str]) -> None:
        self._tokens = dict(tokens)

    async def token(self, profile: str) -> str:
        value = self._tokens.get(profile, "")
        if not _valid_credential(value):
            raise OAuthCredentialError(f"OAuth bearer is absent for profile {profile}")
        return value


Exchange = Callable[[str, Mapping[str, str]], Mapping[str, object]]


class RotatingOAuthBearerSource:
    """Rotate refresh grants under one cross-process private-file lock."""

    def __init__(
        self,
        path: Path,
        *,
        now: Callable[[], float] = time.time,
        exchange: Exchange | None = None,
    ) -> None:
        if not path.is_absolute():
            raise ValueError("OAuth credential file path must be absolute")
        self.path = path
        self._now = now
        self._exchange = exchange or _exchange_refresh_token

    async def token(self, profile: str) -> str:
        if profile not in PROFILES:
            raise OAuthCredentialError("unknown OAuth credential profile")
        return await asyncio.to_thread(self._token_locked, profile)

    def _token_locked(self, profile: str) -> str:
        lock_path = self.path.with_name(f".{self.path.name}.lock")
        descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        try:
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "rb+", closefd=False):
                fcntl.flock(descriptor, fcntl.LOCK_EX)
                state = _read_state(self.path)
                current = state.profiles.get(profile)
                if current is None:
                    raise OAuthCredentialError(f"OAuth profile is absent: {profile}")
                now = int(self._now())
                if (
                    current.access_token is not None
                    and current.access_expires_at is not None
                    and current.access_expires_at > now + REFRESH_SKEW_SECONDS
                ):
                    return current.access_token
                response = self._exchange(
                    state.token_endpoint,
                    {
                        "grant_type": "refresh_token",
                        "refresh_token": current.refresh_token,
                        "client_id": current.client_id,
                        "resource": state.resource,
                    },
                )
                access_token, refresh_token, expires_in = _validated_exchange(response)
                updated_profiles = dict(state.profiles)
                updated_profiles[profile] = current.model_copy(
                    update={
                        "refresh_token": refresh_token,
                        "access_token": access_token,
                        "access_expires_at": now + expires_in,
                    }
                )
                _atomic_state_write(self.path, state.model_copy(update={"profiles": updated_profiles}))
                return access_token
        except OAuthCredentialError:
            raise
        except Exception:
            raise OAuthCredentialError("OAuth bearer rotation failed closed") from None
        finally:
            with suppress(OSError):
                os.close(descriptor)


def bearer_source_from_environment(tokens: Mapping[str, str]) -> BearerSource:
    path = os.getenv("MY_DATA_HUB_MCP_OAUTH_CREDENTIAL_FILE", "").strip()
    if path:
        return RotatingOAuthBearerSource(Path(path))
    return StaticBearerSource(tokens)


def validate_oauth_credential_file(path: Path, *, required_profiles: frozenset[str]) -> None:
    if not required_profiles or not required_profiles.issubset(PROFILES):
        raise ValueError("required OAuth profiles are invalid")
    state = _read_state(path)
    missing = required_profiles - set(state.profiles)
    if missing:
        raise OAuthCredentialError(
            "OAuth credential file lacks required profiles: " + ",".join(sorted(missing))
        )


def _valid_credential(value: object) -> bool:
    return (
        isinstance(value, str)
        and MIN_CREDENTIAL_LENGTH <= len(value) <= MAX_CREDENTIAL_LENGTH
        and not any(character.isspace() for character in value)
    )


def _read_state(path: Path) -> OAuthCredentialState:
    try:
        info = path.stat(follow_symlinks=False)
    except OSError:
        raise OAuthCredentialError("OAuth credential file is unavailable") from None
    if (
        path.is_symlink()
        or not stat.S_ISREG(info.st_mode)
        or stat.S_IMODE(info.st_mode) != 0o600
        or info.st_uid != os.getuid()
        or not 1 <= info.st_size <= MAX_CREDENTIAL_BYTES
    ):
        raise OAuthCredentialError("OAuth credential file is not an owner-owned mode-0600 file")
    try:
        return cast(OAuthCredentialState, OAuthCredentialState.model_validate_json(path.read_bytes()))
    except Exception:
        raise OAuthCredentialError("OAuth credential file failed exact validation") from None


def _atomic_state_write(path: Path, state: OAuthCredentialState) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(canonical_json_bytes(state.model_dump(mode="json")) + b"\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        os.chmod(path, 0o600)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if temporary.exists():
            temporary.unlink()


def _validated_exchange(response: Mapping[str, object]) -> tuple[str, str, int]:
    access_token = response.get("access_token")
    refresh_token = response.get("refresh_token")
    token_type = response.get("token_type")
    expires_in = response.get("expires_in")
    if (
        not _valid_credential(access_token)
        or not _valid_credential(refresh_token)
        or not isinstance(token_type, str)
        or token_type.lower() != "bearer"
        or not isinstance(expires_in, int)
        or isinstance(expires_in, bool)
        or not 120 <= expires_in <= 3600
    ):
        raise OAuthCredentialError("OAuth token response differs from the bounded rotation contract")
    return cast(str, access_token), cast(str, refresh_token), expires_in


def _exchange_refresh_token(endpoint: str, parameters: Mapping[str, str]) -> Mapping[str, object]:
    request = urllib.request.Request(
        endpoint,
        data=urllib.parse.urlencode(parameters).encode("ascii"),
        headers={"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    opener = urllib.request.build_opener(_RejectRedirects())
    try:
        with opener.open(request, timeout=20) as response:
            raw = response.read(32 * 1024 + 1)
    except (OSError, urllib.error.HTTPError, urllib.error.URLError):
        raise OAuthCredentialError("OAuth token endpoint is unavailable") from None
    if not 1 <= len(raw) <= 32 * 1024:
        raise OAuthCredentialError("OAuth token response exceeds its metadata bound")
    try:
        value = json.loads(raw)
    except (UnicodeError, json.JSONDecodeError):
        raise OAuthCredentialError("OAuth token response is invalid JSON") from None
    if not isinstance(value, Mapping):
        raise OAuthCredentialError("OAuth token response is not an object")
    return value


class _RejectRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args: object, **kwargs: object) -> None:
        return None
