#!/usr/bin/env python3
"""Fetch one tunnel key from Lockbox without printing or retaining the response."""

from __future__ import annotations

import json
import os
import stat
import urllib.request
from pathlib import Path

METADATA_TOKEN_URL = (
    "http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token"
)
LOCKBOX_URL = "https://payload.lockbox.api.cloud.yandex.net/lockbox/v1/secrets/{secret_id}/payload"
OPENSSH_KEY_HEADER = "-----BEGIN " + "OPENSSH PRIVATE KEY-----\n"


def bounded_json(url: str, *, headers: dict[str, str], limit: int = 64 * 1024) -> dict[str, object]:
    request = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(request, timeout=15) as response:
        body = response.read(limit + 1)
    if len(body) > limit:
        raise RuntimeError("bounded metadata/Lockbox response exceeded")
    value = json.loads(body)
    if not isinstance(value, dict):
        raise RuntimeError("unexpected JSON response")
    return value


def main() -> int:
    secret_id = os.environ.get("MY_DATA_HUB_EDGE_TUNNEL_SECRET_ID", "")
    destination = Path(os.environ.get("MY_DATA_HUB_EDGE_TUNNEL_KEY_FILE", ""))
    if not secret_id or not destination.is_absolute():
        raise RuntimeError("secret ID and absolute destination are required")
    token_payload = bounded_json(METADATA_TOKEN_URL, headers={"Metadata-Flavor": "Google"})
    token = token_payload.get("access_token")
    if not isinstance(token, str) or len(token) < 32:
        raise RuntimeError("metadata IAM token is absent")
    payload = bounded_json(
        LOCKBOX_URL.format(secret_id=secret_id), headers={"Authorization": f"Bearer {token}"}
    )
    matches = [
        item.get("textValue")
        for item in payload.get("entries", [])
        if isinstance(item, dict) and item.get("key") == "tunnel_private_key"
    ]
    if len(matches) != 1 or not isinstance(matches[0], str):
        raise RuntimeError("exact Lockbox tunnel key entry is absent or ambiguous")
    key = matches[0]
    if not key.startswith(OPENSSH_KEY_HEADER) or len(key.encode()) > 16 * 1024:
        raise RuntimeError("Lockbox entry is not a bounded OpenSSH private key")
    destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.tmp")
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.write(fd, key.encode())
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(temporary, destination)
    os.chmod(destination, stat.S_IRUSR)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
