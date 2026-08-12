#!/usr/bin/env python3
"""Render bounded cloud-init from reviewed static edge assets."""

from __future__ import annotations

import argparse
import base64
import re
from pathlib import Path

SECRET_ID = re.compile(r"^[a-z0-9]{20}$")
KNOWN_HOST = re.compile(r"^188\.227\.84\.107 (ssh-ed25519|ecdsa-sha2-nistp256) [A-Za-z0-9+/=]+$")


def _b64(value: str) -> str:
    return base64.b64encode(value.encode()).decode("ascii")


def render(*, root: Path, secret_id: str, known_host: str) -> str:
    if not SECRET_ID.fullmatch(secret_id):
        raise ValueError("Lockbox secret ID has an unexpected shape")
    if not KNOWN_HOST.fullmatch(known_host):
        raise ValueError("devstand known-host line must bind the fixed IPv4 and one approved key type")
    template = (root / "cloud-init.yaml.tpl").read_text()
    replacements = {
        "__EDGE_NGINX_CONFIG_B64__": _b64((root / "edge-nginx.conf").read_text()),
        "__EDGE_PROXY_CONFIG_B64__": _b64((root / "proxy.conf").read_text()),
        "__EDGE_FETCH_KEY_SCRIPT_B64__": _b64((root / "fetch-lockbox-key.py").read_text()),
        "__EDGE_AUTOSSH_SERVICE_B64__": _b64((root / "autossh.service").read_text()),
        "__EDGE_SECRET_ID__": secret_id,
        "__DEVSTAND_KNOWN_HOST_B64__": _b64(known_host + "\n"),
    }
    for marker, value in replacements.items():
        if template.count(marker) != 1:
            raise ValueError(f"cloud-init marker is missing or ambiguous: {marker}")
        template = template.replace(marker, value)
    if "__EDGE_" in template or "__DEVSTAND_" in template:
        raise ValueError("unrendered cloud-init marker remains")
    if len(template.encode()) > 256 * 1024:
        raise ValueError("cloud-init exceeds the bounded metadata contract")
    return template


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--secret-id", required=True)
    parser.add_argument("--known-host-file", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    known_lines = [line.strip() for line in args.known_host_file.read_text().splitlines() if line.strip()]
    if len(known_lines) != 1:
        raise ValueError("known-host file must contain exactly one nonempty host key")
    output = render(root=Path(__file__).resolve().parent, secret_id=args.secret_id, known_host=known_lines[0])
    args.output.write_text(output)
    args.output.chmod(0o600)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
