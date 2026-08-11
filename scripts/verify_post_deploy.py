#!/usr/bin/env python3
"""Fail-closed remote and signed host-evidence post-deploy acceptance."""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import ipaddress
import json
import os
import re
import socket
import ssl
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from cryptography.hazmat.primitives.serialization import load_pem_public_key

from scripts.verify_remote_mcp import verify_acceptance

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA = re.compile(r"^[0-9a-f]{40}$")
_SOURCE_IDENTITY = re.compile(r"^[A-Za-z0-9_.-]{1,100}/[A-Za-z0-9_.-]{1,100}$")
_KEY_ID = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
_B64URL_SIGNATURE = re.compile(r"^[A-Za-z0-9_-]{86}$")
_IMAGE_ID = re.compile(r"^sha256:[0-9a-f]{64}$")
_SERVICES = frozenset({"control-plane", "remote-mcp", "oauth-server"})
_LOOPBACK_PORTS = [8080, 8765, 8780]
_FORBIDDEN_PUBLIC_PORTS = (5432, 8080, 8765, 8780)
PUBLIC_MCP_URL = "https://mcp-datahub.kenigevents.ru/mcp"
AUTHORIZATION_SERVER = "https://identity.kenigevents.ru"
_SECRET_FRAGMENTS = (
    "authorization",
    "cookie",
    "credential",
    "database_url",
    "password",
    "private_key",
    "secret",
    "token",
)


@dataclass(frozen=True, slots=True)
class PublicEndpoint:
    url: str
    hostname: str
    host_header: str

    @classmethod
    def parse(cls, value: str) -> PublicEndpoint:
        parsed = urlsplit(value)
        if (
            value != PUBLIC_MCP_URL
            or parsed.scheme != "https"
            or parsed.hostname != "mcp-datahub.kenigevents.ru"
            or parsed.port is not None
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path != "/mcp"
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("endpoint must be the owner-approved canonical HTTPS MCP resource")
        return cls(value, parsed.hostname.casefold(), parsed.hostname.casefold())

    @property
    def resource_metadata_url(self) -> str:
        return f"https://{self.host_header}/.well-known/oauth-protected-resource/mcp"


def _utc(value: object, name: str) -> datetime:
    if not isinstance(value, str) or len(value) > 64:
        raise ValueError(f"deployment evidence {name} is not a bounded timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"deployment evidence {name} is invalid") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"deployment evidence {name} must include UTC offset")
    return parsed.astimezone(UTC)


def _exact_keys(value: object, expected: set[str], name: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != expected:
        raise ValueError(f"deployment evidence {name} fields differ from the contract")
    return value


def _reject_secret_material(value: object) -> None:
    if isinstance(value, dict):
        for key, nested in value.items():
            lowered = str(key).casefold()
            if any(fragment in lowered for fragment in _SECRET_FRAGMENTS):
                raise ValueError("deployment evidence contains a secret-bearing field")
            _reject_secret_material(nested)
    elif isinstance(value, list):
        for nested in value:
            _reject_secret_material(nested)
    elif isinstance(value, str):
        lowered = value.casefold()
        if "-----begin" in lowered or ("://" in value and "@" in value.partition("://")[2]):
            raise ValueError("deployment evidence contains credential-like material")
        if len(value) > 4096:
            raise ValueError("deployment evidence contains an oversized string")


def _canonical_unsigned(receipt: dict[str, Any]) -> bytes:
    unsigned = dict(receipt)
    unsigned.pop("signature", None)
    return json.dumps(
        unsigned,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _load_ed25519_public_key(pem: str) -> Ed25519PublicKey:
    if not pem or len(pem.encode("utf-8")) > 8192:
        raise ValueError("deployment evidence public key is absent or oversized")
    try:
        key = load_pem_public_key(pem.encode("utf-8"))
    except Exception as exc:
        raise ValueError("deployment evidence public key is invalid") from exc
    if not isinstance(key, Ed25519PublicKey):
        raise ValueError("deployment evidence public key must be Ed25519")
    return key


def validate_deployment_evidence(
    raw_receipt: str,
    public_key_pem: str,
    *,
    expected_commit: str,
    expected_source_identity: str,
    expected_key_id: str,
    now: datetime | None = None,
    max_age_seconds: int = 86_400,
) -> dict[str, object]:
    """Verify one fresh, signed and sanitized host-local deployment receipt."""

    if not 300 <= max_age_seconds <= 604_800:
        raise ValueError("deployment evidence maximum age must be between 5 minutes and 7 days")
    if not raw_receipt or len(raw_receipt.encode("utf-8")) > 32_768:
        raise ValueError("deployment evidence is absent or oversized")
    try:
        parsed = json.loads(raw_receipt)
    except json.JSONDecodeError as exc:
        raise ValueError("deployment evidence is not valid JSON") from exc
    receipt = _exact_keys(
        parsed,
        {
            "schema_version",
            "source_identity",
            "deployed_commit",
            "source_tree_sha256",
            "installed_release_tree_sha256",
            "host_id_sha256",
            "issued_at",
            "expires_at",
            "checks",
            "signature",
        },
        "root",
    )
    _reject_secret_material({key: value for key, value in receipt.items() if key != "signature"})
    if receipt["schema_version"] != "my-data-hub-deployment-evidence.v1":
        raise ValueError("deployment evidence schema version is unsupported")
    if receipt["deployed_commit"] != expected_commit:
        raise ValueError("deployment evidence commit differs from the requested deployment")
    if receipt["source_identity"] != expected_source_identity:
        raise ValueError("deployment evidence source identity differs from the requested source")
    if (
        not isinstance(receipt["source_tree_sha256"], str)
        or not _SHA256.fullmatch(receipt["source_tree_sha256"])
        or not isinstance(receipt["installed_release_tree_sha256"], str)
        or not _SHA256.fullmatch(receipt["installed_release_tree_sha256"])
        or receipt["source_tree_sha256"] != receipt["installed_release_tree_sha256"]
    ):
        raise ValueError("deployment evidence installed release differs from its source tree")
    if not isinstance(receipt["host_id_sha256"], str) or not _SHA256.fullmatch(receipt["host_id_sha256"]):
        raise ValueError("deployment evidence host identity must be a sanitized SHA-256 reference")

    signature = _exact_keys(receipt["signature"], {"algorithm", "key_id", "value"}, "signature")
    if signature["algorithm"] != "Ed25519" or signature["key_id"] != expected_key_id:
        raise ValueError("deployment evidence signing identity differs from policy")
    encoded_signature = signature["value"]
    if not isinstance(encoded_signature, str) or not _B64URL_SIGNATURE.fullmatch(encoded_signature):
        raise ValueError("deployment evidence signature is invalid")
    try:
        signature_bytes = base64.urlsafe_b64decode(encoded_signature + "==")
        _load_ed25519_public_key(public_key_pem).verify(signature_bytes, _canonical_unsigned(receipt))
    except Exception as exc:
        raise ValueError("deployment evidence signature verification failed") from exc

    current = (now or datetime.now(UTC)).astimezone(UTC)
    issued_at = _utc(receipt["issued_at"], "issued_at")
    expires_at = _utc(receipt["expires_at"], "expires_at")
    if (
        issued_at > current + timedelta(minutes=2)
        or current - issued_at > timedelta(seconds=max_age_seconds)
        or expires_at <= current
        or expires_at - issued_at > timedelta(seconds=max_age_seconds)
    ):
        raise ValueError("deployment evidence is stale, future-dated or overlong")

    checks = _exact_keys(
        receipt["checks"],
        {
            "services",
            "service_image_ids",
            "database_process_present",
            "pgdata_present",
            "database_environment_present",
            "my_data_hub_public_listener_ports",
            "my_data_hub_loopback_listener_ports",
            "process_kill",
            "reboot_autostart",
        },
        "checks",
    )
    if checks["services"] != {service: "running" for service in sorted(_SERVICES)}:
        raise ValueError("deployment evidence does not show all control services running")
    image_ids = checks["service_image_ids"]
    if (
        not isinstance(image_ids, dict)
        or set(image_ids) != _SERVICES
        or any(not isinstance(value, str) or not _IMAGE_ID.fullmatch(value) for value in image_ids.values())
        or len(set(image_ids.values())) != 1
    ):
        raise ValueError("deployment evidence does not bind the exact immutable service image")
    if any(
        checks[name] is not False
        for name in ("database_process_present", "pgdata_present", "database_environment_present")
    ):
        raise ValueError("deployment evidence reports forbidden local database state")
    if (
        checks["my_data_hub_public_listener_ports"] != []
        or checks["my_data_hub_loopback_listener_ports"] != _LOOPBACK_PORTS
    ):
        raise ValueError("deployment evidence my-data-hub listener inventory differs from the exact allowlist")

    process_kill = _exact_keys(
        checks["process_kill"],
        {
            "target_service",
            "killed_at",
            "recovered_at",
            "before_process_sha256",
            "after_process_sha256",
            "recovered",
        },
        "process_kill",
    )
    if process_kill["target_service"] not in _SERVICES or process_kill["recovered"] is not True:
        raise ValueError("deployment evidence process-kill recovery did not pass")
    before = process_kill["before_process_sha256"]
    after = process_kill["after_process_sha256"]
    if (
        not isinstance(before, str)
        or not isinstance(after, str)
        or not _SHA256.fullmatch(before)
        or not _SHA256.fullmatch(after)
        or before == after
    ):
        raise ValueError("deployment evidence process replacement references are invalid")
    killed_at = _utc(process_kill["killed_at"], "process_kill.killed_at")
    recovered_at = _utc(process_kill["recovered_at"], "process_kill.recovered_at")

    reboot = _exact_keys(
        checks["reboot_autostart"],
        {
            "rebooted_at",
            "verified_at",
            "boot_id_sha256",
            "systemd_unit",
            "unit_enabled",
            "linger_enabled",
            "autostart_services",
        },
        "reboot_autostart",
    )
    if (
        reboot["systemd_unit"] != "my-data-hub-control-plane.service"
        or reboot["unit_enabled"] is not True
        or reboot["linger_enabled"] is not True
        or reboot["autostart_services"] != sorted(_SERVICES)
        or not isinstance(reboot["boot_id_sha256"], str)
        or not _SHA256.fullmatch(reboot["boot_id_sha256"])
    ):
        raise ValueError("deployment evidence reboot/autostart receipt differs from policy")
    rebooted_at = _utc(reboot["rebooted_at"], "reboot_autostart.rebooted_at")
    verified_at = _utc(reboot["verified_at"], "reboot_autostart.verified_at")
    if (
        not killed_at <= recovered_at <= rebooted_at <= verified_at <= issued_at
        or issued_at - killed_at > timedelta(seconds=max_age_seconds)
    ):
        raise ValueError("deployment evidence recovery timestamps are out of order")

    canonical = _canonical_unsigned(receipt)
    return {
        "verified": True,
        "schema_version": receipt["schema_version"],
        "source_identity": expected_source_identity,
        "deployed_commit": expected_commit,
        "host_id_sha256": receipt["host_id_sha256"],
        "source_tree_sha256": receipt["source_tree_sha256"],
        "installed_release_tree_sha256": receipt["installed_release_tree_sha256"],
        "service_image_ids": dict(image_ids),
        "signing_key_id": expected_key_id,
        "evidence_sha256": hashlib.sha256(canonical).hexdigest(),
        "process_kill_recovered": True,
        "reboot_autostart_verified": True,
        "local_database_absent": True,
    }


def resolve_global_addresses(endpoint: PublicEndpoint) -> tuple[str, ...]:
    resolved = {
        item[4][0]
        for item in socket.getaddrinfo(endpoint.hostname, 443, type=socket.SOCK_STREAM)
    }
    addresses = tuple(
        sorted(address for address in resolved if ipaddress.ip_address(address).is_global)
    )
    if not addresses:
        raise RuntimeError("public MCP DNS has no globally routable address")
    return addresses


def verify_dns_tls(
    endpoint: PublicEndpoint,
    *,
    addresses: tuple[str, ...] | None = None,
    timeout_seconds: float = 5,
) -> dict[str, object]:
    addresses = addresses or resolve_global_addresses(endpoint)
    context = ssl.create_default_context()
    with (
        socket.create_connection((endpoint.hostname, 443), timeout=timeout_seconds) as raw,
        context.wrap_socket(raw, server_hostname=endpoint.hostname) as secured,
    ):
        certificate = secured.getpeercert(binary_form=True)
        tls_version = secured.version()
    if not certificate or tls_version not in {"TLSv1.2", "TLSv1.3"}:
        raise RuntimeError("public MCP TLS did not present a verified modern certificate")
    return {
        "dns_global_address_count": len(addresses),
        "tls_version": tls_version,
        "certificate_sha256": hashlib.sha256(certificate).hexdigest(),
    }


def verify_forbidden_public_ports(
    endpoint: PublicEndpoint,
    *,
    addresses: tuple[str, ...] | None = None,
    ports: tuple[int, ...] = _FORBIDDEN_PUBLIC_PORTS,
    timeout_seconds: float = 2,
) -> dict[str, object]:
    addresses = addresses or resolve_global_addresses(endpoint)

    def probe(target: tuple[str, int]) -> tuple[str, int] | None:
        address, port = target
        try:
            with socket.create_connection((address, port), timeout=timeout_seconds):
                return target
        except OSError:
            return None

    targets = [(address, port) for address in addresses for port in ports]
    with ThreadPoolExecutor(max_workers=min(32, len(targets))) as executor:
        exposed = [target for target in executor.map(probe, targets) if target]
    if exposed:
        raise RuntimeError("a forbidden devstand service port is publicly reachable")
    return {
        "probed_global_address_count": len(addresses),
        "probed_closed_ports": list(ports),
        "probe_count": len(targets),
        "public_database_port_open": False,
    }


async def verify_http_negatives(
    endpoint: PublicEndpoint,
    *,
    client: Any | None = None,
) -> dict[str, object]:
    import httpx

    owns_client = client is None
    if client is None:
        client = httpx.AsyncClient(follow_redirects=False, timeout=httpx.Timeout(10, connect=5))
    try:
        return await _verify_http_with_client(endpoint, client)
    finally:
        if owns_client:
            await client.aclose()


async def _verify_http_with_client(endpoint: PublicEndpoint, client: Any) -> dict[str, object]:
    metadata = await client.get(endpoint.resource_metadata_url)
    wrong_host = await client.get(
        endpoint.resource_metadata_url,
        headers={"Host": "invalid.example"},
    )
    wrong_origin = await client.get(
        endpoint.resource_metadata_url,
        headers={"Host": endpoint.host_header, "Origin": "https://invalid.example"},
    )
    missing_auth = await client.post(
        endpoint.url,
        headers={"content-type": "application/json"},
        content=b"{}",
    )
    wrong_auth = await client.post(
        endpoint.url,
        headers={
            "content-type": "application/json",
            "authorization": "Bearer post-deploy-invalid-marker",
        },
        content=b"{}",
    )
    if metadata.status_code != 200:
        raise RuntimeError("OAuth protected-resource metadata is unavailable")
    document = metadata.json()
    authorization_servers = document.get("authorization_servers")
    if (
        document.get("resource") != endpoint.url
        or authorization_servers != [AUTHORIZATION_SERVER]
    ):
        raise RuntimeError("OAuth protected-resource metadata differs from the endpoint contract")
    issuer_url = AUTHORIZATION_SERVER
    discovery = await client.get(f"{issuer_url}/.well-known/oauth-authorization-server")
    if discovery.status_code != 200:
        raise RuntimeError("OAuth authorization server discovery is unavailable")
    authorization_metadata = discovery.json()
    expected_endpoints = {
        "issuer": issuer_url,
        "authorization_endpoint": f"{issuer_url}/authorize",
        "token_endpoint": f"{issuer_url}/token",
        "jwks_uri": f"{issuer_url}/.well-known/jwks.json",
    }
    if any(authorization_metadata.get(name) != value for name, value in expected_endpoints.items()):
        raise RuntimeError("OAuth authorization server discovery differs from the exact issuer")
    jwks_response = await client.get(expected_endpoints["jwks_uri"])
    if jwks_response.status_code != 200:
        raise RuntimeError("OAuth JWKS is unavailable")
    keys = jwks_response.json().get("keys")
    private_members = {"d", "p", "q", "dp", "dq", "qi", "oth"}
    if (
        not isinstance(keys, list)
        or not 1 <= len(keys) <= 5
        or any(
            not isinstance(key, dict)
            or key.get("kty") != "RSA"
            or key.get("alg") != "RS256"
            or not key.get("kid")
            or private_members.intersection(key)
            for key in keys
        )
    ):
        raise RuntimeError("OAuth JWKS is not a bounded public RS256 key set")
    expected = ((wrong_host, 403), (wrong_origin, 403), (missing_auth, 401), (wrong_auth, 401))
    if any(response.status_code != status for response, status in expected):
        raise RuntimeError("one or more public Host/Origin/auth negative checks failed open")
    if "www-authenticate" not in missing_auth.headers or "www-authenticate" not in wrong_auth.headers:
        raise RuntimeError("unauthenticated MCP rejection omitted its OAuth challenge")
    return {
        "resource_metadata": True,
        "wrong_host_rejected": True,
        "wrong_origin_rejected": True,
        "missing_auth_rejected": True,
        "wrong_auth_rejected": True,
        "authorization_server": issuer_url,
        "published_jwks_keys": len(keys),
    }


async def verify_all(
    *,
    endpoint: PublicEndpoint,
    token: str,
    expected_commit: str,
    expected_source_identity: str,
    evidence: dict[str, object],
    cold_start_timeout_seconds: float,
) -> dict[str, object]:
    addresses = await asyncio.wait_for(
        asyncio.to_thread(resolve_global_addresses, endpoint),
        timeout=10,
    )
    dns_tls = await asyncio.wait_for(
        asyncio.to_thread(verify_dns_tls, endpoint, addresses=addresses),
        timeout=15,
    )
    ports = await asyncio.wait_for(
        asyncio.to_thread(verify_forbidden_public_ports, endpoint, addresses=addresses),
        timeout=15,
    )
    negatives = await asyncio.wait_for(verify_http_negatives(endpoint), timeout=30)
    remote = await asyncio.wait_for(
        verify_acceptance(
            endpoint.url,
            token,
            expected_commit,
            cold_start_timeout_seconds=cold_start_timeout_seconds,
        ),
        timeout=cold_start_timeout_seconds + 120,
    )
    return {
        "schema_version": "my-data-hub-post-deploy-verification.v1",
        "ok": True,
        "endpoint": endpoint.url,
        "expected_commit": expected_commit,
        "expected_source_identity": expected_source_identity,
        "dns_tls": dns_tls,
        "public_port_boundary": ports,
        "http_negatives": negatives,
        "remote_mcp": remote,
        "deployment_evidence": evidence,
    }


def _read_argument(path_value: str, environment_name: str) -> str:
    if path_value:
        path = Path(path_value)
        if path.is_symlink() or not path.is_file() or path.stat().st_size > 32_768:
            raise ValueError("post-deploy input file is absent, symbolic or oversized")
        return path.read_text(encoding="utf-8")
    return os.getenv(environment_name, "")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", default=os.getenv("MY_DATA_HUB_MCP_CANARY_ENDPOINT", ""))
    parser.add_argument("--token", default=os.getenv("MY_DATA_HUB_MCP_CANARY_TOKEN", ""))
    parser.add_argument("--expected-commit", default=os.getenv("MY_DATA_HUB_EXPECTED_DEPLOY_COMMIT", ""))
    parser.add_argument(
        "--expected-source-identity",
        default=os.getenv("MY_DATA_HUB_EXPECTED_SOURCE_IDENTITY", ""),
    )
    parser.add_argument("--evidence-file", default="")
    parser.add_argument("--evidence-public-key-file", default="")
    parser.add_argument(
        "--expected-evidence-key-id",
        default=os.getenv("MY_DATA_HUB_DEPLOY_EVIDENCE_KEY_ID", ""),
    )
    parser.add_argument("--max-evidence-age-seconds", type=int, default=86_400)
    parser.add_argument("--cold-start-timeout-seconds", type=float, default=900)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    try:
        endpoint = PublicEndpoint.parse(args.endpoint)
        if not args.token or len(args.token) > 16_384:
            raise ValueError("reader canary credential is absent or oversized")
        if not _GIT_SHA.fullmatch(args.expected_commit):
            raise ValueError("expected commit must be an exact lowercase Git SHA")
        if not _SOURCE_IDENTITY.fullmatch(args.expected_source_identity):
            raise ValueError("expected source identity is invalid")
        if not _KEY_ID.fullmatch(args.expected_evidence_key_id):
            raise ValueError("expected deployment evidence key id is invalid")
        if not 30 <= args.cold_start_timeout_seconds <= 1800:
            raise ValueError("cold start timeout must be between 30 seconds and 30 minutes")
        raw_evidence = _read_argument(
            args.evidence_file,
            "MY_DATA_HUB_DEPLOY_EVIDENCE_JSON",
        )
        public_key_pem = _read_argument(
            args.evidence_public_key_file,
            "MY_DATA_HUB_DEPLOY_EVIDENCE_PUBLIC_KEY_PEM",
        )
        evidence = validate_deployment_evidence(
            raw_evidence,
            public_key_pem,
            expected_commit=args.expected_commit,
            expected_source_identity=args.expected_source_identity,
            expected_key_id=args.expected_evidence_key_id,
            max_age_seconds=args.max_evidence_age_seconds,
        )
        report = asyncio.run(
            verify_all(
                endpoint=endpoint,
                token=args.token,
                expected_commit=args.expected_commit,
                expected_source_identity=args.expected_source_identity,
                evidence=evidence,
                cold_start_timeout_seconds=args.cold_start_timeout_seconds,
            )
        )
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(json.dumps({"ok": True, "output": str(output), "expected_commit": args.expected_commit}))
        return 0
    except Exception as exc:
        # Neither bearer credentials nor raw signed evidence are included in this
        # deliberately non-diagnostic failure message.
        print(f"post-deploy verification failed ({type(exc).__name__})", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
