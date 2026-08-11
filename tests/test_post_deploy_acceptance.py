from __future__ import annotations

import base64
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from scripts.verify_post_deploy import (
    PublicEndpoint,
    main,
    validate_deployment_evidence,
    verify_dns_tls,
    verify_forbidden_public_ports,
    verify_http_negatives,
)
from scripts.verify_remote_mcp import READ_ONLY_TOOLS, verify_acceptance_session

COMMIT = "a" * 40
SOURCE = "onedayonemasterpiece/my-data-hub"
KEY_ID = "devstand-evidence-2026-08"
NOW = datetime(2026, 8, 11, 4, 0, tzinfo=UTC)
ENDPOINT = PublicEndpoint.parse("https://mcp.example.test/mcp")


def _evidence(private_key: Ed25519PrivateKey, **changes: object) -> str:
    receipt: dict[str, object] = {
        "schema_version": "my-data-hub-deployment-evidence.v1",
        "source_identity": SOURCE,
        "deployed_commit": COMMIT,
        "host_id_sha256": "1" * 64,
        "issued_at": (NOW - timedelta(minutes=1)).isoformat().replace("+00:00", "Z"),
        "expires_at": (NOW + timedelta(hours=1)).isoformat().replace("+00:00", "Z"),
        "checks": {
            "services": {
                "control-plane": "running",
                "oauth-server": "running",
                "remote-mcp": "running",
            },
            "database_process_present": False,
            "pgdata_present": False,
            "database_environment_present": False,
            "my_data_hub_public_listener_ports": [],
            "my_data_hub_loopback_listener_ports": [8080, 8765, 8780],
            "process_kill": {
                "target_service": "remote-mcp",
                "killed_at": (NOW - timedelta(minutes=5)).isoformat().replace("+00:00", "Z"),
                "recovered_at": (NOW - timedelta(minutes=4)).isoformat().replace("+00:00", "Z"),
                "before_process_sha256": "2" * 64,
                "after_process_sha256": "3" * 64,
                "recovered": True,
            },
            "reboot_autostart": {
                "rebooted_at": (NOW - timedelta(minutes=3)).isoformat().replace("+00:00", "Z"),
                "verified_at": (NOW - timedelta(minutes=2)).isoformat().replace("+00:00", "Z"),
                "boot_id_sha256": "4" * 64,
                "systemd_unit": "my-data-hub-control-plane.service",
                "unit_enabled": True,
                "linger_enabled": True,
                "autostart_services": ["control-plane", "remote-mcp", "oauth-server"],
            },
        },
    }
    receipt.update(changes)
    canonical = json.dumps(receipt, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    signature = base64.urlsafe_b64encode(private_key.sign(canonical)).rstrip(b"=").decode()
    receipt["signature"] = {"algorithm": "Ed25519", "key_id": KEY_ID, "value": signature}
    return json.dumps(receipt, sort_keys=True)


@pytest.fixture
def evidence_material() -> tuple[Ed25519PrivateKey, str, str]:
    private_key = Ed25519PrivateKey.generate()
    public_pem = private_key.public_key().public_bytes(
        serialization.Encoding.PEM,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode()
    return private_key, public_pem, _evidence(private_key)


def test_signed_host_evidence_binds_identity_absence_and_recovery(
    evidence_material: tuple[Ed25519PrivateKey, str, str],
) -> None:
    _, public_pem, raw = evidence_material
    result = validate_deployment_evidence(
        raw,
        public_pem,
        expected_commit=COMMIT,
        expected_source_identity=SOURCE,
        expected_key_id=KEY_ID,
        now=NOW,
    )
    assert result["verified"] is True
    assert result["deployed_commit"] == COMMIT
    assert result["source_identity"] == SOURCE
    assert result["local_database_absent"] is True
    assert result["process_kill_recovered"] is True
    assert result["reboot_autostart_verified"] is True
    assert len(str(result["evidence_sha256"])) == 64


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ({"deployed_commit": "b" * 40}, "commit differs"),
        ({"source_identity": "attacker/repository"}, "source identity differs"),
        ({"host_id_sha256": "raw-hostname"}, "sanitized SHA-256"),
    ],
)
def test_evidence_rejects_wrong_unsigned_identity_before_trust(
    evidence_material: tuple[Ed25519PrivateKey, str, str],
    mutation: dict[str, object],
    message: str,
) -> None:
    _, public_pem, raw = evidence_material
    payload = json.loads(raw)
    payload.update(mutation)
    with pytest.raises(ValueError, match=message):
        validate_deployment_evidence(
            json.dumps(payload),
            public_pem,
            expected_commit=COMMIT,
            expected_source_identity=SOURCE,
            expected_key_id=KEY_ID,
            now=NOW,
        )


def test_evidence_rejects_tamper_staleness_and_failed_host_checks(
    evidence_material: tuple[Ed25519PrivateKey, str, str],
) -> None:
    private_key, public_pem, raw = evidence_material
    tampered = json.loads(raw)
    tampered["checks"]["my_data_hub_public_listener_ports"] = [5432]
    with pytest.raises(ValueError, match="signature verification"):
        validate_deployment_evidence(
            json.dumps(tampered),
            public_pem,
            expected_commit=COMMIT,
            expected_source_identity=SOURCE,
            expected_key_id=KEY_ID,
            now=NOW,
        )

    stale = _evidence(
        private_key,
        issued_at=(NOW - timedelta(days=2)).isoformat().replace("+00:00", "Z"),
        expires_at=(NOW + timedelta(minutes=1)).isoformat().replace("+00:00", "Z"),
    )
    with pytest.raises(ValueError, match="stale"):
        validate_deployment_evidence(
            stale,
            public_pem,
            expected_commit=COMMIT,
            expected_source_identity=SOURCE,
            expected_key_id=KEY_ID,
            now=NOW,
        )

    failed_payload = json.loads(raw)
    failed_payload.pop("signature")
    failed_payload["checks"]["database_process_present"] = True
    failed = _evidence(private_key, checks=failed_payload["checks"])
    with pytest.raises(ValueError, match="forbidden local database"):
        validate_deployment_evidence(
            failed,
            public_pem,
            expected_commit=COMMIT,
            expected_source_identity=SOURCE,
            expected_key_id=KEY_ID,
            now=NOW,
        )


def test_evidence_rejects_missing_reboot_or_process_replacement(
    evidence_material: tuple[Ed25519PrivateKey, str, str],
) -> None:
    private_key, public_pem, raw = evidence_material
    checks = json.loads(raw)["checks"]
    checks["process_kill"]["after_process_sha256"] = checks["process_kill"]["before_process_sha256"]
    bad_process = _evidence(private_key, checks=checks)
    with pytest.raises(ValueError, match="process replacement"):
        validate_deployment_evidence(
            bad_process,
            public_pem,
            expected_commit=COMMIT,
            expected_source_identity=SOURCE,
            expected_key_id=KEY_ID,
            now=NOW,
        )
    checks = json.loads(raw)["checks"]
    checks["reboot_autostart"]["unit_enabled"] = False
    bad_reboot = _evidence(private_key, checks=checks)
    with pytest.raises(ValueError, match="reboot/autostart"):
        validate_deployment_evidence(
            bad_reboot,
            public_pem,
            expected_commit=COMMIT,
            expected_source_identity=SOURCE,
            expected_key_id=KEY_ID,
            now=NOW,
        )


def test_endpoint_contract_dns_tls_and_closed_port_probes(monkeypatch: pytest.MonkeyPatch) -> None:
    assert ENDPOINT.resource_metadata_url.endswith("/.well-known/oauth-protected-resource/mcp")
    with pytest.raises(ValueError, match="port 443"):
        PublicEndpoint.parse("http://mcp.example.test:8765/mcp")

    monkeypatch.setattr(
        "scripts.verify_post_deploy.socket.getaddrinfo",
        lambda *args, **kwargs: [(2, 1, 6, "", ("8.8.8.8", 443))],
    )

    class Context:
        def __enter__(self):
            return self

        def __exit__(self, *args: object) -> None:
            return None

    class Secured(Context):
        def getpeercert(self, *, binary_form: bool = False) -> bytes:
            assert binary_form
            return b"verified-certificate"

        def version(self) -> str:
            return "TLSv1.3"

    class TLSContext:
        def wrap_socket(self, raw: object, *, server_hostname: str) -> Secured:
            assert server_hostname == "mcp.example.test"
            return Secured()

    monkeypatch.setattr("scripts.verify_post_deploy.socket.create_connection", lambda *args, **kwargs: Context())
    monkeypatch.setattr("scripts.verify_post_deploy.ssl.create_default_context", lambda: TLSContext())
    tls = verify_dns_tls(ENDPOINT)
    assert tls["tls_version"] == "TLSv1.3"
    assert len(str(tls["certificate_sha256"])) == 64

    def closed(*args: object, **kwargs: object) -> object:
        raise ConnectionRefusedError

    monkeypatch.setattr("scripts.verify_post_deploy.socket.create_connection", closed)
    assert verify_forbidden_public_ports(ENDPOINT)["public_database_port_open"] is False


@pytest.mark.asyncio
async def test_http_oauth_and_negative_contracts() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/.well-known/oauth-protected-resource/mcp":
            if request.headers.get("host") == "invalid.example":
                return httpx.Response(403, json={"error": "host_not_allowed"})
            if request.headers.get("origin") == "https://invalid.example":
                return httpx.Response(403, json={"error": "origin_not_allowed"})
            return httpx.Response(
                200,
                json={
                    "resource": ENDPOINT.url,
                    "authorization_servers": ["https://identity.example.test"],
                },
            )
        if path == "/.well-known/oauth-authorization-server":
            return httpx.Response(
                200,
                json={
                    "issuer": "https://identity.example.test",
                    "authorization_endpoint": "https://identity.example.test/authorize",
                    "token_endpoint": "https://identity.example.test/token",
                    "jwks_uri": "https://identity.example.test/.well-known/jwks.json",
                },
            )
        if path == "/.well-known/jwks.json":
            return httpx.Response(
                200,
                json={"keys": [{"kty": "RSA", "kid": "key-1", "alg": "RS256", "n": "n", "e": "AQAB"}]},
            )
        if path == "/mcp":
            return httpx.Response(401, headers={"www-authenticate": "Bearer"}, json={"error": "invalid_token"})
        raise AssertionError(path)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await verify_http_negatives(ENDPOINT, client=client)
    assert result == {
        "resource_metadata": True,
        "wrong_host_rejected": True,
        "wrong_origin_rejected": True,
        "missing_auth_rejected": True,
        "wrong_auth_rejected": True,
        "authorization_server": "https://identity.example.test",
        "published_jwks_keys": 1,
    }


class FakeSession:
    def __init__(self) -> None:
        self.master_calls = 0
        self.data_calls = 0

    async def list_tools(self) -> object:
        return SimpleNamespace(tools=[SimpleNamespace(name=name) for name in sorted(READ_ONLY_TOOLS)])

    async def call_tool(self, name: str, arguments: dict[str, object]) -> object:
        if name == "platform.status":
            payload = {
                "deployed_commit": COMMIT,
                "control_plane_ready": True,
                "master_state": "ABSENT",
                "canonical_database_location": "kaggle-master-only",
            }
        elif name == "master.status":
            self.master_calls += 1
            payload = (
                {"master_state": "ABSENT", "instance_id": None}
                if self.master_calls == 1
                else {"master_state": "ACTIVE", "instance_id": "master-1", "master_epoch": 7}
            )
        elif name == "operation.get":
            assert arguments == {"operation_id": "operation-cold-1"}
            payload = {"found": True, "state": "REGISTERING"}
        elif name == "data.query":
            assert arguments["sql"] == (
                "SELECT count(*) AS row_count FROM region_talk.bloggers_ru_v1"
            )
            self.data_calls += 1
            payload = (
                {
                    "operation_id": "operation-cold-1",
                    "master_state": "REQUESTED",
                    "terminal": False,
                }
                if self.data_calls == 1
                else {
                    "columns": ["row_count"],
                    "rows": [{"row_count": 0}],
                    "truncated": False,
                    "master_epoch": 7,
                    "canonical_revision": 0,
                }
            )
        else:
            raise AssertionError(name)
        return SimpleNamespace(structured_content=payload, content=[], is_error=False)


@pytest.mark.asyncio
async def test_remote_acceptance_proves_absent_cold_ensure_and_bounded_read() -> None:
    result = await verify_acceptance_session(
        FakeSession(),
        expected_commit=COMMIT,
        cold_start_timeout_seconds=30,
        poll_interval_seconds=0,
    )
    assert result["initial_master_state"] == "ABSENT"
    assert result["cold_operation_id"] == "operation-cold-1"
    assert result["active_master_epoch"] == 7
    assert result["canonical_revision"] == 0
    assert result["writes_discoverable"] is False


def test_cli_failure_never_logs_token_or_raw_evidence(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    secret = "reader-secret-that-must-not-appear"
    evidence = '{"sensitive":"raw-evidence-marker"}'
    monkeypatch.setenv("MY_DATA_HUB_MCP_CANARY_ENDPOINT", ENDPOINT.url)
    monkeypatch.setenv("MY_DATA_HUB_MCP_CANARY_TOKEN", secret)
    monkeypatch.setenv("MY_DATA_HUB_EXPECTED_DEPLOY_COMMIT", COMMIT)
    monkeypatch.setenv("MY_DATA_HUB_EXPECTED_SOURCE_IDENTITY", SOURCE)
    monkeypatch.setenv("MY_DATA_HUB_DEPLOY_EVIDENCE_JSON", evidence)
    monkeypatch.setenv("MY_DATA_HUB_DEPLOY_EVIDENCE_PUBLIC_KEY_PEM", "invalid")
    monkeypatch.setenv("MY_DATA_HUB_DEPLOY_EVIDENCE_KEY_ID", KEY_ID)
    monkeypatch.setattr(
        "sys.argv",
        ["verify_post_deploy.py", "--output", str(tmp_path / "report.json")],
    )
    assert main() == 1
    captured = capsys.readouterr()
    assert secret not in captured.out + captured.err
    assert evidence not in captured.out + captured.err
    assert "post-deploy verification failed" in captured.err


def test_post_deploy_workflow_requires_signed_evidence_and_exact_checkout() -> None:
    workflow = Path(".github/workflows/post-deploy.yml").read_text(encoding="utf-8")
    assert "ref: ${{ inputs.expected_commit }}" in workflow
    assert "persist-credentials: false" in workflow
    assert "MY_DATA_HUB_DEPLOY_EVIDENCE_JSON" in workflow
    assert "MY_DATA_HUB_DEPLOY_EVIDENCE_PUBLIC_KEY_PEM" in workflow
    assert "verify_post_deploy.py" in workflow
    assert "post-deploy-verification.json" in workflow
    assert "git rev-parse HEAD" in workflow
