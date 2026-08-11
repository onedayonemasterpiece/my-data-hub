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
from jsonschema import Draft202012Validator, FormatChecker

from scripts.verify_post_deploy import (
    AUTHORIZATION_SERVER,
    PublicEndpoint,
    main,
    validate_deployment_evidence,
    verify_all,
    verify_dns_tls,
    verify_forbidden_public_ports,
    verify_http_negatives,
)
from scripts.verify_remote_mcp import READ_ONLY_TOOLS, verify_acceptance_session

COMMIT = "a" * 40
SOURCE = "onedayonemasterpiece/my-data-hub"
KEY_ID = "devstand-evidence-2026-08"
NOW = datetime(2026, 8, 11, 4, 0, tzinfo=UTC)
ENDPOINT = PublicEndpoint.parse("https://mcp-datahub.kenigevents.ru/mcp")


def _evidence(private_key: Ed25519PrivateKey, **changes: object) -> str:
    receipt: dict[str, object] = {
        "schema_version": "my-data-hub-deployment-evidence.v1",
        "source_identity": SOURCE,
        "deployed_commit": COMMIT,
        "source_tree_sha256": "8" * 64,
        "installed_release_tree_sha256": "8" * 64,
        "host_id_sha256": "1" * 64,
        "issued_at": (NOW - timedelta(minutes=1)).isoformat().replace("+00:00", "Z"),
        "expires_at": (NOW + timedelta(hours=1)).isoformat().replace("+00:00", "Z"),
        "checks": {
            "services": {
                "control-plane": "running",
                "oauth-server": "running",
                "remote-mcp": "running",
            },
            "service_image_ids": {
                "control-plane": "sha256:" + "9" * 64,
                "oauth-server": "sha256:" + "9" * 64,
                "remote-mcp": "sha256:" + "9" * 64,
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
                "autostart_services": ["control-plane", "oauth-server", "remote-mcp"],
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


def test_evidence_rejects_source_tree_or_immutable_image_drift(
    evidence_material: tuple[Ed25519PrivateKey, str, str],
) -> None:
    private_key, public_pem, raw = evidence_material
    mismatched_tree = _evidence(private_key, installed_release_tree_sha256="7" * 64)
    with pytest.raises(ValueError, match="installed release differs"):
        validate_deployment_evidence(
            mismatched_tree,
            public_pem,
            expected_commit=COMMIT,
            expected_source_identity=SOURCE,
            expected_key_id=KEY_ID,
            now=NOW,
        )
    checks = json.loads(raw)["checks"]
    checks["service_image_ids"]["remote-mcp"] = "sha256:" + "6" * 64
    mismatched_image = _evidence(private_key, checks=checks)
    with pytest.raises(ValueError, match="immutable service image"):
        validate_deployment_evidence(
            mismatched_image,
            public_pem,
            expected_commit=COMMIT,
            expected_source_identity=SOURCE,
            expected_key_id=KEY_ID,
            now=NOW,
        )


def test_endpoint_contract_dns_tls_and_closed_port_probes(monkeypatch: pytest.MonkeyPatch) -> None:
    assert ENDPOINT.resource_metadata_url.endswith("/.well-known/oauth-protected-resource/mcp")
    with pytest.raises(ValueError, match="owner-approved"):
        PublicEndpoint.parse("http://mcp.example.test:8765/mcp")
    with pytest.raises(ValueError, match="owner-approved"):
        PublicEndpoint.parse("https://attacker.example/mcp")

    monkeypatch.setattr(
        "scripts.verify_post_deploy.socket.getaddrinfo",
        lambda *args, **kwargs: [
            (2, 1, 6, "", ("8.8.8.8", 443)),
            (10, 1, 6, "", ("2606:4700:4700::1111", 443, 0, 0)),
        ],
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
            assert server_hostname == "mcp-datahub.kenigevents.ru"
            return Secured()

    monkeypatch.setattr("scripts.verify_post_deploy.socket.create_connection", lambda *args, **kwargs: Context())
    monkeypatch.setattr("scripts.verify_post_deploy.ssl.create_default_context", lambda: TLSContext())
    tls = verify_dns_tls(ENDPOINT)
    assert tls["tls_version"] == "TLSv1.3"
    assert len(str(tls["certificate_sha256"])) == 64

    def closed(*args: object, **kwargs: object) -> object:
        raise ConnectionRefusedError

    calls: list[tuple[str, int]] = []

    def closed_and_record(target: tuple[str, int], **kwargs: object) -> object:
        calls.append(target)
        raise ConnectionRefusedError

    monkeypatch.setattr("scripts.verify_post_deploy.socket.create_connection", closed_and_record)
    port_result = verify_forbidden_public_ports(ENDPOINT)
    assert port_result["public_database_port_open"] is False
    assert port_result["probe_count"] == 8
    assert set(calls) == {
        (address, port)
        for address in ("8.8.8.8", "2606:4700:4700::1111")
        for port in (5432, 8080, 8765, 8780)
    }


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
                    "authorization_servers": [AUTHORIZATION_SERVER],
                },
            )
        if path == "/.well-known/oauth-authorization-server":
            return httpx.Response(
                200,
                json={
                    "issuer": AUTHORIZATION_SERVER,
                    "authorization_endpoint": f"{AUTHORIZATION_SERVER}/authorize",
                    "token_endpoint": f"{AUTHORIZATION_SERVER}/token",
                    "jwks_uri": f"{AUTHORIZATION_SERVER}/.well-known/jwks.json",
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
        "authorization_server": AUTHORIZATION_SERVER,
        "published_jwks_keys": 1,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("wrong_host_status", [400, 404, 421])
async def test_http_accepts_fail_closed_edge_host_denials(wrong_host_status: int) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/.well-known/oauth-protected-resource/mcp":
            if request.headers.get("host") == "invalid.example":
                return httpx.Response(wrong_host_status)
            if request.headers.get("origin") == "https://invalid.example":
                return httpx.Response(403)
            return httpx.Response(
                200,
                json={
                    "resource": ENDPOINT.url,
                    "authorization_servers": [AUTHORIZATION_SERVER],
                },
            )
        if path == "/.well-known/oauth-authorization-server":
            return httpx.Response(
                200,
                json={
                    "issuer": AUTHORIZATION_SERVER,
                    "authorization_endpoint": f"{AUTHORIZATION_SERVER}/authorize",
                    "token_endpoint": f"{AUTHORIZATION_SERVER}/token",
                    "jwks_uri": f"{AUTHORIZATION_SERVER}/.well-known/jwks.json",
                },
            )
        if path == "/.well-known/jwks.json":
            return httpx.Response(
                200,
                json={"keys": [{"kty": "RSA", "kid": "key-1", "alg": "RS256"}]},
            )
        if path == "/mcp":
            return httpx.Response(401, headers={"www-authenticate": "Bearer"})
        raise AssertionError(path)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await verify_http_negatives(ENDPOINT, client=client)
    assert result["wrong_host_rejected"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [200, 302, 500])
async def test_http_rejects_non_policy_edge_host_outcomes(status: int) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/.well-known/oauth-protected-resource/mcp":
            if request.headers.get("host") == "invalid.example":
                return httpx.Response(status)
            if request.headers.get("origin") == "https://invalid.example":
                return httpx.Response(403)
            return httpx.Response(
                200,
                json={
                    "resource": ENDPOINT.url,
                    "authorization_servers": [AUTHORIZATION_SERVER],
                },
            )
        if path == "/.well-known/oauth-authorization-server":
            return httpx.Response(
                200,
                json={
                    "issuer": AUTHORIZATION_SERVER,
                    "authorization_endpoint": f"{AUTHORIZATION_SERVER}/authorize",
                    "token_endpoint": f"{AUTHORIZATION_SERVER}/token",
                    "jwks_uri": f"{AUTHORIZATION_SERVER}/.well-known/jwks.json",
                },
            )
        if path == "/.well-known/jwks.json":
            return httpx.Response(
                200,
                json={"keys": [{"kty": "RSA", "kid": "key-1", "alg": "RS256"}]},
            )
        if path == "/mcp":
            return httpx.Response(401, headers={"www-authenticate": "Bearer"})
        raise AssertionError(path)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(RuntimeError, match="negative checks failed open"):
            await verify_http_negatives(ENDPOINT, client=client)


@pytest.mark.asyncio
async def test_http_rejects_unapproved_advertised_authorization_server() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/.well-known/oauth-protected-resource/mcp":
            return httpx.Response(
                200,
                json={
                    "resource": ENDPOINT.url,
                    "authorization_servers": ["https://attacker.example"],
                },
            )
        return httpx.Response(401, headers={"www-authenticate": "Bearer"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(RuntimeError, match="endpoint contract"):
            await verify_http_negatives(ENDPOINT, client=client)


@pytest.mark.asyncio
async def test_real_verify_all_shape_matches_committed_report_schema(
    evidence_material: tuple[Ed25519PrivateKey, str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, public_pem, raw = evidence_material
    evidence = validate_deployment_evidence(
        raw,
        public_pem,
        expected_commit=COMMIT,
        expected_source_identity=SOURCE,
        expected_key_id=KEY_ID,
        now=NOW,
    )
    monkeypatch.setattr(
        "scripts.verify_post_deploy.resolve_global_addresses",
        lambda endpoint: ("8.8.8.8",),
    )
    monkeypatch.setattr(
        "scripts.verify_post_deploy.verify_dns_tls",
        lambda endpoint, *, addresses: {
            "dns_global_address_count": 1,
            "tls_version": "TLSv1.3",
            "certificate_sha256": "5" * 64,
        },
    )
    monkeypatch.setattr(
        "scripts.verify_post_deploy.verify_forbidden_public_ports",
        lambda endpoint, *, addresses: {
            "probed_global_address_count": 1,
            "probed_closed_ports": [5432, 8080, 8765, 8780],
            "probe_count": 4,
            "public_database_port_open": False,
        },
    )

    async def negatives(endpoint: PublicEndpoint) -> dict[str, object]:
        return {
            "resource_metadata": True,
            "wrong_host_rejected": True,
            "wrong_origin_rejected": True,
            "missing_auth_rejected": True,
            "wrong_auth_rejected": True,
            "authorization_server": AUTHORIZATION_SERVER,
            "published_jwks_keys": 1,
        }

    async def remote(*args: object, **kwargs: object) -> dict[str, object]:
        return {
            "tools": sorted(READ_ONLY_TOOLS),
            "deployed_commit": COMMIT,
            "initial_master_state": "ABSENT",
            "cold_operation_id": "operation-1",
            "active_master_epoch": 1,
            "canonical_revision": 0,
            "data_read_rows": 1,
            "writes_discoverable": False,
        }

    monkeypatch.setattr("scripts.verify_post_deploy.verify_http_negatives", negatives)
    monkeypatch.setattr("scripts.verify_post_deploy.verify_acceptance", remote)
    report = await verify_all(
        endpoint=ENDPOINT,
        token="not-emitted",
        expected_commit=COMMIT,
        expected_source_identity=SOURCE,
        evidence=evidence,
        cold_start_timeout_seconds=30,
    )
    schema = json.loads(
        Path("schemas/post-deploy-verification.v1.schema.json").read_text(encoding="utf-8")
    )
    errors = list(
        Draft202012Validator(schema, format_checker=FormatChecker()).iter_errors(report)
    )
    assert errors == []
    assert "not-emitted" not in json.dumps(report)


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


def test_post_deploy_workflow_uses_trusted_verifier_and_scopes_secrets() -> None:
    workflow = Path(".github/workflows/post-deploy.yml").read_text(encoding="utf-8")
    assert "ref: ${{ github.event.repository.default_branch }}" in workflow
    assert "persist-credentials: false" in workflow
    assert "MY_DATA_HUB_APPROVED_DEPLOY_COMMIT" in workflow
    assert 'merge-base --is-ancestor "$MY_DATA_HUB_EXPECTED_DEPLOY_COMMIT" HEAD' in workflow
    assert "trusted/scripts/verify_post_deploy.py" in workflow
    assert "MY_DATA_HUB_MCP_CANARY_ENDPOINT: https://mcp-datahub.kenigevents.ru/mcp" in workflow
    assert "inputs.endpoint" not in workflow
    assert "MY_DATA_HUB_DEPLOY_EVIDENCE_JSON" in workflow
    assert "MY_DATA_HUB_DEPLOY_EVIDENCE_PUBLIC_KEY_PEM" in workflow
    assert "post-deploy-verification.json" in workflow
    job_environment = workflow.partition("    steps:")[0]
    assert "MY_DATA_HUB_MCP_CANARY_TOKEN" not in job_environment


def test_committed_oauth_connection_metadata_matches_owner_runtime_paths() -> None:
    metadata = json.loads(Path("examples/oauth/provider-metadata.v1.json").read_text(encoding="utf-8"))
    assert metadata["issuer"] == AUTHORIZATION_SERVER
    assert metadata["authorization_endpoint"] == f"{AUTHORIZATION_SERVER}/authorize"
    assert metadata["token_endpoint"] == f"{AUTHORIZATION_SERVER}/token"
    assert metadata["jwks_uri"] == f"{AUTHORIZATION_SERVER}/.well-known/jwks.json"
    assert metadata["token_endpoint_auth_methods_supported"] == ["none"]
    assert metadata["client_id_metadata_document_supported"] is False
    schema = json.loads(Path("schemas/oauth/provider-metadata.v1.schema.json").read_text(encoding="utf-8"))
    for field in ("issuer", "authorization_endpoint", "token_endpoint", "jwks_uri"):
        assert schema["properties"][field]["const"] == metadata[field]
