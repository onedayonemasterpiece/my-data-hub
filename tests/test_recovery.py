from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import jsonschema

ROOT = Path(__file__).resolve().parents[1]
RECOVERY = ROOT / "scripts" / "recovery"
COMMIT = "0b6b7311081bdfecdd4f3004e5d6842a42f64253"


def _run(*arguments: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, *arguments],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


def _manifest(tmp_path: Path) -> tuple[Path, Path, dict[str, object]]:
    artifact = tmp_path / "fixture.dump.age"
    artifact.write_bytes(b"age-encrypted-fixture\x00\x01")
    manifest = tmp_path / "fixture.manifest.json"
    result = _run(
        str(RECOVERY / "create_manifest.py"),
        "--artifact",
        str(artifact),
        "--output",
        str(manifest),
        "--started-at",
        "2026-08-09T20:00:00Z",
        "--completed-at",
        "2026-08-09T20:01:00Z",
        "--source-instance",
        "production-primary",
        "--source-environment",
        "production",
        "--repository-commit",
        COMMIT,
        "--pg-dump-version",
        "pg_dump (PostgreSQL) 18.0",
        "--age-recipient",
        "age1examplefixture",
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return artifact, manifest, json.loads(manifest.read_text(encoding="utf-8"))


def _adapter(path: Path, *, corrupt_readback: bool = False) -> Path:
    adapter = path / ("corrupt-adapter" if corrupt_readback else "adapter")
    corrupt = "printf corruption > \"$MDH_RECOVERY_DESTINATION_PATH\"" if corrupt_readback else (
        "cp \"$REMOTE_STORE\" \"$MDH_RECOVERY_DESTINATION_PATH\""
    )
    adapter.write_text(
        "#!/bin/sh\nset -eu\n"
        "if [ \"$MDH_RECOVERY_ACTION\" = upload ]; then\n"
        "  cp \"$MDH_RECOVERY_SOURCE_PATH\" \"$REMOTE_STORE\"\n"
        "else\n"
        f"  {corrupt}\n"
        "fi\n",
        encoding="utf-8",
    )
    adapter.chmod(0o700)
    return adapter


def _offhost_evidence(tmp_path: Path, artifact: Path, manifest: Path) -> Path:
    adapter = _adapter(tmp_path)
    evidence = tmp_path / "offhost.json"
    environment = os.environ.copy()
    environment.update(
        {
            "MY_DATA_HUB_OFFHOST_UPLOAD_CONFIRM": "UPLOAD_ENCRYPTED_BACKUP",
            "MY_DATA_HUB_OFFHOST_PRIVATE_CONFIRM": "PRIVATE_ENCRYPTED_STORAGE",
            "MY_DATA_HUB_OFFHOST_IS_REMOTE": "OFF_HOST_PRIVATE_STORAGE",
            "MY_DATA_HUB_OFFHOST_UPLOAD_ADAPTER": str(adapter),
            "MY_DATA_HUB_OFFHOST_READBACK_ADAPTER": str(adapter),
            "MY_DATA_HUB_OFFHOST_ADAPTER_ENV_ALLOWLIST": "REMOTE_STORE",
            "REMOTE_STORE": str(tmp_path / "remote-object"),
        }
    )
    result = _run(
        str(RECOVERY / "offhost_roundtrip.py"),
        "--artifact",
        str(artifact),
        "--manifest",
        str(manifest),
        "--provider",
        "fixture-private-store",
        "--object-locator",
        "s3://private-bucket/example.dump.age",
        "--evidence",
        str(evidence),
        env=environment,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return evidence


def test_recovery_receipt_example_validates_and_self_hashes() -> None:
    schema = json.loads((ROOT / "schemas/recovery-receipt.v1.schema.json").read_text())
    example = json.loads((ROOT / "examples/recovery-receipt.v1.json").read_text())
    jsonschema.Draft202012Validator(schema, format_checker=jsonschema.FormatChecker()).validate(example)
    unsigned = {key: value for key, value in example.items() if key != "receipt_sha256"}
    encoded = json.dumps(unsigned, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    assert example["receipt_sha256"] == hashlib.sha256(encoded).hexdigest()
    serialized = json.dumps(example)
    assert "postgresql://" not in serialized
    assert "identity" not in serialized.lower()


def test_manifest_is_encrypted_only_and_detects_artifact_tampering(tmp_path: Path) -> None:
    artifact, manifest, payload = _manifest(tmp_path)
    assert payload["artifact"]["encryption"] == "age"  # type: ignore[index]
    assert "database_url" not in json.dumps(payload).lower()
    artifact.write_bytes(b"tampered")
    result = _run(
        str(RECOVERY / "verify_artifact.py"),
        "--artifact",
        str(artifact),
        "--manifest",
        str(manifest),
    )
    assert result.returncode != 0
    assert "does not match manifest" in result.stderr


def test_offhost_roundtrip_requires_exact_provider_readback(tmp_path: Path) -> None:
    artifact, manifest, payload = _manifest(tmp_path)
    evidence_path = _offhost_evidence(tmp_path, artifact, manifest)
    evidence = json.loads(evidence_path.read_text())
    expected = payload["artifact"]["sha256"]  # type: ignore[index]
    assert evidence["uploaded_sha256"] == expected
    assert evidence["readback_sha256"] == expected
    assert evidence["exact_match"] is True

    corrupt = _adapter(tmp_path, corrupt_readback=True)
    failed_evidence = tmp_path / "must-not-exist.json"
    environment = os.environ.copy()
    environment.update(
        {
            "MY_DATA_HUB_OFFHOST_UPLOAD_CONFIRM": "UPLOAD_ENCRYPTED_BACKUP",
            "MY_DATA_HUB_OFFHOST_PRIVATE_CONFIRM": "PRIVATE_ENCRYPTED_STORAGE",
            "MY_DATA_HUB_OFFHOST_IS_REMOTE": "OFF_HOST_PRIVATE_STORAGE",
            "MY_DATA_HUB_OFFHOST_UPLOAD_ADAPTER": str(corrupt),
            "MY_DATA_HUB_OFFHOST_READBACK_ADAPTER": str(corrupt),
            "MY_DATA_HUB_OFFHOST_ADAPTER_ENV_ALLOWLIST": "REMOTE_STORE",
            "REMOTE_STORE": str(tmp_path / "remote-object-corrupt"),
        }
    )
    result = _run(
        str(RECOVERY / "offhost_roundtrip.py"),
        "--artifact",
        str(artifact),
        "--manifest",
        str(manifest),
        "--provider",
        "fixture-private-store",
        "--object-locator",
        "s3://private-bucket/example.dump.age",
        "--evidence",
        str(failed_evidence),
        env=environment,
    )
    assert result.returncode != 0
    assert "do not exactly match" in result.stderr
    assert not failed_evidence.exists()


def test_restore_orchestrates_fresh_isolated_target_and_writes_receipt(tmp_path: Path) -> None:
    artifact, manifest, _ = _manifest(tmp_path)
    evidence = _offhost_evidence(tmp_path, artifact, manifest)
    bin_directory = tmp_path / "bin"
    bin_directory.mkdir()
    marker = tmp_path / "pg-restore-ran"
    (bin_directory / "psql").write_text("#!/bin/sh\nprintf 'recovery_db|0\\n'\n")
    (bin_directory / "age").write_text(
        "#!/bin/sh\nset -eu\nwhile [ $# -gt 0 ]; do\n"
        "  if [ \"$1\" = --output ]; then out=$2; shift 2; else shift; fi\n"
        "done\nprintf 'custom-dump' > \"$out\"\n"
    )
    (bin_directory / "pg_restore").write_text(f"#!/bin/sh\nprintf ran > '{marker}'\n")
    python_wrapper = bin_directory / "recovery-python"
    python_wrapper.write_text(
        "#!/bin/sh\nif [ \"${1:-}\" = -m ]; then\n"
        "  if [ \"${2:-}\" = my_data_hub.recovery_verify ]; then\n"
        "    printf '{\"ok\":true,\"evidence\":{\"schema_revision\":10,"
        "\"canonical_revision\":0,\"postgres_major\":18,"
        "\"extension_versions\":{\"vector\":\"0.8.1\"}}}\\n'\n"
        "  fi\n"
        "  exit 0\nfi\nexec "
        f"'{sys.executable}' \"$@\"\n"
    )
    for executable in bin_directory.iterdir():
        executable.chmod(0o700)

    identity = tmp_path / "age.identity"
    identity.write_text("AGE-SECRET-KEY-fixture\n")
    identity.chmod(0o600)
    receipt = tmp_path / "receipt.json"
    environment = os.environ.copy()
    environment.update(
        {
            "PATH": f"{bin_directory}:{environment['PATH']}",
            "MY_DATA_HUB_RECOVERY_PYTHON": str(python_wrapper),
            "MY_DATA_HUB_RESTORE_CONFIRM": "RESTORE_MY_DATA_HUB",
            "MY_DATA_HUB_RESTORE_ISOLATED_CONFIRM": "ISOLATED_FRESH_TARGET",
            "MY_DATA_HUB_RESTORE_DATABASE_URL": "postgresql://fixture.invalid/recovery_db",
            "MY_DATA_HUB_RESTORE_TARGET_ID": "isolated-recovery-1",
            "MY_DATA_HUB_RESTORE_EXPECTED_DATABASE": "recovery_db",
            "MY_DATA_HUB_RESTORE_AGE_IDENTITY_FILE": str(identity),
            "MY_DATA_HUB_RECOVERY_RECEIPT": str(receipt),
        }
    )
    result = subprocess.run(
        [str(ROOT / "scripts/restore_postgres.sh"), str(artifact), str(manifest), str(evidence)],
        cwd=ROOT,
        env=environment,
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert marker.is_file()
    value = json.loads(receipt.read_text())
    assert value["status"] == "succeeded"
    assert value["restore"]["relations_before"] == 0
    assert value["restore"]["automatic_promotion"] is False
    assert value["restore"]["restored_state_verify"] == "passed"
    schema = json.loads((ROOT / "schemas/recovery-receipt.v1.schema.json").read_text())
    jsonschema.Draft202012Validator(schema).validate(value)


def test_restore_rejects_nonfresh_target_before_decrypt_or_restore(tmp_path: Path) -> None:
    source = (ROOT / "scripts/restore_postgres.sh").read_text(encoding="utf-8")
    freshness = source.index('if [[ "$relations_before" != "0" ]]')
    decrypt = source.index("age --decrypt")
    restore = source.index("pg_restore --exit-on-error")
    assert freshness < decrypt < restore
    assert "--clean" not in source
    assert "ISOLATED_FRESH_TARGET" in source
    assert "-m my_data_hub db verify" in source
    assert "automatic promotion is forbidden" in source


def test_backup_streams_pg_dump_directly_into_age(tmp_path: Path) -> None:
    source = (ROOT / "scripts/backup_postgres.sh").read_text(encoding="utf-8")
    assert "pg_dump --format=custom" in source
    assert "| age --encrypt" in source
    assert ".dump.age" in source
    assert 'dump="$base.dump"' not in source
    assert "plaintext backups are forbidden" in source

    bin_directory = tmp_path / "bin"
    bin_directory.mkdir()
    pg_dump = bin_directory / "pg_dump"
    pg_dump.write_text(
        "#!/bin/sh\nif [ \"${1:-}\" = --version ]; then\n"
        "  printf 'pg_dump (PostgreSQL) 18.0\\n'\n"
        "else\n"
        "  printf 'custom-format-dump'\n"
        "fi\n"
    )
    age = bin_directory / "age"
    age.write_text(
        "#!/bin/sh\nset -eu\nwhile [ $# -gt 0 ]; do\n"
        "  if [ \"$1\" = --output ]; then out=$2; shift 2; else shift; fi\n"
        "done\ncat > \"$out\"\n"
    )
    pg_dump.chmod(0o700)
    age.chmod(0o700)
    backup_root = tmp_path / "backups"
    environment = os.environ.copy()
    environment.update(
        {
            "PATH": f"{bin_directory}:{environment['PATH']}",
            "MY_DATA_HUB_BACKUP_DATABASE_URL": "postgresql://fixture.invalid/hub",
            "MY_DATA_HUB_BACKUP_AGE_RECIPIENT": "age1fixture",
            "MY_DATA_HUB_BACKUP_SOURCE_INSTANCE": "production-primary",
            "MY_DATA_HUB_BACKUP_SOURCE_ENVIRONMENT": "production",
            "MY_DATA_HUB_BACKUP_ROOT": str(backup_root),
            "MY_DATA_HUB_RECOVERY_PYTHON": sys.executable,
        }
    )
    result = subprocess.run(
        [str(ROOT / "scripts/backup_postgres.sh")],
        cwd=ROOT,
        env=environment,
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    encrypted = list(backup_root.glob("*.dump.age"))
    manifests = list(backup_root.glob("*.manifest.json"))
    assert len(encrypted) == len(manifests) == 1
    assert encrypted[0].read_bytes() == b"custom-format-dump"
    assert not list(backup_root.glob("*.dump"))
    payload = json.loads(manifests[0].read_text())
    assert payload["artifact"]["file_name"] == encrypted[0].name
    assert payload["artifact"]["sha256"] == hashlib.sha256(encrypted[0].read_bytes()).hexdigest()
