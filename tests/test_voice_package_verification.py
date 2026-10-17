import argparse
import hashlib
import importlib.util
import io
import json
import sys
import tarfile
import unittest
from pathlib import Path
from unittest.mock import patch

# This file lives at tests/test_voice_package_verification.py in the repository.
_verifier_path = Path(__file__).resolve().parents[1] / "deploy" / "control-plane" / "verify_voice_package.py"
_spec = importlib.util.spec_from_file_location("verify_voice_package", _verifier_path)
if _spec is None or _spec.loader is None:
    raise ImportError("Cannot load deploy/control-plane/verify_voice_package.py")
verifier = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(verifier)


def digest(body):
    return hashlib.sha256(body).hexdigest()


def archive(files):
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w") as handle:
        for path, body in files.items():
            member = tarfile.TarInfo("src/my_data_hub/" + path)
            member.size = len(body)
            handle.addfile(member, io.BytesIO(body))
    return output.getvalue()


class PackageVerifierTests(unittest.TestCase):
    def test_manifest_hash_is_order_independent(self):
        self.assertEqual(
            verifier.manifest_hash({"a.py": "a", "b.py": "b"}),
            verifier.manifest_hash({"b.py": "b", "a.py": "a"}),
        )

    def test_reports_missing_extra_changed_and_nested_paths(self):
        expected = {"__init__.py": "same", "nested/a.py": "old", "missing.py": "old"}
        actual = {"__init__.py": "same", "nested/a.py": "new", "extra.py": "new"}
        self.assertEqual(
            verifier.compare(expected, actual),
            {"missing": ["missing.py"], "extra": ["extra.py"], "changed": ["nested/a.py"]},
        )

    def test_effective_installed_package_ignores_stale_app_src(self):
        sha, image_id = "a" * 40, "sha256:" + "b" * 64
        expected = {"__init__.py": b"", "voice/worker.py": b"new source\n"}
        installed = {path: digest(body) for path, body in expected.items()}
        package = {"package_root": "/usr/local/lib/python3.12/site-packages/my_data_hub", "files": installed}
        responses = [b"/repo\n", sha.encode(), archive(expected), image_id.encode(), json.dumps(package).encode()]
        args = argparse.Namespace(image="voice:release", container=None, expected_ref=sha)
        with patch.object(verifier, "capture", side_effect=responses) as capture:
            result = verifier.verify(args)
        self.assertEqual(result["status"], "matched")
        self.assertEqual(result["package_root"], package["package_root"])
        command = capture.call_args_list[-1].args[0]
        self.assertIn(image_id, command)
        self.assertNotIn("voice:release", command)
        self.assertIn("--read-only", command)
        self.assertIn("--rm", command)
        self.assertEqual(command[-1], verifier.CHILD_SCRIPT)
        self.assertNotIn("/app/src", " ".join(command))
        self.assertEqual(result["installed_file_count"], 2)

    def test_container_uses_only_exec_and_detects_drift(self):
        sha, container = "a" * 40, "c" * 64
        expected = {"__init__.py": b"", "worker.py": b"expected"}
        package = {
            "package_root": "/venv/lib/python3.12/site-packages/my_data_hub",
            "files": {"__init__.py": digest(b""), "worker.py": digest(b"different")},
        }
        responses = [b"/repo\n", sha.encode(), archive(expected), json.dumps(package).encode()]
        args = argparse.Namespace(image=None, container=container, expected_ref=sha)
        with patch.object(verifier, "capture", side_effect=responses) as capture:
            result = verifier.verify(args)
        self.assertEqual(result["status"], "mismatch")
        self.assertEqual(result["mismatches"]["changed"], ["worker.py"])
        docker_calls = [call.args[0] for call in capture.call_args_list if call.args[0][0] == "docker"]
        self.assertEqual(docker_calls, [["docker", "exec", container, "python", "-c", verifier.CHILD_SCRIPT]])

    def test_archive_uses_only_python_sources(self):
        result = verifier.source_manifest(
            archive({"__init__.py": b"", "nested/x.py": b"source", "config.json": b"must not fingerprint"})
        )
        self.assertEqual(result, {"__init__.py": digest(b""), "nested/x.py": digest(b"source")})

    def test_capture_rejects_excess_output_before_child_exits(self):
        command = [sys.executable, "-c", "import os, time\nos.write(1, b'x' * 4096)\ntime.sleep(10)"]
        with self.assertRaisesRegex(verifier.VerificationError, "^command_output_limit$"):
            verifier.capture(command, limit=1024, timeout=2)

    def test_capture_terminates_silent_child_at_deadline(self):
        command = [sys.executable, "-c", "import time\ntime.sleep(10)"]
        with self.assertRaisesRegex(verifier.VerificationError, "^command_timeout$"):
            verifier.capture(command, timeout=0.05)

    def test_capture_suppresses_stderr(self):
        command = [sys.executable, "-c", "import sys\nsys.stderr.write('suppressed')\nprint('safe')"]
        self.assertEqual(verifier.capture(command), b"safe\n")


if __name__ == "__main__":
    unittest.main()
