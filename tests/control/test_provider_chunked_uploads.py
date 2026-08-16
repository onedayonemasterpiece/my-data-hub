from __future__ import annotations

import hashlib
import json
from base64 import b64encode
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from threading import Event
from types import SimpleNamespace
from uuid import uuid4

import pytest

from my_data_hub.control_plane.provider_uploads import (
    MAX_ACTIVE_DECLARED_BYTES,
    MAX_ACTIVE_UPLOADS,
    MAX_PRINCIPAL_ACTIVE_UPLOADS,
    MAX_PRINCIPAL_DECLARED_BYTES,
    MAX_UPLOAD_CHUNK_BYTES,
    MAX_UPLOAD_FILE_BYTES,
    MAX_UPLOAD_FILES,
    MAX_UPLOAD_TOTAL_BYTES,
    MIN_UPLOAD_DISK_RESERVE_BYTES,
    ProviderChunkedUploadStore,
    ProviderUploadConflict,
    ProviderUploadError,
    ProviderUploadExpired,
)
from my_data_hub.mcp.oauth import AccessIdentity


class Clock:
    def __init__(self, value: int = 2_000_000_000) -> None:
        self.value = value

    def __call__(self) -> float:
        return float(self.value)


def ample_disk(_path: Path) -> SimpleNamespace:
    return SimpleNamespace(free=10 * 1024 * 1024 * 1024)


def identity(*, subject: str = "owner", client_id: str = "opencode") -> AccessIdentity:
    return AccessIdentity(
        subject=subject,
        client_id=client_id,
        scopes=frozenset({"provider:write"}),
        audience="mcp",
        token_id="not-persisted",
        expires_at=2_100_000_000,
        issuer="https://issuer.example",
        issued_at=1_900_000_000,
        resource="https://mcp.example/mcp",
    )


def start_arguments(files: dict[str, bytes]) -> dict[str, object]:
    return {
        "resource_ref": "owner/photo-batch",
        "control_class": "mcp_managed",
        "private": True,
        "payload": {
            "kind": "dataset",
            "upload_id": str(uuid4()),
            "task_id": str(uuid4()),
            "effect_id": str(uuid4()),
            "idempotency_key": f"upload-{uuid4()}",
            "title": "Private photo batch",
            "disposable": False,
            "files": [
                {
                    "path": path,
                    "byte_size": len(content),
                    "sha256": hashlib.sha256(content).hexdigest(),
                }
                for path, content in sorted(files.items())
            ],
            "ttl_seconds": 3600,
        },
    }


def reference(arguments: dict[str, object]) -> dict[str, object]:
    payload = arguments["payload"]
    assert isinstance(payload, dict)
    return {
        "resource_ref": arguments["resource_ref"],
        "control_class": arguments["control_class"],
        "private": arguments["private"],
        "payload": {"upload_id": payload["upload_id"], "task_id": payload["task_id"]},
    }


def put(
    store: ProviderChunkedUploadStore,
    arguments: dict[str, object],
    path: str,
    offset: int,
    content: bytes,
    *,
    principal: AccessIdentity | None = None,
) -> dict[str, object]:
    payload = arguments["payload"]
    assert isinstance(payload, dict)
    request = {
        "resource_ref": arguments["resource_ref"],
        "control_class": arguments["control_class"],
        "private": arguments["private"],
        "payload": {
            "upload_id": payload["upload_id"],
            "task_id": payload["task_id"],
            "path": path,
            "offset": offset,
            "encoding": "base64",
            "content_base64": b64encode(content).decode("ascii"),
            "byte_size": len(content),
            "sha256": hashlib.sha256(content).hexdigest(),
        },
    }
    return store.put_chunk(request, principal or identity())


def upload_all(
    store: ProviderChunkedUploadStore,
    arguments: dict[str, object],
    files: dict[str, bytes],
) -> None:
    for path, content in sorted(files.items()):
        for offset in range(0, len(content), MAX_UPLOAD_CHUNK_BYTES):
            put(store, arguments, path, offset, content[offset : offset + MAX_UPLOAD_CHUNK_BYTES])


def test_ten_file_upload_is_bounded_private_restart_safe_and_response_loss_safe(
    tmp_path: Path,
) -> None:
    clock = Clock()
    root = tmp_path / "provider-uploads"
    files = {
        f"photos/{index:02d}.jpg": bytes([index + 1]) * 250_000
        for index in range(10)
    }
    arguments = start_arguments(files)
    store = ProviderChunkedUploadStore(root, clock=clock)

    started = store.start(arguments, identity())
    assert started["state"] == "OPEN"
    assert started["file_count"] == 10
    assert root.stat().st_mode & 0o077 == 0
    upload_all(store, arguments, files)

    # A fresh object over the same staging root proves restart-safe metadata.
    restarted = ProviderChunkedUploadStore(root, clock=clock)
    ready = restarted.status(reference(arguments), identity())
    assert ready["state"] == "READY"
    assert all(item["complete"] for item in ready["files"])
    # A process may die after persisting FINALIZING but before it can persist
    # the adapter receipt.  The identical intent is safe to reconcile again.
    state_path = next(restarted.uploads.glob("*/state.json"))
    state_payload = json.loads(state_path.read_text())
    state_payload["state"] = "FINALIZING"
    state_path.write_text(json.dumps(state_payload, sort_keys=True, separators=(",", ":")))
    state_path.chmod(0o600)
    restarted = ProviderChunkedUploadStore(root, clock=clock)
    observed: dict[str, bytes] = {}

    def finalize(_state, assembled: Path, _principal):  # type: ignore[no-untyped-def]
        for file in assembled.rglob("*"):
            assert file.stat().st_mode & 0o077 == 0
            if file.is_file():
                observed[file.relative_to(assembled).as_posix()] = file.read_bytes()
        return {"provider_ref": "owner/photo-batch", "provider_version": 1}

    first = restarted.finalize(reference(arguments), identity(), finalize)
    assert first["state"] == "FINALIZED"
    assert observed == files
    assert not any(restarted.uploads.iterdir())
    assert all(item["complete"] for item in first["files"])

    # Losing the first response never repeats the provider callback.
    replay = restarted.finalize(
        reference(arguments),
        identity(),
        lambda *_args: pytest.fail("terminal receipt must satisfy finalize replay"),
    )
    assert replay == first
    receipt = next(restarted.receipts.glob("*.json"))
    assert receipt.stat().st_mode & 0o077 == 0
    receipt_payload = json.loads(receipt.read_text())
    assert all("chunks" not in item for item in receipt_payload["files"])
    assert b64encode(files["photos/00.jpg"]) not in receipt.read_bytes()


def test_chunk_replay_restart_conflict_binding_and_abort_cleanup(tmp_path: Path) -> None:
    files = {"data.bin": b"private-content" * 2000}
    arguments = start_arguments(files)
    store = ProviderChunkedUploadStore(tmp_path / "uploads")
    started = store.start(arguments, identity())
    assert store.start(arguments, identity()) == started
    content = files["data.bin"][:MAX_UPLOAD_CHUNK_BYTES]
    accepted = put(store, arguments, "data.bin", 0, content)
    assert accepted["replayed"] is False

    restarted = ProviderChunkedUploadStore(tmp_path / "uploads")
    replayed = put(restarted, arguments, "data.bin", 0, content)
    assert replayed["replayed"] is True
    with pytest.raises(PermissionError, match="bound"):
        restarted.status(reference(arguments), identity(client_id="chatgpt"))

    bad = bytearray(content)
    bad[0] ^= 1
    with pytest.raises(ProviderUploadConflict, match="conflicts"):
        put(restarted, arguments, "data.bin", 0, bytes(bad))
    terminal = restarted.status(reference(arguments), identity())
    assert terminal["state"] == "QUARANTINED"
    assert not any(restarted.uploads.iterdir())

    other = start_arguments({"small.txt": b"ok"})
    restarted.start(other, identity())
    aborted = restarted.abort(reference(other), identity())
    assert aborted["state"] == "ABORTED"
    assert restarted.abort(reference(other), identity()) == aborted


def test_manifest_chunk_and_ttl_bounds_are_enforced_before_staging(tmp_path: Path) -> None:
    store = ProviderChunkedUploadStore(tmp_path / "uploads")

    too_many = start_arguments({"safe": b"x"})
    payload = too_many["payload"]
    assert isinstance(payload, dict)
    payload["files"] = [
        {"path": f"{index}.bin", "byte_size": 1, "sha256": "a" * 64}
        for index in range(MAX_UPLOAD_FILES + 1)
    ]
    with pytest.raises(ProviderUploadError, match="manifest"):
        store.start(too_many, identity())

    too_large = start_arguments({"safe": b"x"})
    payload = too_large["payload"]
    assert isinstance(payload, dict)
    payload["files"] = [
        {"path": "large.bin", "byte_size": MAX_UPLOAD_FILE_BYTES + 1, "sha256": "a" * 64}
    ]
    with pytest.raises(ProviderUploadError, match="declaration"):
        store.start(too_large, identity())

    total = start_arguments({"safe": b"x"})
    payload = total["payload"]
    assert isinstance(payload, dict)
    payload["files"] = [
        {"path": f"{index}.bin", "byte_size": MAX_UPLOAD_FILE_BYTES, "sha256": "a" * 64}
        for index in range(5)
    ]
    with pytest.raises(ProviderUploadError, match="total size"):
        store.start(total, identity())

    invalid_ttl = start_arguments({"safe": b"x"})
    payload = invalid_ttl["payload"]
    assert isinstance(payload, dict)
    payload["ttl_seconds"] = 299
    with pytest.raises(ProviderUploadError, match="TTL"):
        store.start(invalid_ttl, identity())

    active = start_arguments({"safe": b"x" * (MAX_UPLOAD_CHUNK_BYTES + 1)})
    store.start(active, identity())
    with pytest.raises(ProviderUploadError, match="declaration"):
        put(store, active, "safe", 0, b"x" * (MAX_UPLOAD_CHUNK_BYTES + 1))


def test_tamper_expiry_traversal_and_symlink_are_terminal_or_rejected(tmp_path: Path) -> None:
    clock = Clock()
    arguments = start_arguments({"folder/file.bin": b"abc"})
    store = ProviderChunkedUploadStore(tmp_path / "uploads", clock=clock)
    store.start(arguments, identity())
    put(store, arguments, "folder/file.bin", 0, b"abc")
    chunk = next(store.uploads.rglob("*.part"))
    chunk.write_bytes(b"abd")
    with pytest.raises(ProviderUploadConflict, match="tampered"):
        store.finalize(reference(arguments), identity(), lambda *_args: {})
    assert store.status(reference(arguments), identity())["state"] == "QUARANTINED"

    expiring = start_arguments({"file.bin": b"expiry"})
    expiring_payload = expiring["payload"]
    assert isinstance(expiring_payload, dict)
    expiring_payload["ttl_seconds"] = 300
    store.start(expiring, identity())
    clock.value += 301
    with pytest.raises(ProviderUploadExpired):
        store.status(reference(expiring), identity())
    assert store.status(reference(expiring), identity())["state"] == "EXPIRED"

    for unsafe in ("../escape", "/absolute", "folder\\file", "PG_VERSION", "data.sql"):
        request = start_arguments({"safe": b"x"})
        payload = request["payload"]
        assert isinstance(payload, dict)
        manifest = payload["files"]
        assert isinstance(manifest, list)
        manifest[0]["path"] = unsafe
        with pytest.raises(ProviderUploadError):
            store.start(request, identity())

    link = tmp_path / "link"
    link.symlink_to(tmp_path / "elsewhere")
    with pytest.raises(ValueError, match="real directory"):
        ProviderChunkedUploadStore(link)


def test_reaper_removes_expired_raw_upload_and_old_terminal_receipt(tmp_path: Path) -> None:
    clock = Clock()
    store = ProviderChunkedUploadStore(tmp_path / "uploads", clock=clock)
    arguments = start_arguments({"file.bin": b"raw"})
    payload = arguments["payload"]
    assert isinstance(payload, dict)
    payload["ttl_seconds"] = 300
    store.start(arguments, identity())
    put(store, arguments, "file.bin", 0, b"raw")
    clock.value += 301
    assert store.reap_expired() == {"expired_uploads": 1, "receipts_removed": 0}
    assert not any(store.uploads.iterdir())
    assert store.status(reference(arguments), identity())["state"] == "EXPIRED"
    clock.value += 7 * 86_400 + 1
    assert store.reap_expired() == {"expired_uploads": 0, "receipts_removed": 1}


def test_terminal_receipt_is_authoritative_over_orphan_active_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import my_data_hub.control_plane.provider_uploads as upload_module

    clock = Clock()
    store = ProviderChunkedUploadStore(tmp_path / "uploads", clock=clock)
    original_rmtree = upload_module.shutil.rmtree

    def preserve_upload_directory(path, *args, **kwargs):  # type: ignore[no-untyped-def]
        candidate = Path(path)
        if candidate.parent == store.uploads:
            return None
        return original_rmtree(path, *args, **kwargs)

    finalized_request = start_arguments({"final.bin": b"final"})
    store.start(finalized_request, identity())
    put(store, finalized_request, "final.bin", 0, b"final")
    monkeypatch.setattr(upload_module.shutil, "rmtree", preserve_upload_directory)
    finalized = store.finalize(
        reference(finalized_request),
        identity(),
        lambda *_args: {"provider_ref": "owner/photo-batch", "provider_version": 1},
    )
    finalized_id = finalized["upload_id"]
    finalized_receipt = store._receipt_path(finalized_id).read_bytes()
    assert store._directory(finalized_id).exists()

    aborted_request = start_arguments({"abort.bin": b"abort"})
    store.start(aborted_request, identity())
    aborted = store.abort(reference(aborted_request), identity())
    aborted_id = aborted["upload_id"]
    aborted_receipt = store._receipt_path(aborted_id).read_bytes()
    assert store._directory(aborted_id).exists()

    quarantined_request = start_arguments({"bad.bin": b"original"})
    store.start(quarantined_request, identity())
    put(store, quarantined_request, "bad.bin", 0, b"original")
    with pytest.raises(ProviderUploadConflict):
        put(store, quarantined_request, "bad.bin", 0, b"changed!")
    quarantined_payload = quarantined_request["payload"]
    assert isinstance(quarantined_payload, dict)
    quarantined_id = str(quarantined_payload["upload_id"])
    quarantined_receipt = store._receipt_path(quarantined_id).read_bytes()
    assert store._directory(quarantined_id).exists()

    monkeypatch.setattr(upload_module.shutil, "rmtree", original_rmtree)
    assert store.status(reference(finalized_request), identity())["state"] == "FINALIZED"
    assert not store._directory(finalized_id).exists()
    assert store._receipt_path(finalized_id).read_bytes() == finalized_receipt
    clock.value += 3601
    assert store.reap_expired() == {"expired_uploads": 0, "receipts_removed": 0}
    assert not any(store.uploads.iterdir())
    assert store._receipt_path(finalized_id).read_bytes() == finalized_receipt
    assert store._receipt_path(aborted_id).read_bytes() == aborted_receipt
    assert store._receipt_path(quarantined_id).read_bytes() == quarantined_receipt
    assert store.start(finalized_request, identity()) == finalized
    assert store.finalize(
        reference(finalized_request),
        identity(),
        lambda *_args: pytest.fail("authoritative receipt must prevent a second effect"),
    ) == finalized
    assert store.abort(reference(aborted_request), identity()) == aborted
    assert store.start(aborted_request, identity()) == aborted
    assert store.status(reference(quarantined_request), identity())["state"] == "QUARANTINED"


def test_global_and_principal_quotas_are_restart_and_concurrency_safe(tmp_path: Path) -> None:
    root = tmp_path / "quota"
    store = ProviderChunkedUploadStore(root, disk_usage=ample_disk)

    requests = [start_arguments({"tiny.bin": bytes([index % 251 + 1])}) for index in range(MAX_ACTIVE_UPLOADS + 8)]

    def start_one(index: int) -> str:
        try:
            store.start(requests[index], identity(subject=f"owner-{index}", client_id=f"client-{index}"))
        except ProviderUploadError:
            return "rejected"
        return "accepted"

    with ThreadPoolExecutor(max_workers=12) as executor:
        outcomes = list(executor.map(start_one, range(len(requests))))
    assert outcomes.count("accepted") == MAX_ACTIVE_UPLOADS
    assert outcomes.count("rejected") == 8

    principal_root = tmp_path / "principal-quota"
    principal_store = ProviderChunkedUploadStore(principal_root, disk_usage=ample_disk)
    owner = identity(subject="same-owner", client_id="same-client")
    active = [start_arguments({"tiny.bin": b"x"}) for _ in range(MAX_PRINCIPAL_ACTIVE_UPLOADS)]
    for request in active:
        principal_store.start(request, owner)
    restarted = ProviderChunkedUploadStore(principal_root, disk_usage=ample_disk)
    assert restarted.start(active[0], owner)["state"] == "OPEN"
    with pytest.raises(ProviderUploadError, match="principal active"):
        restarted.start(start_arguments({"extra.bin": b"x"}), owner)


def test_declared_byte_and_disk_reserve_quotas_are_admitted_without_allocation(tmp_path: Path) -> None:
    def declared(size: int) -> dict[str, object]:
        request = start_arguments({"declared.bin": b"x"})
        payload = request["payload"]
        assert isinstance(payload, dict)
        remaining = size
        files = []
        while remaining:
            byte_size = min(remaining, MAX_UPLOAD_FILE_BYTES)
            files.append(
                {
                    "path": f"declared-{len(files)}.bin",
                    "byte_size": byte_size,
                    "sha256": "a" * 64,
                }
            )
            remaining -= byte_size
        payload["files"] = files
        return request

    principal_store = ProviderChunkedUploadStore(
        tmp_path / "principal-bytes", disk_usage=ample_disk
    )
    owner = identity(subject="byte-owner", client_id="byte-client")
    for _ in range(MAX_PRINCIPAL_DECLARED_BYTES // MAX_UPLOAD_TOTAL_BYTES):
        principal_store.start(declared(MAX_UPLOAD_TOTAL_BYTES), owner)
    with pytest.raises(ProviderUploadError, match="principal active"):
        principal_store.start(declared(1), owner)

    global_store = ProviderChunkedUploadStore(tmp_path / "global-bytes", disk_usage=ample_disk)
    for index in range(MAX_ACTIVE_DECLARED_BYTES // MAX_UPLOAD_TOTAL_BYTES):
        global_store.start(
            declared(MAX_UPLOAD_TOTAL_BYTES),
            identity(subject=f"global-{index}", client_id=f"global-client-{index}"),
        )
    with pytest.raises(ProviderUploadError, match="global active"):
        global_store.start(
            declared(1), identity(subject="global-extra", client_id="global-client-extra")
        )

    request = declared(2_500_000)
    reserve_store = ProviderChunkedUploadStore(
        tmp_path / "disk-reserve",
        disk_usage=lambda _path: SimpleNamespace(
            free=MIN_UPLOAD_DISK_RESERVE_BYTES + 2_500_000 - 1
        ),
    )
    with pytest.raises(ProviderUploadError, match="disk reserve"):
        reserve_store.start(request, identity())

    available = {"free": MIN_UPLOAD_DISK_RESERVE_BYTES + 10_000}
    write_store = ProviderChunkedUploadStore(
        tmp_path / "write-reserve",
        disk_usage=lambda _path: SimpleNamespace(free=available["free"]),
    )
    write_request = start_arguments({"tiny.bin": b"x"})
    write_store.start(write_request, identity())
    available["free"] = MIN_UPLOAD_DISK_RESERVE_BYTES
    with pytest.raises(ProviderUploadError, match="disk reserve"):
        put(write_store, write_request, "tiny.bin", 0, b"x")


def test_chunks_or_assembled_ancestor_symlink_never_writes_outside_staging(tmp_path: Path) -> None:
    store = ProviderChunkedUploadStore(tmp_path / "uploads")
    outside = tmp_path / "outside"
    outside.mkdir()
    arguments = start_arguments({"nested/file.bin": b"private"})
    store.start(arguments, identity())
    payload = arguments["payload"]
    assert isinstance(payload, dict)
    upload_directory = store._directory(str(payload["upload_id"]))
    chunks = upload_directory / "chunks"
    chunks.rmdir()
    chunks.symlink_to(outside, target_is_directory=True)
    with pytest.raises(ProviderUploadConflict, match="private and real"):
        put(store, arguments, "nested/file.bin", 0, b"private")
    assert not any(outside.iterdir())

    chunks.unlink()
    chunks.mkdir(mode=0o700)
    put(store, arguments, "nested/file.bin", 0, b"private")
    assembled = upload_directory / "assembled"
    assembled.symlink_to(outside, target_is_directory=True)
    with pytest.raises(ProviderUploadConflict, match="assembled"):
        store.finalize(reference(arguments), identity(), lambda *_args: {})
    assert not any(outside.iterdir())
    assert store.status(reference(arguments), identity())["state"] == "QUARANTINED"


@pytest.mark.parametrize(
    ("operation", "terminal"),
    [
        ("status", "QUARANTINED"),
        ("finalize", "FINALIZED"),
        ("abort", "ABORTED"),
        ("start", "ABORTED"),
    ],
)
def test_terminalization_between_precheck_and_active_lock_returns_authoritative_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
    terminal: str,
) -> None:
    store = ProviderChunkedUploadStore(tmp_path / operation)
    arguments = start_arguments({"race.bin": b"race"})
    store.start(arguments, identity())
    original_lock = store._lock
    attempting_active_lock = Event()

    @contextmanager
    def observed_lock(upload_id: str, *, create: bool = False):  # type: ignore[no-untyped-def]
        attempting_active_lock.set()
        with original_lock(upload_id, create=create) as directory:
            yield directory

    def run_operation() -> dict[str, object]:
        if operation == "status":
            return store.status(reference(arguments), identity())
        if operation == "finalize":
            return store.finalize(
                reference(arguments),
                identity(),
                lambda *_args: pytest.fail("terminal race must not execute a second effect"),
            )
        if operation == "abort":
            return store.abort(reference(arguments), identity())
        return store.start(arguments, identity())

    payload = arguments["payload"]
    assert isinstance(payload, dict)
    upload_id = payload["upload_id"]
    assert isinstance(upload_id, str)
    with ThreadPoolExecutor(max_workers=1) as pool:
        with original_lock(upload_id) as directory:
            monkeypatch.setattr(store, "_lock", observed_lock)
            future = pool.submit(run_operation)
            assert attempting_active_lock.wait(timeout=2)
            state = json.loads((directory / "state.json").read_text())
            store._terminal(directory, state, terminal)
        result = future.result(timeout=2)
    assert result["state"] == terminal
    assert not any(store.uploads.iterdir())
    assert store.status(reference(arguments), identity())["state"] == terminal
