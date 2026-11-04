#!/usr/bin/env python3
"""One-shot synthetic P3; run in the documented credentialed central context.

Uses MDH adapter contracts inspected at bcd7acfb6d0cbe635a90528dc30ed1f22c2f2ae2.
No production storage implementation or deployed-HTTP-gateway test. V1 cleanup
claim entails two 100-MB synthetic readbacks on Dev, never a 10-GB readback.
Existing state refuses replay except explicit recovery of the v1 claim readback.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
from uuid import UUID, uuid4, uuid5

SIZES = (100_000_000, 1_000_000_000, 10_000_000_000)
PAYLOAD = "p3-payload.blob"
RESULT = "p3-upload-result.json"


def producer_source(run_id, task_run_id, provider_ref, create_url, size):
    config = json.dumps(dict(run_id=str(run_id), task_run_id=str(task_run_id),
                             provider_ref=provider_ref, create_url=create_url, size=size))
    return ("CONFIG = " + repr(config) + "\n" + '''
import hashlib, http.client, json, os, time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit


def produce():
    cfg = json.loads(CONFIG)
    keys = ("KAGGLE_USERNAME", "KAGGLE_KEY", "KAGGLE_API_TOKEN")
    auth = Path.home() / ".kaggle"
    if any(os.environ.get(k) for k in keys) or any((auth / p).exists() for p in ("kaggle.json", "access_token")):
        raise RuntimeError("unexpected credential in synthetic producer")
    path = Path("/tmp") / ("p3-" + cfg["task_run_id"] + ".blob")
    digest, generated, sent = hashlib.sha256(), 0, 0
    began = time.monotonic()
    connection = None
    try:
        with path.open("xb") as output:
            while generated < cfg["size"]:
                count = min(1024 * 1024, cfg["size"] - generated)
                seed = (cfg["run_id"] + ":" + str(generated)).encode()
                chunk = hashlib.shake_256(seed).digest(count)
                output.write(chunk)
                digest.update(chunk)
                generated += count
        generation_seconds = time.monotonic() - began
        parsed = urlsplit(cfg["create_url"])
        if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password or parsed.fragment or parsed.port not in (None, 443):
            raise RuntimeError("invalid upload capability")
        connection = http.client.HTTPSConnection(parsed.hostname, timeout=600)
        upload_started_at = datetime.now(timezone.utc).isoformat()
        began = time.monotonic()
        connection.putrequest("PUT", parsed.path + ("?" + parsed.query if parsed.query else ""))
        connection.putheader("Content-Length", str(cfg["size"]))
        connection.putheader("Content-Type", "application/octet-stream")
        connection.endheaders()
        with path.open("rb") as source:
            while chunk := source.read(1024 * 1024):
                connection.send(chunk)
                sent += len(chunk)
        response = connection.getresponse()
        response.read(4096)
        if not 200 <= response.status < 300 or sent != cfg["size"]:
            raise RuntimeError("synthetic PUT failed")
        result = dict(run_id=cfg["run_id"], task_run_id=cfg["task_run_id"],
                      actual_bytes=sent, file_sha256=digest.hexdigest(), credential_free=True,
                      generation_seconds=generation_seconds, upload_started_at=upload_started_at,
                      upload_seconds=time.monotonic() - began, http_status=response.status)
    finally:
        if connection is not None:
            connection.close()
        path.unlink(missing_ok=True)
    work = Path("/kaggle/working")
    receipt = dict(schema_version="my-data-hub-run-receipt.v1", task_run_id=cfg["task_run_id"],
                   provider_ref=cfg["provider_ref"], source_version=1,
                   source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), terminal_state="complete")
    (work / "my-data-hub-run-receipt.json").write_text(json.dumps(receipt))
    (work / "p3-upload-result.json").write_text(json.dumps(result))


try:
    produce()
except Exception as error:
    print(json.dumps({"outcome": "FAILED", "error_type": type(error).__name__}))
    raise SystemExit(1)
''').encode()


def save(path, state, phase, **fields):
    state.update(phase=phase, updated_at=datetime.now(UTC).isoformat(), **fields)
    with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, delete=False) as handle:
        temporary = Path(handle.name)
        json.dump(state, handle, indent=2)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def measure(args, state):
    from my_data_hub.control_plane.ledger import ControlLedger
    from my_data_hub.hashing import canonical_json_bytes, sha256_value
    from my_data_hub.providers.kaggle.adapter import KaggleProviderAdapter
    from my_data_hub.providers.kaggle.control_journal import ControlLedgerKaggleJournal
    from my_data_hub.providers.kaggle.contracts import (
        BrokeredDatasetFile, EffectOutcome, KaggleAmbiguousMutation, KaggleDatasetIdentity, MutationAction,
        PollPolicy, ProviderEffectIntent, ProviderEffectReceipt, TaskResourceClaim,
    )
    from my_data_hub.providers.models import ControlClass, ProviderKind

    journal = ControlLedgerKaggleJournal(ControlLedger(args.ledger))
    adapter = KaggleProviderAdapter.from_environment(journal=journal)
    owner = adapter.provider_identity().username
    dataset = f"{owner}/mdh-p3-{args.run_id.hex[:12]}"
    if not args.resume_creation_readback:
        if adapter.current_private_dataset_version(provider_ref=dataset) is not None:
            raise RuntimeError("planned disposable Dataset already exists")
        save(args.state, state, "PLANNED", dataset=dataset, samples=[], claims=[], cleanup=[],
             provider_identity=adapter.provider_identity().model_dump(mode="json"))

    def effect(label, action, ref, arguments, fingerprint=None):
        return ProviderEffectIntent.create(
            operation_id=uuid5(args.run_id, label), effect_id=uuid5(args.run_id, label + ":effect"),
            idempotency_key=f"p3:{args.run_id}:{label}", task_id=args.run_id, action=action,
            provider_ref=ref, arguments=arguments, expected_fingerprint=fingerprint,
            requested_at=datetime.now(UTC))

    claims = []

    def register_creation_claim(sample, intent):
        save(args.state, state, "CREATION_CLAIM_READBACK")
        if args.creation_identity:
            identity = KaggleDatasetIdentity.model_validate_json(args.creation_identity.read_text())
            sample["diagnostic_readback_observed_at"] = identity.observed_at.isoformat()
            sample["diagnostic_readback_identity_path"] = str(args.creation_identity)
            sample["synthetic_creation_readback_seconds"] = None  # Diagnostic duration was not measured here.
        else:
            sample["synthetic_creation_readback_started_at"] = datetime.now(UTC).isoformat()
            readback_began = time.monotonic()
            identity = adapter.read_private_dataset(provider_ref=dataset, version=1)
            sample["synthetic_creation_readback_seconds"] = time.monotonic() - readback_began
        if (identity.provider_ref != dataset or identity.version != 1 or identity.privacy != "private"
                or identity.fingerprint.value != sha256_value(dict(provider_ref=dataset, version=1,
                    privacy="private", package_sha256=identity.package_sha256))):
            raise RuntimeError("creation readback identity does not match this exact private v1")
        claim = TaskResourceClaim.create(
            task_id=args.run_id, effect_id=intent.effect_id, provider_ref=dataset, kind=ProviderKind.DATASET,
            control_class=ControlClass.MCP_MANAGED, disposable=True, fingerprint=identity.fingerprint,
            provider_version=1, registered_at=intent.requested_at)
        journal.persist_resource_claim(claim)
        claims.append(claim)
        state["claims"].append(claim.model_dump(mode="json"))

    if args.resume_creation_readback:
        sample = state["samples"][0]
        if (state["dataset"] != dataset or len(state["claims"]) != 1 or state["cleanup"]
                or sample["version"] != 1 or sample["actual_bytes"] != SIZES[0]
                or sample["dataset_version_ref"] != f"{dataset}/1"):
            raise RuntimeError("state is not the single completed v1 awaiting its creation claim")
        effect_id = str(uuid5(args.run_id, "dataset:1:effect"))
        with journal.ledger._reader() as connection:
            row = connection.execute("SELECT intent_json FROM provider_effect_intents WHERE effect_id=?", (effect_id,)).fetchone()
        intent = ProviderEffectIntent.model_validate_json(row["intent_json"])
        receipt = ProviderEffectReceipt.model_validate(journal.ledger.latest_provider_effect_receipt(effect_id))
        if (intent.task_id != args.run_id or intent.provider_ref != dataset or intent.action is not MutationAction.CREATE_DATASET
                or intent.arguments_sha256 != sha256_value(dict(version=1, expected_files=sample["expected_files"]))
                or receipt.outcome is not EffectOutcome.APPLIED or receipt.provider_version != 1
                or receipt.effect_id != intent.effect_id or receipt.provider_ref != dataset):
            raise RuntimeError("existing ledger does not prove this exact v1 creation")
        for payload in state["claims"]:
            claim = TaskResourceClaim.model_validate(payload)
            if claim.task_id != args.run_id or claim.provider_ref != f"{dataset}-v1" or claim.kind is not ProviderKind.NOTEBOOK:
                raise RuntimeError("saved producer claim belongs to another resource")
            journal.assert_resource_claim(claim)
            claims.append(claim)
        if (adapter.current_private_dataset_version(provider_ref=dataset) != 1
                or not adapter.reconcile_brokered_checkpoint_dataset(provider_ref=dataset, version=1,
                    expected_files=tuple(tuple(item) for item in sample["expected_files"]))):
            raise RuntimeError("remote v1 no longer matches the saved successful finalization")
        state["continuation_started_at"] = datetime.now(UTC).isoformat()
        state["previous_failure"] = dict(phase=state.pop("stopped_phase"), error_type=state.pop("error_type"))
        register_creation_claim(sample, intent)
        save(args.state, state, "VERSION_READY")

    for version, size in list(enumerate(SIZES, 1))[1 if args.resume_creation_readback else 0:]:
        sample = dict(version=version, planned_bytes=size)
        state["samples"].append(sample)
        notebook = f"{owner}/mdh-p3-{args.run_id.hex[:12]}-v{version}"
        task_run_id = uuid5(args.run_id, f"producer:{version}")
        save(args.state, state, "BLOB_STARTING", version=version, producer_ref=notebook)
        grant = adapter.start_brokered_dataset_blob(
            file_name=PAYLOAD, content_length=size, content_type="application/octet-stream",
            last_modified_epoch_seconds=int(time.time()))
        source = producer_source(args.run_id, task_run_id, notebook, grant.create_url, size)
        source_sha = hashlib.sha256(source).hexdigest()
        intent = effect(f"producer:{version}", MutationAction.PUSH_NOTEBOOK, notebook,
                        dict(task_run_id=str(task_run_id), source_sha256=source_sha, dataset_sources=(),
                             control_class=ControlClass.MCP_MANAGED.value, disposable=True, enable_internet=True))
        save(args.state, state, "PRODUCER_STARTING", source_sha256=source_sha)
        producer = adapter.push_private_notebook(
            intent=intent, task_run_id=task_run_id, source=source, title=notebook.split("/")[1],
            code_file="run.py", kernel_type="script", language="python", control_class=ControlClass.MCP_MANAGED,
            disposable=True, dataset_sources=(), enable_internet=True, accelerator="none",
            timeout_seconds=args.timeout_seconds)
        claims.append(producer.claim)
        state["claims"].append(producer.claim.model_dump(mode="json"))
        sample["producer_run"] = producer.run.model_dump(mode="json")
        save(args.state, state, "PRODUCER_RUNNING")
        adapter.poll_run(producer.run, PollPolicy(interval_seconds=15, timeout_seconds=args.timeout_seconds, max_polls=2000))
        adapter.read_exact_run_output(producer.run)
        with tempfile.TemporaryDirectory(prefix="p3-result-") as folder:
            adapter.download_exact_run_output_file(producer.run, destination=Path(folder), file_name=RESULT, max_bytes=16384)
            result = json.loads((Path(folder) / RESULT).read_text())
        if (result["run_id"] != str(args.run_id) or result["task_run_id"] != str(task_run_id)
                or result["actual_bytes"] != size or result["credential_free"] is not True):
            raise RuntimeError("producer result does not match the planned run")
        sample.update(result)
        description = canonical_json_bytes(dict(
            operation_id=str(uuid5(args.run_id, f"dataset:{version}")), master_run_ref=producer.run.provider_run_ref,
            epoch=1, manifest_sha256=sha256_value({"files": [[PAYLOAD, size, result["file_sha256"]]]}),
            file_sha256=result["file_sha256"], total_bytes=size)).decode()
        sample["expected_files"] = [[PAYLOAD, size, description]]
        intent = effect(f"dataset:{version}", MutationAction.CREATE_DATASET if version == 1 else MutationAction.VERSION_DATASET,
                        dataset, dict(version=version, expected_files=sample["expected_files"]))
        journal.persist_intent(intent)
        sample["finalize_started_at"] = datetime.now(UTC).isoformat()
        save(args.state, state, "FINALIZING", expected_version=version)
        began = time.monotonic()
        ambiguous = False
        try:
            observed = adapter.finalize_brokered_checkpoint_dataset(
                provider_ref=dataset, title=dataset.split("/")[1],
                files=(BrokeredDatasetFile(PAYLOAD, size, description, grant.blob_token),),
                version_notes=f"Synthetic P3 {args.run_id} size={size}", expected_previous_version=version - 1 or None)
            if observed != version:
                raise RuntimeError("unexpected finalized version")
        except KaggleAmbiguousMutation:
            ambiguous = True
        sample["finalizer_call_seconds"] = time.monotonic() - began  # Includes adapter reconciliation.
        sample["finalizer_returned_at"] = datetime.now(UTC).isoformat()
        sample["finalizer_ambiguous"] = ambiguous
        del grant, source
        while time.monotonic() - began <= args.timeout_seconds:
            status = json.loads(adapter.api.dataset_status(dataset, format="json(status,current_version_number)"))
            sample["dataset_status"] = status
            save(args.state, state, "WAITING_READY")
            if status.get("status", "").lower() == "ready" and status.get("current_version_number") == version:
                if not ambiguous or adapter.reconcile_brokered_checkpoint_dataset(
                    provider_ref=dataset, version=version, expected_files=((PAYLOAD, size, description),)):
                    break
            time.sleep(5)
        else:
            raise TimeoutError("expected Dataset version did not become ready; never re-finalize blindly")
        sample["ready_at"] = datetime.now(UTC).isoformat()
        sample["ready_after_finalize_seconds"] = time.monotonic() - began
        sample["dataset_version_ref"] = f"{dataset}/{version}"
        journal.persist_receipt(ProviderEffectReceipt(
            operation_id=intent.operation_id, effect_id=intent.effect_id, action=intent.action, provider_ref=dataset,
            outcome=EffectOutcome.APPLIED, attempts=1, provider_version=version, observed_at=datetime.now(UTC),
            detail_code="p3_synthetic_dataset_version_ready"))
        if version == 1:
            register_creation_claim(sample, intent)
        save(args.state, state, "VERSION_READY")

    for claim in reversed(claims):
        if claim.kind is ProviderKind.DATASET:
            if adapter.current_private_dataset_version(provider_ref=dataset) != len(SIZES):
                raise RuntimeError("Dataset changed outside this measurement")
        action = MutationAction.DELETE_DATASET if claim.kind is ProviderKind.DATASET else MutationAction.DELETE_NOTEBOOK
        intent = effect(f"cleanup:{claim.provider_ref}", action, claim.provider_ref,
                        dict(claim_sha256=claim.claim_sha256, provider_version=claim.provider_version), claim.fingerprint)
        save(args.state, state, "CLEANUP", deleting_ref=claim.provider_ref)
        began = time.monotonic()
        receipt = adapter.delete_task_created_resource(intent=intent, claim=claim)
        state["cleanup"].append(dict(provider_ref=claim.provider_ref, seconds=time.monotonic() - began,
                                    synthetic_readback_bytes=SIZES[0] if claim.kind is ProviderKind.DATASET else 0,
                                    receipt=receipt.model_dump(mode="json")))
        save(args.state, state, "CLEANUP")
    save(args.state, state, "COMPLETE", completed_at=datetime.now(UTC).isoformat())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", type=Path, required=True)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--run-id", type=UUID, default=None)
    parser.add_argument("--timeout-seconds", type=int, default=7200)
    parser.add_argument("--resume-creation-readback", action="store_true")
    parser.add_argument("--creation-identity", type=Path)
    args = parser.parse_args()
    if not 60 <= args.timeout_seconds <= 43200:
        parser.error("timeout must be 60..43200 seconds")
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    if subprocess.check_output(["git", "status", "--porcelain=v1"], text=True).strip():
        parser.error("run from a clean MDH source checkout; keep evidence outside it")
    if args.resume_creation_readback:
        state = json.loads(args.state.read_text())
        if (state["phase"] != "INCOMPLETE" or state["stopped_phase"] != "CREATION_CLAIM_READBACK"
                or state["commit"] != commit or len(state["samples"]) != 1 or not args.ledger.is_file()
                or (args.run_id is not None and str(args.run_id) != state["run_id"])):
            parser.error("resume requires the original ledger, clean commit, and exact v1 readback failure")
        args.run_id = UUID(state["run_id"])
    else:
        if args.creation_identity:
            parser.error("creation identity is permitted only with explicit v1 readback continuation")
        args.run_id = args.run_id or uuid4()
        args.state.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(args.state, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        os.close(descriptor)
        state = dict(run_id=str(args.run_id), commit=commit, sizes=SIZES, deployed_http_test=False,
                     started_at=datetime.now(UTC).isoformat())
        save(args.state, state, "STARTING")
    try:
        measure(args, state)
    except Exception as error:
        save(args.state, state, "INCOMPLETE", stopped_phase=state["phase"], error_type=type(error).__name__)
        print(json.dumps({"outcome": "INCOMPLETE", "state": str(args.state), "error_type": type(error).__name__}))
        return 1
    print(json.dumps({"outcome": "COMPLETE", "state": str(args.state)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
