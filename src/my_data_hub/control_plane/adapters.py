from __future__ import annotations

import hashlib
import hmac
import json
import time
from base64 import urlsafe_b64decode, urlsafe_b64encode
from collections.abc import Callable, Mapping
from dataclasses import asdict
from datetime import datetime, timedelta
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from my_data_hub.auth.control import (
    OAuthAuditEvent,
    OAuthClientRecord,
    OAuthRevocationQuery,
)
from my_data_hub.control_plane.acceptance_evidence import AcceptanceEvidenceController
from my_data_hub.control_plane.ledger import ControlLedger
from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.mcp.contracts import (
    ControlPlaneReader,
    EnsureMasterReceipt,
    MasterResolver,
    MasterSnapshot,
    MasterState,
    WriteGate,
    WritePermit,
)
from my_data_hub.mcp.oauth import AccessIdentity
from my_data_hub.mcp.sql_policy import change_request_sha256
from my_data_hub.orchestrator.master import MasterCoordinator
from my_data_hub.providers import ProviderPolicy
from my_data_hub.providers.exchange import (
    EXCHANGE_MANIFEST_PATH,
    MAX_EXCHANGE_TTL,
    ExchangeManifest,
    validate_exchange_manifest_for_mutation,
)
from my_data_hub.providers.kaggle import KaggleProviderAdapter, mapping_sha256
from my_data_hub.providers.kaggle.contracts import (
    EffectOutcome,
    MutationAction,
    ProviderEffectIntent,
    ProviderEffectReceipt,
    TaskResourceClaim,
)
from my_data_hub.providers.models import (
    ControlClass,
    Origin,
    ProviderAction,
    ProviderKind,
    ProviderResource,
    ResourceLease,
)
from my_data_hub.providers.policy import PolicyDenied


def _revocation_reference(query: OAuthRevocationQuery) -> str:
    return json.dumps(asdict(query), sort_keys=True, separators=(",", ":"))


def _b64(value: bytes) -> str:
    return urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


class LedgerWriteGate(WriteGate):
    """Durable preview/apply/checkpoint gate for the separately enabled operator profile."""

    def __init__(
        self,
        ledger: ControlLedger,
        *,
        signing_secret: bytes,
        clock: Callable[[], float] = time.time,
        permit_ttl_seconds: int = 120,
    ) -> None:
        if len(signing_secret) < 32:
            raise ValueError("write-gate signing secret must contain at least 32 bytes")
        if not 30 <= permit_ttl_seconds <= 300:
            raise ValueError("write permit TTL must be between 30 and 300 seconds")
        self.ledger = ledger
        self.signing_secret = signing_secret
        self.clock = clock
        self.permit_ttl_seconds = permit_ttl_seconds

    def authorize_write(
        self,
        *,
        principal: AccessIdentity,
        tool: str,
        arguments: Mapping[str, Any],
        master: MasterSnapshot,
    ) -> WritePermit:
        if master.state is not MasterState.ACTIVE or not master.instance_id or not master.epoch:
            raise PermissionError("operator writes require an exact ACTIVE master epoch")
        if master.canonical_revision is None:
            raise PermissionError("operator writes require an observed canonical revision")
        checkpoint = self._verified_checkpoint(master.canonical_revision)
        if tool.startswith("provider.resources.") or tool in {
            "provider.acceptance.dataset.lifecycle",
            "provider.acceptance.notebook.lifecycle",
            "provider.acceptance.claim.cleanup",
        }:
            resource_class = (
                "mcp_managed"
                if tool.startswith("provider.acceptance.")
                else str(arguments.get("control_class", ""))
            )
            if resource_class not in {"mcp_managed", "mcp_exchange"}:
                raise PermissionError("provider resource class is not MCP-controlled")
            return WritePermit(
                permit_id=self._digest(
                    {"tool": tool, "principal": principal.subject, "arguments": dict(arguments), "epoch": master.epoch}
                ),
                tool=tool,
                principal=principal.subject,
                client_id=principal.client_id,
                master_epoch=master.epoch,
                canonical_revision=master.canonical_revision,
                expires_at=int(self.clock()) + self.permit_ttl_seconds,
                preview_bound=True,
                checkpoint_lifecycle_bound=True,
                pre_change_checkpoint_verified=True,
                allowed_resource_class=resource_class,
                private_resource_only=True,
            )
        if tool not in {"data.change.preview", "data.change.apply"}:
            raise PermissionError("write gate does not authorize this tool")
        request_sha256 = self._change_request_sha(arguments)
        if arguments.get("expected_revision") != master.canonical_revision:
            raise PermissionError("operator request revision differs from ACTIVE canonical state")
        if tool == "data.change.preview":
            operation_id = self._digest(
                {
                    "kind": "mcp-write-v1",
                    "principal": principal.subject,
                    "client_id": principal.client_id,
                    "idempotency_key": arguments.get("idempotency_key"),
                }
            )
            record, _created = self.ledger.ensure_mcp_write_operation(
                operation_id=operation_id,
                idempotency_key=str(arguments["idempotency_key"]),
                principal_id=principal.subject,
                client_id=principal.client_id,
                master_instance_id=master.instance_id,
                epoch=master.epoch,
                expected_revision=master.canonical_revision,
                request_sha256=request_sha256,
                pre_change_checkpoint_id=str(checkpoint["checkpoint_id"]),
            )
            if record["state"] not in {"REQUESTED", "PREVIEWED"}:
                raise PermissionError("preview operation is no longer previewable")
            preview_bound = False
        else:
            receipt = self._verify_preview(str(arguments.get("preview_receipt", "")))
            operation_id = str(receipt.get("operation_id", ""))
            record = self.ledger.mcp_write_operation(operation_id)
            if (
                record is None
                or record["principal_id"] != principal.subject
                or record["client_id"] != principal.client_id
                or record["master_instance_id"] != master.instance_id
                or record["epoch"] != master.epoch
                or record["expected_revision"] != master.canonical_revision
                or record["request_sha256"] != request_sha256
                or record["pre_change_checkpoint_id"] != checkpoint["checkpoint_id"]
            ):
                raise PermissionError("apply does not bind the durable preview and current checkpoint")
            self.ledger.begin_mcp_write_apply(
                operation_id, preview_receipt=str(arguments["preview_receipt"])
            )
            preview_bound = True
        return WritePermit(
            permit_id=operation_id,
            tool=tool,
            principal=principal.subject,
            client_id=principal.client_id,
            master_epoch=master.epoch,
            canonical_revision=master.canonical_revision,
            expires_at=int(self.clock()) + self.permit_ttl_seconds,
            preview_bound=preview_bound,
            checkpoint_lifecycle_bound=True,
            pre_change_checkpoint_verified=True,
        )

    def record_write_result(
        self,
        *,
        permit: WritePermit,
        result: Mapping[str, Any],
    ) -> dict[str, Any]:
        if permit.tool == "data.change.preview":
            affected = int(result.get("affected_rows", -1))
            payload = {
                "kind": "mcp-write-preview-v1",
                "operation_id": permit.permit_id,
                "principal": permit.principal,
                "client_id": permit.client_id,
                "master_epoch": permit.master_epoch,
                "canonical_revision": permit.canonical_revision,
                "request_sha256": self.ledger.mcp_write_operation(permit.permit_id)["request_sha256"],  # type: ignore[index]
                "expires_at": int(self.clock()) + 300,
            }
            receipt = self._sign(payload)
            record = self.ledger.record_mcp_write_preview(
                permit.permit_id, preview_receipt=receipt, affected_rows=affected
            )
            return {
                **result,
                "operation_id": permit.permit_id,
                "status": record["state"],
                "preview_receipt": receipt,
                "pre_change_checkpoint_id": record["pre_change_checkpoint_id"],
            }
        if permit.tool == "data.change.apply":
            affected = int(result.get("affected_rows", -1))
            revision = int(result.get("canonical_revision", -1))
            record = self.ledger.record_mcp_write_commit(
                permit.permit_id, affected_rows=affected, committed_revision=revision
            )
            return {
                **result,
                "operation_id": permit.permit_id,
                "status": record["state"],
                "pre_change_checkpoint_id": record["pre_change_checkpoint_id"],
            }
        return dict(result)

    def reconciliation_request(
        self,
        *,
        principal: AccessIdentity,
        master: MasterSnapshot,
        operation_id: str | None = None,
        arguments: Mapping[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """Return the exact metadata-only lookup for an ambiguous apply."""

        if master.state is not MasterState.ACTIVE or master.instance_id is None or master.epoch is None:
            return None
        if arguments is not None:
            preview = self._verify_preview(str(arguments.get("preview_receipt", "")))
            requested_operation_id = str(preview.get("operation_id", ""))
        else:
            requested_operation_id = str(operation_id or "")
        record = self.ledger.mcp_write_operation(requested_operation_id)
        if record is None:
            return None
        if (
            record["principal_id"] != principal.subject
            or record["client_id"] != principal.client_id
            or record["master_instance_id"] != master.instance_id
            or record["epoch"] != master.epoch
        ):
            raise PermissionError("operator reconciliation differs from the durable write identity")
        if arguments is not None and record["request_sha256"] != change_request_sha256(arguments):
            raise PermissionError("operator retry differs from the original exact request")
        if record["state"] == "PREVIEWED":
            return None
        if record["state"] != "APPLYING":
            raise PermissionError("operator apply was already admitted; use data.change.status")
        return {
            "operation_id": requested_operation_id,
            "request_sha256": record["request_sha256"],
            "master_instance_id": record["master_instance_id"],
            "master_epoch": record["epoch"],
            "expected_revision": record["expected_revision"],
            "principal_id": record["principal_id"],
            "client_id": record["client_id"],
        }

    def record_reconciled_write(
        self,
        *,
        operation_id: str,
        receipt: Mapping[str, Any],
    ) -> dict[str, Any]:
        if receipt.get("found") is not True or str(receipt.get("operation_id", "")) != operation_id:
            raise PermissionError("canonical operator receipt was not found")
        record = self.ledger.reconcile_mcp_write_commit(
            operation_id,
            request_sha256=str(receipt.get("request_sha256", "")),
            master_instance_id=str(receipt.get("master_instance_id", "")),
            epoch=int(receipt.get("master_epoch", 0)),
            expected_revision=int(receipt.get("expected_revision", -1)),
            principal_id=str(receipt.get("principal_id", "")),
            client_id=str(receipt.get("client_id", "")),
            affected_rows=int(receipt.get("affected_rows", -1)),
            committed_revision=int(receipt.get("committed_revision", -1)),
            committed_at=str(receipt.get("committed_at", "")),
        )
        return {
            "operation_id": operation_id,
            "status": record["state"],
            "affected_rows": record["affected_rows"],
            "master_epoch": record["epoch"],
            "canonical_revision": record["committed_revision"],
            "pre_change_checkpoint_id": record["pre_change_checkpoint_id"],
            "reconciled": True,
        }

    def write_status(self, operation_id: str, principal: AccessIdentity) -> dict[str, Any]:
        record = self.ledger.mcp_write_operation(operation_id)
        if record is None or (
            record["principal_id"] != principal.subject or record["client_id"] != principal.client_id
        ):
            return {"found": False}
        if record["state"] in {"COMMITTED_PENDING_CHECKPOINT", "CHECKPOINTING"}:
            if record["state"] == "COMMITTED_PENDING_CHECKPOINT":
                record = self.ledger.advance_mcp_write_checkpoint(operation_id, state="CHECKPOINTING")
            post = self._post_change_checkpoint(record)
            if post is not None:
                record = self.ledger.advance_mcp_write_checkpoint(
                    operation_id,
                    state="CHECKPOINT_VERIFIED",
                    post_change_checkpoint_id=str(post["checkpoint_id"]),
                )
                record = self.ledger.advance_mcp_write_checkpoint(
                    operation_id,
                    state="DURABLE_COMPLETE",
                    post_change_checkpoint_id=str(post["checkpoint_id"]),
                )
        return {
            "found": True,
            "operation_id": operation_id,
            "state": record["state"],
            "master_epoch": record["epoch"],
            "expected_revision": record["expected_revision"],
            "committed_revision": record["committed_revision"],
            "pre_change_checkpoint_id": record["pre_change_checkpoint_id"],
            "post_change_checkpoint_id": record["post_change_checkpoint_id"],
            "retry_allowed": record["state"] in {"REQUESTED", "PREVIEWED"},
            "reconciliation_required": record["state"] == "APPLYING",
        }

    def _verified_checkpoint(self, revision: int) -> dict[str, Any]:
        head = self.ledger.checkpoint_head("postgres-master")
        candidate = (
            self.ledger.checkpoint_candidate(head.current_checkpoint_id)
            if head is not None and head.current_checkpoint_id
            else None
        )
        manifest = candidate.get("manifest") if candidate else None
        if (
            candidate is None
            or candidate.get("status") != "VERIFIED"
            or not candidate.get("verified_at")
            or not candidate.get("version_ref")
            or not isinstance(manifest, dict)
            or manifest.get("canonical_revision") != revision
        ):
            raise PermissionError("operator gate requires an exact verified checkpoint")
        return candidate

    def _post_change_checkpoint(self, record: dict[str, Any]) -> dict[str, Any] | None:
        try:
            candidate = self._verified_checkpoint(int(record["committed_revision"]))
        except PermissionError:
            return None
        if (
            candidate["checkpoint_id"] == record["pre_change_checkpoint_id"]
            or str(candidate["verified_at"]) <= str(record["committed_at"] or "")
        ):
            return None
        return candidate

    def _sign(self, payload: dict[str, Any]) -> str:
        raw = canonical_json_bytes(payload)
        return f"{_b64(raw)}.{_b64(hmac.new(self.signing_secret, raw, hashlib.sha256).digest())}"

    def _verify_preview(self, token: str) -> dict[str, Any]:
        try:
            encoded, signature = token.split(".", 1)
            raw = urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4))
            supplied = urlsafe_b64decode(signature + "=" * (-len(signature) % 4))
            expected = hmac.new(self.signing_secret, raw, hashlib.sha256).digest()
            payload = json.loads(raw)
        except Exception as exc:
            raise PermissionError("preview receipt is invalid") from exc
        if not hmac.compare_digest(supplied, expected) or not isinstance(payload, dict):
            raise PermissionError("preview receipt is invalid")
        if payload.get("kind") != "mcp-write-preview-v1" or int(payload.get("expires_at", 0)) <= int(self.clock()):
            raise PermissionError("preview receipt is expired or has the wrong contract")
        return payload

    @staticmethod
    def _change_request_sha(arguments: Mapping[str, Any]) -> str:
        return change_request_sha256(arguments)

    @staticmethod
    def _digest(payload: dict[str, Any]) -> str:
        return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


class ControlLedgerOAuthAuthority:
    """OAuth revocation/client/audit authority that remains available with no master."""

    def __init__(self, ledger: ControlLedger) -> None:
        self.ledger = ledger

    def is_revoked(self, query: OAuthRevocationQuery) -> bool:
        return self.ledger.is_oauth_reference_revoked(_revocation_reference(query))

    def get_client(self, issuer: str, client_id: str) -> OAuthClientRecord | None:
        row = self.ledger.oauth_client(issuer, client_id)
        if row is None:
            return None
        return OAuthClientRecord(
            issuer=issuer,
            client_id=client_id,
            enabled=bool(row["enabled"]),
            allowed_scopes=frozenset(row["allowed_scopes"]),
        )

    def record_oauth_audit(self, event: OAuthAuditEvent) -> None:
        self.ledger.append_audit(
            action=f"oauth:{event.event}:{event.outcome}",
            audit_ref=hashlib.sha256(repr(event).encode()).hexdigest(),
            principal_id=event.subject,
            client_id=event.client_id,
            operation_id=event.operation_id,
            epoch=event.master_epoch,
            revision=event.canonical_revision,
            metadata={"issuer": event.issuer, "tool": event.tool},
        )

    def record_mcp_audit(self, event: OAuthAuditEvent) -> None:
        self.record_oauth_audit(event)


class KaggleMCPProviderGateway:
    """Exact metadata gateway over the repository's single Kaggle adapter."""

    def __init__(self, ledger: ControlLedger, adapter: KaggleProviderAdapter) -> None:
        self.ledger = ledger
        self.adapter = adapter
        self.policy = ProviderPolicy()

    def invoke(self, tool: str, arguments: Mapping[str, Any], principal: AccessIdentity) -> dict[str, Any]:
        provider_ref = str(arguments.get("resource_ref", ""))
        control_class = ControlClass(str(arguments.get("control_class", "")))
        if control_class not in {ControlClass.MCP_MANAGED, ControlClass.MCP_EXCHANGE}:
            raise PermissionError("provider gateway accepts only MCP-controlled resources")
        if arguments.get("private") is not True:
            raise PermissionError("provider gateway accepts private resources only")
        payload = arguments.get("payload")
        if not isinstance(payload, Mapping):
            raise ValueError("provider payload must be an exact object")
        if tool == "provider.resources.create":
            return self._create(provider_ref, control_class, payload, principal)
        if tool == "provider.resources.version":
            return self._version(provider_ref, control_class, payload, principal)
        if tool == "provider.resources.run":
            return self._run(provider_ref, control_class, payload, principal)
        if tool == "provider.resources.read":
            return self._read(provider_ref, control_class, payload, principal)
        if tool == "provider.resources.delete":
            return self._delete(provider_ref, control_class, payload, principal)
        raise ValueError("unsupported provider gateway tool")

    def _create(
        self,
        provider_ref: str,
        control_class: ControlClass,
        payload: Mapping[str, Any],
        principal: AccessIdentity,
    ) -> dict[str, Any]:
        expected = {"kind", "task_id", "effect_id", "idempotency_key", "title", "disposable", "files"}
        if control_class is ControlClass.MCP_EXCHANGE:
            expected.add("exchange_manifest")
        self._exact_keys(payload, expected)
        if payload["kind"] != "dataset":
            raise ValueError("provider create supports exact private datasets")
        files = self._files(payload["files"])
        exchange_manifest: ExchangeManifest | None = None
        if control_class is ControlClass.MCP_EXCHANGE:
            if payload["disposable"] is not True:
                raise PermissionError("exchange datasets require an authenticated creator and expiry cleanup")
            manifest_payload = payload["exchange_manifest"]
            if not isinstance(manifest_payload, Mapping):
                raise ValueError("exchange manifest must be an exact object")
            exchange_manifest = validate_exchange_manifest_for_mutation(
                manifest_payload,
                creator=principal.subject,
                provider_ref=provider_ref,
                provider_version=1,
                file_contents=files,
                now=self.ledger.clock.now(),
            )
            files[EXCHANGE_MANIFEST_PATH] = canonical_json_bytes(
                exchange_manifest.model_dump(mode="json")
            )
        arguments = {
            "content_tree_sha256": mapping_sha256(files),
            "control_class": control_class.value,
            "disposable": bool(payload["disposable"]),
        }
        intent = self._intent(payload, provider_ref, MutationAction.CREATE_DATASET, arguments=arguments)
        result = self.adapter.create_private_dataset(
            intent=intent,
            files=files,
            title=str(payload["title"]),
            control_class=control_class,
            disposable=bool(payload["disposable"]),
        )
        self._register_dataset(result, created_by=principal.subject, exchange_manifest=exchange_manifest)
        return self._dataset_response(result)

    def _version(
        self,
        provider_ref: str,
        control_class: ControlClass,
        payload: Mapping[str, Any],
        principal: AccessIdentity,
    ) -> dict[str, Any]:
        expected = {
            "kind", "task_id", "effect_id", "idempotency_key", "claim_sha256", "version_notes", "files"
        }
        if control_class is ControlClass.MCP_EXCHANGE:
            expected.add("exchange_manifest")
        self._exact_keys(payload, expected)
        if payload["kind"] != "dataset":
            raise ValueError("provider version supports exact private datasets")
        claim = self._claim(provider_ref, control_class, str(payload["claim_sha256"]), ProviderKind.DATASET)
        files = self._files(payload["files"])
        exchange_manifest: ExchangeManifest | None = None
        if control_class is ControlClass.MCP_EXCHANGE:
            self._authorize_exchange_access(claim, principal, action="mutate")
            manifest_payload = payload["exchange_manifest"]
            if not isinstance(manifest_payload, Mapping):
                raise ValueError("exchange manifest must be an exact object")
            exchange_manifest = validate_exchange_manifest_for_mutation(
                manifest_payload,
                creator=principal.subject,
                provider_ref=provider_ref,
                provider_version=claim.provider_version + 1,
                file_contents=files,
                now=self.ledger.clock.now(),
            )
            files[EXCHANGE_MANIFEST_PATH] = canonical_json_bytes(
                exchange_manifest.model_dump(mode="json")
            )
        notes = str(payload["version_notes"])
        arguments = {
            "content_tree_sha256": mapping_sha256(files),
            "previous_version": claim.provider_version,
            "version_notes_sha256": hashlib.sha256(notes.encode()).hexdigest(),
        }
        intent = self._intent(
            payload,
            provider_ref,
            MutationAction.VERSION_DATASET,
            arguments=arguments,
            expected_fingerprint=claim.fingerprint,
        )
        lease = self._authorize_mutation(claim, principal, ProviderAction.CREATE_VERSION, intent.effect_id)
        try:
            result = self.adapter.create_private_dataset_version(
                intent=intent, claim=claim, files=files, version_notes=notes
            )
        finally:
            self.ledger.release_resource_lease(str(lease.lease_id), principal.subject, lease.fencing_token)
        self._register_dataset(result, created_by=principal.subject, exchange_manifest=exchange_manifest)
        return self._dataset_response(result)

    def _run(
        self,
        provider_ref: str,
        control_class: ControlClass,
        payload: Mapping[str, Any],
        principal: AccessIdentity,
    ) -> dict[str, Any]:
        required = {
            "kind", "task_id", "effect_id", "idempotency_key", "task_run_id", "title",
            "code_file", "kernel_type", "language", "source_utf8", "dataset_inputs", "disposable",
        }
        optional = {"timeout_seconds"}
        if set(payload) - optional != required or not required <= set(payload):
            raise ValueError("provider run payload fields differ from the exact contract")
        if payload["kind"] != "notebook" or control_class is not ControlClass.MCP_MANAGED:
            raise PermissionError("provider run is limited to mcp_managed notebooks")
        source = str(payload["source_utf8"]).encode("utf-8")
        if len(source) > 256 * 1024:
            raise ValueError("provider notebook source exceeds the bounded contract")
        task_id = UUID(str(payload["task_id"]))
        task_run_id = UUID(str(payload["task_run_id"]))
        source_sha256 = hashlib.sha256(source).hexdigest()
        sources, input_claims = self._authorize_run_inputs(
            notebook_ref=provider_ref,
            task_id=task_id,
            value=payload["dataset_inputs"],
            principal=principal,
        )
        arguments = {
            "task_run_id": str(task_run_id),
            "source_sha256": source_sha256,
            "dataset_sources": sources,
            "dataset_inputs": input_claims,
            "control_class": control_class.value,
            "disposable": bool(payload["disposable"]),
        }
        intent = self._intent(payload, provider_ref, MutationAction.PUSH_NOTEBOOK, arguments=arguments)
        result = self.adapter.push_private_notebook(
            intent=intent,
            task_run_id=task_run_id,
            source=source,
            title=str(payload["title"]),
            code_file=str(payload["code_file"]),
            kernel_type=str(payload["kernel_type"]),
            language=str(payload["language"]),
            control_class=control_class,
            disposable=bool(payload["disposable"]),
            dataset_sources=sources,
            timeout_seconds=(int(payload["timeout_seconds"]) if "timeout_seconds" in payload else None),
        )
        metadata = {
            "claim": result.claim.model_dump(mode="json"),
            "source": result.source.model_dump(mode="json"),
            "run": result.run.model_dump(mode="json"),
        }
        self.ledger.register_provider_resource(
            provider="kaggle",
            resource_ref=result.source.provider_ref,
            resource_kind=ProviderKind.NOTEBOOK.value,
            source_identity=str(result.claim.task_id),
            source_version=str(result.source.source_version),
            control_class=result.claim.control_class.value,
            private=True,
            state="running",
            metadata=metadata,
        )
        return {
            "operation_id": str(result.effect.operation_id),
            "effect_id": str(result.effect.effect_id),
            "task_id": str(result.claim.task_id),
            "task_run_id": str(result.run.task_run_id),
            "provider_ref": result.source.provider_ref,
            "provider_run_ref": result.run.provider_run_ref,
            "provider_kernel_id": result.run.provider_kernel_id,
            "source_version": result.source.source_version,
            "source_sha256": result.source.source_sha256,
            "fingerprint": result.source.fingerprint.model_dump(mode="json"),
            "claim_sha256": result.claim.claim_sha256,
            "outcome": result.effect.outcome.value,
            "attempts": result.effect.attempts,
        }

    def _read(
        self,
        provider_ref: str,
        control_class: ControlClass,
        payload: Mapping[str, Any],
        principal: AccessIdentity,
    ) -> dict[str, Any]:
        self._exact_keys(payload, {"kind", "claim_sha256"})
        kind = ProviderKind(str(payload["kind"]))
        claim = self._claim(provider_ref, control_class, str(payload["claim_sha256"]), kind)
        resource = self._resource(claim, state="recorded")
        if control_class is ControlClass.MCP_EXCHANGE:
            self._authorize_exchange_access(claim, principal, action="read")
        self.policy.authorize(
            resource,
            ProviderAction.DOWNLOAD if kind is ProviderKind.DATASET else ProviderAction.READ_SOURCE,
            principal=principal.subject,
            now=self.ledger.clock.now(),
        )
        metadata = self.ledger.provider_resource(provider_ref, str(claim.provider_version))
        if metadata is None:
            raise PermissionError("provider resource has no exact registered projection")
        if kind is ProviderKind.DATASET:
            identity = self.adapter.read_private_dataset(
                provider_ref=provider_ref, version=claim.provider_version
            )
            return {
                "claim_sha256": claim.claim_sha256,
                "task_id": str(claim.task_id),
                "provider_ref": provider_ref,
                "provider_version": identity.version,
                "package_sha256": identity.package_sha256,
                "fingerprint": identity.fingerprint.model_dump(mode="json"),
                "private": True,
            }
        source = self.adapter.read_private_notebook_source(
            provider_ref=provider_ref,
            source_version=claim.provider_version,
            expected_source_sha256=None,
        )
        run = dict(metadata["metadata"].get("run", {}))
        return {
            "claim_sha256": claim.claim_sha256,
            "task_id": str(claim.task_id),
            "task_run_id": run.get("task_run_id"),
            "provider_ref": provider_ref,
            "provider_run_ref": run.get("provider_run_ref"),
            "provider_kernel_id": run.get("provider_kernel_id"),
            "source_version": source.source_version,
            "source_sha256": source.source_sha256,
            "fingerprint": source.fingerprint.model_dump(mode="json"),
            "private": True,
        }

    def _delete(
        self,
        provider_ref: str,
        control_class: ControlClass,
        payload: Mapping[str, Any],
        principal: AccessIdentity,
    ) -> dict[str, Any]:
        self._exact_keys(
            payload, {"kind", "task_id", "effect_id", "idempotency_key", "claim_sha256"}
        )
        kind = ProviderKind(str(payload["kind"]))
        claim = self._claim(
            provider_ref,
            control_class,
            str(payload["claim_sha256"]),
            kind,
            require_disposable=True,
        )
        exchange_access: Mapping[str, Any] | None = None
        if control_class is ControlClass.MCP_EXCHANGE:
            exchange_access = self._authorize_exchange_access(claim, principal, action="delete")
        arguments = {"claim_sha256": claim.claim_sha256, "provider_version": claim.provider_version}
        action = MutationAction.DELETE_DATASET if kind is ProviderKind.DATASET else MutationAction.DELETE_NOTEBOOK
        intent = self._intent(
            payload,
            provider_ref,
            action,
            arguments=arguments,
            expected_fingerprint=claim.fingerprint,
        )
        self.ledger.persist_provider_effect_intent(intent.model_dump(mode="json"))
        receipt_payload = self.ledger.latest_provider_effect_receipt(str(intent.effect_id))
        result = ProviderEffectReceipt.model_validate(receipt_payload) if receipt_payload else None
        if result is not None:
            if (
                result.operation_id != intent.operation_id
                or result.action != intent.action
                or result.provider_ref != intent.provider_ref
            ):
                raise PermissionError("provider delete receipt differs from the exact cleanup intent")
            if result.outcome not in {
                EffectOutcome.APPLIED,
                EffectOutcome.ALREADY_APPLIED,
                EffectOutcome.NOT_FOUND,
            }:
                result = None
        if result is None:
            lease = self._authorize_mutation(claim, principal, ProviderAction.DELETE, intent.effect_id)
            try:
                result = self.adapter.delete_task_created_resource(intent=intent, claim=claim)
            finally:
                self.ledger.release_resource_lease(str(lease.lease_id), principal.subject, lease.fencing_token)
        response = {
            "operation_id": str(result.operation_id),
            "effect_id": str(result.effect_id),
            "task_id": str(claim.task_id),
            "provider_ref": provider_ref,
            "claim_sha256": claim.claim_sha256,
            "action": result.action.value,
            "outcome": result.outcome.value,
            "attempts": result.attempts,
        }
        if exchange_access is not None:
            retention_receipt = {
                "contract_version": "mcp-exchange-cleanup-retention.v1",
                "package_id": str(exchange_access.get("package_id", "")),
                "manifest_sha256": str(exchange_access.get("manifest_sha256", "")),
                "expires_at": str(exchange_access.get("expires_at", "")),
                "maximum_ttl_seconds": int(MAX_EXCHANGE_TTL.total_seconds()),
                "operation_id": str(result.operation_id),
                "effect_id": str(result.effect_id),
                "claim_sha256": claim.claim_sha256,
                "resource_state": "absent",
            }
            response["retention_receipt"] = retention_receipt
            response["retention_receipt_sha256"] = hashlib.sha256(
                canonical_json_bytes(retention_receipt)
            ).hexdigest()
        return response

    def _authorize_mutation(
        self,
        claim: TaskResourceClaim,
        principal: AccessIdentity,
        action: ProviderAction,
        effect_id: UUID,
    ) -> ResourceLease:
        now = self.ledger.clock.now()
        # Leases fence concurrent attempts, not logical idempotency. A released
        # lease must not prevent the same effect from reconciling an UNCERTAIN
        # provider outcome, so every bounded attempt receives a fresh identity.
        lease_id = str(uuid4())
        record = self.ledger.acquire_resource_lease(
            lease_id=lease_id,
            resource_kind=claim.kind.value,
            resource_ref=claim.provider_ref,
            holder_id=principal.subject,
            lease_until=now + timedelta(minutes=2),
        )
        lease = ResourceLease(
            lease_id=UUID(record.lease_id),
            provider_ref=record.resource_ref,
            principal=record.holder_id,
            fencing_token=record.epoch,
            acquired_at=record.acquired_at,
            expires_at=record.lease_until,
        )
        try:
            self.policy.authorize(
                self._resource(claim, state="recorded"),
                action,
                principal=principal.subject,
                now=now,
                expected_fingerprint=claim.fingerprint,
                lease=lease,
            )
        except Exception:
            self.ledger.release_resource_lease(str(lease.lease_id), principal.subject, lease.fencing_token)
            raise
        return lease

    def _claim(
        self,
        provider_ref: str,
        control_class: ControlClass,
        claim_sha256: str,
        kind: ProviderKind,
        require_disposable: bool = False,
    ) -> TaskResourceClaim:
        payload = self.ledger.provider_resource_claim(claim_sha256)
        if payload is None:
            raise PermissionError("provider resource has no exact registered claim")
        claim = TaskResourceClaim.model_validate(payload)
        if (
            claim.provider_ref != provider_ref
            or claim.control_class is not control_class
            or claim.kind is not kind
            or (require_disposable and not claim.disposable)
        ):
            raise PermissionError("provider claim does not authorize this exact resource operation")
        return claim

    def _resource(self, claim: TaskResourceClaim, *, state: str) -> ProviderResource:
        return ProviderResource(
            provider="kaggle",
            provider_ref=claim.provider_ref,
            kind=claim.kind,
            owner=claim.provider_ref.split("/", 1)[0],
            origin=Origin.MCP,
            control_class=claim.control_class,
            private=True,
            fingerprint=claim.fingerprint,
            state=state,
            observed_at=self.ledger.clock.now(),
        )

    def _intent(
        self,
        payload: Mapping[str, Any],
        provider_ref: str,
        action: MutationAction,
        *,
        arguments: Mapping[str, Any],
        expected_fingerprint: Any = None,
    ) -> ProviderEffectIntent:
        task_id = UUID(str(payload["task_id"]))
        effect_id = UUID(str(payload["effect_id"]))
        operation_id = uuid5(
            NAMESPACE_URL,
            f"mcp-provider:{task_id}:{payload['idempotency_key']}",
        )
        return ProviderEffectIntent.create(
            operation_id=operation_id,
            effect_id=effect_id,
            idempotency_key=str(payload["idempotency_key"]),
            task_id=task_id,
            action=action,
            provider_ref=provider_ref,
            expected_fingerprint=expected_fingerprint,
            arguments=arguments,
            requested_at=self.ledger.clock.now(),
        )

    def _register_dataset(
        self,
        result: Any,
        *,
        created_by: str,
        exchange_manifest: ExchangeManifest | None = None,
    ) -> None:
        metadata = {
            "claim": result.claim.model_dump(mode="json"),
            "identity": result.identity.model_dump(mode="json"),
            "mcp_access": {"created_by": created_by},
        }
        if exchange_manifest is not None:
            metadata["exchange_access"] = {
                "contract_version": exchange_manifest.contract_version,
                "package_id": str(exchange_manifest.package_id),
                "manifest_sha256": exchange_manifest.manifest_sha256,
                "created_at": exchange_manifest.created_at.isoformat(),
                "expires_at": exchange_manifest.expires_at.isoformat(),
                "created_by": exchange_manifest.created_by,
                "target_project": exchange_manifest.target_project,
                "intended_recipients": list(exchange_manifest.intended_recipients),
                "sensitivity": exchange_manifest.sensitivity,
            }
        self.ledger.register_provider_resource(
            provider="kaggle",
            resource_ref=result.identity.provider_ref,
            resource_kind=ProviderKind.DATASET.value,
            source_identity=str(result.claim.task_id),
            source_version=str(result.identity.version),
            control_class=result.claim.control_class.value,
            private=True,
            state="complete",
            metadata=metadata,
        )

    def _authorize_run_inputs(
        self,
        *,
        notebook_ref: str,
        task_id: UUID,
        value: Any,
        principal: AccessIdentity,
    ) -> tuple[tuple[str, ...], tuple[dict[str, Any], ...]]:
        """Resolve only exact, claim-bound private Dataset inputs.

        Kaggle attaches Dataset sources with the configured account's
        credentials. A caller-supplied slug/latest would therefore bypass the
        control-class boundary. Every source is resolved to an already
        registered numeric version before the adapter sees it.
        """

        if not isinstance(value, list) or len(value) > 16:
            raise PermissionError("provider run requires a bounded exact registered input claim list")
        notebook_owner = notebook_ref.split("/", 1)[0]
        observed: set[tuple[str, int]] = set()
        sources: list[str] = []
        normalized: list[dict[str, Any]] = []
        for item in value:
            if not isinstance(item, Mapping) or set(item) != {
                "resource_ref",
                "provider_version",
                "claim_sha256",
                "control_class",
            }:
                raise PermissionError("provider run requires exact registered input claims")
            resource_ref = item["resource_ref"]
            provider_version = item["provider_version"]
            claim_sha256 = item["claim_sha256"]
            control_class_value = item["control_class"]
            if (
                not isinstance(resource_ref, str)
                or len(resource_ref.split("/")) != 2
                or not isinstance(provider_version, int)
                or isinstance(provider_version, bool)
                or provider_version < 1
                or not isinstance(claim_sha256, str)
                or not isinstance(control_class_value, str)
            ):
                raise PermissionError("provider run input must bind an exact numeric registered claim")
            try:
                input_class = ControlClass(control_class_value)
            except ValueError as exc:
                raise PermissionError("provider run input control class is forbidden") from exc
            if input_class not in {ControlClass.MCP_MANAGED, ControlClass.MCP_EXCHANGE}:
                raise PermissionError("provider run input control class is forbidden")
            claim = self._claim(
                resource_ref,
                input_class,
                claim_sha256,
                ProviderKind.DATASET,
            )
            if claim.provider_version != provider_version:
                raise PermissionError("provider run input claim does not bind the exact numeric version")
            projection = self.ledger.provider_resource(resource_ref, str(provider_version))
            if (
                projection is None
                or projection.get("provider") != "kaggle"
                or projection.get("resource_kind") != ProviderKind.DATASET.value
                or projection.get("source_identity") != str(claim.task_id)
                or projection.get("source_version") != str(provider_version)
                or projection.get("control_class") != input_class.value
                or projection.get("private") is not True
            ):
                raise PermissionError("provider run input lacks an exact registered projection")
            if input_class is ControlClass.MCP_MANAGED:
                access = projection.get("metadata", {}).get("mcp_access")
                if (
                    claim.task_id != task_id
                    or resource_ref.split("/", 1)[0] != notebook_owner
                    or not isinstance(access, Mapping)
                    or access.get("created_by") != principal.subject
                ):
                    raise PermissionError("mcp_managed run input requires the same task and owner")
            else:
                self._authorize_exchange_access(claim, principal, action="read")
            self.policy.authorize(
                self._resource(claim, state=str(projection.get("state", "recorded"))),
                ProviderAction.DOWNLOAD,
                principal=principal.subject,
                now=self.ledger.clock.now(),
            )
            source_key = (resource_ref, provider_version)
            if source_key in observed:
                raise PermissionError("provider run input claims must be unique")
            observed.add(source_key)
            sources.append(f"{resource_ref}/{provider_version}")
            normalized.append(
                {
                    "resource_ref": resource_ref,
                    "provider_version": provider_version,
                    "claim_sha256": claim.claim_sha256,
                    "control_class": input_class.value,
                }
            )
        return tuple(sources), tuple(normalized)

    def _authorize_exchange_access(
        self,
        claim: TaskResourceClaim,
        principal: AccessIdentity,
        *,
        action: str,
    ) -> Mapping[str, Any]:
        resource = self.ledger.provider_resource(claim.provider_ref, str(claim.provider_version))
        access = resource.get("metadata", {}).get("exchange_access") if resource else None
        if not isinstance(access, Mapping):
            raise PermissionError("exchange resource lacks an exact access manifest")
        expires_at = str(access.get("expires_at", ""))
        try:
            expiry = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
        except ValueError as exc:
            raise PermissionError("exchange access expiry is invalid") from exc
        expired = expiry.tzinfo is None or self.ledger.clock.now() >= expiry
        creator = str(access.get("created_by", ""))
        recipients = access.get("intended_recipients")
        if action == "read":
            if expired:
                raise PermissionError("exchange resource has expired")
            if not isinstance(recipients, list) or principal.subject not in recipients:
                raise PermissionError("principal is not an intended exchange recipient")
        elif action == "mutate":
            if expired:
                raise PermissionError("exchange resource has expired")
            if principal.subject != creator:
                raise PermissionError("only the exchange creator may mutate or delete the package")
        elif action == "delete":
            # Expiry removes read/mutation authority but must never make the
            # exact creator's idempotent retention cleanup impossible.
            if principal.subject != creator:
                raise PermissionError("only the exchange creator may mutate or delete the package")
        else:  # pragma: no cover - closed internal call set
            raise ValueError("unsupported exchange access action")
        return access

    @staticmethod
    def _dataset_response(result: Any) -> dict[str, Any]:
        return {
            "operation_id": str(result.effect.operation_id),
            "effect_id": str(result.effect.effect_id),
            "task_id": str(result.claim.task_id),
            "provider_ref": result.identity.provider_ref,
            "provider_version": result.identity.version,
            "package_sha256": result.identity.package_sha256,
            "fingerprint": result.identity.fingerprint.model_dump(mode="json"),
            "claim_sha256": result.claim.claim_sha256,
            "outcome": result.effect.outcome.value,
            "attempts": result.effect.attempts,
        }

    @staticmethod
    def _files(value: Any) -> dict[str, bytes]:
        if not isinstance(value, Mapping) or not value or len(value) > 100:
            raise ValueError("provider files must be a bounded path/string object")
        result: dict[str, bytes] = {}
        total = 0
        for path, content in value.items():
            if not isinstance(path, str) or not isinstance(content, str):
                raise ValueError("provider file paths and contents must be UTF-8 strings")
            encoded = content.encode("utf-8")
            total += len(encoded)
            if total > 256 * 1024:
                raise ValueError("provider files exceed the bounded request contract")
            result[path] = encoded
        return result

    @staticmethod
    def _exact_keys(payload: Mapping[str, Any], expected: set[str]) -> None:
        if set(payload) != expected:
            raise ValueError("provider payload fields differ from the exact contract")


class LedgerControlReader(ControlPlaneReader):
    def __init__(
        self,
        ledger: ControlLedger,
        *,
        deployed_commit: str | None = None,
        write_gate: LedgerWriteGate | None = None,
        provider_gateway: KaggleMCPProviderGateway | None = None,
        acceptance_evidence: AcceptanceEvidenceController | None = None,
    ) -> None:
        if deployed_commit is not None and (
            len(deployed_commit) != 40 or any(character not in "0123456789abcdef" for character in deployed_commit)
        ):
            raise ValueError("deployed commit must be an exact lowercase Git SHA")
        self.ledger = ledger
        self.deployed_commit = deployed_commit
        self.write_gate = write_gate
        self.provider_gateway = provider_gateway
        self.acceptance_evidence = acceptance_evidence or (
            AcceptanceEvidenceController(ledger, provider_gateway) if provider_gateway is not None else None
        )

    def invoke_control(self, tool: str, arguments: dict[str, Any], principal: AccessIdentity) -> dict[str, Any]:
        if tool == "platform.status":
            return {
                "control_plane_ready": True,
                "control_ledger": "sqlite-wal",
                **({"deployed_commit": self.deployed_commit} if self.deployed_commit else {}),
            }
        if tool == "data.change.status" and self.write_gate is not None:
            return self.write_gate.write_status(str(arguments.get("operation_id", "")), principal)
        if tool in {"operation.get", "data.change.status"}:
            record = self.ledger.get_operation(str(arguments.get("operation_id", "")))
            return (
                {"found": False}
                if record is None
                else {
                    "found": True,
                    "operation_id": record.operation_id,
                    "operation_kind": record.operation_kind,
                    "state": record.state,
                    "updated_at": record.updated_at.isoformat(),
                }
            )
        if tool == "checkpoint.status":
            head = self.ledger.checkpoint_head("postgres-master")
            current = (
                self.ledger.checkpoint_candidate(head.current_checkpoint_id)
                if head and head.current_checkpoint_id
                else None
            )
            previous = (
                self.ledger.checkpoint_candidate(head.previous_checkpoint_id)
                if head and head.previous_checkpoint_id
                else None
            )
            return {
                "current_checkpoint_id": head.current_checkpoint_id if head else None,
                "previous_checkpoint_id": head.previous_checkpoint_id if head else None,
                "current_exact_version_ref": current.get("version_ref") if current else None,
                "previous_exact_version_ref": previous.get("version_ref") if previous else None,
                "verified_at": current.get("verified_at") if current else None,
                "current": self._checkpoint_projection(current),
                "previous": self._checkpoint_projection(previous),
                "generation": head.generation if head else 0,
                "freshness": "absent" if head is None else "recorded",
            }
        if tool == "connector.coverage":
            rows = self.ledger.connector_coverage_metadata(limit=100)
            if rows:
                complete = sum(row["state"] == "COMPLETE" for row in rows)
                observed = sorted(str(row["observed_at"]) for row in rows)
                return {
                    "available": True,
                    "bounded": True,
                    "source": "control-ledger-metadata",
                    "connector_count": len(rows),
                    "complete_count": complete,
                    "oldest_observed_at": observed[0],
                    "newest_observed_at": observed[-1],
                }
            # Connector business state belongs to the Kaggle PostgreSQL primary;
            # absence of its metadata heartbeat must not trigger a row query.
            return {
                "available": False,
                "bounded": True,
                "source": "control-ledger-metadata",
                "blocker_code": "CONNECTOR_METADATA_HEARTBEAT_ABSENT",
                "connector_count": 0,
                "complete_count": 0,
            }
        if tool == "provider.protected_resource.probe":
            resource_ref = str(arguments.get("resource_ref", ""))
            rows = self.ledger.list_provider_resources(limit=500)
            resource = next((row for row in rows if row["resource_ref"] == resource_ref), None)
            if resource is None:
                return {"evaluated": False, "denied": False, "mutation_attempted": False}
            model = ProviderResource.model_validate(
                {
                    "provider": resource["provider"],
                    "provider_ref": resource["resource_ref"],
                    "kind": resource["resource_kind"],
                    "owner": resource["resource_ref"].split("/", 1)[0],
                    "origin": "orchestrator",
                    "control_class": resource["control_class"],
                    "private": resource["private"],
                    "fingerprint": None,
                    "state": resource["state"],
                    "observed_at": resource["observed_at"],
                    "workload": "scheduled-acceptance",
                }
            )
            try:
                ProviderPolicy().authorize(
                    model,
                    ProviderAction.DELETE,
                    principal=principal.subject,
                    now=self.ledger.clock.now(),
                )
            except PolicyDenied as exc:
                return {
                    "evaluated": True,
                    "protected": True,
                    "denied": exc.code == "PROTECTED_RESOURCE_DENIED",
                    "reason_code": exc.code,
                    "mutation_attempted": False,
                }
            return {"evaluated": True, "protected": True, "denied": False, "mutation_attempted": False}
        if tool in {"checkpoint.restore.request", "master.rotation.request"}:
            return self._acceptance_action_request(tool, arguments, principal)
        if tool == "provider.resources.status":
            limit = min(int(arguments.get("limit", 100)), 100)
            resources = self.ledger.list_provider_resources(limit=limit)
            return {"resources": resources, "count": len(resources), "bounded": True}
        if tool == "runtime.events.history":
            if set(arguments) not in (
                {"run_id", "attempt_id", "epoch"},
                {"run_id", "attempt_id", "epoch", "limit"},
            ):
                raise ValueError("runtime event history arguments differ from the exact contract")
            limit = min(int(arguments.get("limit", 100)), 200)
            events = self.ledger.runtime_event_history(
                run_id=str(arguments.get("run_id", "")),
                attempt_id=str(arguments.get("attempt_id", "")),
                epoch=int(arguments.get("epoch", 0)),
                limit=limit,
            )
            return {"events": events, "count": len(events), "bounded": True}
        if tool == "provider.acceptance.claim.get":
            if set(arguments) != {"scenario_id", "task_id"}:
                raise ValueError("acceptance claim arguments differ from the exact contract")
            if self.acceptance_evidence is None:
                raise PermissionError("provider acceptance evidence controller is not configured")
            return self.acceptance_evidence.claim_get(
                str(arguments.get("scenario_id", "")), str(arguments.get("task_id", ""))
            )
        if tool in {
            "provider.acceptance.dataset.lifecycle",
            "provider.acceptance.notebook.lifecycle",
            "provider.acceptance.claim.cleanup",
        }:
            if self.acceptance_evidence is None:
                raise PermissionError("provider acceptance evidence controller is not configured")
            if tool == "provider.acceptance.dataset.lifecycle":
                return self.acceptance_evidence.dataset_lifecycle(arguments, principal)
            if tool == "provider.acceptance.notebook.lifecycle":
                return self.acceptance_evidence.notebook_lifecycle(arguments, principal)
            return self.acceptance_evidence.cleanup(arguments, principal)
        if tool in {
            "provider.resources.create",
            "provider.resources.version",
            "provider.resources.run",
            "provider.resources.read",
            "provider.resources.delete",
        }:
            if self.provider_gateway is None:
                raise PermissionError("provider MCP gateway is not configured")
            return self.provider_gateway.invoke(tool, arguments, principal)
        if tool == "embedding.coverage":
            return {"e5": {"coverage": 0.0}, "bge_m3": {"coverage": 0.0}, "master_state": "ABSENT"}
        if tool == "embedding.production.capabilities":
            service = self.ledger.resolve_service("postgres-master")
            operation = (
                self.ledger.operation_for_attempt(service.run_id, service.attempt_id)
                if service is not None
                else None
            )
            head = self.ledger.checkpoint_head("postgres-master")
            if (
                service is None
                or service.state != "ACTIVE"
                or service.master_instance_id is None
                or service.canonical_revision is None
                or operation is None
                or operation.state != "ACTIVE"
                or head is None
                or head.current_checkpoint_id is None
            ):
                return {
                    "admission_ready": False,
                    "master_state": "ACTIVE" if service is not None else "ABSENT",
                    "blocker_code": "EMBEDDING_PRODUCTION_PREREQUISITE_ABSENT",
                }
            from my_data_hub.embeddings.production import (
                WORKER_ASSETS,
                EmbeddingProductionAdmissionBinding,
                EmbeddingProductionCapabilities,
            )

            return EmbeddingProductionCapabilities(
                interface="mcp_observer",
                admission_ready=True,
                binding=EmbeddingProductionAdmissionBinding(
                    master_instance_id=service.master_instance_id,
                    run_id=service.run_id,
                    attempt_id=service.attempt_id,
                    epoch=service.epoch,
                    canonical_revision=service.canonical_revision,
                    blogger_checkpoint_id=head.current_checkpoint_id,
                ),
                execution_location="active_kaggle_master",
                request_acceptance="durable_idempotent_ledger.v1",
                stage_contract="transactional_import_then_checkpoint.v1",
                completion_evidence="terminal_request_status_and_closure_receipt_only",
                worker_assets=WORKER_ASSETS,
            ).model_dump(mode="json", exclude_none=True)
        raise ValueError(f"unsupported bounded control tool: {tool}")

    def _checkpoint_projection(self, candidate: dict[str, Any] | None) -> dict[str, Any] | None:
        if candidate is None:
            return None
        source = self.ledger.get_operation(str(candidate["operation_id"]))
        return {
            "checkpoint_id": candidate["checkpoint_id"],
            "exact_version_ref": candidate["version_ref"],
            "manifest_sha256": candidate["manifest_sha256"],
            "verified_at": candidate["verified_at"],
            "status": candidate["status"],
            "source_epoch": candidate["epoch"],
            "source_state": source.state if source else None,
            "canonical_revision": (
                candidate["manifest"].get("canonical_revision") if isinstance(candidate.get("manifest"), dict) else None
            ),
        }

    def _acceptance_action_request(
        self,
        tool: str,
        arguments: dict[str, Any],
        principal: AccessIdentity,
    ) -> dict[str, Any]:
        """Persist an exact request for the production reconciliation worker."""

        request_key = str(arguments.get("idempotency_key", ""))
        if not 8 <= len(request_key) <= 200 or any(ord(char) < 32 for char in request_key):
            raise ValueError("acceptance request idempotency_key is invalid")
        target = "current"
        if tool == "checkpoint.restore.request":
            target = str(arguments.get("target", ""))
            if target not in {"current", "previous"}:
                raise ValueError("restore target must be current or previous")
        timeout = arguments.get("timeout_seconds")
        if not isinstance(timeout, int) or isinstance(timeout, bool) or not 60 <= timeout <= 3600:
            raise ValueError("acceptance request timeout_seconds must be between 60 and 3600")
        operation_kind = (
            "checkpoint_restore_smoke" if tool == "checkpoint.restore.request" else "forced_master_rotation"
        )
        idempotency_key = f"scheduled-acceptance:{tool}:{request_key}"
        existing = self.ledger.get_operation_by_idempotency_key(idempotency_key)
        if existing is not None:
            stored = existing.identity
            replay_fields: dict[str, object] = {
                "tool": tool,
                "target": target,
                "checkpoint_id": arguments.get("checkpoint_id"),
                "exact_version_ref": arguments.get("exact_version_ref"),
                "timeout_seconds": timeout,
            }
            if tool == "master.rotation.request":
                replay_fields["expected_active_epoch"] = arguments.get("expected_active_epoch")
                replay_fields["expected_canonical_revision"] = arguments.get("expected_canonical_revision")
            if (
                existing.operation_kind != operation_kind
                or stored.get("principal") != principal.subject
                or any(stored.get(key) != value for key, value in replay_fields.items())
            ):
                raise ValueError("acceptance request identity was reused for different intent")
            return {
                "accepted": True,
                "duplicate": True,
                "request_sha256": stored.get("request_sha256"),
                "operation_id": existing.operation_id,
                "state": existing.state,
                "target": stored.get("target"),
                "checkpoint_id": stored.get("checkpoint_id"),
                "exact_version_ref": stored.get("exact_version_ref"),
                "head_generation": stored.get("head_generation"),
                "execution_supported": True,
            }
        head = self.ledger.checkpoint_head("postgres-master")
        if head is None or head.current_checkpoint_id is None:
            raise ValueError("checkpoint HEAD is absent")
        checkpoint_id = head.previous_checkpoint_id if target == "previous" else head.current_checkpoint_id
        if checkpoint_id is None:
            raise ValueError("requested checkpoint generation is absent")
        candidate = self.ledger.checkpoint_candidate(checkpoint_id)
        if candidate is None or candidate.get("status") != "VERIFIED":
            raise ValueError("requested checkpoint is not verified")
        exact_version = str(candidate.get("version_ref") or "")
        if arguments.get("checkpoint_id") != checkpoint_id or arguments.get("exact_version_ref") != exact_version:
            raise ValueError("request does not bind the exact checkpoint HEAD generation")
        rotation_binding: tuple[object, object, object] | None = None
        if tool == "master.rotation.request":
            expected_epoch = arguments.get("expected_active_epoch")
            manifest = candidate.get("manifest")
            expected_revision = arguments.get("expected_canonical_revision")
            if not isinstance(manifest, dict) or expected_revision != manifest.get("canonical_revision"):
                raise ValueError("rotation request does not bind the checkpoint canonical revision")
            source_operation = self.ledger.get_operation(str(candidate.get("operation_id", "")))
            source_identity = source_operation.identity if source_operation is not None else {}
            rotation_binding = (expected_epoch, source_operation, source_identity)
        intent = {
            "tool": tool,
            "target": target,
            "checkpoint_id": checkpoint_id,
            "exact_version_ref": exact_version,
            "head_generation": head.generation,
            "timeout_seconds": timeout,
        }
        if tool == "master.rotation.request":
            intent["expected_active_epoch"] = arguments["expected_active_epoch"]
            intent["expected_canonical_revision"] = arguments["expected_canonical_revision"]
        digest = hashlib.sha256(
            json.dumps(intent, sort_keys=True, separators=(",", ":")).encode() + b":" + request_key.encode()
        ).hexdigest()
        if tool == "master.rotation.request":
            assert rotation_binding is not None
            expected_epoch, source_operation, source_identity = rotation_binding
            service = self.ledger.resolve_service("postgres-master")
            if (
                service is not None
                or source_operation is None
                or source_operation.state != "STOPPED"
                or expected_epoch != candidate.get("epoch")
                or expected_epoch != source_identity.get("epoch")
                or candidate.get("master_instance_id") != source_identity.get("master_instance_id")
            ):
                raise ValueError("rotation requires the exact checkpoint source master to be durably stopped")
        if not self.ledger.acceptance_consumer_available():
            return {
                "accepted": False,
                "duplicate": False,
                "request_sha256": digest,
                "operation_id": None,
                "state": "BLOCKED",
                "target": target,
                "checkpoint_id": checkpoint_id,
                "exact_version_ref": exact_version,
                "head_generation": head.generation,
                "execution_supported": False,
                "blocker_code": "ACCEPTANCE_CONSUMER_OR_PROVIDER_UNAVAILABLE",
            }
        operation, created = self.ledger.ensure_operation(
            operation_id=digest,
            idempotency_key=idempotency_key,
            operation_kind=operation_kind,
            intent=intent,
            initial_state="REQUESTED",
            identity={"principal": principal.subject, "request_sha256": digest, **intent},
        )
        return {
            "accepted": True,
            "duplicate": not created,
            "request_sha256": digest,
            "operation_id": operation.operation_id,
            "state": operation.state,
            "target": target,
            "checkpoint_id": checkpoint_id,
            "exact_version_ref": exact_version,
            "head_generation": head.generation,
            "execution_supported": True,
        }


class LedgerMasterResolver(MasterResolver):
    def __init__(self, ledger: ControlLedger) -> None:
        self.ledger = ledger

    def resolve_master(self, principal: AccessIdentity) -> MasterSnapshot:
        service = self.ledger.resolve_service("postgres-master")
        if service is not None:
            provider_run_ref: str | None = None
            provider_kernel_id: int | None = None
            operation = self.ledger.operation_for_attempt(service.run_id, service.attempt_id)
            trigger = (
                self.ledger.get_effect_by_idempotency_key(f"{operation.operation_id}:trigger_run")
                if operation is not None
                else None
            )
            exact_identity = trigger.receipt.get("exact_identity") if trigger and trigger.receipt else None
            if isinstance(exact_identity, Mapping):
                observed_ref = exact_identity.get("provider_run_ref")
                observed_kernel = exact_identity.get("provider_kernel_id")
                if isinstance(observed_ref, str) and observed_ref:
                    provider_run_ref = observed_ref
                if isinstance(observed_kernel, int):
                    provider_kernel_id = observed_kernel
            if provider_run_ref is None and trigger is not None and trigger.receipt is not None:
                observed_ref = trigger.receipt.get("exact_ref")
                if isinstance(observed_ref, str) and observed_ref:
                    provider_run_ref = observed_ref
            return MasterSnapshot(
                state=MasterState.ACTIVE,
                instance_id=service.master_instance_id,
                epoch=service.epoch,
                canonical_revision=service.canonical_revision,
                lease_expires_at=service.lease_until.isoformat(),
                provider_run_ref=provider_run_ref,
                provider_kernel_id=provider_kernel_id,
                capabilities=frozenset(service.capabilities),
            )
        operations = self.ledger.incomplete_operations("ensure_master")
        if operations:
            latest = operations[-1]
            try:
                state = MasterState(latest.state)
            except ValueError:
                state = MasterState.REQUESTED
            return MasterSnapshot(state=state, operation_id=latest.operation_id)
        request = self.ledger.latest_master_request()
        if request is not None:
            return MasterSnapshot(
                state=MasterState.REQUESTED,
                operation_id=str(request["operation_id"]),
            )
        return MasterSnapshot(state=MasterState.ABSENT)

    def ensure_master(self, principal: AccessIdentity, *, intent: str) -> EnsureMasterReceipt:
        key = f"mcp:{principal.subject}:{intent}"
        identity = MasterCoordinator.identity_for(key)
        request, created = self.ledger.request_master(
            request_id=hashlib.sha256(f"request:{key}".encode()).hexdigest(),
            idempotency_key=key,
            requested_by=principal.subject,
            intent=intent,
            operation_id=identity["operation_id"],
        )
        return EnsureMasterReceipt(
            operation_id=str(request["operation_id"]),
            state=MasterState.REQUESTED,
            duplicate=not created,
            intent=intent,
        )
