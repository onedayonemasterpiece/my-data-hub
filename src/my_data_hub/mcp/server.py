from __future__ import annotations

import asyncio
import inspect
import os
from dataclasses import dataclass, replace
from typing import Any, Literal
from uuid import UUID

from my_data_hub.auth.context import current_identity
from my_data_hub.auth.control import OAuthAuditEvent
from my_data_hub.auth.metadata import ProtectedResourceMetadata, protected_resource_metadata_url
from my_data_hub.config import ConfigurationError, Settings
from my_data_hub.mcp.catalog import TOOL_CONTRACTS, visible_tools
from my_data_hub.mcp.contracts import (
    ControlPlaneReader,
    MasterResolver,
    MasterSessionBroker,
    MCPAuditSink,
    WriteGate,
)
from my_data_hub.mcp.oauth import AccessIdentity, OAuthBearerValidator
from my_data_hub.mcp.provider_schemas import (
    ProviderCreatePayload,
    ProviderDeletePayload,
    ProviderDownloadPayload,
    ProviderListPayload,
    ProviderReadPayload,
    ProviderRunPayload,
    ProviderUploadChunkPayload,
    ProviderUploadReferencePayload,
    ProviderUploadStartPayload,
    ProviderVersionPayload,
)
from my_data_hub.mcp.region_talk_schemas import (
    RegionTalkCursor,
    RegionTalkFilter,
    RegionTalkIdempotencyKey,
    RegionTalkLimit,
    RegionTalkMaxBytes,
    RegionTalkPipelineController,
    RegionTalkQuery,
    RegionTalkSourceRevision,
    validate_region_talk_arguments,
)
from my_data_hub.mcp.service import HubService
from my_data_hub.mcp.sql_policy import BoundedSQLPolicy
from my_data_hub.mcp.transport import ToolSecurityMetadataMiddleware
from my_data_hub.showcase.gateway import ShowcaseGatewayClient
from my_data_hub.showcase.manager import ShowcaseManager
from my_data_hub.showcase.models import ShowcaseItem, ShowcaseView
from my_data_hub.workloads.bloggers.discovery import (
    SubmitDiscoveryBatch,
    validate_submit_discovery_batch,
)

_SHOWCASE_TOOL_NAMES = frozenset(
    {
        "showcase.list",
        "showcase.get_link",
        "showcase.get_source",
        "showcase.apply",
        "showcase.rebuild",
        "showcase.create_view",
        "showcase.rotate_link",
        "showcase.revoke_link",
    }
)

READER_PROFILE_TOOLS = frozenset(
    {
        "platform.status",
        "master.status",
        "operation.get",
        "checkpoint.status",
        "embedding.coverage",
        "embedding.production.capabilities",
        "provider.resources.status",
        "bloggers.list",
        "bloggers.get",
        "bloggers.search",
        "bloggers.statistics",
        "region_talk.inventory",
        "region_talk.articles.list",
        "region_talk.articles.get",
        "region_talk.articles.search",
        "region_talk.posts.list",
        "region_talk.posts.get",
        "region_talk.posts.search",
        "region_talk.queue.list",
        "region_talk.queue.summary",
        "region_talk.pipeline.status",
        "showcase.list",
    }
)

PROVIDER_ONLY_TOOLS = frozenset(
    {
        "platform.status",
        "provider.resources.status",
        "provider.resources.create",
        "provider.resources.version",
        "provider.resources.run",
        "provider.resources.read",
        "provider.resources.list",
        "provider.resources.download",
        "provider.inventory.live",
        "provider.resources.delete",
        "provider.upload.start",
        "provider.upload.put_chunk",
        "provider.upload.status",
        "provider.upload.finalize",
        "provider.upload.abort",
        "provider.acceptance.claim.get",
        "provider.acceptance.claim.cleanup",
    }
)
UNIFIED_BOOTSTRAP_TOOLS = PROVIDER_ONLY_TOOLS | READER_PROFILE_TOOLS


def oauth_resource_metadata_url(resource: str) -> str:
    """Backward-compatible name for the RFC 9728 path-derived URL."""

    try:
        return protected_resource_metadata_url(resource)
    except ValueError as exc:
        raise ConfigurationError(str(exc)) from exc


@dataclass(frozen=True, slots=True)
class MCPDependencies:
    resolver: MasterResolver | None = None
    broker: MasterSessionBroker | None = None
    control: ControlPlaneReader | None = None
    write_gate: WriteGate | None = None
    audit: MCPAuditSink | None = None
    sql_policy: BoundedSQLPolicy | None = None
    acceptance_scenarios_enabled: bool = False
    provider_only_profile_enabled: bool = False
    unified_bootstrap_profile_enabled: bool = False
    reader_profile_enabled: bool = False
    region_talk_controller: RegionTalkPipelineController | None = None
    region_talk_pipeline_run_enabled: bool = False
    showcase_manager: ShowcaseManager | ShowcaseGatewayClient | None = None


def _showcase_enabled() -> bool:
    return os.getenv("MY_DATA_HUB_SHOWCASE_ENABLED", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _showcase_backend_from_env(
    settings: Settings,
    fallback: AccessIdentity | None,
) -> ShowcaseManager | ShowcaseGatewayClient:
    if settings.mcp_remote_enabled:
        return ShowcaseGatewayClient.from_env(default_identity=fallback)
    return ShowcaseManager.from_env()


def _with_showcase_manager(
    dependencies: MCPDependencies,
    *,
    settings: Settings,
    fallback: AccessIdentity | None,
) -> MCPDependencies:
    if dependencies.showcase_manager is not None or not _showcase_enabled():
        return dependencies
    return replace(
        dependencies,
        showcase_manager=_showcase_backend_from_env(settings, fallback),
    )


def _local_identity(settings: Settings) -> AccessIdentity | None:
    if settings.mcp_remote_enabled:
        return None
    return AccessIdentity(
        subject="local-stdio",
        client_id="my-data-hub-local",
        scopes=settings.mcp_scopes,
        audience="local-stdio",
        token_id="local-process",
        expires_at=2**63 - 1,
        issuer="local-process",
        issued_at=0,
        resource="local-stdio",
    )


def _auth_error(resource_metadata_url: str, *, insufficient_scope: bool = True):  # type: ignore[no-untyped-def]
    from mcp.types import CallToolResult, TextContent

    code = "insufficient_scope" if insufficient_scope else "invalid_token"
    description = "The authenticated identity is not authorized for this tool."
    challenge = f'Bearer resource_metadata="{resource_metadata_url}", error="{code}", error_description="{description}"'
    return CallToolResult(
        content=[TextContent(type="text", text="Authentication or additional authorization is required.")],
        isError=True,
        _meta={"mcp/www_authenticate": [challenge]},
    )


def _profile_tool_names(dependencies: MCPDependencies) -> set[str]:
    names = set(TOOL_CONTRACTS)
    if dependencies.showcase_manager is None:
        names -= _SHOWCASE_TOOL_NAMES
    if not dependencies.acceptance_scenarios_enabled:
        names -= {"acceptance.scenario.request", "acceptance.scenario.status"}
    if dependencies.region_talk_controller is None or not dependencies.region_talk_pipeline_run_enabled:
        names.discard("region_talk.pipeline.run")
    if dependencies.provider_only_profile_enabled:
        names &= PROVIDER_ONLY_TOOLS
    if dependencies.unified_bootstrap_profile_enabled:
        unified_tools = UNIFIED_BOOTSTRAP_TOOLS
        if dependencies.showcase_manager is not None:
            unified_tools |= _SHOWCASE_TOOL_NAMES
        names &= unified_tools
    if dependencies.reader_profile_enabled:
        names &= READER_PROFILE_TOOLS
    return names


def _configured_security_schemes(settings: Settings, dependencies: MCPDependencies) -> list[dict[str, Any]]:
    scopes = sorted(
        {
            TOOL_CONTRACTS[name].scope
            for name in _profile_tool_names(dependencies)
            if TOOL_CONTRACTS[name].scope in settings.mcp_scopes
        }
    )
    return [{"type": "oauth2", "scopes": scopes}]


def _catalog_tool_names(
    identity: AccessIdentity | None,
    *,
    profile_tools: set[str],
    configured_scopes: frozenset[str],
) -> frozenset[str]:
    """Return callable tools plus bounded owner-only incremental-auth discovery.

    ChatGPT refreshes an app by calling ``tools/list`` with its current grant. If
    a newly deployed tool is hidden solely because that old grant predates the
    new scope, the client can never discover the action that would trigger an
    incremental authorization request.  The unified owner/operator grant is
    identified by its existing ``provider:write`` capability.  It may discover
    enabled Showcase schemas, but ``call_tool`` continues to require each exact
    Showcase scope.  Reader grants therefore remain unchanged.
    """

    allowed = set(visible_tools(identity))
    if identity is not None and "provider:write" in identity.scopes:
        allowed.update(
            name
            for name in _SHOWCASE_TOOL_NAMES
            if TOOL_CONTRACTS[name].scope in configured_scopes
        )
    return frozenset(allowed & profile_tools)


def create_server(
    settings: Settings,
    *,
    dependencies: MCPDependencies | None = None,
    default_identity: AccessIdentity | None = None,
):  # type: ignore[no-untyped-def]
    try:
        from mcp.server import MCPServer
        from mcp.types import ToolAnnotations
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("install my-data-hub to run the MCP server") from exc

    fallback = default_identity or _local_identity(settings)
    deps = _with_showcase_manager(
        dependencies or MCPDependencies(),
        settings=settings,
        fallback=fallback,
    )
    profile_tools = _profile_tool_names(deps)
    server_security_schemes = _configured_security_schemes(settings, deps)
    metadata_url = (
        oauth_resource_metadata_url(settings.mcp_oauth_resource)
        if settings.mcp_oauth_resource
        else "https://invalid.example/.well-known/oauth-protected-resource"
    )

    class IdentityAwareMCPServer(MCPServer):  # type: ignore[misc]
        security_schemes = server_security_schemes

        def _identity(self) -> AccessIdentity | None:
            return current_identity() or fallback

        async def list_tools(self):  # type: ignore[no-untyped-def]
            tools = await super().list_tools()
            allowed = _catalog_tool_names(
                self._identity(),
                profile_tools=profile_tools,
                configured_scopes=settings.mcp_scopes,
            )
            visible = [tool for tool in tools if tool.name in allowed]
            for tool in visible:
                if tool.name.startswith("region_talk."):
                    tool.input_schema["additionalProperties"] = False
            return visible

        async def call_tool(self, name, arguments, context=None):  # type: ignore[no-untyped-def]
            identity = self._identity()
            contract = TOOL_CONTRACTS.get(name)
            profile_denied = str(name) not in profile_tools
            if contract is None or identity is None or contract.scope not in identity.scopes or profile_denied:
                if identity is not None and deps.audit is not None:
                    recorded = deps.audit.record_mcp_audit(
                        OAuthAuditEvent(
                            event="mcp_tool",
                            outcome="scope_denied",
                            issuer=identity.issuer,
                            client_id=identity.client_id,
                            subject=identity.subject,
                            token_id=identity.token_id,
                            tool=str(name),
                        )
                    )
                    if inspect.isawaitable(recorded):
                        await recorded
                return _auth_error(metadata_url, insufficient_scope=identity is not None)
            if str(name).startswith("region_talk."):
                validate_region_talk_arguments(str(name), arguments or {})
            try:
                result = await super().call_tool(name, arguments, context)
            except Exception:
                if str(name).startswith("showcase.") and identity is not None and deps.audit is not None:
                    showcase_tool_audit = deps.audit.record_mcp_audit(
                        OAuthAuditEvent(
                            event="mcp_tool",
                            outcome="denied_or_failed",
                            issuer=identity.issuer,
                            client_id=identity.client_id,
                            subject=identity.subject,
                            token_id=identity.token_id,
                            tool=str(name),
                        )
                    )
                    if inspect.isawaitable(showcase_tool_audit):
                        await showcase_tool_audit
                raise
            if str(name).startswith("showcase.") and identity is not None and deps.audit is not None:
                showcase_tool_audit = deps.audit.record_mcp_audit(
                    OAuthAuditEvent(
                        event="mcp_tool",
                        outcome="accepted",
                        issuer=identity.issuer,
                        client_id=identity.client_id,
                        subject=identity.subject,
                        token_id=identity.token_id,
                        tool=str(name),
                    )
                )
                if inspect.isawaitable(showcase_tool_audit):
                    await showcase_tool_audit
            return result

    service = HubService(
        deps.resolver,
        broker=deps.broker,
        control=deps.control,
        write_gate=deps.write_gate,
        audit=deps.audit,
        sql_policy=deps.sql_policy,
        region_talk_controller=deps.region_talk_controller,
        fallback_identity=fallback,
    )
    mcp = IdentityAwareMCPServer(
        "my-data-hub",
        version="0.2.0",
        instructions=(
            "MCP 2026-07-28 bounded domain tools. The reader catalog contains no writes. "
            "Writes require an identity-bound preview and checkpoint lifecycle permit."
        ),
    )

    def register(name: str, function):  # type: ignore[no-untyped-def]
        contract = TOOL_CONTRACTS[name]
        annotations = ToolAnnotations(**contract.annotations())
        meta = {"securitySchemes": contract.security_schemes()}
        return mcp.tool(name=name, annotations=annotations, meta=meta, structured_output=True)(function)

    async def platform_status() -> dict[str, Any]:
        return await service.invoke("platform.status", {})

    async def master_status() -> dict[str, Any]:
        return await service.invoke("master.status", {})

    async def master_ensure() -> dict[str, Any]:
        return await service.invoke("master.ensure", {})

    async def operation_get(operation_id: str) -> dict[str, Any]:
        return await service.invoke("operation.get", {"operation_id": operation_id})

    async def checkpoint_status() -> dict[str, Any]:
        return await service.invoke("checkpoint.status", {})

    def showcase_manager() -> ShowcaseManager | ShowcaseGatewayClient:
        if deps.showcase_manager is None:
            raise RuntimeError("IdeaHub Showcase is not enabled")
        return deps.showcase_manager

    async def showcase_list() -> dict[str, Any]:
        return await asyncio.to_thread(showcase_manager().list_surfaces)

    async def showcase_get_link(view_id: str) -> dict[str, Any]:
        return await asyncio.to_thread(showcase_manager().get_link, view_id)

    async def showcase_get_source(view_id: str) -> dict[str, Any] | list[Any]:
        return await asyncio.to_thread(showcase_manager().get_source, view_id)

    async def showcase_apply(view_id: str, expected_source_revision: str, view: ShowcaseView | None, idempotency_key: str, items: list[ShowcaseItem] = [], dry_run: bool = True, publish: bool = False) -> dict[str, Any] | list[Any]:
        return await asyncio.to_thread(showcase_manager().apply, view_id, expected_source_revision=expected_source_revision, view=view, items=items, dry_run=dry_run, publish=publish, idempotency_key=idempotency_key)

    async def showcase_rebuild(view_id: str, idempotency_key: str) -> dict[str, Any] | list[Any]:
        return await asyncio.to_thread(
            showcase_manager().rebuild,
            view_id,
            idempotency_key=idempotency_key,
        )

    async def showcase_create_view(view_id: str, idempotency_key: str) -> dict[str, Any] | list[Any]:
        return await asyncio.to_thread(
            showcase_manager().create_view,
            view_id,
            idempotency_key=idempotency_key,
        )

    async def showcase_rotate_link(view_id: str, idempotency_key: str) -> dict[str, Any] | list[Any]:
        return await asyncio.to_thread(
            showcase_manager().rotate_link,
            view_id,
            idempotency_key=idempotency_key,
        )

    async def showcase_revoke_link(view_id: str, idempotency_key: str) -> dict[str, Any] | list[Any]:
        return await asyncio.to_thread(
            showcase_manager().revoke_link,
            view_id,
            idempotency_key=idempotency_key,
        )

    async def acceptance_scenario_request(
        task_id: str,
        scenario: Literal["FM04", "FM05", "FM07", "FM08", "FM09", "FM10", "FM11", "FM12", "FM14", "FM15", "FM24"],
        idempotency_key: str,
        source_revision: str,
        target_operation_id: str | None = None,
    ) -> dict[str, Any]:
        return await service.invoke("acceptance.scenario.request", locals())

    async def acceptance_scenario_status(task_id: str) -> dict[str, Any]:
        return await service.invoke("acceptance.scenario.status", locals())

    async def checkpoint_restore_request(
        target: str,
        checkpoint_id: str,
        exact_version_ref: str,
        timeout_seconds: int,
        idempotency_key: str,
    ) -> dict[str, Any]:
        return await service.invoke("checkpoint.restore.request", locals())

    async def master_rotation_request(
        checkpoint_id: str,
        exact_version_ref: str,
        expected_active_epoch: int,
        expected_canonical_revision: int,
        timeout_seconds: int,
        idempotency_key: str,
    ) -> dict[str, Any]:
        return await service.invoke("master.rotation.request", locals())

    async def connector_coverage() -> dict[str, Any]:
        return await service.invoke("connector.coverage", {})

    async def runtime_stale_epoch_probe(expected_active_epoch: int, submitted_epoch: int) -> dict[str, Any]:
        return await service.invoke("runtime.stale_epoch.probe", locals())

    async def provider_protected_resource_probe(resource_ref: str) -> dict[str, Any]:
        return await service.invoke("provider.protected_resource.probe", locals())

    async def embedding_coverage() -> dict[str, Any]:
        return await service.invoke("embedding.coverage", {})

    async def embedding_production_capabilities() -> dict[str, Any]:
        return await service.invoke("embedding.production.capabilities", {})

    async def provider_status(limit: int = 100) -> dict[str, Any]:
        return await service.invoke("provider.resources.status", {"limit": limit})

    async def runtime_events_history(run_id: str, attempt_id: str, epoch: int, limit: int = 100) -> dict[str, Any]:
        return await service.invoke("runtime.events.history", locals())

    async def provider_acceptance_dataset_lifecycle(
        scenario_id: str,
        task_id: str,
        idempotency_key: str,
        resource_ref: str,
        title: str,
        file_name: str,
        file_sha256: str,
        file_utf8: str,
        version_file_sha256: str,
        version_file_utf8: str,
    ) -> dict[str, Any]:
        return await service.invoke("provider.acceptance.dataset.lifecycle", locals())

    async def provider_acceptance_notebook_lifecycle(
        scenario_id: str,
        task_id: str,
        task_run_id: str,
        idempotency_key: str,
        resource_ref: str,
        title: str,
        code_file: str,
        source_utf8: str,
        dataset_inputs: list[dict[str, Any]],
        output_file_name: str,
        expected_output_sha256: str,
        max_output_bytes: int,
    ) -> dict[str, Any]:
        return await service.invoke("provider.acceptance.notebook.lifecycle", locals())

    async def provider_acceptance_claim_get(scenario_id: str, task_id: str) -> dict[str, Any]:
        return await service.invoke("provider.acceptance.claim.get", locals())

    async def provider_acceptance_claim_cleanup(
        scenario_id: str,
        task_id: str,
        claim_sha256: str,
        provider_run_ref: str,
        output_receipt_sha256: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        return await service.invoke("provider.acceptance.claim.cleanup", locals())

    async def bloggers_list(
        project_slug: str,
        after_name: str | None = None,
        after_blogger_id: str | None = None,
        limit: int = 50,
    ) -> dict[str, Any]:
        return await service.invoke("bloggers.list", locals())

    async def bloggers_get(project_slug: str, blogger_id: str) -> dict[str, Any]:
        return await service.invoke("bloggers.get", locals())

    async def bloggers_search(
        project_slug: str,
        query: str | None = None,
        after_name: str | None = None,
        after_blogger_id: str | None = None,
        limit: int = 20,
    ) -> dict[str, Any]:
        return await service.invoke("bloggers.search", locals())

    async def bloggers_provenance(blogger_id: str, limit: int = 50) -> dict[str, Any]:
        return await service.invoke("bloggers.provenance", {"blogger_id": blogger_id, "limit": limit})

    async def bloggers_statistics(project_slug: str) -> dict[str, Any]:
        return await service.invoke("bloggers.statistics", locals())

    async def bloggers_migration_accounting(export_batch_id: str) -> dict[str, Any]:
        return await service.invoke("bloggers.migration.accounting", {"export_batch_id": export_batch_id})

    async def region_talk_inventory() -> dict[str, Any]:
        return await service.invoke("region_talk.inventory", {})

    async def region_talk_articles_list(
        cursor: RegionTalkCursor | None = None,
        limit: RegionTalkLimit = 50,
        status: RegionTalkFilter | None = None,
        category: RegionTalkFilter | None = None,
        max_bytes: RegionTalkMaxBytes = 262_144,
    ) -> dict[str, Any]:
        return await service.invoke("region_talk.articles.list", locals())

    async def region_talk_articles_get(
        item_id: UUID,
        max_bytes: RegionTalkMaxBytes = 262_144,
    ) -> dict[str, Any]:
        return await service.invoke("region_talk.articles.get", locals())

    async def region_talk_articles_search(
        query: RegionTalkQuery,
        cursor: RegionTalkCursor | None = None,
        limit: RegionTalkLimit = 50,
        status: RegionTalkFilter | None = None,
        category: RegionTalkFilter | None = None,
        max_bytes: RegionTalkMaxBytes = 262_144,
    ) -> dict[str, Any]:
        return await service.invoke("region_talk.articles.search", locals())

    async def region_talk_posts_list(
        cursor: RegionTalkCursor | None = None,
        limit: RegionTalkLimit = 50,
        status: RegionTalkFilter | None = None,
        platform: RegionTalkFilter | None = None,
        max_bytes: RegionTalkMaxBytes = 262_144,
    ) -> dict[str, Any]:
        return await service.invoke("region_talk.posts.list", locals())

    async def region_talk_posts_get(
        item_id: UUID,
        max_bytes: RegionTalkMaxBytes = 262_144,
    ) -> dict[str, Any]:
        return await service.invoke("region_talk.posts.get", locals())

    async def region_talk_posts_search(
        query: RegionTalkQuery,
        cursor: RegionTalkCursor | None = None,
        limit: RegionTalkLimit = 50,
        status: RegionTalkFilter | None = None,
        platform: RegionTalkFilter | None = None,
        max_bytes: RegionTalkMaxBytes = 262_144,
    ) -> dict[str, Any]:
        return await service.invoke("region_talk.posts.search", locals())

    async def region_talk_queue_list(
        cursor: RegionTalkCursor | None = None,
        limit: RegionTalkLimit = 50,
        status: RegionTalkFilter | None = None,
        category: RegionTalkFilter | None = None,
        max_bytes: RegionTalkMaxBytes = 262_144,
    ) -> dict[str, Any]:
        return await service.invoke("region_talk.queue.list", locals())

    async def region_talk_queue_summary() -> dict[str, Any]:
        return await service.invoke("region_talk.queue.summary", {})

    async def region_talk_pipeline_status() -> dict[str, Any]:
        return await service.invoke("region_talk.pipeline.status", {})

    async def region_talk_pipeline_run(
        source_revision: RegionTalkSourceRevision,
        idempotency_key: RegionTalkIdempotencyKey,
    ) -> dict[str, Any]:
        return await service.invoke(
            "region_talk.pipeline.run",
            {
                "source_revision": source_revision,
                "idempotency_key": idempotency_key,
            },
        )

    async def data_query(
        sql: str,
        parameters: list[Any] | None = None,
        max_rows: int = 200,
        max_bytes: int = 262_144,
        timeout_ms: int = 5_000,
    ) -> dict[str, Any]:
        return await service.invoke(
            "data.query",
            {
                "sql": sql,
                "parameters": parameters or [],
                "max_rows": max_rows,
                "max_bytes": max_bytes,
                "timeout_ms": timeout_ms,
            },
        )

    async def data_change_preview(
        sql: str,
        parameters: list[Any],
        expected_revision: int,
        max_affected_rows: int,
        idempotency_key: str,
    ) -> dict[str, Any]:
        return await service.invoke("data.change.preview", locals())

    async def data_change_apply(
        sql: str,
        parameters: list[Any],
        expected_revision: int,
        max_affected_rows: int,
        idempotency_key: str,
        preview_receipt: str,
    ) -> dict[str, Any]:
        return await service.invoke("data.change.apply", locals())

    async def data_change_status(operation_id: str) -> dict[str, Any]:
        return await service.invoke("data.change.status", {"operation_id": operation_id})

    async def bloggers_import_preview(batch_id: str, expected_revision: int, idempotency_key: str) -> dict[str, Any]:
        return await service.invoke("bloggers.import.preview", locals())

    async def bloggers_import_apply(
        batch_id: str, expected_revision: int, idempotency_key: str, preview_receipt: str
    ) -> dict[str, Any]:
        return await service.invoke("bloggers.import.apply", locals())

    async def bloggers_import_status(operation_id: str) -> dict[str, Any]:
        return await service.invoke("bloggers.import.status", locals())

    async def submit_discovery_batch(payload: SubmitDiscoveryBatch) -> dict[str, Any]:
        # The MCP SDK advertises the closed structural model.  Revalidate the
        # received JSON through the official two-stage validator before any
        # ACTIVE-master or connector action.
        validated = validate_submit_discovery_batch(payload.model_dump(mode="json", exclude_none=True))
        return await service.invoke(
            "submit_discovery_batch",
            {"payload": validated.model_dump(mode="json", exclude_none=True)},
        )

    def provider_arguments(
        resource_ref: str,
        control_class: str,
        private: bool,
        payload: ProviderCreatePayload
        | ProviderVersionPayload
        | ProviderRunPayload
        | ProviderReadPayload
        | ProviderListPayload
        | ProviderDownloadPayload
        | ProviderDeletePayload
        | ProviderUploadStartPayload
        | ProviderUploadChunkPayload
        | ProviderUploadReferencePayload,
    ) -> dict[str, Any]:
        return {
            "resource_ref": resource_ref,
            "control_class": control_class,
            "private": private,
            "payload": payload.model_dump(mode="json", exclude_none=True),
        }

    async def provider_resources_create(
        resource_ref: str,
        control_class: Literal["mcp_managed", "mcp_exchange"],
        private: Literal[True],
        payload: ProviderCreatePayload,
    ) -> dict[str, Any]:
        return await service.invoke(
            "provider.resources.create",
            provider_arguments(resource_ref, control_class, private, payload),
        )

    async def provider_resources_version(
        resource_ref: str,
        control_class: Literal["mcp_managed", "mcp_exchange"],
        private: Literal[True],
        payload: ProviderVersionPayload,
    ) -> dict[str, Any]:
        return await service.invoke(
            "provider.resources.version",
            provider_arguments(resource_ref, control_class, private, payload),
        )

    async def provider_resources_run(
        resource_ref: str,
        control_class: Literal["mcp_managed"],
        private: Literal[True],
        payload: ProviderRunPayload,
    ) -> dict[str, Any]:
        """Create and start one private disposable Kaggle notebook.

        Use a new ``owner/slug`` resource_ref, set ``title`` to that exact
        slug, generate unique UUIDs, and embed the exact ``task_run_id`` text
        in ``source_utf8``. Dataset inputs may be empty. Internet is allowed
        only for disposable runs without Dataset inputs. Declare deterministic
        top-level expected outputs so they can later be listed and downloaded.
        Poll with ``provider.resources.read`` using the returned claim, then
        use ``provider.resources.list`` / ``provider.resources.download``.
        """
        return await service.invoke(
            "provider.resources.run",
            provider_arguments(resource_ref, control_class, private, payload),
        )

    async def provider_resources_read(
        resource_ref: str,
        control_class: Literal["mcp_managed", "mcp_exchange"],
        private: Literal[True],
        payload: ProviderReadPayload,
    ) -> dict[str, Any]:
        """Read an exact claim-bound Dataset identity or live notebook run status."""
        return await service.invoke(
            "provider.resources.read",
            provider_arguments(resource_ref, control_class, private, payload),
        )

    async def provider_resources_list(
        resource_ref: str,
        control_class: Literal["mcp_managed", "mcp_exchange"],
        private: Literal[True],
        payload: ProviderListPayload,
    ) -> dict[str, Any]:
        """List exact Dataset files or the declared outputs of a notebook run."""
        return await service.invoke(
            "provider.resources.list",
            provider_arguments(resource_ref, control_class, private, payload),
        )

    async def provider_resources_download(
        resource_ref: str,
        control_class: Literal["mcp_managed", "mcp_exchange"],
        private: Literal[True],
        payload: ProviderDownloadPayload,
    ) -> dict[str, Any]:
        """Download a bounded chunk of an exact Dataset file or declared notebook output."""
        return await service.invoke(
            "provider.resources.download",
            provider_arguments(resource_ref, control_class, private, payload),
        )

    async def provider_inventory_live(limit: int = 100) -> dict[str, Any]:
        return await service.invoke("provider.inventory.live", {"limit": limit})

    async def provider_upload_start(
        resource_ref: str,
        control_class: Literal["mcp_managed"],
        private: Literal[True],
        payload: ProviderUploadStartPayload,
    ) -> dict[str, Any]:
        return await service.invoke(
            "provider.upload.start",
            provider_arguments(resource_ref, control_class, private, payload),
        )

    async def provider_upload_put_chunk(
        resource_ref: str,
        control_class: Literal["mcp_managed"],
        private: Literal[True],
        payload: ProviderUploadChunkPayload,
    ) -> dict[str, Any]:
        return await service.invoke(
            "provider.upload.put_chunk",
            provider_arguments(resource_ref, control_class, private, payload),
        )

    async def provider_upload_status(
        resource_ref: str,
        control_class: Literal["mcp_managed"],
        private: Literal[True],
        payload: ProviderUploadReferencePayload,
    ) -> dict[str, Any]:
        return await service.invoke(
            "provider.upload.status",
            provider_arguments(resource_ref, control_class, private, payload),
        )

    async def provider_upload_finalize(
        resource_ref: str,
        control_class: Literal["mcp_managed"],
        private: Literal[True],
        payload: ProviderUploadReferencePayload,
    ) -> dict[str, Any]:
        return await service.invoke(
            "provider.upload.finalize",
            provider_arguments(resource_ref, control_class, private, payload),
        )

    async def provider_upload_abort(
        resource_ref: str,
        control_class: Literal["mcp_managed"],
        private: Literal[True],
        payload: ProviderUploadReferencePayload,
    ) -> dict[str, Any]:
        return await service.invoke(
            "provider.upload.abort",
            provider_arguments(resource_ref, control_class, private, payload),
        )

    async def provider_resources_delete(
        resource_ref: str,
        control_class: Literal["mcp_managed", "mcp_exchange"],
        private: Literal[True],
        payload: ProviderDeletePayload,
    ) -> dict[str, Any]:
        return await service.invoke(
            "provider.resources.delete",
            provider_arguments(resource_ref, control_class, private, payload),
        )

    functions = {
        "platform.status": platform_status,
        "master.status": master_status,
        "master.ensure": master_ensure,
        "operation.get": operation_get,
        "checkpoint.status": checkpoint_status,
        "showcase.list": showcase_list,
        "showcase.get_link": showcase_get_link,
        "showcase.get_source": showcase_get_source,
        "showcase.apply": showcase_apply,
        "showcase.rebuild": showcase_rebuild,
        "showcase.create_view": showcase_create_view,
        "showcase.rotate_link": showcase_rotate_link,
        "showcase.revoke_link": showcase_revoke_link,
        "acceptance.scenario.request": acceptance_scenario_request,
        "acceptance.scenario.status": acceptance_scenario_status,
        "checkpoint.restore.request": checkpoint_restore_request,
        "master.rotation.request": master_rotation_request,
        "connector.coverage": connector_coverage,
        "runtime.stale_epoch.probe": runtime_stale_epoch_probe,
        "provider.protected_resource.probe": provider_protected_resource_probe,
        "embedding.coverage": embedding_coverage,
        "embedding.production.capabilities": embedding_production_capabilities,
        "provider.resources.status": provider_status,
        "runtime.events.history": runtime_events_history,
        "provider.acceptance.dataset.lifecycle": provider_acceptance_dataset_lifecycle,
        "provider.acceptance.notebook.lifecycle": provider_acceptance_notebook_lifecycle,
        "provider.acceptance.claim.get": provider_acceptance_claim_get,
        "provider.acceptance.claim.cleanup": provider_acceptance_claim_cleanup,
        "provider.resources.create": provider_resources_create,
        "provider.resources.version": provider_resources_version,
        "provider.resources.run": provider_resources_run,
        "provider.resources.read": provider_resources_read,
        "provider.resources.list": provider_resources_list,
        "provider.resources.download": provider_resources_download,
        "provider.inventory.live": provider_inventory_live,
        "provider.resources.delete": provider_resources_delete,
        "provider.upload.start": provider_upload_start,
        "provider.upload.put_chunk": provider_upload_put_chunk,
        "provider.upload.status": provider_upload_status,
        "provider.upload.finalize": provider_upload_finalize,
        "provider.upload.abort": provider_upload_abort,
        "bloggers.list": bloggers_list,
        "bloggers.get": bloggers_get,
        "bloggers.search": bloggers_search,
        "bloggers.provenance": bloggers_provenance,
        "bloggers.statistics": bloggers_statistics,
        "bloggers.migration.accounting": bloggers_migration_accounting,
        "region_talk.inventory": region_talk_inventory,
        "region_talk.articles.list": region_talk_articles_list,
        "region_talk.articles.get": region_talk_articles_get,
        "region_talk.articles.search": region_talk_articles_search,
        "region_talk.posts.list": region_talk_posts_list,
        "region_talk.posts.get": region_talk_posts_get,
        "region_talk.posts.search": region_talk_posts_search,
        "region_talk.queue.list": region_talk_queue_list,
        "region_talk.queue.summary": region_talk_queue_summary,
        "region_talk.pipeline.status": region_talk_pipeline_status,
        "region_talk.pipeline.run": region_talk_pipeline_run,
        "data.query": data_query,
        "data.change.preview": data_change_preview,
        "data.change.apply": data_change_apply,
        "data.change.status": data_change_status,
        "bloggers.import.preview": bloggers_import_preview,
        "bloggers.import.apply": bloggers_import_apply,
        "bloggers.import.status": bloggers_import_status,
        "submit_discovery_batch": submit_discovery_batch,
    }
    for tool_name in TOOL_CONTRACTS:
        register(tool_name, functions[tool_name])
    return mcp


def create_streamable_http_app(
    settings: Settings,
    *,
    dependencies: MCPDependencies,
    validator: OAuthBearerValidator,
):  # type: ignore[no-untyped-def]
    """Build the MCP 2026-07-28 stateless Streamable HTTP resource server."""

    from contextlib import asynccontextmanager
    from urllib.parse import urlsplit

    from mcp.server.transport_security import TransportSecuritySettings
    from starlette.applications import Starlette
    from starlette.responses import JSONResponse
    from starlette.routing import Mount, Route

    from my_data_hub.mcp.admission import AdmissionLimits, OAuthAdmissionSecurity

    dependencies = _with_showcase_manager(
        dependencies,
        settings=settings,
        fallback=None,
    )
    server = create_server(settings, dependencies=dependencies)
    mcp_app = server.streamable_http_app(
        host=settings.mcp_host,
        streamable_http_path="/mcp",
        json_response=True,
        stateless_http=True,
        max_request_body_size=1_048_576,
        transport_security=TransportSecuritySettings(
            allowed_hosts=list(settings.mcp_allowed_hosts),
            allowed_origins=list(settings.mcp_allowed_origins),
        ),
    )
    metadata_tools = _profile_tool_names(dependencies)
    configured_security_schemes = _configured_security_schemes(settings, dependencies)
    resource_metadata = ProtectedResourceMetadata(
        resource=settings.mcp_oauth_resource,
        authorization_servers=(settings.mcp_oauth_issuer,),
        scopes_supported=frozenset(
            TOOL_CONTRACTS[name].scope
            for name in metadata_tools
            if name in TOOL_CONTRACTS and TOOL_CONTRACTS[name].scope in settings.mcp_scopes
        ),
    )
    metadata_url = oauth_resource_metadata_url(settings.mcp_oauth_resource)
    metadata_path = urlsplit(metadata_url).path

    async def metadata(_request):  # type: ignore[no-untyped-def]
        return JSONResponse(resource_metadata.document(), headers={"Cache-Control": "no-store"})

    @asynccontextmanager
    async def lifespan(_app):  # type: ignore[no-untyped-def]
        async with server.session_manager.run():
            yield

    mounted = Starlette(
        routes=[
            Route(metadata_path, metadata, methods=["GET"]),
            Mount(
                "/",
                app=ToolSecurityMetadataMiddleware(mcp_app, security_schemes=configured_security_schemes),
            ),
        ],
        lifespan=lifespan,
    )
    return OAuthAdmissionSecurity(
        mounted,
        validator=validator,
        required_scopes=frozenset(),
        allowed_origins=settings.mcp_allowed_origins,
        allowed_hosts=settings.mcp_allowed_hosts,
        trusted_proxy_ips=settings.mcp_trusted_proxies,
        limits=AdmissionLimits(
            max_request_bytes=1_048_576,
            max_response_bytes=2_097_152,
            max_concurrency=16,
            requests_per_window=120,
            rate_window_seconds=60,
            request_timeout_seconds=30,
        ),
        metadata_path=metadata_path,
        resource_metadata_url=metadata_url,
    )


def serve(*, transport: str) -> None:
    if transport == "streamable-http":
        from my_data_hub.mcp.runtime import serve as serve_remote

        serve_remote()
        return
    settings = Settings.from_env()
    if transport != "stdio":
        raise ConfigurationError(f"unsupported MCP transport: {transport}")
    create_server(settings).run(transport="stdio")


def main() -> None:
    serve(transport="stdio")
