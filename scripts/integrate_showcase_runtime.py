from __future__ import annotations

import re
from pathlib import Path
from textwrap import dedent

ROOT = Path(__file__).resolve().parents[1]


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def write(path: str, source: str) -> None:
    target = ROOT / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(source.rstrip() + "\n", encoding="utf-8")


def replace_once(source: str, old: str, new: str, label: str) -> str:
    if new in source:
        return source
    if old not in source:
        raise RuntimeError(f"cannot locate {label}")
    return source.replace(old, new, 1)


def patch_manager() -> None:
    path = "src/my_data_hub/showcase/manager.py"
    source = read(path)
    source = replace_once(
        source,
        "    def rebuild(self, view_id: str) -> dict[str, Any]:",
        "    def rebuild(\n"
        "        self, view_id: str, *, idempotency_key: str | None = None\n"
        "    ) -> dict[str, Any]:",
        "ShowcaseManager.rebuild signature",
    )
    source = replace_once(
        source,
        "    def create_view(self, view_id: str, *, publish: bool = True) -> dict[str, Any]:",
        "    def create_view(\n"
        "        self,\n"
        "        view_id: str,\n"
        "        *,\n"
        "        publish: bool = True,\n"
        "        idempotency_key: str | None = None,\n"
        "    ) -> dict[str, Any]:",
        "ShowcaseManager.create_view signature",
    )
    source = source.replace(
        "                result = self.rebuild(view_id)\n",
        "                result = self.rebuild(\n"
        "                    view_id, idempotency_key=idempotency_key\n"
        "                )\n",
        1,
    )

    rotate_start = source.index("    def rotate_link(") if "    def rotate_link(" in source else source.index(
        "    def rotate_link(self, view_id: str)"
    )
    revoke_start = source.index("    def revoke_link", rotate_start)
    rotate = dedent(
        '''
        def rotate_link(
            self,
            view_id: str,
            *,
            slug: str | None = None,
            idempotency_key: str | None = None,
        ) -> dict[str, Any]:
            with self._lock:
                bundle = self.source.load_bundle(view_id)
                previous_state = self.state.load()
                previous = previous_state.surfaces.get(view_id)
                if previous is None:
                    raise ShowcaseNotFoundError(view_id)
                old_slug = previous.slug
                new_slug = slug or self._new_slug()
                with TemporaryDirectory(prefix=f"showcase-rotate-{view_id}-") as temp:
                    output = Path(temp) / "dist"
                    receipt = self.builder.build(
                        bundle, slug=new_slug, output_dir=output
                    )
                    published_url = self.publisher.publish(output, receipt)
                receipt = receipt.model_copy(update={"url": published_url})
                now = datetime.now(UTC)
                current = self.state.load()
                current_surface = current.surfaces.get(view_id)
                if current_surface is None or current_surface.slug != old_slug:
                    raise RuntimeError("showcase state changed during link rotation")
                current.surfaces[view_id] = SurfaceState(
                    view_id=view_id,
                    slug=new_slug,
                    active=True,
                    created_at=current_surface.created_at,
                    updated_at=now,
                    last_build=receipt,
                )
                self.state.save(current)
                self.publisher.revoke(view_id=view_id, slug=old_slug)
                return {
                    "schema_version": 1,
                    "status": "rotated",
                    "view_id": view_id,
                    "url": published_url,
                    "old_url_revoked": self._url(old_slug),
                    "receipt": receipt.model_dump(mode="json"),
                }

        '''
    )
    rotate = "\n".join("    " + line if line else "" for line in rotate.splitlines())
    source = source[:rotate_start] + rotate + source[revoke_start:]
    source = replace_once(
        source,
        "    def revoke_link(self, view_id: str) -> dict[str, Any]:",
        "    def revoke_link(\n"
        "        self, view_id: str, *, idempotency_key: str | None = None\n"
        "    ) -> dict[str, Any]:",
        "ShowcaseManager.revoke_link signature",
    )
    write(path, source)


def patch_config() -> None:
    path = "src/my_data_hub/config.py"
    source = read(path)
    if '"showcase:read"' not in source:
        marker = "        remote_read_scopes = {\n"
        if marker not in source:
            raise RuntimeError("remote_read_scopes marker missing")
        source = source.replace(marker, marker + '            "showcase:read",\n', 1)
    if '"showcase:write"' not in source:
        marker = "        remote_write_scopes = {\n"
        if marker not in source:
            raise RuntimeError("remote_write_scopes marker missing")
        source = source.replace(marker, marker + '            "showcase:write",\n', 1)
    write(path, source)


def patch_server() -> None:
    path = "src/my_data_hub/mcp/server.py"
    source = read(path)
    source = source.replace(
        "from my_data_hub.showcase.manager import ShowcaseManager\n",
        "from my_data_hub.showcase.gateway import ShowcaseGatewayClient\n"
        "from my_data_hub.showcase.manager import ShowcaseManager\n",
        1,
    )
    source = source.replace(
        "    showcase_manager: ShowcaseManager | None = None\n",
        "    showcase_manager: ShowcaseManager | ShowcaseGatewayClient | None = None\n",
        1,
    )
    if '"showcase.list"' not in source[source.index("READER_PROFILE_TOOLS"):source.index("PROVIDER_ONLY_TOOLS")]:
        marker = '        "region_talk.pipeline.status",\n'
        if marker not in source:
            raise RuntimeError("reader profile insertion point missing")
        source = source.replace(marker, marker + '        "showcase.list",\n', 1)

    function_start = source.index("def _with_showcase_manager(")
    function_end = source.index("\n\ndef _local_identity", function_start)
    backend_functions = dedent(
        '''
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
        '''
    ).strip()
    source = source[:function_start] + backend_functions + source[function_end:]

    old_order = dedent(
        '''
            deps = _with_showcase_manager(dependencies or MCPDependencies())
            profile_tools = _profile_tool_names(deps)
            server_security_schemes = _configured_security_schemes(settings, deps)
            fallback = default_identity or _local_identity(settings)
        '''
    )
    new_order = dedent(
        '''
            fallback = default_identity or _local_identity(settings)
            deps = _with_showcase_manager(
                dependencies or MCPDependencies(),
                settings=settings,
                fallback=fallback,
            )
            profile_tools = _profile_tool_names(deps)
            server_security_schemes = _configured_security_schemes(settings, deps)
        '''
    )
    source = replace_once(source, old_order, new_order, "create_server dependency order")

    source = source.replace(
        "    def showcase_manager() -> ShowcaseManager:\n",
        "    def showcase_manager() -> ShowcaseManager | ShowcaseGatewayClient:\n",
        1,
    )
    start = source.index("    async def showcase_rebuild(")
    end = source.index("    async def acceptance_scenario_request(", start)
    wrappers = dedent(
        '''
        async def showcase_rebuild(
            view_id: str, idempotency_key: str
        ) -> dict[str, Any] | list[Any]:
            return await asyncio.to_thread(
                showcase_manager().rebuild,
                view_id,
                idempotency_key=idempotency_key,
            )

        async def showcase_create_view(
            view_id: str, idempotency_key: str
        ) -> dict[str, Any] | list[Any]:
            return await asyncio.to_thread(
                showcase_manager().create_view,
                view_id,
                idempotency_key=idempotency_key,
            )

        async def showcase_rotate_link(
            view_id: str, idempotency_key: str
        ) -> dict[str, Any] | list[Any]:
            return await asyncio.to_thread(
                showcase_manager().rotate_link,
                view_id,
                idempotency_key=idempotency_key,
            )

        async def showcase_revoke_link(
            view_id: str, idempotency_key: str
        ) -> dict[str, Any] | list[Any]:
            return await asyncio.to_thread(
                showcase_manager().revoke_link,
                view_id,
                idempotency_key=idempotency_key,
            )

        '''
    )
    wrappers = "\n".join("    " + line if line else "" for line in wrappers.splitlines())
    source = source[:start] + wrappers + source[end:]

    anchor = "            return await super().call_tool(name, arguments, context)"
    if "showcase_tool_audit" not in source:
        replacement = dedent(
            '''
            try:
                result = await super().call_tool(name, arguments, context)
            except Exception:
                if (
                    str(name).startswith("showcase.")
                    and identity is not None
                    and deps.audit is not None
                ):
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
            if (
                str(name).startswith("showcase.")
                and identity is not None
                and deps.audit is not None
            ):
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
            '''
        ).strip()
        replacement = "\n".join("            " + line if line else "" for line in replacement.splitlines())
        source = replace_once(source, anchor, replacement, "showcase MCP audit boundary")
    write(path, source)


def patch_standalone_server() -> None:
    path = "src/my_data_hub/showcase/mcp_server.py"
    source = read(path)
    start = source.index('    @tool("showcase.rebuild"')
    end = source.index("\n    return mcp", start)
    wrappers = dedent(
        '''
        @tool("showcase.rebuild", read_only=False, idempotent=True)
        async def showcase_rebuild(
            view_id: str, idempotency_key: str
        ) -> dict[str, Any]:
            return await asyncio.to_thread(
                control.rebuild, view_id, idempotency_key=idempotency_key
            )

        @tool("showcase.create_view", read_only=False, idempotent=True)
        async def showcase_create_view(
            view_id: str, idempotency_key: str, publish: bool = True
        ) -> dict[str, Any]:
            return await asyncio.to_thread(
                control.create_view,
                view_id,
                publish=publish,
                idempotency_key=idempotency_key,
            )

        @tool("showcase.rotate_link", read_only=False, destructive=True, idempotent=False)
        async def showcase_rotate_link(
            view_id: str, idempotency_key: str
        ) -> dict[str, Any]:
            return await asyncio.to_thread(
                control.rotate_link, view_id, idempotency_key=idempotency_key
            )

        @tool("showcase.revoke_link", read_only=False, destructive=True, idempotent=True)
        async def showcase_revoke_link(
            view_id: str, idempotency_key: str
        ) -> dict[str, Any]:
            return await asyncio.to_thread(
                control.revoke_link, view_id, idempotency_key=idempotency_key
            )
        '''
    ).strip()
    wrappers = "\n".join("    " + line if line else "" for line in wrappers.splitlines())
    write(path, source[:start] + wrappers + source[end:])


def patch_pyproject() -> None:
    path = "pyproject.toml"
    source = read(path)
    entry = 'my-data-hub-showcase-runtime = "my_data_hub.showcase.runtime:main"\n'
    if entry not in source:
        marker = 'my-data-hub-showcase-mcp = "my_data_hub.showcase.mcp_server:main"\n'
        if marker not in source:
            raise RuntimeError("pyproject script insertion point missing")
        source = source.replace(marker, marker + entry, 1)
    write(path, source)


def main() -> None:
    patch_manager()
    patch_config()
    patch_server()
    patch_standalone_server()
    patch_pyproject()
    print("showcase runtime v2 integrated against current main")


if __name__ == "__main__":
    main()
