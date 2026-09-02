from __future__ import annotations

import re
from pathlib import Path
from textwrap import dedent

ROOT = Path(__file__).resolve().parents[1]


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def write(path: str, value: str) -> None:
    target = ROOT / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(value.rstrip() + "\n", encoding="utf-8")


def harden_catalog() -> None:
    path = "src/my_data_hub/mcp/catalog.py"
    source = read(path)
    source = re.sub(
        r'\("showcase\.get_link",\s*"showcase:read"\)',
        '("showcase.get_link", "showcase:write")',
        source,
        count=1,
    )
    if '("showcase.get_link", "showcase:write")' not in source:
        raise RuntimeError("showcase.get_link owner scope is missing")
    write(path, source)


def harden_server() -> None:
    path = "src/my_data_hub/mcp/server.py"
    source = read(path)
    start = source.index("READER_PROFILE_TOOLS = frozenset(")
    end = source.index("\n)\n\nPROVIDER_ONLY_TOOLS", start)
    profile = source[start:end]
    profile = profile.replace('        "showcase.get_link",\n', "")
    if '"showcase.list"' not in profile or '"showcase.get_link"' in profile:
        raise RuntimeError("showcase reader profile is unsafe")
    write(path, source[:start] + profile + source[end:])


def harden_runtime() -> None:
    path = "src/my_data_hub/showcase/runtime.py"
    source = read(path)
    old = (
        '        required_scope = "showcase:write" '
        'if tool in SHOWCASE_WRITE_TOOLS else "showcase:read"'
    )
    new = dedent(
        """
        required_scope = (
            "showcase:write"
            if tool in SHOWCASE_WRITE_TOOLS or tool == "showcase.get_link"
            else "showcase:read"
        )
        """
    ).strip()
    if old in source:
        source = source.replace(old, "        " + new.replace("\n", "\n        "), 1)
    if 'tool == "showcase.get_link"' not in source:
        raise RuntimeError("runtime full-link owner guard is missing")
    write(path, source)


def harden_compose() -> None:
    path = "compose.showcase.yaml"
    source = read(path)
    source = source.replace(
        "${MY_DATA_HUB_SHOWCASE_MEMORY_LIMIT:-768m}",
        "${MY_DATA_HUB_SHOWCASE_MEMORY_LIMIT:-512m}",
    ).replace(
        "${MY_DATA_HUB_SHOWCASE_CPU_LIMIT:-1.50}",
        "${MY_DATA_HUB_SHOWCASE_CPU_LIMIT:-1.00}",
    ).replace(
        "size=384m,mode=700,uid=65532,gid=65532",
        "size=256m,mode=700,uid=65532,gid=65532",
    )
    for required in ("512m", "1.00", "size=256m"):
        if required not in source:
            raise RuntimeError(f"compose hardening missing: {required}")
    write(path, source)


def update_env_examples() -> None:
    path = ".env.example"
    source = read(path).rstrip() + "\n"
    additions = {
        "MY_DATA_HUB_SHOWCASE_GATEWAY_URL": "",
        "MY_DATA_HUB_SHOWCASE_GATEWAY_TOKEN_FILE": "",
        "MY_DATA_HUB_SHOWCASE_GATEWAY_TIMEOUT_SECONDS": "45",
        "MY_DATA_HUB_SHOWCASE_RUNTIME_ENV_FILE": "",
        "MY_DATA_HUB_SHOWCASE_RUNTIME_HOST": "127.0.0.1",
        "MY_DATA_HUB_SHOWCASE_RUNTIME_PORT": "8790",
        "MY_DATA_HUB_SHOWCASE_RUNTIME_TOKEN_FILE": "",
        "MY_DATA_HUB_SHOWCASE_OPERATION_JOURNAL": "",
        "MY_DATA_HUB_SHOWCASE_MAX_REQUEST_BYTES": "65536",
        "MY_DATA_HUB_SHOWCASE_SITE_TEMPLATE_DIR": "",
        "MY_DATA_HUB_SHOWCASE_SITE_DIR": "",
        "MY_DATA_HUB_SHOWCASE_IMAGE": "my-data-hub-showcase:local",
        "MY_DATA_HUB_SHOWCASE_MEMORY_LIMIT": "512m",
        "MY_DATA_HUB_SHOWCASE_CPU_LIMIT": "1.00",
        "MY_DATA_HUB_SHOWCASE_STATE_VOLUME": "my-data-hub-showcase-state",
        "MY_DATA_HUB_MCP_SCOPES_WITH_SHOWCASE": "",
    }
    missing = [name for name in additions if f"{name}=" not in source]
    if missing:
        source += "\n# IdeaHub Showcase: public MCP edge / private renderer split\n"
        for name in missing:
            source += f"{name}={additions[name]}\n"
    write(path, source)

    code_paths = [
        "src/my_data_hub/showcase/source.py",
        "src/my_data_hub/showcase/publisher.py",
        "src/my_data_hub/showcase/state.py",
        "src/my_data_hub/showcase/builder.py",
        "src/my_data_hub/showcase/manager.py",
        "src/my_data_hub/showcase/runtime.py",
    ]
    names: set[str] = {"MY_DATA_HUB_ARTIFACT_ROOT"}
    for code_path in code_paths:
        names.update(re.findall(r"MY_DATA_HUB_SHOWCASE_[A-Z0-9_]+", read(code_path)))
    names -= {
        "MY_DATA_HUB_SHOWCASE_GATEWAY_URL",
        "MY_DATA_HUB_SHOWCASE_GATEWAY_TOKEN_FILE",
        "MY_DATA_HUB_SHOWCASE_GATEWAY_TIMEOUT_SECONDS",
        "MY_DATA_HUB_SHOWCASE_RUNTIME_ENV_FILE",
        "MY_DATA_HUB_SHOWCASE_IMAGE",
        "MY_DATA_HUB_SHOWCASE_MEMORY_LIMIT",
        "MY_DATA_HUB_SHOWCASE_CPU_LIMIT",
        "MY_DATA_HUB_SHOWCASE_STATE_VOLUME",
    }
    current_values: dict[str, str] = {}
    for line in source.splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            key, value = line.split("=", 1)
            current_values[key] = value
    safe_defaults = {
        "MY_DATA_HUB_ARTIFACT_ROOT": "/var/lib/my-data-hub-showcase",
        "MY_DATA_HUB_SHOWCASE_RUNTIME_HOST": "127.0.0.1",
        "MY_DATA_HUB_SHOWCASE_RUNTIME_PORT": "8790",
        "MY_DATA_HUB_SHOWCASE_MAX_REQUEST_BYTES": "65536",
        "MY_DATA_HUB_SHOWCASE_SITE_TEMPLATE_DIR": "/opt/showcase-site",
        "MY_DATA_HUB_SHOWCASE_SITE_DIR": "/work/showcase-site",
        "MY_DATA_HUB_SHOWCASE_OPERATION_JOURNAL": (
            "/var/lib/my-data-hub-showcase/showcase-operations.json"
        ),
        "MY_DATA_HUB_SHOWCASE_ORIGIN": "https://ideas.kenigevents.ru",
        "MY_DATA_HUB_SHOWCASE_GITHUB_REPOSITORY": (
            "onedayonemasterpiece/idea-hub"
        ),
        "MY_DATA_HUB_SHOWCASE_GITHUB_REF": "main",
        "MY_DATA_HUB_SHOWCASE_GITHUB_ROOT": "showcase",
    }
    lines = [
        "# Private environment for the isolated my-data-hub showcase runtime.",
        "# Copy outside the repository, chmod 0600, and fill adapter secrets.",
        "# Never pass this file to remote-mcp.",
        "",
    ]
    for name in sorted(names):
        value = safe_defaults.get(name, current_values.get(name, ""))
        if any(
            marker in name
            for marker in ("TOKEN", "SECRET", "KEY", "PASSWORD", "CREDENTIAL")
        ):
            value = ""
        lines.append(f"{name}={value}")
    write("deploy/showcase-runtime/runtime.env.example", "\n".join(lines))


def write_runbook() -> None:
    write(
        "docs/operations/ideahub-showcase-runtime.md",
        dedent(
            """
            # IdeaHub Showcase: production runtime

            ## Ownership boundary

            `onedayonemasterpiece/idea-hub` is the curated source of cards,
            audience views, source references and visual decisions. It does not
            execute Astro, publish files, store active secret links or expose MCP
            tools.

            `onedayonemasterpiece/my-data-hub` owns the complete display runtime:

            ```text
            MCP client
              -> OAuth remote-mcp edge (Python only; no GitHub/publisher credentials)
              -> authenticated loopback showcase gateway
              -> isolated showcase-runtime (GitHub source + Astro + publisher + private state)
            ```

            The standard `remote-mcp` image stays Python-only. Node, the read-only
            source credential and publication credentials exist only in
            `showcase-runtime`.

            ## One-time host inputs

            Create `/srv/my-data-hub/showcase/gateway.key` as a distinct random
            token and `/srv/my-data-hub/showcase/runtime.env` from
            `deploy/showcase-runtime/runtime.env.example`. Both files must be
            regular, owner-only `0600` files readable by runtime UID/GID `65532`.
            Never commit their contents or pass the runtime env to `remote-mcp`.

            Set the host deployment env:

            ```dotenv
            MY_DATA_HUB_SHOWCASE_GATEWAY_TOKEN_FILE=/srv/my-data-hub/showcase/gateway.key
            MY_DATA_HUB_SHOWCASE_RUNTIME_ENV_FILE=/srv/my-data-hub/showcase/runtime.env
            MY_DATA_HUB_SHOWCASE_RUNTIME_PORT=8790
            MY_DATA_HUB_SHOWCASE_IMAGE=my-data-hub-showcase:local
            MY_DATA_HUB_SHOWCASE_MEMORY_LIMIT=512m
            MY_DATA_HUB_SHOWCASE_CPU_LIMIT=1.00
            MY_DATA_HUB_SHOWCASE_STATE_VOLUME=my-data-hub-showcase-state
            MY_DATA_HUB_MCP_SCOPES_WITH_SHOWCASE=<existing-owner-scopes>,showcase:read,showcase:write
            ```

            The OAuth owner/operator client receives `showcase:read` and
            `showcase:write`. A generic reader may receive only `showcase:read`:
            it can call `showcase.list` and receives masked URLs.
            `showcase.get_link` returns the full secret URL and therefore requires
            owner/operator scope `showcase:write`, despite being annotated as a
            read-only operation.

            ## Build and start

            ```bash
            docker compose \
              -f compose.control-plane.yaml \
              -f compose.showcase.yaml \
              --env-file /srv/my-data-hub/control-plane.env \
              build showcase-runtime
            docker compose \
              -f compose.control-plane.yaml \
              -f compose.showcase.yaml \
              --env-file /srv/my-data-hub/control-plane.env \
              up -d showcase-runtime remote-mcp
            curl --fail --silent http://127.0.0.1:8790/health/ready
            ```

            Do not expose port `8790` in Nginx, a tunnel, firewall rule or Docker
            published-port rule.

            ## Live acceptance

            Use unique idempotency keys containing the exact `idea-hub` source
            commit:

            ```text
            showcase.create_view(view_id="main", idempotency_key="create:main:<source-sha>")
            showcase.get_link(view_id="main")
            showcase.rebuild(view_id="main", idempotency_key="rebuild:main:<source-sha>")
            showcase.get_link(view_id="main")              # URL unchanged
            showcase.rotate_link(view_id="main", idempotency_key="rotate:main:<source-sha>:1")
            showcase.get_link(view_id="main")              # new URL
            verify old URL is unavailable
            repeat rotate with the same key                 # duplicate, no third URL
            ```

            Leave the new rotated main URL active. Verify catalog and detail
            pages plus `noindex`, `X-Robots-Tag` and `Referrer-Policy`. Save only
            a masked URL and hashes in the deployment receipt; retrieve the full
            URL through `showcase.get_link`.

            ## Rollback

            Stop only `showcase-runtime`, restart `remote-mcp` without the overlay
            and remove showcase scopes. Preserve the named state volume until
            every active URL has been explicitly revoked or migrated. This runtime
            requires neither PostgreSQL nor Kaggle.
            """
        ),
    )


def write_tests() -> None:
    write(
        "tests/showcase/test_env_contract_v2.py",
        dedent(
            '''
            from __future__ import annotations

            import re
            from pathlib import Path

            import yaml

            ROOT = Path(__file__).resolve().parents[2]


            def test_every_showcase_environment_name_is_documented() -> None:
                code = "\\n".join(
                    path.read_text(encoding="utf-8")
                    for path in (ROOT / "src/my_data_hub/showcase").glob("*.py")
                )
                used = set(re.findall(r"MY_DATA_HUB_SHOWCASE_[A-Z0-9_]+", code))
                documented = "\\n".join(
                    [
                        (ROOT / ".env.example").read_text(encoding="utf-8"),
                        (ROOT / "deploy/showcase-runtime/runtime.env.example").read_text(
                            encoding="utf-8"
                        ),
                        (ROOT / "compose.showcase.yaml").read_text(encoding="utf-8"),
                        (ROOT / "docs/operations/ideahub-showcase-runtime.md").read_text(
                            encoding="utf-8"
                        ),
                    ]
                )
                assert sorted(name for name in used if name not in documented) == []


            def test_loopback_gateway_matches_existing_control_plane_network_mode() -> None:
                base = yaml.safe_load(
                    (ROOT / "compose.control-plane.yaml").read_text(encoding="utf-8")
                )
                overlay = yaml.safe_load(
                    (ROOT / "compose.showcase.yaml").read_text(encoding="utf-8")
                )
                assert base["services"]["remote-mcp"]["network_mode"] == "host"
                remote = overlay["services"]["remote-mcp"]
                assert remote["environment"]["MY_DATA_HUB_SHOWCASE_GATEWAY_URL"].startswith(
                    "http://127.0.0.1:"
                )
                runtime = overlay["services"]["showcase-runtime"]
                assert "ports" not in runtime
                assert runtime["network_mode"] == "host"
                assert runtime["mem_limit"].endswith("512m}")
                assert runtime["cpus"].endswith("1.00}")


            def test_full_link_tool_is_not_in_reader_profile() -> None:
                server = (ROOT / "src/my_data_hub/mcp/server.py").read_text(
                    encoding="utf-8"
                )
                start = server.index("READER_PROFILE_TOOLS = frozenset(")
                end = server.index("\\n)\\n\\nPROVIDER_ONLY_TOOLS", start)
                profile = server[start:end]
                assert '"showcase.list"' in profile
                assert '"showcase.get_link"' not in profile
                catalog = (ROOT / "src/my_data_hub/mcp/catalog.py").read_text(
                    encoding="utf-8"
                )
                assert '("showcase.get_link", "showcase:write")' in catalog
            '''
        ),
    )

    path = "tests/showcase/test_runtime_v2.py"
    source = read(path)
    if "def test_full_link_requires_owner_write_scope" not in source:
        source += dedent(
            '''


            def test_full_link_requires_owner_write_scope(tmp_path: Path) -> None:
                manager = FakeManager()
                app = create_app(
                    controller=ShowcaseOperationController(
                        manager,
                        ShowcaseOperationJournal(tmp_path / "operations.json"),
                    ),
                    token=TOKEN,
                )
                client = TestClient(app)
                body = {
                    "tool": "showcase.get_link",
                    "arguments": {"view_id": "main"},
                    "principal": principal("showcase:read").model_dump(),
                }
                headers = {"Authorization": f"Bearer {TOKEN}"}
                assert client.post(
                    "/internal/mcp-showcase/invoke", headers=headers, json=body
                ).status_code == 403
                body["principal"] = principal("showcase:write").model_dump()
                response = client.post(
                    "/internal/mcp-showcase/invoke", headers=headers, json=body
                )
                assert response.status_code == 200
                assert response.json()["result"]["url"].endswith(
                    f"/{OLD_SLUG}/"
                )
            '''
        )
    write(path, source)


def main() -> None:
    harden_catalog()
    harden_server()
    harden_runtime()
    harden_compose()
    update_env_examples()
    write_runbook()
    write_tests()
    print("showcase runtime final production contract applied")


if __name__ == "__main__":
    main()
