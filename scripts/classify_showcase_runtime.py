from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PATH = ROOT / "scripts/validate_repository.py"


def replace_once(source: str, old: str, new: str, label: str) -> str:
    if old not in source:
        if new in source:
            return source
        raise RuntimeError(f"validator marker not found: {label}")
    return source.replace(old, new, 1)


def main() -> None:
    source = PATH.read_text(encoding="utf-8")
    source = replace_once(
        source,
        'compose_files == {"compose.yaml", "compose.control-plane.yaml"}',
        'compose_files == {"compose.yaml", "compose.control-plane.yaml", "compose.showcase.yaml"}',
        "Compose profile inventory",
    )
    source = replace_once(
        source,
        '        "deploy/control-plane/Dockerfile",\n',
        '        "deploy/control-plane/Dockerfile",\n'
        '        "deploy/showcase-runtime/Dockerfile",\n'
        '        "deploy/showcase-runtime/runtime.env.example",\n',
        "deployment file inventory",
    )
    source = replace_once(
        source,
        'workflows == {"ci.yml", "nightly.yml", "post-deploy.yml", "provider-real.yml"}',
        'workflows == {\n'
        '            "ci.yml",\n'
        '            "ideahub-showcase.yml",\n'
        '            "nightly.yml",\n'
        '            "post-deploy.yml",\n'
        '            "provider-real.yml",\n'
        '        }',
        "workflow inventory",
    )
    source = replace_once(
        source,
        'relative in {"compose.yaml", "compose.control-plane.yaml"}',
        'relative in {"compose.yaml", "compose.control-plane.yaml", "compose.showcase.yaml"}',
        "YAML service inventory",
    )
    PATH.write_text(source, encoding="utf-8")
    print("showcase runtime classified in fail-closed repository inventory")


if __name__ == "__main__":
    main()
