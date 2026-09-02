from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory

from .models import BuildReceipt, ShowcaseBundle


class ShowcaseBuildError(RuntimeError):
    """Raised when the deterministic Astro renderer fails or emits unsafe output."""


class AstroShowcaseBuilder:
    def __init__(
        self,
        *,
        site_root: Path,
        origin: str,
        npm_command: str = "npm",
        timeout_seconds: int = 180,
    ) -> None:
        self.site_root = site_root.expanduser().resolve()
        self.origin = origin.rstrip("/")
        self.npm_command = npm_command
        self.timeout_seconds = timeout_seconds

    def build(self, bundle: ShowcaseBundle, *, slug: str, output_dir: Path) -> BuildReceipt:
        if not (self.site_root / "package.json").is_file():
            raise ShowcaseBuildError(f"Astro site is missing: {self.site_root}")
        if not (self.site_root / "node_modules").is_dir():
            raise ShowcaseBuildError(
                "Astro dependencies are not installed; run npm ci in showcase-site during deployment"
            )
        output_dir = output_dir.expanduser().resolve()
        shutil.rmtree(output_dir, ignore_errors=True)
        output_dir.parent.mkdir(parents=True, exist_ok=True)
        with TemporaryDirectory(prefix="showcase-input-") as temp:
            input_path = Path(temp) / "showcase.json"
            input_path.write_text(
                json.dumps(bundle.published(), ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            env = {
                **os.environ,
                "SHOWCASE_INPUT": str(input_path),
                "SHOWCASE_SLUG": slug,
                "SHOWCASE_ORIGIN": self.origin,
                "SHOWCASE_OUT_DIR": str(output_dir),
            }
            process = subprocess.run(
                [self.npm_command, "run", "test:build"],
                cwd=self.site_root,
                env=env,
                text=True,
                capture_output=True,
                timeout=self.timeout_seconds,
                check=False,
            )
        if process.returncode != 0:
            detail = "\n".join(part for part in (process.stdout, process.stderr) if part).strip()
            raise ShowcaseBuildError(f"Astro build failed ({process.returncode}):\n{detail[-8000:]}")
        manifest_path = output_dir / "showcase-build.json"
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ShowcaseBuildError("checked build manifest is missing or invalid") from exc
        return BuildReceipt(
            view_id=bundle.view.id,
            source_revision=bundle.source_revision,
            slug=slug,
            url=f"{self.origin}/v/{slug}/",
            tree_sha256=str(manifest["tree_sha256"]),
            file_count=int(manifest["file_count"]),
            html_count=int(manifest["html_count"]),
        )
