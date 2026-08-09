#!/usr/bin/env bash
set -Eeuo pipefail
umask 022

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${1:-$ROOT/dist}"
mkdir -p "$OUT"
cd "$ROOT"

version="$(python - <<'PY'
from pathlib import Path
import tomllib

print(tomllib.loads(Path('pyproject.toml').read_text())['project']['version'])
PY
)"
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
name="my-data-hub-${version}-${stamp}"
tarball="$OUT/${name}.tar.gz"
zipball="$OUT/${name}.zip"
checksums="$OUT/${name}.SHA256SUMS.txt"

if git rev-parse --is-inside-work-tree >/dev/null 2>&1 && git rev-parse --verify HEAD >/dev/null 2>&1; then
  git archive --format=tar.gz --prefix="${name}/" --output="$tarball" HEAD
  git archive --format=zip --prefix="${name}/" --output="$zipball" HEAD
else
  tar \
    --exclude='.git' \
    --exclude='.venv' \
    --exclude='.pytest_cache' \
    --exclude='.ruff_cache' \
    --exclude='dist' \
    --exclude='backups/*' \
    --exclude='artifacts/*' \
    --exclude='__pycache__' \
    --exclude='*.pyc' \
    --transform="s#^\./#${name}/#" \
    -czf "$tarball" .

  python - "$ROOT" "$zipball" "$name" <<'PY'
from pathlib import Path
import sys
import zipfile

root = Path(sys.argv[1])
out = Path(sys.argv[2])
prefix = sys.argv[3]
excluded_parts = {'.git', '.venv', '.pytest_cache', '.ruff_cache', 'dist', '__pycache__'}
excluded_roots = {'artifacts', 'backups'}

with zipfile.ZipFile(out, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
    for path in sorted(root.rglob('*')):
        relative = path.relative_to(root)
        if not path.is_file():
            continue
        if any(part in excluded_parts for part in relative.parts):
            continue
        if relative.parts and relative.parts[0] in excluded_roots and path.name != '.gitkeep':
            continue
        if path.suffix == '.pyc':
            continue
        archive.write(path, Path(prefix) / relative)
PY
fi

(
  cd "$OUT"
  sha256sum "$(basename "$tarball")" "$(basename "$zipball")" > "$(basename "$checksums")"
)

printf '%s\n' "$tarball" "$zipball" "$checksums"
