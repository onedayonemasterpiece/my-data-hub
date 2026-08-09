#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

DATABASE_URL="${MY_DATA_HUB_BACKUP_DATABASE_URL:-${MY_DATA_HUB_DATABASE_URL:-}}"
BACKUP_ROOT="${MY_DATA_HUB_BACKUP_ROOT:-./backups}"
RETENTION_DAYS="${MY_DATA_HUB_BACKUP_RETENTION_DAYS:-14}"

if [[ -z "$DATABASE_URL" ]]; then
  echo "MY_DATA_HUB_BACKUP_DATABASE_URL or MY_DATA_HUB_DATABASE_URL is required" >&2
  exit 2
fi
command -v pg_dump >/dev/null || { echo "pg_dump is required" >&2; exit 2; }
command -v sha256sum >/dev/null || { echo "sha256sum is required" >&2; exit 2; }

mkdir -p "$BACKUP_ROOT"
chmod 700 "$BACKUP_ROOT"
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
base="$BACKUP_ROOT/my-data-hub-$stamp"
tmp="$base.dump.tmp"
dump="$base.dump"
manifest="$base.manifest.json"

cleanup() { rm -f "$tmp"; }
trap cleanup EXIT

pg_dump --dbname="$DATABASE_URL" --format=custom --compress=9 \
  --no-owner --no-privileges --file="$tmp"
mv "$tmp" "$dump"
sha="$(sha256sum "$dump" | awk '{print $1}')"
bytes="$(stat -c '%s' "$dump")"

python - "$dump" "$manifest" "$sha" "$bytes" <<'PY'
from __future__ import annotations
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

dump, manifest, sha, byte_size = sys.argv[1:]
payload = {
    "schema_version": "my-data-hub-postgres-backup.v1",
    "created_at": datetime.now(UTC).isoformat(),
    "dump_file": Path(dump).name,
    "sha256": sha,
    "byte_size": int(byte_size),
    "format": "pg_dump-custom",
    "source": "MY_DATA_HUB_BACKUP_DATABASE_URL",
}
Path(manifest).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
os.chmod(manifest, 0o600)
PY
chmod 600 "$dump"

find "$BACKUP_ROOT" -maxdepth 1 -type f \
  \( -name 'my-data-hub-*.dump' -o -name 'my-data-hub-*.manifest.json' \) \
  -mtime "+$RETENTION_DAYS" -delete

echo "backup=$dump"
echo "manifest=$manifest"
echo "sha256=$sha"
