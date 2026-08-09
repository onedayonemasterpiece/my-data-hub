#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

if [[ $# -ne 2 ]]; then
  echo "usage: $0 BACKUP.dump BACKUP.manifest.json" >&2
  exit 2
fi
if [[ "${MY_DATA_HUB_RESTORE_CONFIRM:-}" != "RESTORE_MY_DATA_HUB" ]]; then
  echo "set MY_DATA_HUB_RESTORE_CONFIRM=RESTORE_MY_DATA_HUB" >&2
  exit 2
fi
DATABASE_URL="${MY_DATA_HUB_RESTORE_DATABASE_URL:-}"
if [[ -z "$DATABASE_URL" ]]; then
  echo "MY_DATA_HUB_RESTORE_DATABASE_URL is required" >&2
  exit 2
fi
command -v pg_restore >/dev/null || { echo "pg_restore is required" >&2; exit 2; }

DUMP="$1"
MANIFEST="$2"
python - "$DUMP" "$MANIFEST" <<'PY'
from __future__ import annotations
import hashlib
import json
import sys
from pathlib import Path

dump = Path(sys.argv[1])
manifest = json.loads(Path(sys.argv[2]).read_text(encoding="utf-8"))
if manifest.get("schema_version") != "my-data-hub-postgres-backup.v1":
    raise SystemExit("unsupported backup manifest")
if manifest.get("dump_file") != dump.name:
    raise SystemExit("manifest dump_file does not match")
digest = hashlib.sha256(dump.read_bytes()).hexdigest()
if digest != manifest.get("sha256"):
    raise SystemExit("backup SHA-256 mismatch")
if dump.stat().st_size != manifest.get("byte_size"):
    raise SystemExit("backup byte size mismatch")
print({"verified": True, "sha256": digest, "bytes": dump.stat().st_size})
PY

pg_restore --dbname="$DATABASE_URL" --clean --if-exists --no-owner --no-privileges "$DUMP"
echo "restore completed; run: my-data-hub db verify"
