#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

DATABASE_URL="${MY_DATA_HUB_BACKUP_DATABASE_URL:-${MY_DATA_HUB_DATABASE_URL:-}}"
BACKUP_ROOT="${MY_DATA_HUB_BACKUP_ROOT:-./backups}"
RETENTION_DAYS="${MY_DATA_HUB_BACKUP_RETENTION_DAYS:-14}"
AGE_RECIPIENT="${MY_DATA_HUB_BACKUP_AGE_RECIPIENT:-}"
SOURCE_INSTANCE="${MY_DATA_HUB_BACKUP_SOURCE_INSTANCE:-}"
SOURCE_ENVIRONMENT="${MY_DATA_HUB_BACKUP_SOURCE_ENVIRONMENT:-}"
SCRIPT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPOSITORY_ROOT="$(cd -- "$SCRIPT_ROOT/.." && pwd)"
PYTHON_BIN="${MY_DATA_HUB_RECOVERY_PYTHON:-python3}"

if [[ -z "$DATABASE_URL" ]]; then
  echo "MY_DATA_HUB_BACKUP_DATABASE_URL or MY_DATA_HUB_DATABASE_URL is required" >&2
  exit 2
fi
command -v pg_dump >/dev/null || { echo "pg_dump is required" >&2; exit 2; }
command -v sha256sum >/dev/null || { echo "sha256sum is required" >&2; exit 2; }
command -v age >/dev/null || { echo "age is required; plaintext backups are forbidden" >&2; exit 2; }
command -v "$PYTHON_BIN" >/dev/null || { echo "$PYTHON_BIN is required" >&2; exit 2; }
if [[ -z "$AGE_RECIPIENT" || "$AGE_RECIPIENT" == -* || "$AGE_RECIPIENT" == *$'\n'* ]]; then
  echo "MY_DATA_HUB_BACKUP_AGE_RECIPIENT must be a valid age recipient" >&2
  exit 2
fi
if [[ -z "$SOURCE_INSTANCE" || -z "$SOURCE_ENVIRONMENT" ]]; then
  echo "MY_DATA_HUB_BACKUP_SOURCE_INSTANCE and MY_DATA_HUB_BACKUP_SOURCE_ENVIRONMENT are required" >&2
  exit 2
fi
if [[ ! "$RETENTION_DAYS" =~ ^[0-9]+$ ]]; then
  echo "MY_DATA_HUB_BACKUP_RETENTION_DAYS must be a non-negative integer" >&2
  exit 2
fi

REPOSITORY_COMMIT="${MY_DATA_HUB_BACKUP_REPOSITORY_COMMIT:-}"
if [[ -z "$REPOSITORY_COMMIT" ]]; then
  REPOSITORY_COMMIT="$(git -C "$REPOSITORY_ROOT" rev-parse HEAD 2>/dev/null || true)"
fi
if [[ ! "$REPOSITORY_COMMIT" =~ ^[a-f0-9]{40,64}$ ]]; then
  echo "MY_DATA_HUB_BACKUP_REPOSITORY_COMMIT or the current Git HEAD must be a commit digest" >&2
  exit 2
fi

mkdir -p "$BACKUP_ROOT"
chmod 700 "$BACKUP_ROOT"
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
base="$BACKUP_ROOT/my-data-hub-$stamp"
tmp="$base.dump.age.tmp"
encrypted="$base.dump.age"
manifest="$base.manifest.json"
started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
if [[ -e "$encrypted" || -e "$manifest" ]]; then
  echo "backup generation already exists for timestamp $stamp" >&2
  exit 1
fi

cleanup() { rm -f "$tmp"; }
trap cleanup EXIT

# pg_dump streams directly into age. No plaintext dump is ever written to local storage.
PGDATABASE="$DATABASE_URL" pg_dump --format=custom --compress=9 \
  --no-owner --no-privileges --file=- \
  | age --encrypt --recipient "$AGE_RECIPIENT" --output "$tmp"
test -s "$tmp" || { echo "encrypted backup is empty" >&2; exit 1; }
chmod 600 "$tmp"
mv "$tmp" "$encrypted"
completed_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
pg_dump_version="$(pg_dump --version)"
"$PYTHON_BIN" "$SCRIPT_ROOT/recovery/create_manifest.py" \
  --artifact "$encrypted" \
  --output "$manifest" \
  --started-at "$started_at" \
  --completed-at "$completed_at" \
  --source-instance "$SOURCE_INSTANCE" \
  --source-environment "$SOURCE_ENVIRONMENT" \
  --repository-commit "$REPOSITORY_COMMIT" \
  --pg-dump-version "$pg_dump_version" \
  --age-recipient "$AGE_RECIPIENT"
sha="$(sha256sum "$encrypted" | awk '{print $1}')"

find "$BACKUP_ROOT" -maxdepth 1 -type f \
  \( -name 'my-data-hub-*.dump.age' -o -name 'my-data-hub-*.manifest.json' \) \
  -mtime "+$RETENTION_DAYS" -delete

echo "backup=$encrypted"
echo "manifest=$manifest"
echo "encrypted_sha256=$sha"
