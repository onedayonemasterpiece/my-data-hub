#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

if [[ $# -lt 2 || $# -gt 3 ]]; then
  echo "usage: $0 BACKUP.dump.age BACKUP.manifest.json [OFFHOST.evidence.json]" >&2
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
command -v psql >/dev/null || { echo "psql is required" >&2; exit 2; }
command -v age >/dev/null || { echo "age is required" >&2; exit 2; }
PYTHON_BIN="${MY_DATA_HUB_RECOVERY_PYTHON:-python3}"
command -v "$PYTHON_BIN" >/dev/null || { echo "$PYTHON_BIN is required" >&2; exit 2; }

if [[ "${MY_DATA_HUB_RESTORE_ISOLATED_CONFIRM:-}" != "ISOLATED_FRESH_TARGET" ]]; then
  echo "set MY_DATA_HUB_RESTORE_ISOLATED_CONFIRM=ISOLATED_FRESH_TARGET" >&2
  exit 2
fi
TARGET_ID="${MY_DATA_HUB_RESTORE_TARGET_ID:-}"
EXPECTED_DATABASE="${MY_DATA_HUB_RESTORE_EXPECTED_DATABASE:-}"
AGE_IDENTITY_FILE="${MY_DATA_HUB_RESTORE_AGE_IDENTITY_FILE:-}"
RECEIPT="${MY_DATA_HUB_RECOVERY_RECEIPT:-}"
OFFHOST_EVIDENCE="${3:-${MY_DATA_HUB_OFFHOST_EVIDENCE:-}}"
if [[ -z "$TARGET_ID" || -z "$EXPECTED_DATABASE" || -z "$AGE_IDENTITY_FILE" || -z "$RECEIPT" || -z "$OFFHOST_EVIDENCE" ]]; then
  echo "restore target id/database, age identity, off-host evidence, and receipt path are required" >&2
  exit 2
fi
if [[ ! -f "$AGE_IDENTITY_FILE" || -L "$AGE_IDENTITY_FILE" ]]; then
  echo "MY_DATA_HUB_RESTORE_AGE_IDENTITY_FILE must be a regular non-symlink file" >&2
  exit 2
fi
identity_mode="$(stat -c '%a' "$AGE_IDENTITY_FILE")"
if [[ "$identity_mode" != "600" && "$identity_mode" != "400" ]]; then
  echo "age identity must have mode 0600 or 0400" >&2
  exit 2
fi
if [[ "$(stat -c '%u' "$AGE_IDENTITY_FILE")" != "$(id -u)" ]]; then
  echo "age identity must be owned by the restore process user" >&2
  exit 2
fi
if [[ -e "$RECEIPT" ]]; then
  echo "recovery receipt output already exists; choose a new path" >&2
  exit 2
fi
if [[ -n "${MY_DATA_HUB_BACKUP_DATABASE_URL:-}" && "$DATABASE_URL" == "$MY_DATA_HUB_BACKUP_DATABASE_URL" ]] || \
   [[ -n "${MY_DATA_HUB_DATABASE_URL:-}" && "$DATABASE_URL" == "$MY_DATA_HUB_DATABASE_URL" ]]; then
  echo "restore URL must not equal a configured canonical/backup database URL" >&2
  exit 2
fi

ENCRYPTED="$1"
MANIFEST="$2"
SCRIPT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
"$PYTHON_BIN" "$SCRIPT_ROOT/recovery/verify_artifact.py" --artifact "$ENCRYPTED" --manifest "$MANIFEST"
evidence_verification="$("$PYTHON_BIN" "$SCRIPT_ROOT/recovery/verify_offhost_evidence.py" \
  --artifact "$ENCRYPTED" --manifest "$MANIFEST" --offhost-evidence "$OFFHOST_EVIDENCE")"
source_instance="$(printf '%s\n' "$evidence_verification" | sed -n 's/^source_instance=//p')"
"$PYTHON_BIN" "$SCRIPT_ROOT/recovery/validate_target.py" \
  --target-id "$TARGET_ID" --target-database "$EXPECTED_DATABASE" --source-instance "$source_instance"

# A single query binds both the database identity and the absence of user relations.
freshness="$(PGDATABASE="$DATABASE_URL" psql --no-psqlrc --set=ON_ERROR_STOP=1 --tuples-only --no-align \
  --command="SELECT current_database() || '|' || count(*)::text FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relkind IN ('r','p','v','m','S','f') AND n.nspname NOT IN ('pg_catalog','information_schema') AND n.nspname !~ '^pg_toast';")"
freshness="${freshness//$'\n'/}"
actual_database="${freshness%%|*}"
relations_before="${freshness##*|}"
if [[ "$actual_database" != "$EXPECTED_DATABASE" ]]; then
  echo "connected database does not match MY_DATA_HUB_RESTORE_EXPECTED_DATABASE" >&2
  exit 1
fi
if [[ "$relations_before" != "0" ]]; then
  echo "restore target is not fresh: found $relations_before user relations" >&2
  exit 1
fi

restore_started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
plain="$(mktemp "${TMPDIR:-/tmp}/my-data-hub-restore.XXXXXX.dump")"
cleanup() { rm -f "$plain"; }
trap cleanup EXIT
chmod 600 "$plain"
age --decrypt --identity "$AGE_IDENTITY_FILE" --output "$plain" "$ENCRYPTED"
PGDATABASE="$DATABASE_URL" pg_restore --exit-on-error --single-transaction \
  --no-owner --no-privileges "$plain"

# The application verifier is mandatory. Its output may contain deployment metadata, so
# it is not copied into the receipt.
MY_DATA_HUB_DATABASE_URL="$DATABASE_URL" "$PYTHON_BIN" -m my_data_hub db verify >/dev/null
verification_file="$(mktemp "${TMPDIR:-/tmp}/my-data-hub-verify.XXXXXX.json")"
cleanup() { rm -f "$plain" "$verification_file"; }
MY_DATA_HUB_RESTORE_DATABASE_URL="$DATABASE_URL" "$PYTHON_BIN" \
  -m my_data_hub.recovery_verify --database-url "$DATABASE_URL" >"$verification_file"
restore_completed_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
"$PYTHON_BIN" "$SCRIPT_ROOT/recovery/write_receipt.py" \
  --artifact "$ENCRYPTED" \
  --manifest "$MANIFEST" \
  --offhost-evidence "$OFFHOST_EVIDENCE" \
  --target-id "$TARGET_ID" \
  --target-database "$EXPECTED_DATABASE" \
  --started-at "$restore_started_at" \
  --completed-at "$restore_completed_at" \
  --relations-before "$relations_before" \
  --verification "$verification_file" \
  --output "$RECEIPT"
if [[ -n "${MY_DATA_HUB_RECOVERY_CONTROL_DATABASE_URL:-}" ]]; then
  "$PYTHON_BIN" -m my_data_hub.recovery_record \
    --database-url "$MY_DATA_HUB_RECOVERY_CONTROL_DATABASE_URL" --receipt "$RECEIPT"
else
  echo "warning: recovery receipt is not recorded in recovery.evidence; operator gate remains closed" >&2
fi
echo "restore completed into isolated target; automatic promotion is forbidden"
