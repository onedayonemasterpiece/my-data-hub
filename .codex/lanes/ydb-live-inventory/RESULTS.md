# YDB live read-only inventory result

## Outcome

- **Done:** used an ephemeral IAM token impersonating the dedicated
  `my-data-hub-ydb-reader` service account (`ajeri3qs6jbijih0bs5d`). The live
  database binding is exactly `ydb.viewer`; no token or source row is recorded in
  Git evidence.
- **Done:** described the single source table and its 27-column schema, and ran a
  no-sample/no-`LIMIT` live aggregate at `2026-08-11T23:27:05Z`. It observed 266
  rows, 266 distinct `record_id` values, 14 batches, 14 source files, 202
  `confirmed_external`, and 64 `needs_externality_review`.
- **Blocked:** the complete ordered payload export. Three bounded
  `QuerySnapshotReadOnly` attempts (30 s then 60 s backoff) and a lower-level
  ordered table-read attempt were rejected with YDB error 200803 /
  `Throughput limit exceeded` (`CLIENT_RESOURCE_EXHAUSTED`). The database is
  running with throttling enabled and an effective configured limit of zero.
- **Not done / not claimed:** no file hash, logical hash, or record-id-set hash is
  reported for a full export because no full payload was returned. The protected
  export root remains empty.
- **Safety:** PostgreSQL was never contacted; YDB rows and provider configuration
  were not mutated. Database metadata was byte-logically identical before and
  after the attempts (`e8c8ff...b46`), and raw export root mode is `0700`.

Sanitized receipt:
`docs/operations/evidence/2026-08-11-operational-mvp/ydb-readonly-export-blocker.json`.

## Protected continuation location

The exact owner-only export root is:

```text
/home/dev/.local/share/my-data-hub/protected/ydb-region-talk-20260811/exports
```

It currently contains no source rows. After an owner changes the external quota
state, continue without changing repository or provider state in this lane:

```bash
set -euo pipefail
umask 077
ROOT=/home/dev/.local/share/my-data-hub/protected/ydb-region-talk-20260811
install -d -m 700 "$ROOT" "$ROOT/exports"
TOKEN_FILE=$(mktemp "$ROOT/.iam-token.XXXXXX")
cleanup_token() {
  python3 - "$TOKEN_FILE" <<'PY'
from pathlib import Path
import sys
path = Path(sys.argv[1])
if path.exists():
    path.write_bytes(b"")
    path.unlink()
PY
}
trap cleanup_token EXIT
yc iam create-token --impersonate-service-account-id ajeri3qs6jbijih0bs5d >"$TOKEN_FILE"
chmod 600 "$TOKEN_FILE"
uv run --extra ydb python scripts/provider/read_only_ydb_blogger_export.py \
  --endpoint grpcs://ydb.serverless.yandexcloud.net:2135 \
  --database /ru-central1/b1ghfk15fpug7mn5439l/etnkibjidis0o6stn2cq \
  --reader-service-account-id ajeri3qs6jbijih0bs5d \
  --iam-token-file "$TOKEN_FILE" \
  --output-root "$ROOT/exports" \
  --max-attempts 4 \
  --initial-backoff-seconds 30
```

On success the command prints only the protected directory, count, manifest hash,
and logical hash. Source rows remain in the mode-`0600` JSONL file outside Git.
