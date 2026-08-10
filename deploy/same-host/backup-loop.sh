#!/usr/bin/env bash
set -Eeuo pipefail

interval="${MY_DATA_HUB_BACKUP_INTERVAL_SECONDS:-86400}"
case "$interval" in (*[!0-9]*|'') echo "invalid backup interval" >&2; exit 2;; esac

while true; do
  timeout --signal=TERM 30m scripts/backup_postgres.sh
  date -u +%Y-%m-%dT%H:%M:%SZ > /var/lib/my-data-hub/backups/last-success
  sleep "$interval"
done
