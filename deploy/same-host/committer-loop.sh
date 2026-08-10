#!/usr/bin/env bash
set -Eeuo pipefail

interval="${MY_DATA_HUB_COMMITTER_INTERVAL_SECONDS:-60}"
case "$interval" in (*[!0-9]*|'') echo "invalid committer interval" >&2; exit 2;; esac

while true; do
  timeout --signal=TERM 55s python scripts/run_connector_committer.py --limit 25 || true
  sleep "$interval"
done
