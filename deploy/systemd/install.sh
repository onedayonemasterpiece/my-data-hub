#!/usr/bin/env bash
set -Eeuo pipefail

if [[ "${1:-}" != "INSTALL_MY_DATA_HUB_SYSTEMD" || "$(id -u)" != 0 ]]; then
  echo "usage: sudo $0 INSTALL_MY_DATA_HUB_SYSTEMD" >&2
  exit 2
fi

users=(api orchestrator mcp committer backup migrator verify canary monitoring)
for suffix in "${users[@]}"; do
  user="mydatahub-$suffix"
  if ! getent group "$user" >/dev/null; then groupadd --system "$user"; fi
  if ! id "$user" >/dev/null 2>&1; then
    useradd --system --gid "$user" --home-dir /nonexistent --shell /usr/sbin/nologin "$user"
  fi
done
if ! getent group mydatahub-artifacts >/dev/null; then groupadd --system mydatahub-artifacts; fi
usermod -a -G mydatahub-artifacts mydatahub-api
usermod -a -G mydatahub-artifacts mydatahub-orchestrator

install -d -o root -g root -m 0700 /etc/my-data-hub
for name in admin api orchestrator mcp committer backup migrator verify identity-verify connector-canary monitoring; do
  path="/etc/my-data-hub/$name.env"
  if [[ ! -f "$path" || -L "$path" || "$(stat -c '%u:%a' "$path")" != "0:600" ]]; then
    echo "$path must already exist as a root-owned, mode-0600 regular file" >&2
    exit 2
  fi
done
install -d -o mydatahub-api -g mydatahub-artifacts -m 2770 /var/lib/my-data-hub/artifacts

unit_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
for unit in "$unit_root"/*.service "$unit_root"/*.timer; do
  install -o root -g root -m 0644 "$unit" "/etc/systemd/system/$(basename "$unit")"
done
systemctl daemon-reload
systemctl enable my-data-hub-backup.timer my-data-hub-connector-committer.timer
