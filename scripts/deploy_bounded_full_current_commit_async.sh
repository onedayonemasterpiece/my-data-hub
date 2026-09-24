#!/usr/bin/env bash
set -Eeuo pipefail

script_dir="$(cd -- "$(dirname -- "$0")" && pwd -P)"
root="$(git -C "$script_dir" rev-parse --show-toplevel)"
commit="$(git -C "$root" rev-parse HEAD)"
if [[ -n "$(git -C "$root" status --porcelain --untracked-files=no)" ]]; then
  echo "tracked worktree changes are forbidden for bounded-full deployment" >&2
  exit 2
fi
log="/tmp/my-data-hub-bounded-full-$commit.log"
uid="$(id -u)"
export XDG_RUNTIME_DIR="/run/user/$uid"
export DBUS_SESSION_BUS_ADDRESS="unix:path=$XDG_RUNTIME_DIR/bus"
MY_DATA_HUB_APPROVED_CONTROL_COMMIT="$commit" nohup \
  "$root/deploy/control-plane/install.sh" INSTALL_MY_DATA_HUB_BOUNDED_FULL \
  >"$log" 2>&1 </dev/null &
printf 'pid=%s\nlog=%s\ncommit=%s\n' "$!" "$log" "$commit"
