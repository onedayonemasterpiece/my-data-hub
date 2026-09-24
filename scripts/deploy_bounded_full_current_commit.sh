#!/usr/bin/env bash
set -Eeuo pipefail

script_dir="$(cd -- "$(dirname -- "$0")" && pwd -P)"
root="$(git -C "$script_dir" rev-parse --show-toplevel)"
commit="$(git -C "$root" rev-parse HEAD)"
if [[ -n "$(git -C "$root" status --porcelain --untracked-files=no)" ]]; then
  echo "tracked worktree changes are forbidden for bounded-full deployment" >&2
  exit 2
fi
export MY_DATA_HUB_APPROVED_CONTROL_COMMIT="$commit"
exec "$root/deploy/control-plane/install.sh" INSTALL_MY_DATA_HUB_BOUNDED_FULL
