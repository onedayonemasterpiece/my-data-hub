#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

action="${1:-}"
if [[ "$action" == "INSTALL_MY_DATA_HUB_SAME_HOST" ]]; then
  echo "FORBIDDEN: local PostgreSQL topology is superseded; no local database will be installed" >&2
  exit 78
fi
if [[ "$action" != "PREPARE_CONTROL_PLANE" && "$action" != "INSTALL_MY_DATA_HUB_CONTROL_PLANE" ]]; then
  echo "usage: $0 PREPARE_CONTROL_PLANE|INSTALL_MY_DATA_HUB_CONTROL_PLANE" >&2
  exit 2
fi
for command_name in curl docker flock git systemctl tar; do
  command -v "$command_name" >/dev/null || { echo "$command_name is required" >&2; exit 2; }
done
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
source_root="$(git -C "$script_dir" rev-parse --show-toplevel)"
commit="$(git -C "$source_root" rev-parse HEAD)"
if [[ -n "$(git -C "$source_root" status --porcelain --untracked-files=no)" ]]; then
  echo "tracked worktree changes are forbidden for a release" >&2
  exit 2
fi
runtime_root="${MY_DATA_HUB_CONTROL_RUNTIME_DIR:-$HOME/.local/state/my-data-hub-control-plane}"
release_root="${MY_DATA_HUB_CONTROL_RELEASE_ROOT:-$HOME/.local/opt/my-data-hub-control-plane}"
release="$release_root/releases/$commit"
current="$release_root/current"
mkdir -p "$runtime_root" "$release_root/releases" "$HOME/.config/systemd/user"
chmod 700 "$runtime_root" "$release_root" "$release_root/releases"
state_dir="$runtime_root/state"
mkdir -p "$state_dir"
chmod 700 "$state_dir"
exec 9>"$runtime_root/install.lock"
flock -n 9 || { echo "another control-plane install is running" >&2; exit 75; }
if [[ ! -d "$release" ]]; then
  staging="$(mktemp -d "$release_root/releases/.staging.XXXXXX")"
  trap 'rm -rf "$staging"' EXIT
  git -C "$source_root" archive "$commit" | tar -x -C "$staging"
  mv "$staging" "$release"
  trap - EXIT
  chmod -R a-w "$release"
fi
export MY_DATA_HUB_IMAGE_TAG="$commit"
docker compose --project-directory "$release" -f "$release/compose.control-plane.yaml" build control-plane
if [[ "$action" == "PREPARE_CONTROL_PLANE" ]]; then
  printf 'prepared_control_plane_commit=%s\nautostart=unchanged\n' "$commit"
  exit 0
fi
unit="$HOME/.config/systemd/user/my-data-hub-control-plane.service"
unit_candidate="$runtime_root/my-data-hub-control-plane.service.$commit"
unit_backup="$runtime_root/my-data-hub-control-plane.service.previous"
previous_release=""
previous_unit_present=false
previous_unit_enabled=false
previous_unit_active=false
if [[ -L "$current" ]]; then
  previous_release="$(readlink -f "$current")"
fi
if [[ -f "$unit" ]]; then
  cp "$unit" "$unit_backup"
  previous_unit_present=true
fi
if systemctl --user is-enabled --quiet my-data-hub-control-plane.service 2>/dev/null; then
  previous_unit_enabled=true
fi
if systemctl --user is-active --quiet my-data-hub-control-plane.service 2>/dev/null; then
  previous_unit_active=true
fi

cat > "$unit_candidate" <<UNIT
[Unit]
Description=my-data-hub lightweight control plane (master may be ABSENT)
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
RemainAfterExit=yes
Environment=MY_DATA_HUB_IMAGE_TAG=$commit
Environment=MY_DATA_HUB_CONTROL_STATE_DIR=$state_dir
Environment=MY_DATA_HUB_CONTROL_UID=$(id -u)
Environment=MY_DATA_HUB_CONTROL_GID=$(id -g)
ExecStart=/usr/bin/docker compose --project-directory $release -f $release/compose.control-plane.yaml up -d --wait control-plane
ExecStop=/usr/bin/docker compose --project-directory $release -f $release/compose.control-plane.yaml down
TimeoutStartSec=300

[Install]
WantedBy=default.target
UNIT
chmod 600 "$unit_candidate"

rollback() {
  exit_code=$?
  trap - ERR
  set +e
  docker compose --project-directory "$release" -f "$release/compose.control-plane.yaml" down
  if [[ "$previous_unit_present" == true ]]; then
    cp "$unit_backup" "$unit"
  else
    rm -f "$unit"
  fi
  systemctl --user daemon-reload
  if [[ "$previous_unit_enabled" == true ]]; then
    systemctl --user enable my-data-hub-control-plane.service
  else
    systemctl --user disable my-data-hub-control-plane.service
  fi
  if [[ "$previous_unit_active" == true ]]; then
    systemctl --user restart my-data-hub-control-plane.service
  else
    systemctl --user stop my-data-hub-control-plane.service
  fi
  if [[ -n "$previous_release" ]]; then
    rollback_link="$release_root/.current.rollback.$$"
    ln -s "$previous_release" "$rollback_link"
    mv -Tf "$rollback_link" "$current"
  else
    rm -f "$current"
  fi
  echo "control-plane install failed; previous unit and release pointer restored" >&2
  exit "$exit_code"
}
trap rollback ERR

mv "$unit_candidate" "$unit"
systemctl --user daemon-reload
systemctl --user enable my-data-hub-control-plane.service
systemctl --user restart my-data-hub-control-plane.service
curl --fail --silent --show-error http://127.0.0.1:8080/health/ready > "$runtime_root/ready.json"
next_link="$release_root/.current.$commit"
ln -sfn "$release" "$next_link"
mv -Tf "$next_link" "$current"
trap - ERR
rm -f "$unit_backup"
printf 'installed_control_plane_commit=%s\nmaster_state=ABSENT\n' "$commit"
