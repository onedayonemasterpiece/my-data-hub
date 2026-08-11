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

require_command() {
  command -v "$1" >/dev/null || { echo "$1 is required" >&2; exit 2; }
}
for command_name in docker flock git tar; do
  require_command "$command_name"
done
docker_path="$(command -v docker)"

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
source_root="$(git -C "$script_dir" rev-parse --show-toplevel)"
commit="$(git -C "$source_root" rev-parse HEAD)"
if [[ -n "$(git -C "$source_root" status --porcelain --untracked-files=no)" ]]; then
  echo "tracked worktree changes are forbidden for a release" >&2
  exit 2
fi

runtime_root="${MY_DATA_HUB_CONTROL_RUNTIME_DIR:-$HOME/.local/state/my-data-hub-control-plane}"
release_root="${MY_DATA_HUB_CONTROL_RELEASE_ROOT:-$HOME/.local/opt/my-data-hub-control-plane}"
case "$runtime_root:$release_root" in
  *[$'\n\r\t ']* ) echo "deployment paths may not contain whitespace" >&2; exit 2 ;;
esac
release="$release_root/releases/$commit"
current="$release_root/current"
[[ ! -L "$runtime_root" && ! -L "$release_root" ]] || {
  echo "runtime and release roots may not be symbolic links" >&2
  exit 2
}
mkdir -p "$runtime_root" "$release_root/releases"
[[ ! -L "$release_root/releases" ]] || { echo "release directory may not be a symbolic link" >&2; exit 2; }
chmod 700 "$runtime_root" "$release_root" "$release_root/releases"
exec 9>"$runtime_root/install.lock"
flock -n 9 || { echo "another control-plane install is running" >&2; exit 75; }

[[ ! -L "$release" ]] || { echo "immutable release may not be a symbolic link" >&2; exit 2; }
if [[ ! -d "$release" ]]; then
  staging="$(mktemp -d "$release_root/releases/.staging.XXXXXX")"
  trap 'rm -rf "$staging"' EXIT
  git -C "$source_root" archive "$commit" | tar -x -C "$staging"
  mv "$staging" "$release"
  trap - EXIT
  chmod -R a-w "$release"
fi

# PREPARE creates only an immutable release/image. It never reads secret files,
# changes the current pointer, starts a container, or touches systemd/autostart.
image="my-data-hub-control-plane:$commit"
"$docker_path" build --tag "$image" --file "$release/deploy/control-plane/Dockerfile" "$release"
if [[ "$action" == "PREPARE_CONTROL_PLANE" ]]; then
  printf 'prepared_control_plane_commit=%s\nruntime=unchanged\nautostart=unchanged\n' "$commit"
  exit 0
fi

for command_name in curl loginctl stat systemctl; do
  require_command "$command_name"
done
if [[ "${MY_DATA_HUB_APPROVED_CONTROL_COMMIT:-}" != "$commit" ]]; then
  echo "INSTALL requires MY_DATA_HUB_APPROVED_CONTROL_COMMIT to equal the exact release commit" >&2
  exit 2
fi
if [[ "$(loginctl show-user "$(id -u)" --property=Linger --value 2>/dev/null)" != "yes" ]]; then
  echo "user lingering must be enabled explicitly before INSTALL so the user unit starts after reboot" >&2
  exit 2
fi

env_root="${MY_DATA_HUB_CONTROL_ENV_DIR:-$runtime_root/env}"
secret_root="${MY_DATA_HUB_CONTROL_SECRET_DIR:-$runtime_root/secrets}"
ledger_dir="${MY_DATA_HUB_CONTROL_LEDGER_DIR:-$runtime_root/control-ledger}"
session_dir="${MY_DATA_HUB_MASTER_SESSION_DIR:-$runtime_root/master-sessions}"
asset_dir="${MY_DATA_HUB_MASTER_ASSET_DIR:-$runtime_root/master-assets}"
tls_ca_file="${MY_DATA_HUB_MASTER_TLS_CA_FILE:-$runtime_root/master-tls/ca.pem}"
provider_env="${MY_DATA_HUB_CONTROL_PROVIDER_ENV_FILE:-$env_root/provider.env}"
mcp_env="${MY_DATA_HUB_MCP_ENV_FILE:-$env_root/mcp-reader.env}"
oauth_env="${MY_DATA_HUB_OAUTH_ENV_FILE:-$env_root/oauth.env}"
oauth_key="${MY_DATA_HUB_OAUTH_SIGNING_KEY_FILE:-$secret_root/oauth-signing-key.pem}"
oauth_overlap_jwks="${MY_DATA_HUB_OAUTH_OVERLAP_JWKS_FILE:-$runtime_root/oauth-public/overlap-jwks.json}"
for path_value in "$env_root" "$secret_root" "$ledger_dir" "$session_dir" "$asset_dir" \
  "$tls_ca_file" "$provider_env" "$mcp_env" "$oauth_env" "$oauth_key" "$oauth_overlap_jwks"; do
  case "$path_value" in
    *[$'\n\r\t ']* ) echo "deployment inputs may not contain whitespace" >&2; exit 2 ;;
  esac
done
mkdir -p "$env_root" "$secret_root" "$ledger_dir" "$session_dir" "$HOME/.config/systemd/user"
for private_dir in "$env_root" "$secret_root" "$ledger_dir" "$session_dir"; do
  [[ ! -L "$private_dir" ]] || { echo "private runtime directories may not be symbolic links" >&2; exit 2; }
done
chmod 700 "$env_root" "$secret_root" "$ledger_dir" "$session_dir" "$HOME/.config/systemd/user"

require_regular_file() {
  local path="$1" label="$2"
  if [[ -L "$path" || ! -f "$path" ]]; then
    echo "$label must be a regular non-symlink file: $path" >&2
    exit 2
  fi
}
require_private_file() {
  local path="$1" label="$2" mode
  require_regular_file "$path" "$label"
  mode="$(stat -c '%a' "$path")"
  if (( (8#$mode & 077) != 0 )); then
    echo "$label must not be group/world accessible: $path" >&2
    exit 2
  fi
}
reject_data_plane_environment() {
  local path="$1" label="$2" line key compact
  while IFS= read -r line; do
    key="${line%%=*}"
    key="${key//[[:space:]]/}"
    key="${key^^}"
    key="${key#EXPORT}"
    compact="${key//_/}"
    if [[ "$compact" == "DATABASEURL" || "$compact" == MYDATAHUB*"DATABASEURL" \
      || "$key" =~ ^PG(HOST|PORT|DATABASE|USER|PASSWORD|PASSFILE|SERVICE|SERVICEFILE)$ ]]; then
      echo "$label contains a forbidden master data-plane environment variable" >&2
      exit 2
    fi
  done < "$path"
}
reject_environment_keys() {
  local path="$1" label="$2" pattern="$3"
  if grep -Eiq "^[[:space:]]*($pattern)[[:space:]]*=" "$path"; then
    echo "$label crosses a secret/environment boundary" >&2
    exit 2
  fi
}
require_private_file "$provider_env" "provider environment"
require_private_file "$mcp_env" "remote MCP environment"
require_private_file "$oauth_env" "OAuth environment"
require_private_file "$oauth_key" "OAuth signing key"
require_regular_file "$oauth_overlap_jwks" "OAuth overlap public JWKS"
require_regular_file "$tls_ca_file" "master TLS CA"
[[ -d "$asset_dir" && ! -L "$asset_dir" ]] || { echo "master asset directory is required" >&2; exit 2; }
for env_file in "$provider_env" "$mcp_env" "$oauth_env"; do
  reject_data_plane_environment "$env_file" "$(basename "$env_file")"
done
reject_environment_keys "$provider_env" "provider environment" \
  'MY_DATA_HUB_OAUTH_SIGNING_KEY|MY_DATA_HUB_OWNER_OIDC_CLIENT_SECRET'
reject_environment_keys "$mcp_env" "remote MCP environment" \
  'KAGGLE_API_TOKEN|MY_DATA_HUB_MASTER_RUNTIME_TOKEN_ROOT|MY_DATA_HUB_OAUTH_SIGNING_KEY'
reject_environment_keys "$oauth_env" "OAuth environment" \
  'KAGGLE_API_TOKEN|MY_DATA_HUB_MASTER_RUNTIME_TOKEN_ROOT|MY_DATA_HUB_KAGGLE_[A-Z0-9_]+'

compose_env="$runtime_root/compose.$commit.env"
cat > "$compose_env" <<ENV
MY_DATA_HUB_IMAGE_TAG=$commit
MY_DATA_HUB_CONTROL_UID=$(id -u)
MY_DATA_HUB_CONTROL_GID=$(id -g)
MY_DATA_HUB_CONTROL_LEDGER_DIR=$ledger_dir
MY_DATA_HUB_MASTER_SESSION_DIR=$session_dir
MY_DATA_HUB_MASTER_ASSET_DIR=$asset_dir
MY_DATA_HUB_MASTER_TLS_CA_FILE=$tls_ca_file
MY_DATA_HUB_CONTROL_PROVIDER_ENV_FILE=$provider_env
MY_DATA_HUB_MCP_ENV_FILE=$mcp_env
MY_DATA_HUB_OAUTH_ENV_FILE=$oauth_env
MY_DATA_HUB_OAUTH_SIGNING_KEY_FILE=$oauth_key
MY_DATA_HUB_OAUTH_OVERLAP_JWKS_FILE=$oauth_overlap_jwks
ENV
chmod 600 "$compose_env"

compose=("$docker_path" compose --env-file "$compose_env" --profile remote-mcp \
  --project-directory "$release" -f "$release/compose.control-plane.yaml")
"${compose[@]}" config --quiet

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
Description=my-data-hub lightweight control, OAuth, and remote MCP processes
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=$compose_env
ExecStartPre=$docker_path info
ExecStart=$docker_path compose --env-file $compose_env --profile remote-mcp --project-directory $release -f $release/compose.control-plane.yaml up --remove-orphans control-plane remote-mcp oauth-server
ExecReload=$docker_path compose --env-file $compose_env --profile remote-mcp --project-directory $release -f $release/compose.control-plane.yaml up -d --wait --remove-orphans control-plane remote-mcp oauth-server
ExecStop=$docker_path compose --env-file $compose_env --profile remote-mcp --project-directory $release -f $release/compose.control-plane.yaml down --remove-orphans
Restart=on-failure
RestartSec=10
TimeoutStartSec=300
TimeoutStopSec=120
SuccessExitStatus=0 130 143

[Install]
WantedBy=default.target
UNIT
chmod 600 "$unit_candidate"

rollback() {
  exit_code=$?
  trap - ERR
  set +e
  "${compose[@]}" down --remove-orphans
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

wait_http() {
  local name="$1" url="$2" host_header="${3:-}" attempt
  for attempt in $(seq 1 30); do
    if [[ -n "$host_header" ]]; then
      curl --fail --silent --show-error --connect-timeout 2 --max-time 5 \
        --header "Host: $host_header" "$url" >/dev/null 2>&1 && return 0
    else
      curl --fail --silent --show-error --connect-timeout 2 --max-time 5 \
        "$url" >/dev/null 2>&1 && return 0
    fi
    sleep 2
  done
  echo "$name did not become healthy on its loopback upstream" >&2
  return 1
}
wait_http control-plane http://127.0.0.1:8080/health/ready
wait_http remote-mcp http://127.0.0.1:8765/.well-known/oauth-protected-resource/mcp mcp-datahub.kenigevents.ru
wait_http oauth-server http://127.0.0.1:8780/.well-known/oauth-authorization-server

curl --fail --silent --show-error --connect-timeout 2 --max-time 5 \
  http://127.0.0.1:8080/health/ready > "$runtime_root/ready.$commit.json"
next_link="$release_root/.current.$commit"
ln -sfn "$release" "$next_link"
mv -Tf "$next_link" "$current"
trap - ERR
rm -f "$unit_backup"
printf 'installed_control_plane_commit=%s\nservices=control-plane,remote-mcp,oauth-server\nmaster_state=ABSENT_or_durable_runtime_state\n' "$commit"
