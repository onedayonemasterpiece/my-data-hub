#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

action="${1:-}"
operator_profile=false
acceptance_supervisor=false
acceptance_scenarios=false
if [[ "$action" == "INSTALL_MY_DATA_HUB_SAME_HOST" ]]; then
  echo "FORBIDDEN: local PostgreSQL topology is superseded; no local database will be installed" >&2
  exit 78
fi
if [[ "$action" == "INSTALL_MY_DATA_HUB_CONTROL_PLANE_OPERATOR" ]]; then
  operator_profile=true
fi
if [[ "$action" != "PREPARE_CONTROL_PLANE" && "$action" != "INSTALL_MY_DATA_HUB_CONTROL_PLANE" \
  && "$operator_profile" != true ]]; then
  echo "usage: $0 PREPARE_CONTROL_PLANE|INSTALL_MY_DATA_HUB_CONTROL_PLANE|INSTALL_MY_DATA_HUB_CONTROL_PLANE_OPERATOR" >&2
  exit 2
fi

require_command() {
  command -v "$1" >/dev/null || { echo "$1 is required" >&2; exit 2; }
}
for command_name in awk df docker flock git tar; do
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
minimum_free_kib="${MY_DATA_HUB_CONTROL_MIN_FREE_KIB:-4194304}"
[[ "$minimum_free_kib" =~ ^[0-9]+$ ]] && (( minimum_free_kib >= 1048576 && minimum_free_kib <= 104857600 )) || {
  echo "MY_DATA_HUB_CONTROL_MIN_FREE_KIB must be 1..100 GiB expressed in KiB" >&2
  exit 2
}
for disk_path in "$runtime_root" "$release_root"; do
  available_kib="$(df -Pk "$disk_path" | awk 'NR==2 {print $4}')"
  [[ "$available_kib" =~ ^[0-9]+$ ]] || { echo "disk headroom could not be measured" >&2; exit 2; }
  if (( available_kib < minimum_free_kib )); then
    echo "control-plane deployment requires at least ${minimum_free_kib} KiB free on $disk_path" >&2
    exit 75
  fi
done
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

for command_name in curl loginctl python3 stat systemctl; do
  require_command "$command_name"
done
python_path="$(command -v python3)"
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
tls_dir="${MY_DATA_HUB_MASTER_TLS_DIR:-$runtime_root/master-tls}"
tls_ca_file="$tls_dir/ca.pem"
provider_env="${MY_DATA_HUB_CONTROL_PROVIDER_ENV_FILE:-$env_root/provider.env}"
mcp_env="${MY_DATA_HUB_MCP_ENV_FILE:-$env_root/mcp-reader.env}"
oauth_env="${MY_DATA_HUB_OAUTH_ENV_FILE:-$env_root/oauth.env}"
oauth_key="${MY_DATA_HUB_OAUTH_SIGNING_KEY_FILE:-$secret_root/oauth-signing-key.pem}"
oauth_overlap_jwks="${MY_DATA_HUB_OAUTH_OVERLAP_JWKS_FILE:-$runtime_root/oauth-public/overlap-jwks.json}"
operator_gate_receipt="${MY_DATA_HUB_OPERATOR_SECURITY_GATE_RECEIPT_FILE:-$runtime_root/operator-security-gate.json}"
operator_gate_key="${MY_DATA_HUB_MCP_WRITE_GATE_SECRET_FILE:-$secret_root/mcp-write-gate.key}"
control_gateway_token="${MY_DATA_HUB_MCP_CONTROL_GATEWAY_TOKEN_FILE:-$secret_root/mcp-control-gateway.token}"
tunnel_broker_socket_dir="${MY_DATA_HUB_TUNNEL_BROKER_SOCKET_DIR:-/run/my-data-hub/tunnel-broker}"
acceptance_socket_dir="${MY_DATA_HUB_ACCEPTANCE_SUPERVISOR_SOCKET_DIR:-$runtime_root/acceptance-supervisor}"
acceptance_key="${MY_DATA_HUB_ACCEPTANCE_SUPERVISOR_KEY_FILE:-$acceptance_socket_dir/supervisor.key}"
checkpoint_acceptance_deployment="${MY_DATA_HUB_CHECKPOINT_ACCEPTANCE_DEPLOYMENT_FILE:-$runtime_root/checkpoint-acceptance-deployment.json}"
if [[ -n "${MY_DATA_HUB_ENABLE_ACCEPTANCE_SCENARIOS:-}" ]]; then
  if [[ "$operator_profile" != true \
    || "${MY_DATA_HUB_ENABLE_ACCEPTANCE_SCENARIOS}" != "I_ACKNOWLEDGE_PROTECTED_ACCEPTANCE_EFFECTS" ]]; then
    echo "acceptance scenarios require operator install and the exact protected-effects acknowledgement" >&2
    exit 2
  fi
  acceptance_scenarios=true
fi
if [[ -n "${MY_DATA_HUB_ENABLE_ACCEPTANCE_SUPERVISOR:-}" ]]; then
  if [[ "$operator_profile" != true \
    || "${MY_DATA_HUB_ENABLE_ACCEPTANCE_SUPERVISOR}" != "I_ACKNOWLEDGE_TASK_BOUND_CONTROL_RESTART" ]]; then
    echo "acceptance supervisor requires operator install and the exact restart acknowledgement" >&2
    exit 2
  fi
  if [[ "${MY_DATA_HUB_CONTROL_PORT:-8080}" != "8080" ]]; then
    echo "acceptance supervisor requires the fixed loopback control health port 8080" >&2
    exit 2
  fi
  acceptance_supervisor=true
fi
for path_value in "$env_root" "$secret_root" "$ledger_dir" "$session_dir" "$asset_dir" \
  "$tls_dir" "$tls_ca_file" "$provider_env" "$mcp_env" "$oauth_env" "$oauth_key" "$oauth_overlap_jwks" \
  "$operator_gate_receipt" "$operator_gate_key" "$control_gateway_token" "$tunnel_broker_socket_dir" \
  "$acceptance_socket_dir" "$acceptance_key" "$checkpoint_acceptance_deployment"; do
  case "$path_value" in
    *[$'\n\r\t ']* ) echo "deployment inputs may not contain whitespace" >&2; exit 2 ;;
  esac
done
mkdir -p "$env_root" "$secret_root" "$ledger_dir" "$session_dir" "$tls_dir" "$HOME/.config/systemd/user"
for private_dir in "$env_root" "$secret_root" "$ledger_dir" "$session_dir" "$tls_dir"; do
  [[ ! -L "$private_dir" ]] || { echo "private runtime directories may not be symbolic links" >&2; exit 2; }
done
chmod 700 "$env_root" "$secret_root" "$ledger_dir" "$session_dir" "$tls_dir" "$HOME/.config/systemd/user"
runtime_uid="$(id -u)"
for private_dir in "$env_root" "$secret_root" "$ledger_dir" "$session_dir" "$tls_dir"; do
  if [[ "$(stat -c '%u' "$private_dir")" != "$runtime_uid" \
    || "$(stat -c '%a' "$private_dir")" != "700" ]]; then
    echo "private runtime directory must be owned by the service user with mode 0700" >&2
    exit 2
  fi
done

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
if [[ ! -e "$tls_ca_file" ]]; then
  : > "$tls_ca_file"
  chmod 600 "$tls_ca_file"
fi
require_private_file "$tls_ca_file" "master TLS CA publication target"
[[ -d "$tunnel_broker_socket_dir" && ! -L "$tunnel_broker_socket_dir" \
  && -S "$tunnel_broker_socket_dir/control.sock" ]] || {
  echo "root-installed epoch tunnel broker socket is required before control deployment" >&2
  exit 2
}
[[ -d "$asset_dir" && ! -L "$asset_dir" ]] || { echo "master asset directory is required" >&2; exit 2; }
python3 "$release/scripts/provider/verify_master_assets.py" \
  --bundle "$asset_dir" --expected-commit "$commit" >/dev/null
for env_file in "$provider_env" "$mcp_env" "$oauth_env"; do
  reject_data_plane_environment "$env_file" "$(basename "$env_file")"
done
reject_environment_keys "$provider_env" "provider environment" \
  'MY_DATA_HUB_OAUTH_SIGNING_KEY|MY_DATA_HUB_OWNER_OIDC_CLIENT_SECRET|MY_DATA_HUB_.*TOKEN_(ROOT|SECRET_NAME)|MY_DATA_HUB_KAGGLE_MASTER_(SOURCE_IDENTITY|SOURCE_VERSION|CHECKPOINT_REF|DATASET_REF|NOTEBOOK_REF|DATASET_DIR|NOTEBOOK_SOURCE)'
reject_environment_keys "$mcp_env" "remote MCP environment" \
  'KAGGLE_[A-Z0-9_]+|MY_DATA_HUB_OAUTH_SIGNING_KEY|MY_DATA_HUB_.*TOKEN_(ROOT|SECRET_NAME)'
reject_environment_keys "$oauth_env" "OAuth environment" \
  'KAGGLE_[A-Z0-9_]+|MY_DATA_HUB_KAGGLE_[A-Z0-9_]+|MY_DATA_HUB_.*TOKEN_(ROOT|SECRET_NAME)'
python3 - "$provider_env" <<'PY'
import json
import re
import sys
from pathlib import Path

values = {}
for line in Path(sys.argv[1]).read_text(encoding="utf-8").splitlines():
    if not line or line.lstrip().startswith("#"):
        continue
    if "=" not in line:
        raise SystemExit("provider environment contains an invalid line")
    key, value = line.split("=", 1)
    key = key.strip()
    if key in values:
        raise SystemExit("provider environment contains a duplicate key")
    values[key] = value.strip()
required = {
    "MY_DATA_HUB_MASTER_TUNNEL_GATEWAY_HOST",
    "MY_DATA_HUB_MASTER_TUNNEL_GATEWAY_PORT",
    "MY_DATA_HUB_MASTER_TUNNEL_GATEWAY_USER",
    "MY_DATA_HUB_MASTER_TUNNEL_REMOTE_PORT",
}
if any(not values.get(key) for key in required):
    raise SystemExit("provider environment lacks the exact master tunnel/TLS binding")
if not re.fullmatch(r"[A-Za-z0-9.-]{1,253}", values["MY_DATA_HUB_MASTER_TUNNEL_GATEWAY_HOST"]):
    raise SystemExit("master tunnel gateway host is invalid")
if not re.fullmatch(r"[a-z_][a-z0-9_-]{0,31}", values["MY_DATA_HUB_MASTER_TUNNEL_GATEWAY_USER"]):
    raise SystemExit("master tunnel gateway user is invalid")
if any(
    not value.isdigit() or not 1 <= int(value) <= 65535
    for value in (
        values["MY_DATA_HUB_MASTER_TUNNEL_GATEWAY_PORT"],
        values["MY_DATA_HUB_MASTER_TUNNEL_REMOTE_PORT"],
    )
):
    raise SystemExit("master tunnel port is invalid")
raw_bindings = values.get("MY_DATA_HUB_KAGGLE_MASTER_SECRET_BINDINGS_JSON", "{}")
try:
    bindings = json.loads(raw_bindings)
except json.JSONDecodeError as exc:
    raise SystemExit("optional master secret-name binding is invalid JSON") from exc
if not isinstance(bindings, dict) or set(bindings) - {"YDB_ACCESS_TOKEN_CREDENTIALS"}:
    raise SystemExit("master secret-name binding is overbroad")
if any(not isinstance(value, str) or not value or len(value) > 200 for value in bindings.values()):
    raise SystemExit("master optional secret name is invalid")
PY

operator_override=""
operator_compose_arg=""
acceptance_override=""
acceptance_compose_arg=""
if [[ "$operator_profile" == true ]]; then
  if [[ "${MY_DATA_HUB_ENABLE_OPERATOR_PROFILE:-}" != "I_ACKNOWLEDGE_REMOTE_CANONICAL_WRITES" ]]; then
    echo "operator install requires the exact MY_DATA_HUB_ENABLE_OPERATOR_PROFILE acknowledgement" >&2
    exit 2
  fi
  require_private_file "$operator_gate_receipt" "operator security gate receipt"
  require_private_file "$operator_gate_key" "operator write-gate signing key"
  require_private_file "$control_gateway_token" "provider control gateway token"
  kaggle_token_count="$(grep -Eic '^[[:space:]]*KAGGLE_API_TOKEN[[:space:]]*=[^[:space:]].*$' "$provider_env" || true)"
  kaggle_username_count="$(grep -Eic '^[[:space:]]*KAGGLE_USERNAME[[:space:]]*=[^[:space:]].*$' "$provider_env" || true)"
  kaggle_key_count="$(grep -Eic '^[[:space:]]*KAGGLE_KEY[[:space:]]*=[^[:space:]].*$' "$provider_env" || true)"
  if ! { [[ "$kaggle_token_count" == 1 && "$kaggle_username_count" == 0 && "$kaggle_key_count" == 0 ]] \
    || [[ "$kaggle_token_count" == 0 && "$kaggle_username_count" == 1 && "$kaggle_key_count" == 1 ]]; }; then
    echo "single control provider environment requires access token OR one legacy username/key pair" >&2
    exit 2
  fi
  python3 "$release/scripts/operator_profile_gate.py" verify \
    --commit "$commit" --receipt "$operator_gate_receipt" --signing-key-file "$operator_gate_key"
  operator_override="$runtime_root/operator-profile.$commit.yaml"
  cat > "$operator_override" <<'YAML'
services:
  control-plane:
    environment:
      MY_DATA_HUB_MCP_OPERATOR_CREDENTIALS_ENABLED: "true"
      MY_DATA_HUB_MCP_PROVIDER_GATEWAY_ENABLED: "true"
      MY_DATA_HUB_MCP_CONTROL_GATEWAY_TOKEN_FILE: /run/secrets/mcp-control-gateway.token
    volumes:
      - "${MY_DATA_HUB_MCP_CONTROL_GATEWAY_TOKEN_FILE:?provider control gateway token is required}:/run/secrets/mcp-control-gateway.token:ro"
  remote-mcp:
    environment:
      MY_DATA_HUB_MCP_WRITE_ENABLED: "true"
      MY_DATA_HUB_MCP_OPERATOR_PROFILE_ENABLED: "true"
      MY_DATA_HUB_MCP_WRITE_GATE_SECRET_FILE: /run/secrets/mcp-write-gate.key
      MY_DATA_HUB_MCP_CONTROL_GATEWAY_URL: http://control-plane:8080/internal/mcp-provider/invoke
      MY_DATA_HUB_MCP_CONTROL_GATEWAY_TOKEN_FILE: /run/secrets/mcp-control-gateway.token
      MY_DATA_HUB_MCP_SCOPES: platform:read,master:read,operation:read,checkpoint:read,embedding:read,provider:read,bloggers:read,data:read,master:ensure,master:rotate,recovery:request,acceptance:probe,acceptance:operate,data:write,migration:operate,provider:write
    volumes:
      - "${MY_DATA_HUB_MCP_WRITE_GATE_SECRET_FILE:?write gate key is required}:/run/secrets/mcp-write-gate.key:ro"
      - "${MY_DATA_HUB_MCP_CONTROL_GATEWAY_TOKEN_FILE:?provider control gateway token is required}:/run/secrets/mcp-control-gateway.token:ro"
YAML
  chmod 600 "$operator_override"
  operator_compose_arg=" -f $operator_override"
fi

acceptance_scenarios_override=""
acceptance_scenarios_compose_arg=""
if [[ "$acceptance_scenarios" == true ]]; then
  require_private_file "$checkpoint_acceptance_deployment" "checkpoint acceptance deployment"
  python3 - "$checkpoint_acceptance_deployment" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
raw = path.read_bytes()
if not 1 <= len(raw) <= 256 * 1024:
    raise SystemExit("checkpoint acceptance deployment exceeds 256 KiB")
value = json.loads(raw)
if value.get("schema_version") != "my-data-hub-checkpoint-acceptance-deployment.v1":
    raise SystemExit("checkpoint acceptance deployment schema is invalid")
if "runtime_root_secret_name" in value:
    raise SystemExit("checkpoint callback roots may not be provisioned as Kaggle User Secrets")
bindings = value.get("kaggle_secret_bindings")
if not isinstance(bindings, dict) or set(bindings) not in (
    {"KAGGLE_API_TOKEN"}, {"KAGGLE_USERNAME", "KAGGLE_KEY"}
):
    raise SystemExit("checkpoint provider secret-name binding is incomplete")
if any(not isinstance(item, str) or not item or len(item) > 200 for item in bindings.values()):
    raise SystemExit("checkpoint provider secret names are invalid")
PY
  acceptance_scenarios_override="$runtime_root/acceptance-scenarios.$commit.yaml"
  cat > "$acceptance_scenarios_override" <<YAML
services:
  control-plane:
    environment:
      MY_DATA_HUB_MCP_ACCEPTANCE_SCENARIOS_ENABLED: "true"
      MY_DATA_HUB_CHECKPOINT_ACCEPTANCE_DEPLOYMENT_FILE: /run/mdh-checkpoint-acceptance/deployment.json
    volumes:
      - "$checkpoint_acceptance_deployment:/run/mdh-checkpoint-acceptance/deployment.json:ro"
  remote-mcp:
    environment:
      MY_DATA_HUB_MCP_ACCEPTANCE_SCENARIOS_ENABLED: "true"
YAML
  chmod 600 "$acceptance_scenarios_override"
  acceptance_scenarios_compose_arg=" -f $acceptance_scenarios_override"
fi

if [[ "$acceptance_supervisor" == true ]]; then
  [[ ! -L "$acceptance_socket_dir" ]] || {
    echo "acceptance supervisor socket directory may not be a symbolic link" >&2
    exit 2
  }
  mkdir -p "$acceptance_socket_dir"
  chmod 700 "$acceptance_socket_dir"
  require_private_file "$acceptance_key" "acceptance supervisor signing key"
  python3 - "$acceptance_key" <<'PY'
from pathlib import Path
import sys

key = Path(sys.argv[1]).read_bytes().strip()
if not 32 <= len(key) <= 256:
    raise SystemExit("acceptance supervisor signing key must contain 32..256 bytes")
PY
  acceptance_override="$runtime_root/acceptance-supervisor.$commit.yaml"
  cat > "$acceptance_override" <<YAML
services:
  control-plane:
    environment:
      MY_DATA_HUB_ACCEPTANCE_SUPERVISOR_SOCKET: /run/mdh-acceptance/control.sock
      MY_DATA_HUB_ACCEPTANCE_SUPERVISOR_KEY_FILE: /run/mdh-acceptance/supervisor.key
    volumes:
      - "$acceptance_socket_dir:/run/mdh-acceptance:ro"
YAML
  chmod 600 "$acceptance_override"
  acceptance_compose_arg=" -f $acceptance_override"
fi

compose_env="$runtime_root/compose.$commit.env"
cat > "$compose_env" <<ENV
MY_DATA_HUB_IMAGE_TAG=$commit
MY_DATA_HUB_CONTROL_UID=$(id -u)
MY_DATA_HUB_CONTROL_GID=$(id -g)
MY_DATA_HUB_CONTROL_LEDGER_DIR=$ledger_dir
MY_DATA_HUB_MASTER_SESSION_DIR=$session_dir
MY_DATA_HUB_MASTER_ASSET_DIR=$asset_dir
MY_DATA_HUB_MASTER_TLS_DIR=$tls_dir
MY_DATA_HUB_CONTROL_PROVIDER_ENV_FILE=$provider_env
MY_DATA_HUB_MCP_ENV_FILE=$mcp_env
MY_DATA_HUB_OAUTH_ENV_FILE=$oauth_env
MY_DATA_HUB_OAUTH_SIGNING_KEY_FILE=$oauth_key
MY_DATA_HUB_OAUTH_OVERLAP_JWKS_FILE=$oauth_overlap_jwks
MY_DATA_HUB_TUNNEL_BROKER_SOCKET_DIR=$tunnel_broker_socket_dir
MY_DATA_HUB_MCP_WRITE_GATE_SECRET_FILE=$operator_gate_key
MY_DATA_HUB_MCP_CONTROL_GATEWAY_TOKEN_FILE=$control_gateway_token
ENV
chmod 600 "$compose_env"

compose_files=(-f "$release/compose.control-plane.yaml")
if [[ -n "$operator_override" ]]; then
  compose_files+=(-f "$operator_override")
fi
if [[ -n "$acceptance_override" ]]; then
  compose_files+=(-f "$acceptance_override")
fi
if [[ -n "$acceptance_scenarios_override" ]]; then
  compose_files+=(-f "$acceptance_scenarios_override")
fi
compose=("$docker_path" compose --env-file "$compose_env" --profile remote-mcp \
  --project-directory "$release" "${compose_files[@]}")
"${compose[@]}" config --quiet

unit="$HOME/.config/systemd/user/my-data-hub-control-plane.service"
unit_candidate="$runtime_root/my-data-hub-control-plane.service.$commit"
unit_backup="$runtime_root/my-data-hub-control-plane.service.previous"
supervisor_unit="$HOME/.config/systemd/user/my-data-hub-acceptance-supervisor.service"
supervisor_unit_candidate="$runtime_root/my-data-hub-acceptance-supervisor.service.$commit"
supervisor_unit_backup="$runtime_root/my-data-hub-acceptance-supervisor.service.previous"
previous_release=""
previous_unit_present=false
previous_unit_enabled=false
previous_unit_active=false
previous_supervisor_unit_present=false
previous_supervisor_unit_enabled=false
previous_supervisor_unit_active=false
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
if [[ -f "$supervisor_unit" ]]; then
  cp "$supervisor_unit" "$supervisor_unit_backup"
  previous_supervisor_unit_present=true
fi
if systemctl --user is-enabled --quiet my-data-hub-acceptance-supervisor.service 2>/dev/null; then
  previous_supervisor_unit_enabled=true
fi
if systemctl --user is-active --quiet my-data-hub-acceptance-supervisor.service 2>/dev/null; then
  previous_supervisor_unit_active=true
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
ExecStart=$docker_path compose --env-file $compose_env --profile remote-mcp --project-directory $release -f $release/compose.control-plane.yaml$operator_compose_arg$acceptance_compose_arg$acceptance_scenarios_compose_arg up --remove-orphans control-plane remote-mcp oauth-server
ExecReload=$docker_path compose --env-file $compose_env --profile remote-mcp --project-directory $release -f $release/compose.control-plane.yaml$operator_compose_arg$acceptance_compose_arg$acceptance_scenarios_compose_arg up -d --wait --remove-orphans control-plane remote-mcp oauth-server
ExecStop=$docker_path compose --env-file $compose_env --profile remote-mcp --project-directory $release -f $release/compose.control-plane.yaml$operator_compose_arg$acceptance_compose_arg$acceptance_scenarios_compose_arg down --remove-orphans
Restart=on-failure
RestartSec=10
TimeoutStartSec=300
TimeoutStopSec=120
SuccessExitStatus=0 130 143

[Install]
WantedBy=default.target
UNIT
chmod 600 "$unit_candidate"

if [[ "$acceptance_supervisor" == true ]]; then
  compose_file_argument="$release/compose.control-plane.yaml"
  cat > "$supervisor_unit_candidate" <<UNIT
[Unit]
Description=my-data-hub task-bound FM08 control restart supervisor
After=docker.service
Before=my-data-hub-control-plane.service

[Service]
Type=simple
ExecStart=$python_path $release/src/my_data_hub/control_plane/acceptance_supervisor.py --socket $acceptance_socket_dir/control.sock --key-file $acceptance_key --journal $acceptance_socket_dir/restart-journal.json --docker $docker_path --compose-env $compose_env --project-directory $release --compose-files $compose_file_argument --allowed-uid $(id -u)
Environment=PYTHONPATH=$release/src
Restart=on-failure
RestartSec=2
NoNewPrivileges=true
PrivateTmp=true

[Install]
WantedBy=default.target
UNIT
  chmod 600 "$supervisor_unit_candidate"
fi


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
  if [[ "$previous_supervisor_unit_present" == true ]]; then
    cp "$supervisor_unit_backup" "$supervisor_unit"
  else
    rm -f "$supervisor_unit"
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
  if [[ "$previous_supervisor_unit_enabled" == true ]]; then
    systemctl --user enable my-data-hub-acceptance-supervisor.service
  else
    systemctl --user disable my-data-hub-acceptance-supervisor.service
  fi
  if [[ "$previous_supervisor_unit_active" == true ]]; then
    systemctl --user restart my-data-hub-acceptance-supervisor.service
  else
    systemctl --user stop my-data-hub-acceptance-supervisor.service
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
if [[ "$acceptance_supervisor" == true ]]; then
  mv "$supervisor_unit_candidate" "$supervisor_unit"
else
  rm -f "$supervisor_unit"
fi
systemctl --user daemon-reload
if [[ "$acceptance_supervisor" == true ]]; then
  systemctl --user enable my-data-hub-acceptance-supervisor.service
  systemctl --user restart my-data-hub-acceptance-supervisor.service
else
  systemctl --user disable my-data-hub-acceptance-supervisor.service 2>/dev/null || true
  systemctl --user stop my-data-hub-acceptance-supervisor.service 2>/dev/null || true
fi
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
rm -f "$supervisor_unit_backup"
printf 'installed_control_plane_commit=%s\nservices=control-plane,remote-mcp,oauth-server\noperator_profile=%s\nacceptance_scenarios=%s\nacceptance_supervisor=%s\nmaster_state=ABSENT_or_durable_runtime_state\n' "$commit" "$operator_profile" "$acceptance_scenarios" "$acceptance_supervisor"
