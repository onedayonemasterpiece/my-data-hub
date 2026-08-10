#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

action="${1:-}"
if [[ "$action" != "PREPARE" && "$action" != "INSTALL_MY_DATA_HUB_SAME_HOST" ]]; then
  echo "usage: $0 PREPARE|INSTALL_MY_DATA_HUB_SAME_HOST" >&2
  exit 2
fi
for command_name in curl docker git python3 seq ssh-keygen systemctl tar; do
  command -v "$command_name" >/dev/null || { echo "$command_name is required" >&2; exit 2; }
done
docker info >/dev/null
docker compose version >/dev/null

source_root="$(git rev-parse --show-toplevel)"
commit="$(git -C "$source_root" rev-parse HEAD)"
if [[ -n "$(git -C "$source_root" status --porcelain --untracked-files=no)" ]]; then
  echo "tracked worktree changes are forbidden for a release" >&2
  exit 2
fi

state_root="${MY_DATA_HUB_RUNTIME_DIR:-$HOME/.local/state/my-data-hub}"
release_root="${MY_DATA_HUB_RELEASE_ROOT:-$HOME/.local/opt/my-data-hub}"
release="$release_root/releases/$commit"
current="$release_root/current"
candidate_env_dir="$state_root/releases/$commit"
mkdir -p "$state_root" "$state_root/backups" "$state_root/recovery" "$state_root/receipts" \
  "$candidate_env_dir" "$release_root/releases" "$HOME/.config/systemd/user"
chmod 700 "$state_root" "$state_root/backups" "$state_root/recovery" "$state_root/receipts" \
  "$state_root/releases" "$candidate_env_dir"

if [[ ! -d "$release" ]]; then
  staging="$(mktemp -d "$release_root/releases/.staging.XXXXXX")"
  trap 'rmdir "$staging" 2>/dev/null || true' EXIT
  git -C "$source_root" archive "$commit" | tar -x -C "$staging"
  mv "$staging" "$release"
  trap - EXIT
fi

secret_file="$state_root/secrets.env"
if [[ ! -e "$secret_file" ]]; then
  secret() { python3 -c 'import secrets; print(secrets.token_hex(32))'; }
  cat > "$secret_file" <<EOF
POSTGRES_PASSWORD=$(secret)
APPLICATION_PASSWORD=$(secret)
CONNECTOR_PASSWORD=$(secret)
ORCHESTRATOR_PASSWORD=$(secret)
MCP_READER_PASSWORD=$(secret)
AUTHENTICATOR_PASSWORD=$(secret)
COMMITTER_PASSWORD=$(secret)
BACKUP_PASSWORD=$(secret)
MIGRATOR_PASSWORD=$(secret)
MONITORING_PASSWORD=$(secret)
OPERATOR_PASSWORD=$(secret)
WORKER_TOKEN=$(secret)
CONNECTOR_TOKEN=$(secret)
EOF
  chmod 600 "$secret_file"
fi
# This file is generated locally with fixed variable names and mode 0600.
# shellcheck disable=SC1090
source "$secret_file"

age_identity="$state_root/recovery/age-identity"
if [[ ! -e "$age_identity" ]]; then
  ssh-keygen -q -t ed25519 -N '' -C 'my-data-hub-backup' -f "$age_identity"
fi
chmod 600 "$age_identity"
chmod 644 "$age_identity.pub"
age_recipient="$(awk '{print $1 " " $2}' "$age_identity.pub")"

database_url() { printf 'postgresql://%s:%s@postgres:5432/my_data_hub' "$1" "$2"; }
admin_url="$(database_url mdh_bootstrap "$POSTGRES_PASSWORD")"
application_url="$(database_url mdh_application_login "$APPLICATION_PASSWORD")"
connector_url="$(database_url mdh_connector_login "$CONNECTOR_PASSWORD")"
orchestrator_url="$(database_url mdh_orchestrator_login "$ORCHESTRATOR_PASSWORD")"
mcp_reader_url="$(database_url mdh_mcp_reader_login "$MCP_READER_PASSWORD")"
authenticator_url="$(database_url mdh_authenticator_login "$AUTHENTICATOR_PASSWORD")"
committer_url="$(database_url mdh_committer_login "$COMMITTER_PASSWORD")"
backup_url="$(database_url mdh_backup_login "$BACKUP_PASSWORD")"
migrator_url="$(database_url mdh_migrator_login "$MIGRATOR_PASSWORD")"
monitoring_url="$(database_url mdh_monitoring_login "$MONITORING_PASSWORD")"
operator_url="$(database_url mdh_operator_login "$OPERATOR_PASSWORD")"

common_environment() {
  cat <<EOF
MY_DATA_HUB_ENVIRONMENT=production
MY_DATA_HUB_INSTANCE_ID=devcoveer-same-host
MY_DATA_HUB_LOG_LEVEL=INFO
MY_DATA_HUB_SCHEDULER_ENABLED=false
MY_DATA_HUB_PRODUCTION_PUBLISH_ENABLED=false
MY_DATA_HUB_MCP_WRITE_ENABLED=false
EOF
}
write_env() {
  local name="$1"
  shift
  { common_environment; printf '%s\n' "$@"; } > "$candidate_env_dir/$name.env"
  chmod 600 "$candidate_env_dir/$name.env"
}

cat > "$candidate_env_dir/postgres.env" <<EOF
POSTGRES_DB=my_data_hub
POSTGRES_USER=mdh_bootstrap
POSTGRES_PASSWORD=$POSTGRES_PASSWORD
EOF
chmod 600 "$candidate_env_dir/postgres.env"

write_env admin \
  "MY_DATA_HUB_DATABASE_URL=$admin_url" \
  "MY_DATA_HUB_ROLE_ADMIN_DATABASE_URL=$admin_url" \
  "MY_DATA_HUB_APPLICATION_DATABASE_URL=$application_url" \
  "MY_DATA_HUB_CONNECTOR_INTAKE_DATABASE_URL=$connector_url" \
  "MY_DATA_HUB_ORCHESTRATOR_DATABASE_URL=$orchestrator_url" \
  "MY_DATA_HUB_MCP_READER_DATABASE_URL=$mcp_reader_url" \
  "MY_DATA_HUB_MCP_REVOCATION_DATABASE_URL=$authenticator_url" \
  "MY_DATA_HUB_CANONICAL_COMMITTER_DATABASE_URL=$committer_url" \
  "MY_DATA_HUB_BACKUP_DATABASE_URL=$backup_url" \
  "MY_DATA_HUB_MIGRATOR_DATABASE_URL=$migrator_url" \
  "MY_DATA_HUB_MONITORING_DATABASE_URL=$monitoring_url" \
  "MY_DATA_HUB_MIGRATION_OPERATOR_DATABASE_URL=$operator_url"

write_env migrator \
  "MY_DATA_HUB_DATABASE_URL=$migrator_url" \
  "MY_DATA_HUB_MIGRATOR_DATABASE_URL=$migrator_url"
write_env api \
  "MY_DATA_HUB_DATABASE_URL=$application_url" \
  "MY_DATA_HUB_APPLICATION_DATABASE_URL=$application_url" \
  "MY_DATA_HUB_CONNECTOR_INTAKE_DATABASE_URL=$connector_url" \
  "MY_DATA_HUB_API_HOST=0.0.0.0" \
  "MY_DATA_HUB_API_PORT=8080" \
  "MY_DATA_HUB_ARTIFACT_ROOT=/var/lib/my-data-hub/artifacts" \
  "MY_DATA_HUB_WORKER_RESULT_TOKEN=$WORKER_TOKEN" \
  "MY_DATA_HUB_CONNECTOR_CREDENTIALS_JSON={\"events-bot.daily-statistics\":\"$CONNECTOR_TOKEN\"}"
write_env orchestrator \
  "MY_DATA_HUB_DATABASE_URL=$orchestrator_url" \
  "MY_DATA_HUB_ORCHESTRATOR_DATABASE_URL=$orchestrator_url" \
  "MY_DATA_HUB_ARTIFACT_ROOT=/var/lib/my-data-hub/artifacts"
write_env committer \
  "MY_DATA_HUB_DATABASE_URL=$committer_url" \
  "MY_DATA_HUB_CANONICAL_COMMITTER_DATABASE_URL=$committer_url" \
  "MY_DATA_HUB_COMMITTER_INTERVAL_SECONDS=60"
write_env backup \
  "MY_DATA_HUB_DATABASE_URL=$backup_url" \
  "MY_DATA_HUB_BACKUP_DATABASE_URL=$backup_url" \
  "MY_DATA_HUB_BACKUP_ROOT=/var/lib/my-data-hub/backups" \
  "MY_DATA_HUB_BACKUP_RETENTION_DAYS=14" \
  "MY_DATA_HUB_BACKUP_AGE_RECIPIENT=\"$age_recipient\"" \
  "MY_DATA_HUB_BACKUP_SOURCE_INSTANCE=devcoveer-same-host" \
  "MY_DATA_HUB_BACKUP_SOURCE_ENVIRONMENT=production" \
  "MY_DATA_HUB_BACKUP_REPOSITORY_COMMIT=$commit" \
  "MY_DATA_HUB_BACKUP_INTERVAL_SECONDS=86400"

# The remote service remains deliberately non-runnable until a real issuer/JWKS and
# public TLS edge are configured.  Accidentally starting the profile fails closed.
write_env mcp \
  "MY_DATA_HUB_DATABASE_URL=$mcp_reader_url" \
  "MY_DATA_HUB_MCP_READER_DATABASE_URL=$mcp_reader_url" \
  "MY_DATA_HUB_MCP_REVOCATION_DATABASE_URL=$authenticator_url" \
  "MY_DATA_HUB_MCP_REMOTE_ENABLED=false" \
  "MY_DATA_HUB_MCP_AUTH_MODE=stdio-environment" \
  "MY_DATA_HUB_MCP_HOST=0.0.0.0" \
  "MY_DATA_HUB_MCP_PORT=8765"

candidate_deployment_env="$candidate_env_dir/deployment.env"
cat > "$candidate_deployment_env" <<EOF
MY_DATA_HUB_ENV_DIR=$candidate_env_dir
MY_DATA_HUB_STATE_DIR=$state_root
MY_DATA_HUB_IMAGE_TAG=$commit
MY_DATA_HUB_RUNTIME_UID=$(id -u)
MY_DATA_HUB_RUNTIME_GID=$(id -g)
POSTGRES_PORT=5432
MY_DATA_HUB_API_PORT=8080
MY_DATA_HUB_MCP_PORT=8765
EOF
chmod 600 "$candidate_deployment_env"

compose() {
  set -a
  # shellcheck disable=SC1090
  source "$candidate_deployment_env"
  set +a
  docker compose --project-directory "$release" -f "$release/compose.same-host.yaml" "$@"
}

previous_release="$(readlink -f "$current" 2>/dev/null || true)"
previous_compose() {
  set -a
  # shellcheck disable=SC1090
  source "$state_root/deployment.env"
  set +a
  docker compose --project-directory "$previous_release" \
    -f "$previous_release/compose.same-host.yaml" "$@"
}

compose build api backup

if [[ "$action" == "PREPARE" ]]; then
  echo "prepared_commit=$commit"
  echo "release=$release"
  echo "runtime=$state_root"
  echo "autostart=unchanged_until_install_gates_pass"
  exit 0
fi

compose up -d postgres
compose run --rm role-bootstrap
compose run --rm login-provision
compose run --rm migrate
compose run --rm role-provision
compose run --rm identity-verify
compose run --rm role-probe
compose up -d postgres api orchestrator connector-committer backup

ready=false
for _attempt in $(seq 1 30); do
  if curl --fail --silent --show-error http://127.0.0.1:8080/health/ready \
      > "$state_root/receipts/api-ready.json"; then
    ready=true
    break
  fi
  sleep 2
done
if [[ "$ready" != true ]]; then
  echo "candidate readiness failed; restoring the previous supervised application set" >&2
  if [[ -n "$previous_release" && -f "$state_root/deployment.env" ]]; then
    previous_compose up -d postgres api orchestrator connector-committer backup
  else
    compose stop api orchestrator connector-committer backup
  fi
  exit 1
fi

# Publish the release and boot configuration only after every database/identity/readiness
# gate has passed. PREPARE and failed INSTALL attempts cannot advance boot state.
next_link="$release_root/.current.$commit"
ln -sfn "$release" "$next_link"
mv -Tf "$next_link" "$current"
deployment_env_tmp="$state_root/.deployment.env.$commit"
cp "$candidate_deployment_env" "$deployment_env_tmp"
chmod 600 "$deployment_env_tmp"
mv -f "$deployment_env_tmp" "$state_root/deployment.env"

unit="$HOME/.config/systemd/user/my-data-hub-compose.service"
unit_tmp="$HOME/.config/systemd/user/.my-data-hub-compose.service.$commit"
cat > "$unit_tmp" <<EOF
[Unit]
Description=my-data-hub same-host Docker Compose reconciliation
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
RemainAfterExit=yes
EnvironmentFile=$state_root/deployment.env
ExecStartPre=/bin/sh -c 'i=0; until /usr/bin/docker info >/dev/null 2>&1; do i=\$((i+1)); [ \$i -lt 60 ] || exit 1; sleep 2; done'
ExecStart=/usr/bin/docker compose --project-directory $current -f $current/compose.same-host.yaml up -d postgres api orchestrator connector-committer backup
TimeoutStartSec=300

[Install]
WantedBy=default.target
EOF
chmod 600 "$unit_tmp"
mv -f "$unit_tmp" "$unit"
systemctl --user daemon-reload
systemctl --user enable my-data-hub-compose.service
if systemctl --user is-active --quiet my-data-hub-compose.service; then
  systemctl --user restart my-data-hub-compose.service
else
  systemctl --user start my-data-hub-compose.service
fi
compose ps --format json > "$state_root/receipts/compose-ps.json"
python3 - "$state_root/receipts/install.json" "$commit" <<'PY'
import hashlib, json, sys
from datetime import datetime, timezone
from pathlib import Path
output, commit = Path(sys.argv[1]), sys.argv[2]
ready = output.with_name("api-ready.json")
payload = {
    "schema_version": "my-data-hub-same-host-install.v1",
    "commit": commit,
    "host": "DevCoveer",
    "installed_at": datetime.now(timezone.utc).isoformat(),
    "autostart": "systemd-user-enabled-and-docker-restart-unless-stopped",
    "remote_mcp": "blocked_pending_tls_and_oauth",
    "api_ready_sha256": hashlib.sha256(ready.read_bytes()).hexdigest(),
}
output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
chmod 600 "$state_root/receipts/"*.json
echo "installed_commit=$commit"
echo "receipt=$state_root/receipts/install.json"
