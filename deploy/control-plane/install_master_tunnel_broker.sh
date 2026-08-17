#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

[[ "${1:-}" == "INSTALL_MY_DATA_HUB_MASTER_TUNNEL_BROKER" ]] || {
  echo "usage: $0 INSTALL_MY_DATA_HUB_MASTER_TUNNEL_BROKER" >&2
  exit 2
}
[[ "$(id -u)" == "0" ]] || { echo "master tunnel broker install requires root" >&2; exit 2; }

require_command() {
  command -v "$1" >/dev/null || { echo "$1 is required" >&2; exit 2; }
}
for command_name in chmod getent grep id install mktemp python3 rm ssh-keygen sshd systemctl useradd usermod; do
  require_command "$command_name"
done

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
source_root="$(cd -- "$script_dir/../.." && pwd -P)"
broker_source="$source_root/src/my_data_hub/tunnel_broker.py"
broker_ipc_source="$source_root/src/my_data_hub/tunnel_broker_ipc.py"
[[ -f "$broker_source" && ! -L "$broker_source" ]] || {
  echo "tunnel broker source must be a regular non-symlink file" >&2
  exit 2
}
[[ -f "$broker_ipc_source" && ! -L "$broker_ipc_source" ]] || {
  echo "tunnel broker IPC source must be a regular non-symlink file" >&2
  exit 2
}

account="${MY_DATA_HUB_TUNNEL_ACCOUNT:-mdh-master-tunnel}"
worker_account="${MY_DATA_HUB_EMBEDDING_TUNNEL_ACCOUNT:-mdh-embedding-worker}"
listen_port="${MY_DATA_HUB_TUNNEL_LISTEN_PORT:-25432}"
state_root="${MY_DATA_HUB_TUNNEL_STATE_ROOT:-/var/lib/my-data-hub/tunnel-broker}"
account_home="${MY_DATA_HUB_TUNNEL_ACCOUNT_HOME:-/var/lib/my-data-hub/tunnel-account}"
ca_private="${MY_DATA_HUB_TUNNEL_CA_PRIVATE_KEY:-/etc/my-data-hub/tunnel-user-ca}"
sshd_fragment="${MY_DATA_HUB_TUNNEL_SSHD_FRAGMENT:-/etc/ssh/sshd_config.d/60-my-data-hub-master-tunnel.conf}"
broker_program="${MY_DATA_HUB_TUNNEL_BROKER_PROGRAM:-/usr/local/libexec/my-data-hub-tunnel-broker}"
broker_ipc_program="${MY_DATA_HUB_TUNNEL_BROKER_IPC_PROGRAM:-/usr/local/libexec/my-data-hub-tunnel-broker-ipc}"
broker_socket="${MY_DATA_HUB_TUNNEL_BROKER_SOCKET:-/run/my-data-hub/tunnel-broker/control.sock}"
control_uid="${MY_DATA_HUB_CONTROL_UID:-1000}"
control_gid="${MY_DATA_HUB_CONTROL_GID:-1000}"
unit_root="${MY_DATA_HUB_TUNNEL_SYSTEMD_DIR:-/etc/systemd/system}"

[[ "$account" =~ ^[a-z_][a-z0-9_-]{0,30}$ && "$worker_account" =~ ^[a-z_][a-z0-9_-]{0,30}$ \
  && "$account" != "$worker_account" ]] || { echo "invalid tunnel account" >&2; exit 2; }
[[ "$listen_port" =~ ^[0-9]+$ ]] && (( listen_port >= 1024 && listen_port <= 65535 )) || {
  echo "tunnel listen port must be within 1024..65535" >&2
  exit 2
}
for path_value in "$state_root" "$account_home" "$ca_private" "$sshd_fragment" "$broker_program" \
  "$broker_ipc_program" "$broker_socket" "$unit_root"; do
  [[ "$path_value" = /* && "$path_value" != *[$'\n\r\t ']* ]] || {
    echo "tunnel broker paths must be absolute and whitespace-free" >&2
    exit 2
  }
done
broker_socket_parent="$(dirname "$broker_socket")"
[[ "$broker_socket_parent" == /run/* ]] || {
  echo "tunnel broker socket must be located below /run" >&2
  exit 2
}
broker_runtime_directory="${broker_socket_parent#/run/}"
[[ "$broker_runtime_directory" =~ ^[A-Za-z0-9_.-]+(/[A-Za-z0-9_.-]+)*$ ]] || {
  echo "tunnel broker runtime directory is invalid" >&2
  exit 2
}
[[ "$control_uid" =~ ^[0-9]+$ && "$control_gid" =~ ^[0-9]+$ ]] || {
  echo "control UID/GID must be numeric" >&2
  exit 2
}
(( control_uid > 0 && control_gid > 0 )) || {
  echo "control UID/GID must be non-root" >&2
  exit 2
}
for directory_path in "$state_root" "$account_home" "$(dirname "$ca_private")" \
  "$(dirname "$sshd_fragment")" "$(dirname "$broker_program")" "$unit_root"; do
  [[ ! -L "$directory_path" ]] || { echo "tunnel broker directories may not be symbolic links" >&2; exit 2; }
done
[[ -f /etc/ssh/sshd_config && ! -L /etc/ssh/sshd_config ]] || {
  echo "OpenSSH server configuration is unavailable" >&2
  exit 2
}
grep -Eq '^[[:space:]]*Include[[:space:]]+/etc/ssh/sshd_config\.d/\*\.conf([[:space:]]|$)' /etc/ssh/sshd_config || {
  echo "sshd_config must already include /etc/ssh/sshd_config.d/*.conf" >&2
  exit 2
}

# Both accounts have no usable password, shell, home content, groups, or other job.
install -d -o root -g root -m 0755 "$account_home"
for tunnel_account in "$account" "$worker_account"; do
  tunnel_home="$account_home/$tunnel_account"
  if getent passwd "$tunnel_account" >/dev/null; then
    IFS=: read -r _ _ _ _ _ observed_home observed_shell < <(getent passwd "$tunnel_account")
    [[ "$observed_home" == "$tunnel_home" && "$observed_shell" == "/usr/sbin/nologin" ]] || {
      echo "existing tunnel account differs from the dedicated contract" >&2
      exit 2
    }
  else
    useradd --system --user-group --create-home --home-dir "$tunnel_home" \
      --shell /usr/sbin/nologin --password '*' "$tunnel_account"
  fi
  usermod --shell /usr/sbin/nologin --password '*' "$tunnel_account"
  install -d -o "$tunnel_account" -g "$tunnel_account" -m 0700 "$tunnel_home"
done
install -d -o root -g root -m 0700 "$state_root" "$(dirname "$ca_private")"
install -d -o root -g root -m 0755 "$(dirname "$broker_program")" "$(dirname "$sshd_fragment")" "$unit_root"
install -o root -g root -m 0755 "$broker_source" "$broker_program"
install -o root -g root -m 0644 "$broker_source" "$(dirname "$broker_ipc_program")/tunnel_broker.py"
install -o root -g root -m 0755 "$broker_ipc_source" "$broker_ipc_program"

if [[ ! -e "$ca_private" && ! -e "$ca_private.pub" ]]; then
  ssh-keygen -q -t ed25519 -N '' -C 'my-data-hub master tunnel user CA' -f "$ca_private"
elif [[ ! -f "$ca_private" || -L "$ca_private" || ! -f "$ca_private.pub" || -L "$ca_private.pub" ]]; then
  echo "tunnel CA is incomplete or not regular" >&2
  exit 2
fi
chmod 0600 "$ca_private"
chmod 0644 "$ca_private.pub"

if [[ ! -e "$state_root/state.json" && ! -e "$state_root/authorized_principals" && ! -e "$state_root/revoked.krl" ]]; then
  "$broker_program" --state-root "$state_root" --ca-private-key "$ca_private" --account "$account" \
    --worker-account "$worker_account" initialize
fi
if [[ -f "$state_root/state.json" && ! -e "$state_root/authorized_worker_principals" ]]; then
  install -o root -g root -m 0644 /dev/null "$state_root/authorized_worker_principals"
fi
for required_state in state.json authorized_principals authorized_worker_principals revoked.krl; do
  [[ -f "$state_root/$required_state" && ! -L "$state_root/$required_state" ]] || {
    echo "tunnel broker state is incomplete: $required_state" >&2
    exit 2
  }
done

candidate="$(mktemp "$(dirname "$sshd_fragment")/.my-data-hub-sshd.XXXXXX")"
fragment_backup=""
cleanup() { python3 - "$candidate" "$fragment_backup" <<'PY'
from pathlib import Path
import sys
for raw in sys.argv[1:]:
    if raw:
        Path(raw).unlink(missing_ok=True)
PY
}
trap cleanup EXIT
"$broker_program" --state-root "$state_root" --ca-private-key "$ca_private" --account "$account" \
  --worker-account "$worker_account" \
  render-sshd-config --listen-port "$listen_port" --output "$candidate"
chmod 0600 "$candidate"
# Validate the standalone Match block before it can affect the host include.
sshd -t -f "$candidate"
if [[ -e "$sshd_fragment" ]]; then
  [[ -f "$sshd_fragment" && ! -L "$sshd_fragment" ]] || {
    echo "existing sshd tunnel fragment is not a regular file" >&2
    exit 2
  }
  fragment_backup="$(mktemp "$state_root/.sshd-fragment.previous.XXXXXX")"
  install -o root -g root -m 0600 "$sshd_fragment" "$fragment_backup"
fi
install -o root -g root -m 0600 "$candidate" "$sshd_fragment"
rollback_sshd_fragment() {
  if [[ -n "$fragment_backup" ]]; then
    install -o root -g root -m 0600 "$fragment_backup" "$sshd_fragment"
  else
    python3 - "$sshd_fragment" <<'PY'
from pathlib import Path
import sys
Path(sys.argv[1]).unlink(missing_ok=True)
PY
  fi
}
if ! sshd -t; then
  rollback_sshd_fragment
  echo "combined sshd configuration rejected the tunnel Match block" >&2
  exit 2
fi

reconcile_unit="$unit_root/my-data-hub-master-tunnel-reconcile.service"
timer_unit="$unit_root/my-data-hub-master-tunnel-reconcile.timer"
cat > "$reconcile_unit" <<UNIT
[Unit]
Description=Fail-close expired my-data-hub master tunnel epochs
After=network.target ssh.service sshd.service

[Service]
Type=oneshot
ExecStart=$broker_program --state-root $state_root --ca-private-key $ca_private --account $account --worker-account $worker_account reconcile
NoNewPrivileges=yes
PrivateTmp=yes
ProtectHome=yes
ProtectSystem=strict
ReadWritePaths=$state_root
UNIT
cat > "$timer_unit" <<'UNIT'
[Unit]
Description=Reconcile my-data-hub master tunnel authorization

[Timer]
OnBootSec=5s
OnUnitActiveSec=5s
AccuracySec=1s
Unit=my-data-hub-master-tunnel-reconcile.service

[Install]
WantedBy=timers.target
UNIT
chmod 0644 "$reconcile_unit" "$timer_unit"
systemctl daemon-reload
if ! systemctl reload ssh.service 2>/dev/null && ! systemctl reload sshd.service; then
  rollback_sshd_fragment
  sshd -t
  systemctl reload ssh.service 2>/dev/null || systemctl reload sshd.service
  echo "sshd reload rejected the tunnel Match block; previous fragment restored" >&2
  exit 2
fi
systemctl enable --now my-data-hub-master-tunnel-reconcile.timer
ipc_unit="$unit_root/my-data-hub-master-tunnel-broker.service"
cat > "$ipc_unit" <<UNIT
[Unit]
Description=Root-owned my-data-hub master tunnel certificate broker
After=network.target ssh.service sshd.service
Before=my-data-hub-master-tunnel-reconcile.timer

[Service]
Type=simple
Group=$control_gid
RuntimeDirectory=$broker_runtime_directory
RuntimeDirectoryMode=0750
ExecStartPre=/usr/bin/rm -f $broker_socket
ExecStart=$broker_ipc_program --state-root $state_root --ca-private-key $ca_private --account $account --worker-account $worker_account --socket $broker_socket --allowed-uid $control_uid --socket-gid $control_gid
Restart=on-failure
RestartSec=2s
UMask=0077
NoNewPrivileges=yes
PrivateTmp=yes
ProtectHome=yes
ProtectSystem=strict
ReadWritePaths=$state_root $(dirname "$broker_socket")

[Install]
WantedBy=multi-user.target
UNIT
chmod 0644 "$ipc_unit"
systemctl daemon-reload
if ! systemctl enable --now my-data-hub-master-tunnel-broker.service; then
  systemctl disable --now my-data-hub-master-tunnel-broker.service >/dev/null 2>&1 || true
  echo "master tunnel broker service failed to start" >&2
  exit 2
fi
if ! systemctl is-active --quiet my-data-hub-master-tunnel-broker.service; then
  systemctl status my-data-hub-master-tunnel-broker.service --no-pager -l >&2 || true
  systemctl disable --now my-data-hub-master-tunnel-broker.service >/dev/null 2>&1 || true
  echo "master tunnel broker service is not active" >&2
  exit 2
fi
if ! python3 - "$broker_socket" "$control_gid" <<'PY'
import os
from pathlib import Path
import stat
import sys
import time

socket_path = Path(sys.argv[1])
expected_gid = int(sys.argv[2])
deadline = time.monotonic() + 5.0
while time.monotonic() < deadline:
    try:
        observed = socket_path.lstat()
    except FileNotFoundError:
        time.sleep(0.05)
        continue
    if (
        stat.S_ISSOCK(observed.st_mode)
        and observed.st_uid == 0
        and observed.st_gid == expected_gid
        and stat.S_IMODE(observed.st_mode) == 0o660
    ):
        break
    raise SystemExit("master tunnel broker socket violates its owner/mode contract")
else:
    raise SystemExit("master tunnel broker socket was not created within five seconds")

parent = socket_path.parent.lstat()
if (
    not stat.S_ISDIR(parent.st_mode)
    or parent.st_uid != 0
    or parent.st_gid != expected_gid
    or stat.S_IMODE(parent.st_mode) != 0o750
):
    raise SystemExit("master tunnel broker runtime directory violates its owner/mode contract")
PY
then
  systemctl status my-data-hub-master-tunnel-broker.service --no-pager -l >&2 || true
  systemctl disable --now my-data-hub-master-tunnel-broker.service >/dev/null 2>&1 || true
  echo "master tunnel broker socket readiness failed" >&2
  exit 2
fi
trap - EXIT
cleanup
printf 'master_tunnel_broker_installed=true\naccount=%s\nlisten=127.0.0.1:%s\nsocket=%s\nactive_epoch=none_until_authenticated_activation\n' \
  "$account" "$listen_port" "$broker_socket"
