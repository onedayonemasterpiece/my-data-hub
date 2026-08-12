#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

[[ "${1:-}" == "CREATE_MY_DATA_HUB_EDGE_TUNNEL_IDENTITY" ]] || {
  echo "usage: $0 CREATE_MY_DATA_HUB_EDGE_TUNNEL_IDENTITY" >&2
  exit 2
}
for command_name in ssh-keygen ssh-keyscan yc python3; do
  command -v "$command_name" >/dev/null || { echo "$command_name is required" >&2; exit 2; }
done
folder_id="${MY_DATA_HUB_YC_FOLDER_ID:-b1g5tck18cgqtjb7rn3s}"
secret_name="${MY_DATA_HUB_EDGE_TUNNEL_SECRET_NAME:-my-data-hub-edge-tunnel}"
authorized_keys="${MY_DATA_HUB_DEVSTAND_AUTHORIZED_KEYS:-$HOME/.ssh/authorized_keys}"
[[ "$authorized_keys" = /* && ! -L "$authorized_keys" ]] || {
  echo "authorized_keys must be an absolute non-symlink path" >&2
  exit 2
}
if yc lockbox secret get --folder-id "$folder_id" --name "$secret_name" >/dev/null 2>&1; then
  echo "refusing to replace an existing tunnel secret" >&2
  exit 78
fi

tmp_dir="$(mktemp -d /dev/shm/my-data-hub-edge-key.XXXXXX)"
cleanup() { python3 - "$tmp_dir" <<'PY'
from pathlib import Path
import shutil,sys
shutil.rmtree(Path(sys.argv[1]), ignore_errors=True)
PY
}
trap cleanup EXIT
key_file="$tmp_dir/tunnel_ed25519"
ssh-keygen -q -t ed25519 -N '' -C 'my-data-hub-edge-tunnel' -f "$key_file"

mkdir -p "$(dirname "$authorized_keys")"
touch "$authorized_keys"
chmod 700 "$(dirname "$authorized_keys")"
chmod 600 "$authorized_keys"
public_key="$(cat "$key_file.pub")"
marker='my-data-hub-edge-tunnel'
if grep -Fq "$marker" "$authorized_keys"; then
  echo "an authorized edge tunnel identity already exists" >&2
  exit 78
fi

python3 - "$key_file" <<'PY' | yc lockbox secret create \
  --folder-id "$folder_id" \
  --name "$secret_name" \
  --description 'Restricted my-data-hub YC edge to DevCoveer loopback tunnel key' \
  --labels project=my-data-hub,scope=edge-tunnel \
  --version-description initial \
  --payload - \
  --format yaml
import json,sys
with open(sys.argv[1], encoding='utf-8') as stream:
    value=stream.read()
print(json.dumps([{'key':'tunnel_private_key','text_value':value}], separators=(',',':')))
PY
printf '%s %s\n' \
  'command="/bin/false",no-agent-forwarding,no-X11-forwarding,no-pty,no-user-rc,permitopen="127.0.0.1:8080",permitopen="127.0.0.1:8765",permitopen="127.0.0.1:8780"' \
  "$public_key" >> "$authorized_keys"
printf 'authorized_key_installed=true\nprivate_key_persisted_on_devstand=false\n'
