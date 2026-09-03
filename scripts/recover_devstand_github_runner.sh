#!/usr/bin/env bash
set -Eeuo pipefail

if [[ ${EUID:-$(id -u)} -ne 0 ]]; then
  echo "ERROR: run as root (for example: curl ... | sudo bash)" >&2
  exit 2
fi

command -v systemctl >/dev/null 2>&1 || {
  echo "ERROR: systemd is unavailable on this host" >&2
  exit 3
}

mapfile -t all_units < <(
  systemctl list-unit-files --type=service --no-legend 'actions.runner.*.service' 2>/dev/null \
    | awk '{print $1}' \
    | sort -u
)

if [[ ${#all_units[@]} -eq 0 ]]; then
  echo "ERROR: no installed GitHub Actions runner service was found" >&2
  exit 4
fi

targets=()
for unit in "${all_units[@]}"; do
  if [[ "$unit" == *my-data-hub* ]]; then
    targets+=("$unit")
    continue
  fi

  workdir=$(systemctl show "$unit" -p WorkingDirectory --value 2>/dev/null || true)
  if [[ -n "$workdir" && -r "$workdir/.runner" ]] \
    && grep -Fqi 'onedayonemasterpiece/my-data-hub' "$workdir/.runner"; then
    targets+=("$unit")
  fi
done

# Older runner installations may not retain the repository name in the unit or
# .runner metadata. Starting installed Actions runner units is safer than
# leaving the known DevCoveer deployment queue permanently unserved.
if [[ ${#targets[@]} -eq 0 ]]; then
  targets=("${all_units[@]}")
fi

for unit in "${targets[@]}"; do
  systemctl unmask "$unit" >/dev/null 2>&1 || true
  systemctl reset-failed "$unit" >/dev/null 2>&1 || true
  systemctl enable "$unit" >/dev/null
  systemctl restart "$unit"
done

deadline=$((SECONDS + 30))
while (( SECONDS < deadline )); do
  active=0
  for unit in "${targets[@]}"; do
    if systemctl is-active --quiet "$unit"; then
      active=$((active + 1))
    fi
  done
  if (( active > 0 )); then
    printf 'RUNNER_RECOVERY_OK active=%d units=' "$active"
    printf '%s ' "${targets[@]}"
    printf '\n'
    exit 0
  fi
  sleep 1
done

for unit in "${targets[@]}"; do
  systemctl --no-pager --full status "$unit" >&2 || true
done

echo "ERROR: runner services did not become active within 30 seconds" >&2
exit 5
