#!/usr/bin/env bash
set -Eeuo pipefail

# Historical compatibility guard. The same-host local-PostgreSQL topology was never
# installed and is forbidden by the 2026-08-10 architecture reset.
if [[ "${1:-}" == "INSTALL_MY_DATA_HUB_SAME_HOST" || "${1:-}" == "PREPARE" ]]; then
  echo "FORBIDDEN: same-host PostgreSQL topology is superseded by the Kaggle-master architecture reset" >&2
  echo "Use deploy/control-plane/install.sh only after separately approving control-plane deployment" >&2
  exit 78
fi
echo "This installer is permanently disabled; local PostgreSQL/PGDATA on devstand is forbidden" >&2
exit 78
