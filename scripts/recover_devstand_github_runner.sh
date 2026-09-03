#!/usr/bin/env bash
set -Eeuo pipefail

cat >&2 <<'EOF'
FORBIDDEN: self-hosted GitHub Actions runners are not authorized on the owner's DevCoveer machine.

This former recovery script is intentionally disabled. It must not install, enable,
unmask, start, restart, or otherwise manage any actions.runner.* service.

Dataset Loop MCP deployment must use an explicitly approved execution path that does
not require an owner-hosted GitHub Actions runner. Do not ask the owner to install one.
EOF

exit 78
