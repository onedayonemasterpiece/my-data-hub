#!/usr/bin/env bash
set -Eeuo pipefail
printf '%s\n' \
  'Yandex VM/ALB provisioning is retired: the owner selected the DevCoveer local edge.' \
  'Use deploy/local-edge/README.md. This command performs no cloud operation.' >&2
exit 78
