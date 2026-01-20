#!/usr/bin/env bash
set -euo pipefail

if [ "${1:-}" = "serve" ]; then
  shift
  exec /usr/local/bin/serve "$@"
else
  exec "$@"
fi
