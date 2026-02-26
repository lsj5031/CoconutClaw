#!/usr/bin/env bash
set -euo pipefail

echo "warning: ./agent.sh is deprecated; forwarding to Rust coconutclaw CLI" >&2

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/scripts" && pwd)"
args=()
for arg in "$@"; do
  if [[ "$arg" == "--once" ]]; then
    args+=("--once")
  else
    args+=("$arg")
  fi
done
exec bash "$SCRIPT_DIR/run.sh" "${args[@]}"
