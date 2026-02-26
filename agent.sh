#!/usr/bin/env bash
set -euo pipefail

echo "warning: ./agent.sh is deprecated; forwarding to Rust coconutclaw CLI" >&2

BIN="${COCONUTCLAW_BIN:-coconutclaw}"
if ! command -v "$BIN" >/dev/null 2>&1; then
  if [[ -x "./target/debug/coconutclaw" ]]; then
    BIN="./target/debug/coconutclaw"
  else
    echo "error: coconutclaw binary not found in PATH. Build with: cargo build -p coconutclaw" >&2
    exit 1
  fi
fi

subcommand="run"
args=()
for arg in "$@"; do
  if [[ "$arg" == "--once" ]]; then
    subcommand="once"
  else
    args+=("$arg")
  fi
done

exec "$BIN" "$subcommand" "${args[@]}"
