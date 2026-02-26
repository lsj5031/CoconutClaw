#!/usr/bin/env bash
set -euo pipefail

echo "warning: scripts/nightly_reflection.sh is deprecated; forwarding to coconutclaw nightly-reflection" >&2

BIN="${COCONUTCLAW_BIN:-coconutclaw}"
if ! command -v "$BIN" >/dev/null 2>&1; then
  if [[ -x "./target/debug/coconutclaw" ]]; then
    BIN="./target/debug/coconutclaw"
  else
    echo "error: coconutclaw binary not found in PATH. Build with: cargo build -p coconutclaw" >&2
    exit 1
  fi
fi

exec "$BIN" nightly-reflection
