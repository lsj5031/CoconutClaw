#!/usr/bin/env bash

resolve_coconutclaw_bin() {
  local root_dir
  root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

  local bin="${COCONUTCLAW_BIN:-coconutclaw}"
  if command -v "$bin" >/dev/null 2>&1; then
    printf "%s\n" "$bin"
    return 0
  fi

  local fallback="$root_dir/target/debug/coconutclaw"
  if [[ -x "$fallback" ]]; then
    printf "%s\n" "$fallback"
    return 0
  fi

  echo "error: coconutclaw binary not found in PATH. Build with: cargo build -p coconutclaw" >&2
  return 1
}
