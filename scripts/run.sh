#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INSTANCE=""
INSTANCE_DIR="."
INSTANCE_SPECIFIED=0
INSTANCE_DIR_SPECIFIED=0
USE_CARGO=0

command_name="run"
inject_text=""
inject_file=""
chat_id=""

usage() {
  cat <<'EOF'
Usage: run.sh [options]

Options:
  --once
  --doctor
  --heartbeat
  --nightly-reflection
  --inject-text <text>
  --inject-file <path>
  --chat-id <id>
  --instance <name>
  --instance-dir <path>
  --use-cargo
  -h, --help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --once)
      command_name="once"
      shift
      ;;
    --doctor)
      command_name="doctor"
      shift
      ;;
    --heartbeat)
      command_name="heartbeat"
      shift
      ;;
    --nightly-reflection)
      command_name="nightly-reflection"
      shift
      ;;
    --inject-text)
      inject_text="${2:-}"
      shift 2
      ;;
    --inject-file)
      inject_file="${2:-}"
      shift 2
      ;;
    --chat-id)
      chat_id="${2:-}"
      shift 2
      ;;
    --instance)
      INSTANCE="${2:-}"
      INSTANCE_SPECIFIED=1
      shift 2
      ;;
    --instance-dir)
      INSTANCE_DIR="${2:-.}"
      INSTANCE_DIR_SPECIFIED=1
      shift 2
      ;;
    --use-cargo)
      USE_CARGO=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ "$INSTANCE_SPECIFIED" -eq 1 && "$INSTANCE_DIR_SPECIFIED" -eq 1 ]]; then
  echo "--instance and --instance-dir are mutually exclusive" >&2
  exit 1
fi

if [[ "$INSTANCE_SPECIFIED" -eq 1 ]]; then
  [[ "$INSTANCE" =~ ^[a-zA-Z0-9_.-]+$ ]] || {
    echo "invalid instance: $INSTANCE (expected [a-zA-Z0-9_.-])" >&2
    exit 1
  }
else
  if [[ "$INSTANCE_DIR" != /* ]]; then
    INSTANCE_DIR="$(cd "$ROOT_DIR" && mkdir -p "$INSTANCE_DIR" && cd "$INSTANCE_DIR" && pwd)"
  else
    mkdir -p "$INSTANCE_DIR"
  fi
fi

if [[ "$INSTANCE_SPECIFIED" -eq 1 ]]; then
  args=(--instance "$INSTANCE" "$command_name")
else
  args=(--instance-dir "$INSTANCE_DIR" "$command_name")
fi
if [[ "$command_name" == "once" ]]; then
  if [[ -n "$inject_text" ]]; then
    args+=(--inject-text "$inject_text")
  fi
  if [[ -n "$inject_file" ]]; then
    args+=(--inject-file "$inject_file")
  fi
  if [[ -n "$chat_id" ]]; then
    args+=(--chat-id "$chat_id")
  fi
fi

cd "$ROOT_DIR"

if [[ "$USE_CARGO" -eq 0 ]]; then
  if command -v coconutclaw >/dev/null 2>&1; then
    exec coconutclaw "${args[@]}"
  fi
  if [[ -x "$ROOT_DIR/target/release/coconutclaw" ]]; then
    exec "$ROOT_DIR/target/release/coconutclaw" "${args[@]}"
  fi
  if [[ -x "$ROOT_DIR/target/debug/coconutclaw" ]]; then
    exec "$ROOT_DIR/target/debug/coconutclaw" "${args[@]}"
  fi
fi

exec cargo run -p coconutclaw -- "${args[@]}"
