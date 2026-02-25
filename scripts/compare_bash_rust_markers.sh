#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INSTANCE_DIR=""
TEXT="ping"
CHAT_ID="1000"

usage() {
  cat <<USAGE
usage: $0 [--instance-dir <path>] [--text <message>] [--chat-id <id>]

Runs one injected text turn via Bash and Rust, then compares marker keys.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --instance-dir)
      INSTANCE_DIR="$2"
      shift 2
      ;;
    --text)
      TEXT="$2"
      shift 2
      ;;
    --chat-id)
      CHAT_ID="$2"
      shift 2
      ;;
    --help)
      usage
      exit 0
      ;;
    *)
      echo "unknown arg: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$INSTANCE_DIR" ]]; then
  INSTANCE_DIR="$ROOT_DIR/tmp/regression_instance"
fi
if [[ "$INSTANCE_DIR" != /* ]]; then
  INSTANCE_DIR="$ROOT_DIR/$INSTANCE_DIR"
fi

mkdir -p "$INSTANCE_DIR" "$INSTANCE_DIR/tmp" "$INSTANCE_DIR/config" "$INSTANCE_DIR/TASKS"

BOT_TOKEN="${TELEGRAM_BOT_TOKEN:-}"
if [[ -z "$BOT_TOKEN" && -f "$ROOT_DIR/.env" ]]; then
  BOT_TOKEN="$(awk -F= '/^TELEGRAM_BOT_TOKEN=/{sub(/^TELEGRAM_BOT_TOKEN=/, "", $0); print $0; exit}' "$ROOT_DIR/.env")"
fi
if [[ -z "$BOT_TOKEN" ]]; then
  BOT_TOKEN="dummy_local_token"
fi

if [[ ! -f "$INSTANCE_DIR/.env" ]]; then
  cat > "$INSTANCE_DIR/.env" <<ENV
TELEGRAM_BOT_TOKEN=$BOT_TOKEN
TELEGRAM_CHAT_ID=$CHAT_ID
AGENT_PROVIDER=${AGENT_PROVIDER:-codex}
CODEX_BIN=${CODEX_BIN:-codex}
CODEX_MODEL=${CODEX_MODEL:-}
CODEX_REASONING_EFFORT=${CODEX_REASONING_EFFORT:-}
EXEC_POLICY=${EXEC_POLICY:-yolo}
WEBHOOK_MODE=off
ALLOWLIST_PATH=./config/allowlist.txt
PI_BIN=${PI_BIN:-pi}
PI_PROVIDER=${PI_PROVIDER:-}
PI_MODEL=${PI_MODEL:-}
PI_MODE=${PI_MODE:-text}
PI_EXTRA_ARGS=${PI_EXTRA_ARGS:-}
TIMEZONE=UTC
SQLITE_DB_PATH=./state.db
LOG_DIR=./LOGS
ENV
fi

for seed_file in SOUL.md MEMORY.md USER.md; do
  if [[ ! -f "$INSTANCE_DIR/$seed_file" && -f "$ROOT_DIR/$seed_file" ]]; then
    cp "$ROOT_DIR/$seed_file" "$INSTANCE_DIR/$seed_file"
  fi
done
if [[ ! -f "$INSTANCE_DIR/SOUL.md" ]]; then
  echo "You are CoconutClaw, a calm and practical local agent." > "$INSTANCE_DIR/SOUL.md"
fi
if [[ ! -f "$INSTANCE_DIR/MEMORY.md" ]]; then
  echo "# Long-Term Memory" > "$INSTANCE_DIR/MEMORY.md"
fi
if [[ ! -f "$INSTANCE_DIR/USER.md" ]]; then
  echo "# User Profile" > "$INSTANCE_DIR/USER.md"
fi
if [[ ! -f "$INSTANCE_DIR/TASKS/pending.md" ]]; then
  if [[ -f "$ROOT_DIR/TASKS/pending.md" ]]; then
    cp "$ROOT_DIR/TASKS/pending.md" "$INSTANCE_DIR/TASKS/pending.md"
  else
    echo "# Pending Tasks" > "$INSTANCE_DIR/TASKS/pending.md"
  fi
fi

: > "$INSTANCE_DIR/config/allowlist.txt"

BASH_OUT="$INSTANCE_DIR/tmp/bash_markers.txt"
RUST_OUT="$INSTANCE_DIR/tmp/rust_markers.txt"

unset_vars=(
  TELEGRAM_BOT_TOKEN TELEGRAM_CHAT_ID AGENT_PROVIDER
  CODEX_BIN CODEX_MODEL CODEX_REASONING_EFFORT EXEC_POLICY
  SQLITE_DB_PATH LOG_DIR PI_BIN PI_PROVIDER PI_MODEL PI_MODE PI_EXTRA_ARGS
  COCONUTCLAW_INSTANCE COCONUTCLAW_DATA_DIR INSTANCE_DIR TIMEZONE
  WEBHOOK_MODE WEBHOOK_BIND WEBHOOK_PUBLIC_URL WEBHOOK_SECRET
  POLL_INTERVAL_SECONDS
)
ENV_CMD=(env)
for key in "${unset_vars[@]}"; do
  ENV_CMD+=("-u" "$key")
done
ENV_CMD+=("PATH=$PATH" "HOME=$HOME")

set +e
"${ENV_CMD[@]}" "$ROOT_DIR/agent.sh" --instance-dir "$INSTANCE_DIR" --inject-text "$TEXT" --chat-id "$CHAT_ID" >"$BASH_OUT" 2>&1
bash_rc=$?
"${ENV_CMD[@]}" cargo run -q -p coconutclaw -- --instance-dir "$INSTANCE_DIR" once --inject-text "$TEXT" --chat-id "$CHAT_ID" >"$RUST_OUT" 2>&1
rust_rc=$?
set -e

extract_marker_keys() {
  local file="$1"
  rg -o '^[A-Z_]+:' "$file" | sed 's/:$//' | sort -u
}

BASH_KEYS="$INSTANCE_DIR/tmp/bash_marker_keys.txt"
RUST_KEYS="$INSTANCE_DIR/tmp/rust_marker_keys.txt"
extract_marker_keys "$BASH_OUT" > "$BASH_KEYS" || true
extract_marker_keys "$RUST_OUT" > "$RUST_KEYS" || true

echo "bash_rc=$bash_rc"
echo "rust_rc=$rust_rc"
echo "bash_out=$BASH_OUT"
echo "rust_out=$RUST_OUT"

if diff -u "$BASH_KEYS" "$RUST_KEYS"; then
  echo "marker_key_diff=none"
else
  echo "marker_key_diff=present"
fi
