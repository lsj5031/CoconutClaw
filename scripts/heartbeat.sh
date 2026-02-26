#!/usr/bin/env bash
set -euo pipefail

echo "warning: scripts/heartbeat.sh is deprecated; forwarding to coconutclaw heartbeat" >&2

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec bash "$SCRIPT_DIR/run.sh" --heartbeat
