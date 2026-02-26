#!/usr/bin/env bash
set -euo pipefail

echo "warning: scripts/nightly_reflection.sh is deprecated; forwarding to coconutclaw nightly-reflection" >&2

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec bash "$SCRIPT_DIR/run.sh" --nightly-reflection
