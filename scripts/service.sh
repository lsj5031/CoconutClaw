#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INSTANCE=""
INSTANCE_DIR="."
INSTANCE_SPECIFIED=0
INSTANCE_DIR_SPECIFIED=0
INSTANCE_KEY="default"
HEARTBEAT_TIME="09:00"
REFLECTION_TIME="22:30"
USE_CARGO=0
SERVICE_LOG_DIR="$ROOT_DIR/LOGS"

BASE_RUN_LABEL="io.coconutclaw.run"
BASE_HEARTBEAT_LABEL="io.coconutclaw.heartbeat"
BASE_REFLECTION_LABEL="io.coconutclaw.nightly_reflection"
BASE_RUN_TASK="coconutclaw.service"
BASE_HEARTBEAT_TASK="coconutclaw-heartbeat.service"
BASE_HEARTBEAT_TIMER="coconutclaw-heartbeat.timer"
BASE_REFLECTION_TASK="coconutclaw-nightly-reflection.service"
BASE_REFLECTION_TIMER="coconutclaw-nightly-reflection.timer"

RUN_LABEL="$BASE_RUN_LABEL"
HEARTBEAT_LABEL="$BASE_HEARTBEAT_LABEL"
REFLECTION_LABEL="$BASE_REFLECTION_LABEL"
RUN_TASK="$BASE_RUN_TASK"
HEARTBEAT_TASK="$BASE_HEARTBEAT_TASK"
HEARTBEAT_TIMER="$BASE_HEARTBEAT_TIMER"
REFLECTION_TASK="$BASE_REFLECTION_TASK"
REFLECTION_TIMER="$BASE_REFLECTION_TIMER"

usage() {
  cat <<'EOF'
Usage: service.sh <install|start|stop|status|uninstall> [options]

Options:
  --instance <name>       Instance name ([a-zA-Z0-9_.-], default: default)
  --instance-dir <path>   Instance directory (default: .)
  --heartbeat <HH:MM>     Daily heartbeat time (default: 09:00)
  --reflection <HH:MM>    Daily reflection time (default: 22:30)
  --use-cargo             Force cargo run in service actions
  -h, --help
EOF
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing dependency: $1" >&2
    exit 1
  }
}

validate_hhmm() {
  [[ "$1" =~ ^([01][0-9]|2[0-3]):([0-5][0-9])$ ]] || {
    echo "invalid time: $1 (expected HH:MM)" >&2
    exit 1
  }
}

validate_instance_name() {
  [[ "$1" =~ ^[a-zA-Z0-9_.-]+$ ]] || {
    echo "invalid instance: $1 (expected [a-zA-Z0-9_.-])" >&2
    exit 1
  }
}

sanitize_identifier() {
  local input="$1"
  echo "$input" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9_.-]+/-/g; s/^-+//; s/-+$//'
}

cksum_identifier() {
  local input="$1"
  printf '%s' "$input" | cksum | awk '{print $1}'
}

configure_service_names() {
  local key=""
  if [[ "$INSTANCE_SPECIFIED" -eq 1 ]]; then
    local normalized
    normalized="$(sanitize_identifier "$INSTANCE")"
    if [[ "$normalized" != "default" ]]; then
      key="$normalized"
      INSTANCE_KEY="$normalized"
    fi
  elif [[ "$INSTANCE_DIR" != "$ROOT_DIR" ]]; then
    local base hash
    base="$(basename "$INSTANCE_DIR")"
    base="$(sanitize_identifier "$base")"
    if [[ -z "$base" ]]; then
      base="instance"
    fi
    hash="$(cksum_identifier "$INSTANCE_DIR")"
    key="dir-$base-$hash"
    INSTANCE_KEY="$key"
  fi

  if [[ -z "$key" ]]; then
    return
  fi

  local label_key="${key//_/-}"
  RUN_TASK="coconutclaw-$key.service"
  HEARTBEAT_TASK="coconutclaw-heartbeat-$key.service"
  HEARTBEAT_TIMER="coconutclaw-heartbeat-$key.timer"
  REFLECTION_TASK="coconutclaw-nightly-reflection-$key.service"
  REFLECTION_TIMER="coconutclaw-nightly-reflection-$key.timer"
  RUN_LABEL="$BASE_RUN_LABEL.$label_key"
  HEARTBEAT_LABEL="$BASE_HEARTBEAT_LABEL.$label_key"
  REFLECTION_LABEL="$BASE_REFLECTION_LABEL.$label_key"
}

linux_systemd_dir() {
  echo "${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
}

mac_launchagents_dir() {
  echo "$HOME/Library/LaunchAgents"
}

run_wrapper_args() {
  local mode="$1"
  local args=()
  if [[ "$INSTANCE_SPECIFIED" -eq 1 ]]; then
    args+=("--instance" "$INSTANCE")
  else
    args+=("--instance-dir" "$INSTANCE_DIR")
  fi
  if [[ "$USE_CARGO" -eq 1 ]]; then
    args+=("--use-cargo")
  fi
  case "$mode" in
    run) ;;
    heartbeat) args+=("--heartbeat") ;;
    reflection) args+=("--nightly-reflection") ;;
    *) echo "invalid mode: $mode" >&2; exit 1 ;;
  esac
  printf "%s " "${args[@]}"
}

write_plist_instance_args() {
  if [[ "$INSTANCE_SPECIFIED" -eq 1 ]]; then
    cat <<EOF
    <string>--instance</string>
    <string>$INSTANCE</string>
EOF
  else
    cat <<EOF
    <string>--instance-dir</string>
    <string>$INSTANCE_DIR</string>
EOF
  fi
}

linux_install() {
  require_cmd systemctl
  local dir
  dir="$(linux_systemd_dir)"
  mkdir -p "$dir"

  cat >"$dir/$RUN_TASK" <<EOF
[Unit]
Description=CoconutClaw Telegram Agent
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=$ROOT_DIR
ExecStart=/usr/bin/env bash $ROOT_DIR/scripts/run.sh $(run_wrapper_args run)
Restart=always
RestartSec=3

[Install]
WantedBy=default.target
EOF

  cat >"$dir/$HEARTBEAT_TASK" <<EOF
[Unit]
Description=CoconutClaw Daily Heartbeat

[Service]
Type=oneshot
WorkingDirectory=$ROOT_DIR
ExecStart=/usr/bin/env bash $ROOT_DIR/scripts/run.sh $(run_wrapper_args heartbeat)
EOF

  cat >"$dir/$HEARTBEAT_TIMER" <<EOF
[Unit]
Description=Run CoconutClaw heartbeat daily

[Timer]
OnCalendar=*-*-* ${HEARTBEAT_TIME}:00
Persistent=true
Unit=$HEARTBEAT_TASK

[Install]
WantedBy=timers.target
EOF

  cat >"$dir/$REFLECTION_TASK" <<EOF
[Unit]
Description=CoconutClaw Nightly Reflection

[Service]
Type=oneshot
WorkingDirectory=$ROOT_DIR
ExecStart=/usr/bin/env bash $ROOT_DIR/scripts/run.sh $(run_wrapper_args reflection)
EOF

  cat >"$dir/$REFLECTION_TIMER" <<EOF
[Unit]
Description=Run CoconutClaw nightly reflection daily

[Timer]
OnCalendar=*-*-* ${REFLECTION_TIME}:00
Persistent=true
Unit=$REFLECTION_TASK

[Install]
WantedBy=timers.target
EOF

  systemctl --user daemon-reload
  systemctl --user enable "$RUN_TASK" "$HEARTBEAT_TIMER" "$REFLECTION_TIMER"
  echo "installed user systemd units in $dir"
  echo "instance key: $INSTANCE_KEY"
}

linux_start() {
  require_cmd systemctl
  systemctl --user start "$RUN_TASK" "$HEARTBEAT_TIMER" "$REFLECTION_TIMER"
}

linux_stop() {
  require_cmd systemctl
  systemctl --user stop "$RUN_TASK" "$HEARTBEAT_TIMER" "$REFLECTION_TIMER" 2>/dev/null || true
}

linux_status() {
  require_cmd systemctl
  systemctl --user status "$RUN_TASK" "$HEARTBEAT_TASK" "$HEARTBEAT_TIMER" "$REFLECTION_TASK" "$REFLECTION_TIMER" --no-pager || true
}

linux_uninstall() {
  require_cmd systemctl
  local dir
  dir="$(linux_systemd_dir)"
  linux_stop
  systemctl --user disable "$RUN_TASK" "$HEARTBEAT_TIMER" "$REFLECTION_TIMER" 2>/dev/null || true
  rm -f "$dir/$RUN_TASK" "$dir/$HEARTBEAT_TASK" "$dir/$HEARTBEAT_TIMER" "$dir/$REFLECTION_TASK" "$dir/$REFLECTION_TIMER"
  systemctl --user daemon-reload
  echo "removed user systemd units from $dir"
}

launchctl_domain() {
  echo "gui/$UID"
}

mac_write_run_plist() {
  local path="$1"
  cat >"$path" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>$RUN_LABEL</string>
  <key>ProgramArguments</key>
  <array>
    <string>/bin/bash</string>
    <string>$ROOT_DIR/scripts/run.sh</string>
EOF
  write_plist_instance_args >>"$path"
  if [[ "$USE_CARGO" -eq 1 ]]; then
    cat >>"$path" <<EOF
    <string>--use-cargo</string>
EOF
  fi
  cat >>"$path" <<EOF
  </array>
  <key>WorkingDirectory</key>
  <string>$ROOT_DIR</string>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>StandardOutPath</key>
  <string>$SERVICE_LOG_DIR/$RUN_LABEL.log</string>
  <key>StandardErrorPath</key>
  <string>$SERVICE_LOG_DIR/$RUN_LABEL.err.log</string>
</dict>
</plist>
EOF
}

mac_write_timer_plist() {
  local path="$1"
  local label="$2"
  local hour="$3"
  local minute="$4"
  local mode="$5"
  cat >"$path" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>$label</string>
  <key>ProgramArguments</key>
  <array>
    <string>/bin/bash</string>
    <string>$ROOT_DIR/scripts/run.sh</string>
EOF
  write_plist_instance_args >>"$path"
  if [[ "$USE_CARGO" -eq 1 ]]; then
    cat >>"$path" <<EOF
    <string>--use-cargo</string>
EOF
  fi
  if [[ "$mode" == "heartbeat" ]]; then
    cat >>"$path" <<EOF
    <string>--heartbeat</string>
EOF
  else
    cat >>"$path" <<EOF
    <string>--nightly-reflection</string>
EOF
  fi
  cat >>"$path" <<EOF
  </array>
  <key>WorkingDirectory</key>
  <string>$ROOT_DIR</string>
  <key>StartCalendarInterval</key>
  <dict>
    <key>Hour</key>
    <integer>$hour</integer>
    <key>Minute</key>
    <integer>$minute</integer>
  </dict>
  <key>StandardOutPath</key>
  <string>$SERVICE_LOG_DIR/$label.log</string>
  <key>StandardErrorPath</key>
  <string>$SERVICE_LOG_DIR/$label.err.log</string>
</dict>
</plist>
EOF
}

mac_bootstrap_plist() {
  local domain plist
  domain="$(launchctl_domain)"
  plist="$1"
  launchctl bootout "$domain" "$plist" 2>/dev/null || true
  launchctl bootstrap "$domain" "$plist"
}

mac_install() {
  require_cmd launchctl
  local dir
  dir="$(mac_launchagents_dir)"
  mkdir -p "$dir" "$SERVICE_LOG_DIR"

  local run_plist="$dir/$RUN_LABEL.plist"
  local heartbeat_plist="$dir/$HEARTBEAT_LABEL.plist"
  local reflection_plist="$dir/$REFLECTION_LABEL.plist"

  local h_hour="${HEARTBEAT_TIME%:*}"
  local h_min="${HEARTBEAT_TIME#*:}"
  local r_hour="${REFLECTION_TIME%:*}"
  local r_min="${REFLECTION_TIME#*:}"

  mac_write_run_plist "$run_plist"
  mac_write_timer_plist "$heartbeat_plist" "$HEARTBEAT_LABEL" "$h_hour" "$h_min" "heartbeat"
  mac_write_timer_plist "$reflection_plist" "$REFLECTION_LABEL" "$r_hour" "$r_min" "reflection"

  mac_bootstrap_plist "$run_plist"
  mac_bootstrap_plist "$heartbeat_plist"
  mac_bootstrap_plist "$reflection_plist"

  echo "installed launchd agents in $dir"
  echo "instance key: $INSTANCE_KEY"
}

mac_start() {
  require_cmd launchctl
  local domain
  domain="$(launchctl_domain)"
  launchctl kickstart -k "$domain/$RUN_LABEL"
  launchctl kickstart -k "$domain/$HEARTBEAT_LABEL" || true
  launchctl kickstart -k "$domain/$REFLECTION_LABEL" || true
}

mac_stop() {
  require_cmd launchctl
  local domain
  domain="$(launchctl_domain)"
  launchctl bootout "$domain/$RUN_LABEL" 2>/dev/null || true
  launchctl bootout "$domain/$HEARTBEAT_LABEL" 2>/dev/null || true
  launchctl bootout "$domain/$REFLECTION_LABEL" 2>/dev/null || true
}

mac_status() {
  require_cmd launchctl
  local domain
  domain="$(launchctl_domain)"
  launchctl print "$domain/$RUN_LABEL" || true
  launchctl print "$domain/$HEARTBEAT_LABEL" || true
  launchctl print "$domain/$REFLECTION_LABEL" || true
}

mac_uninstall() {
  local dir
  dir="$(mac_launchagents_dir)"
  mac_stop
  rm -f "$dir/$RUN_LABEL.plist" "$dir/$HEARTBEAT_LABEL.plist" "$dir/$REFLECTION_LABEL.plist"
  echo "removed launchd agents from $dir"
}

main() {
  if [[ $# -lt 1 ]]; then
    usage >&2
    exit 1
  fi
  if [[ "$1" == "-h" || "$1" == "--help" ]]; then
    usage
    exit 0
  fi
  local action="$1"
  shift

  while [[ $# -gt 0 ]]; do
    case "$1" in
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
      --heartbeat)
        HEARTBEAT_TIME="${2:-}"
        shift 2
        ;;
      --reflection)
        REFLECTION_TIME="${2:-}"
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

  validate_hhmm "$HEARTBEAT_TIME"
  validate_hhmm "$REFLECTION_TIME"

  if [[ "$INSTANCE_SPECIFIED" -eq 1 && "$INSTANCE_DIR_SPECIFIED" -eq 1 ]]; then
    echo "--instance and --instance-dir are mutually exclusive" >&2
    exit 1
  fi

  if [[ "$INSTANCE_SPECIFIED" -eq 1 ]]; then
    validate_instance_name "$INSTANCE"
  else
    if [[ "$INSTANCE_DIR" != /* ]]; then
      INSTANCE_DIR="$(cd "$ROOT_DIR" && mkdir -p "$INSTANCE_DIR" && cd "$INSTANCE_DIR" && pwd)"
    else
      mkdir -p "$INSTANCE_DIR"
    fi
  fi

  configure_service_names

  case "$(uname -s)" in
    Linux*)
      case "$action" in
        install) linux_install ;;
        start) linux_start ;;
        stop) linux_stop ;;
        status) linux_status ;;
        uninstall) linux_uninstall ;;
        *) usage >&2; exit 1 ;;
      esac
      ;;
    Darwin*)
      case "$action" in
        install) mac_install ;;
        start) mac_start ;;
        stop) mac_stop ;;
        status) mac_status ;;
        uninstall) mac_uninstall ;;
        *) usage >&2; exit 1 ;;
      esac
      ;;
    *)
      echo "unsupported OS: $(uname -s)" >&2
      echo "for Windows use scripts/service.ps1" >&2
      exit 1
      ;;
  esac
}

main "$@"
