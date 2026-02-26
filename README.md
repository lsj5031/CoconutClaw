# CoconutClaw (Rust Runtime)

CoconutClaw is a Telegram personal agent runtime implemented in Rust.

## SOTA UX Quick Start

### 1) Clone and configure once

```bash
git clone https://github.com/lsj5031/CoconutClaw.git
cd CoconutClaw
```

Windows PowerShell:

```powershell
Copy-Item config.toml.example config.toml
```

Linux/macOS:

```bash
cp config.toml.example config.toml
```

Edit `config.toml` and set at least:

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

### 2) Sanity check

Windows:

```powershell
.\scripts\run.ps1 -Doctor
```

Linux/macOS:

```bash
bash scripts/run.sh --doctor
```

### 3) Install background runtime (one command)

Windows (Task Scheduler):

```powershell
.\scripts\install.ps1
```

Linux/macOS (systemd user / launchd):

```bash
bash scripts/install.sh
```

Start services:

Windows:

```powershell
.\scripts\start.ps1
```

Linux/macOS:

```bash
bash scripts/start.sh
```

Check status:

Windows:

```powershell
.\scripts\status.ps1
```

Linux/macOS:

```bash
bash scripts/status.sh
```

Stop or uninstall:

Windows:

```powershell
.\scripts\stop.ps1
.\scripts\uninstall.ps1
```

Linux/macOS:

```bash
bash scripts/stop.sh
bash scripts/uninstall.sh
```

## Unified Runtime Commands

- `run`: main runtime loop
- `once`: one-shot turn (`--inject-text`, `--inject-file`)
- `doctor`: environment checks
- `heartbeat`: proactive turn
- `nightly-reflection`: append daily reflection markdown block

PowerShell:

```powershell
.\scripts\run.ps1
.\scripts\run.ps1 -Once -InjectText "hello"
.\scripts\run.ps1 -Heartbeat
.\scripts\run.ps1 -NightlyReflection
```

Bash:

```bash
bash scripts/run.sh
bash scripts/run.sh --once --inject-text "hello"
bash scripts/run.sh --heartbeat
bash scripts/run.sh --nightly-reflection
```

## Config

Default config file: `config.toml`.

If a legacy `.env` exists and `config.toml` is missing, CoconutClaw auto-migrates `.env` to `config.toml` on startup and then removes `.env`.

## Telegram Markdown Replies

```toml
TELEGRAM_PARSE_MODE = "off"       # off | MarkdownV2
TELEGRAM_PARSE_FALLBACK = "plain" # plain | none
```

## ASR / TTS

ASR and TTS are optional and default to off.

- Enable ASR by setting `ASR_CMD_TEMPLATE` or `ASR_URL`.
- Enable TTS by setting `TTS_CMD_TEMPLATE`.
- `doctor` reports required dependencies only when these features are enabled.

## Service Schedule Tuning

Service install defaults:

- heartbeat: `09:00`
- nightly reflection: `22:30`

Override on install:

Windows:

```powershell
.\scripts\install.ps1 -HeartbeatTime 10:00 -ReflectionTime 23:00
```

Linux/macOS:

```bash
bash scripts/install.sh --heartbeat 10:00 --reflection 23:00
```

## Legacy Compatibility Shims

- `./agent.sh`
- `./scripts/heartbeat.sh`
- `./scripts/nightly_reflection.sh`

These wrappers are compatibility-only and forward to the unified runtime scripts.
