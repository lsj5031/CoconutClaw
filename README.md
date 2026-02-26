# CoconutClaw (Rust Runtime)

CoconutClaw is a Telegram personal agent runtime in Rust.  
It supports Windows, Linux, and macOS with the same service lifecycle:

- `install`
- `start`
- `status`
- `stop`
- `uninstall`

## What You Get

- Single binary runtime (`coconutclaw`)
- Unified scripts for all platforms
- Background service support (Windows Task Scheduler / Linux user `systemd` / macOS user `launchd`)
- Optional ASR / TTS integration
- Multi-instance service support (`instance` or `instance-dir`)

## Quick Start (From Release Build)

### 1) Download and unzip the latest release

Get the archive for your platform from:
`https://github.com/lsj5031/CoconutClaw/releases`

### 2) Create `config.toml`

Copy `config.toml.example` to `config.toml`, then set at least:

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

Windows PowerShell:

```powershell
Copy-Item config.toml.example config.toml
```

Linux/macOS:

```bash
cp config.toml.example config.toml
```

### 3) Run environment check

Windows:

```powershell
.\scripts\run.ps1 -Doctor
```

Linux/macOS:

```bash
bash scripts/run.sh --doctor
```

### 4) Install and start background service

Windows:

```powershell
.\scripts\install.ps1
.\scripts\start.ps1
.\scripts\status.ps1
```

Linux/macOS:

```bash
bash scripts/install.sh
bash scripts/start.sh
bash scripts/status.sh
```

## Quick Start (From Source)

```bash
git clone https://github.com/lsj5031/CoconutClaw.git
cd CoconutClaw
```

Then follow the same steps above.

## Day-to-Day Commands

Windows:

```powershell
.\scripts\start.ps1
.\scripts\status.ps1
.\scripts\stop.ps1
.\scripts\uninstall.ps1
```

Linux/macOS:

```bash
bash scripts/start.sh
bash scripts/status.sh
bash scripts/stop.sh
bash scripts/uninstall.sh
```

## Runtime Commands (Manual Mode)

Main runtime:

Windows:

```powershell
.\scripts\run.ps1
```

Linux/macOS:

```bash
bash scripts/run.sh
```

One-shot turn:

Windows:

```powershell
.\scripts\run.ps1 -Once -InjectText "hello"
```

Linux/macOS:

```bash
bash scripts/run.sh --once --inject-text "hello"
```

Heartbeat / nightly reflection:

Windows:

```powershell
.\scripts\run.ps1 -Heartbeat
.\scripts\run.ps1 -NightlyReflection
```

Linux/macOS:

```bash
bash scripts/run.sh --heartbeat
bash scripts/run.sh --nightly-reflection
```

## Multi-Instance Services

Use one of:

- Named instance (`--instance` / `-Instance`)
- Explicit instance directory (`--instance-dir` / `-InstanceDir`)

Do not use both together.

Named instance example:

Windows:

```powershell
.\scripts\install.ps1 -Instance work
.\scripts\start.ps1 -Instance work
.\scripts\status.ps1 -Instance work
```

Linux/macOS:

```bash
bash scripts/install.sh --instance work
bash scripts/start.sh --instance work
bash scripts/status.sh --instance work
```

Instance directory example:

Windows:

```powershell
.\scripts\install.ps1 -InstanceDir .\instances\work
```

Linux/macOS:

```bash
bash scripts/install.sh --instance-dir ./instances/work
```

## Schedule Tuning

Default install schedule:

- heartbeat: `09:00`
- nightly reflection: `22:30`

Override during install:

Windows:

```powershell
.\scripts\install.ps1 -HeartbeatTime 10:00 -ReflectionTime 23:00
```

Linux/macOS:

```bash
bash scripts/install.sh --heartbeat 10:00 --reflection 23:00
```

## Configuration Notes

- Main config file is `config.toml`.
- If `.env` exists and `config.toml` is missing, CoconutClaw migrates `.env` to `config.toml` on startup and removes `.env`.

Telegram parse mode:

```toml
TELEGRAM_PARSE_MODE = "off"       # off | MarkdownV2
TELEGRAM_PARSE_FALLBACK = "plain" # plain | none
```

Optional ASR / TTS:

- Enable ASR via `ASR_CMD_TEMPLATE` or `ASR_URL`.
- Enable TTS via `TTS_CMD_TEMPLATE`.
- `doctor` checks ASR/TTS dependencies only when enabled.

## Legacy Compatibility Wrappers

- `./agent.sh`
- `./scripts/heartbeat.sh`
- `./scripts/nightly_reflection.sh`

These wrappers forward to the unified runtime scripts.
