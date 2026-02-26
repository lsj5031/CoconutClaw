# CoconutClaw

<img src="logo.jpg" alt="CoconutClaw Logo" width="200">

A personal AI assistant that lives in your Telegram. Fast, cross-platform, and self-hosted.

## Features

- **Single Binary** - No runtime dependencies. Download and run.
- **Cross-Platform** - Windows, Linux, macOS with unified commands.
- **Multi-Instance** - Run multiple isolated assistants (work, personal, etc.)
- **Dual AI Providers** - OpenAI Codex and Pi (with extensible provider system).
- **Smart Telegram** - MarkdownV2 with auto-fallback, progress updates, `/cancel` button.
- **Background Service** - Auto-start on boot with systemd / launchd / Task Scheduler.
- **Optional ASR/TTS** - Voice input/output when you need it.
- **Context Persistence** - SQLite-backed memory across restarts.

## Quick Start

### From Release

```bash
# Download from https://github.com/lsj5031/CoconutClaw/releases
unzip coconutclaw-linux-x86_64.zip
cd CoconutClaw
cp config.toml.example config.toml
# Edit config.toml with your Telegram bot token and chat ID
./scripts/install.sh && ./scripts/start.sh
```

### From Source

```bash
git clone https://github.com/lsj5031/CoconutClaw.git
cd CoconutClaw
make release
cp config.toml.example config.toml
# Edit config.toml
./scripts/install.sh && ./scripts/start.sh
```

## Configuration

Minimal `config.toml`:

```toml
TELEGRAM_BOT_TOKEN = "your-bot-token"
TELEGRAM_CHAT_ID = "your-chat-id"
```

Optional settings:

```toml
# AI provider (codex or pi)
AGENT_PROVIDER = "codex"
CODEX_REASONING_EFFORT = "xhigh"  # low | medium | high | xhigh

# Telegram formatting
TELEGRAM_PARSE_MODE = "MarkdownV2"
TELEGRAM_PARSE_FALLBACK = "plain"

# Optional voice
ASR_URL = "http://localhost:8080/asr"
TTS_CMD_TEMPLATE = "tts-cli --text '{text}' --output {output}"
```

## Service Management

| Action | Linux/macOS | Windows |
|--------|-------------|---------|
| Install | `./scripts/install.sh` | `.\scripts\install.ps1` |
| Start | `./scripts/start.sh` | `.\scripts\start.ps1` |
| Status | `./scripts/status.sh` | `.\scripts\status.ps1` |
| Stop | `./scripts/stop.sh` | `.\scripts\stop.ps1` |
| Uninstall | `./scripts/uninstall.sh` | `.\scripts\uninstall.ps1` |

## Multi-Instance

Run separate assistants for different purposes:

```bash
# Named instance
./scripts/install.sh --instance work
./scripts/start.sh --instance work

# Custom directory
./scripts/install.sh --instance-dir ~/instances/personal
```

Each instance has isolated:
- SQLite database
- Configuration
- Lock file
- Logs

## Runtime Commands

```bash
# Manual run (foreground)
./scripts/run.sh

# One-shot message
./scripts/run.sh --once --inject-text "What's the weather?"

# Health check
./scripts/run.sh --doctor

# Scheduled tasks
./scripts/run.sh --heartbeat
./scripts/run.sh --nightly-reflection
```

## Architecture

```
CoconutClaw/
├── crates/
│   ├── coconutclaw-cli/      # Main CLI entry point
│   │   └── src/
│   │       ├── main.rs       # Core agent loop, Telegram polling
│   │       ├── markers.rs    # Output marker parsing/rendering
│   │       ├── store.rs      # SQLite persistence layer
│   │       └── webhook.rs    # Axum-based webhook server
│   ├── coconutclaw-config/   # Configuration loading & migration
│   └── coconutclaw-provider/ # AI provider abstraction (Codex/Pi)
├── scripts/                  # Cross-platform service & media helpers
└── sql/                      # Schema migrations
```

**~5,500 lines of Rust** across 3 crates with 58 tests.

Key design decisions:
- **Axum web framework** - Clean, async webhook handling with minimal footprint.
- **Enum dispatch** - Provider abstraction without trait overhead.
- **fs2 locking** - Prevent concurrent instance conflicts.
- **Webhook + polling** - Flexible Telegram integration with message dedup.
- **Modular CLI crate** - Separate modules for markers, store, webhook.

## Telegram Features

- **Progress updates** - See live progress as the agent works.
- **`/cancel`** - Interrupt long-running operations.
- **`/fresh`** - Reset context for a clean slate.
- **MarkdownV2** - Rich formatting with automatic fallback.

## Scheduled Jobs

Default schedule (customizable on install):
- **Heartbeat** at 09:00 - Health check and status.
- **Nightly reflection** at 22:30 - Daily summary and insights.

```bash
./scripts/install.sh --heartbeat 10:00 --reflection 23:00
```

## Development

```bash
make dev        # Debug build
make release    # Optimized build
make test       # Run 58 tests
make lint       # Clippy checks
```

## License

MIT

## Credits

Built with Rust, axum, reqwest, rusqlite, and telegram-markdown-v2.
