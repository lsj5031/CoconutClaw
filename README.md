# CoconutClaw (Rust Runtime)

CoconutClaw is a Telegram personal agent runtime implemented in Rust.

## Quick start

```bash
git clone https://github.com/lsj5031/CoconutClaw.git
cd CoconutClaw

cp .env.example .env
# Edit .env and set TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID

cargo build -p coconutclaw
cargo run -p coconutclaw -- doctor
```

Run once:

```bash
cargo run -p coconutclaw -- once --inject-text "hello"
```

Run poll loop:

```bash
cargo run -p coconutclaw -- run
```

## Commands

- `coconutclaw run` - main runtime loop
- `coconutclaw once --inject-text "..."` - one-shot turn
- `coconutclaw doctor` - environment checks
- `coconutclaw heartbeat` - proactive daily heartbeat turn
- `coconutclaw nightly-reflection` - append daily reflection markdown block

## Telegram Markdown replies

Configure in `.env`:

```env
TELEGRAM_PARSE_MODE=off        # off | MarkdownV2
TELEGRAM_PARSE_FALLBACK=plain  # plain | none
```

Behavior:

- `off`: send plain text
- `MarkdownV2`: send with Telegram `parse_mode=MarkdownV2`
- `plain` fallback: if Telegram rejects malformed markdown, retry once as plain text
- `none` fallback: do not retry

## ASR / TTS helpers

Rust runtime currently calls:

- `scripts/asr.sh`
- `scripts/tts.sh`

These are lightweight helper scripts and not the old Bash runtime.

## Systemd units

The repository ships Rust-first user units in `systemd/`:

- `coconutclaw.service` -> `coconutclaw run`
- `coconutclaw-heartbeat.service` + timer
- `coconutclaw-nightly-reflection.service` + timer

Install with:

```bash
make install
make start
```

## Legacy compatibility shims

- `./agent.sh`
- `./scripts/heartbeat.sh`
- `./scripts/nightly_reflection.sh`

These wrappers are temporary and forward to Rust CLI commands.
