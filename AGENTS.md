# AGENTS.md — CoconutClaw

## What this is
Rust-powered personal voice agent: Telegram ↔ ASR ↔ provider CLI ↔ TTS.

Primary runtime is `coconutclaw` (`crates/coconutclaw-cli/src/main.rs`).

## Validation

- Rust tests: `cargo test`
- One-shot smoke: `cargo run -p coconutclaw -- once --inject-text "hello"`
- Bash helper lint: `shellcheck agent.sh scripts/_resolve_bin.sh scripts/asr.sh scripts/heartbeat.sh scripts/nightly_reflection.sh scripts/tts.sh`

## Architecture

- `crates/coconutclaw-cli` — main runtime loop, Telegram I/O, context building, marker parsing, storage.
- `crates/coconutclaw-config` — runtime config loading and instance layout.
- `crates/coconutclaw-provider` — provider execution (`codex` / `pi`) and progress extraction.
- `sql/schema.sql` — SQLite schema (`kv`, `turns`, `tasks`).
- Helper scripts kept by design:
  - `scripts/asr.sh`
  - `scripts/tts.sh`
- Compatibility shims:
  - `agent.sh`
  - `scripts/heartbeat.sh`
  - `scripts/nightly_reflection.sh`

## Output contract

Provider output markers:

- `TELEGRAM_REPLY:`
- `VOICE_REPLY:`
- `SEND_PHOTO:`
- `SEND_DOCUMENT:`
- `SEND_VIDEO:`
- `MEMORY_APPEND:`
- `TASK_APPEND:`

Marker lines must stay plain text and prefix format must remain unchanged.
