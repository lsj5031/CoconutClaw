# CoconutClaw Phase 0 Baseline Checklist

Updated: 2026-02-25

## Goal
Freeze the current Bash behavior before migrating runtime responsibilities to Rust.

## Baseline Scope
- Marker contract: `TELEGRAM_REPLY`, `VOICE_REPLY`, `SEND_PHOTO`, `SEND_DOCUMENT`, `SEND_VIDEO`, `MEMORY_APPEND`, `TASK_APPEND`.
- Provider switch behavior: `AGENT_PROVIDER=codex|pi`.
- Text-injection flow: `--inject-text` and `--inject-file`.
- Memory/task side effects: append format in `MEMORY.md` and `TASKS/pending.md`.
- SQLite persistence for `turns` and `tasks`.

## Reproducible Steps
1. Create an isolated instance directory.
2. Run Bash baseline:
   - `./agent.sh --instance-dir <instance_dir> --inject-text "ping" --chat-id <chat_id>`
3. Run Rust MVP on the same instance:
   - `cargo run -q -p coconutclaw -- --instance-dir <instance_dir> once --inject-text "ping" --chat-id <chat_id>`
4. Compare marker keys using:
   - `./scripts/compare_bash_rust_markers.sh --instance-dir <instance_dir> --text "ping" --chat-id <chat_id>`

## Pass Criteria
- Both runs produce `TELEGRAM_REPLY` as first marker line.
- Marker key set is identical or differences are explicitly documented.
- Memory/task append behavior remains additive and readable.
- SQLite `turns` insertion succeeds for both runs.

## Known Differences (Phase 0 + 1)
- Rust `run` command currently targets text-only local flows (stdin or injected text/file).
- Telegram polling/webhook parity remains planned for Phase 3.
