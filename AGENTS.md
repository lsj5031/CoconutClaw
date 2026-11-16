# CoconutClaw

Telegram polling ↔ Codex, with local history/memory and durable task results.

- `crates/coconutclaw-cli` (package `coconutclaw`): runtime, Telegram, task lanes,
  approvals, delivery outbox, recovery, SQLite.
- `crates/coconutclaw-config`: single data-directory configuration.
- `crates/coconutclaw-provider`: existing Codex exec integration, explicit
  session resume, image input, progress, timeout and process-group cancellation.
- `sql/schema.sql`, `sql/memory.sql`: SQLite runtime and scoped local memory.
  No legacy memory imports/projections. Never delete real external user data.

Validate with `cargo fmt --all -- --check`,
`cargo clippy --workspace --all-targets --locked -- -D warnings`, and
`cargo test --workspace --all-targets --locked`.
Do not run injected smoke commands with live Codex or Telegram credentials.
The tests supply local mocks.

Supported plain-text marker prefixes:

- `TELEGRAM_REPLY:`
- `SEND_PHOTO:`
- `SEND_DOCUMENT:`
- `SEND_VIDEO:`
- `MEMORY_APPEND:`
- `TASK_APPEND:`
- `SEND_APPROVAL:`
- `DELEGATE_TASK:`
- `WAIT_FOR_TASK:`

Do not restore voice, alternate providers/transports or named assistant management.
Owner's 16 November always-on amendment authorizes narrow durable inbox-backed
one-off followups and recurring schedules in the existing Rust/SQLite runtime.
Use explicit timezone/DST/missed-run policies and preserve Unknown without
automatic effect retries. Historical schedule rows remain inactive, never
implicitly imported. No actual user jobs without an explicit task/cadence.
Owner steering dropped legacy memory support after Phase 0; see
`docs/memory.md`. Phase 0's historical closeout is in `docs/validation-phase-0.md`.
