# CoconutClaw

Telegram polling ↔ Codex, with local history/memory and durable task results.

- `crates/coconutclaw-cli` (package `coconutclaw`): runtime, Telegram, task lanes,
  approvals, delivery outbox, recovery, SQLite.
- `crates/coconutclaw-config`: single data-directory configuration.
- `crates/coconutclaw-provider`: existing Codex exec integration, explicit
  session resume, image input, progress, timeout and process-group cancellation.
- `sql/schema.sql`: additive schema; preserve legacy tables and user records.

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

Do not restore voice, alternate providers/transports, named assistant management
or general scheduling. Historical schema columns are compatibility data, not
supported features. See `docs/migration-phase-0.md`.
