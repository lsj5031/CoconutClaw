# CoconutClaw

A local Telegram agent using Codex for conversation and delegated tasks.

## Run

Build with `cargo build --release -p coconutclaw`. Rust and a C compiler are
required (SQLite is bundled). Codex must already be installed and authenticated
locally. CoconutClaw does not install tools or manage credentials.

Copy `config.toml.example` into your data directory as `config.toml`, and set
the Telegram bot token and allowed chat IDs there. Do not commit that file.

```sh
coconutclaw --data-dir /absolute/path/to/data doctor --json
coconutclaw --data-dir /absolute/path/to/data run
```

The default data directory remains the old default assistant directory:
`$XDG_STATE_HOME/coconutclaw/default`, or
`~/.local/state/coconutclaw/default` on Unix. Windows uses
`%LOCALAPPDATA%/CoconutClaw/default`. `--data-dir` now selects the actual
directory containing `config.toml` and `state.db`, not its parent.
`COCONUTCLAW_DATA_DIR` has the same meaning.

`once --inject-text "hello"` and `once --inject-file /path/to/file` invoke Codex
and print markers without sending Telegram messages. They are **not dry runs**.
Use a mock `CODEX_BIN` when testing offline. `run --inject-text` is the same
one-shot mode. `doctor` checks local configuration and executable availability,
not authentication or connectivity.

## Behavior

- Telegram polling only, restricted to configured chat IDs.
- One ordered conversation lane and explicit Codex session per chat.
- `/fresh` starts a new context/session without deleting history.
- Images are attached through Codex's image input; documents and videos are
  available as local file paths. No local vision engine or audio transcription.
- `SOUL.md`, `TASKS/pending.md` and SQLite history provide local context.
  Chat-scoped SQLite memory has compact curated context and bounded FTS5
  search/source reads. `MEMORY_APPEND` proposes a candidate, not authority.
  Use `/memory remember <text>`, `list [before-id]`, `search <text>`, `get <id>`,
  `promote <id>`, `correct <active-id> <text>` or `undo`.
  See [local memory](docs/memory.md) for budgets, scope and limitations.
- A `DELEGATE_TASK` request creates a durable task ID with a separate Codex
  session and no parent conversation history. Its labeled result is delivered
  to, and retained in the history of, the originating chat.
- `/tasks`, `/cancel [task-id]`, `/approve <approval-id>` and
  `/reject <approval-id>` are scoped to the originating chat.
- `SEND_APPROVAL` pauses the request until an explicit approval. Approval is
  advisory model coordination, not a sandbox or substitute for Codex's own
  execution policy. The default `EXEC_POLICY = "safe"` does not bypass Codex
  approvals/sandbox; `"yolo"` explicitly enables the dangerous bypass.

Final results and per-operation delivery acknowledgments live outside Codex
context. Failed delivery retries the saved output, never re-runs Codex.
After a crash, a saved final turn can be finalized without another agent call;
started attempts without a final result are marked outcome-unknown and are
not automatically retried. Pending approvals survive restart.
Provider progress is persisted as a coalesced, bounded latest status even when
the Telegram placeholder fails. Managed Markdown projections use a single writer,
synced staging files and atomic replacement, preserving surrounding manual bytes.

Telegram has no transactional send API. A crash after a successful send but
before its local acknowledgment can duplicate a message or attachment.
Cancellation stops the local process group; it cannot undo actions already
performed. Codex subprocess reconciliation across a host crash is not claimed.
Handled SIGINT/SIGTERM stops intake and queue advancement, cancels active groups,
and joins execution workers with a ten-second shutdown deadline. HTTP work is
interruptible without transferring SQLite or projection ownership to HTTP threads.
If the deadline is exhausted, the process reports failure and unfinished started
attempts are reconciled as outcome-unknown on restart. Queued jobs remain saved.

## Service

`service install|start|stop|status|uninstall` manages only the single runtime
service, using systemd user services on Linux or launchd on macOS. These commands
change local service state only when explicitly invoked. They do not manage old
timers or named-instance services.

## Configuration and migration

See [Phase 0 migration](docs/migration-phase-0.md) **before using existing data**.
Removed settings, including empty/off settings, produce actionable errors.
Environment values override retained config keys. Unknown TOML keys are errors.
`config.toml.example` lists the complete supported configuration.
Supplied types, numeric domains, timezone/URL values and state path conflicts are
validated before layout creation. Diagnostics redact values from their full error
chains. Raw and `telegram:`-prefixed historical chat IDs share history and `/fresh`
boundaries without rewriting historical rows.

## Development

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --all-targets --locked
```

Tests use local fake Telegram endpoints and fake Codex executables. No live
messaging or agent calls are required. See [architecture](docs/architecture.md).
The [Phase 0 validation record](docs/validation-phase-0.md) lists checks and limitations.
Historical release notes and `docs/superpowers/` describe earlier versions,
not the supported runtime.
