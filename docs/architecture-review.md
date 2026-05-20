# CoconutClaw Architecture Review

*Last updated: 2026-05-19 (rev 3) — runtime `coconutclaw` v0.3.1 (post in-flight refactor).*

This document records two things:

1. A targeted analysis of whether adopting the **Codex `app-server`** (the JSON‑RPC long‑lived mode of the `codex` CLI) would benefit CoconutClaw.
2. A broader **architecture‑improvement backlog**, with status updated to reflect the refactor currently in the working tree (new `commands/`, `loops/`, `recovery/`, `cancel/`, `scheduling/`, `store/` modules; `types.rs`, `util.rs`; provider trait; marker `Effect` enum; new `DeliveryTarget` helper methods; `notify`-driven `CancelRouter`; in-memory webhook channel replacing the file queue).

It is meant as a living engineering note, not a roadmap commitment.

---

## Refactor status snapshot (working tree, uncommitted)

Diff stats vs `main` (`7c696ec`):

```text
crates/coconutclaw-cli/src/main.rs            -3366 lines  (5616 → 2514; prod ~3169 → ~130)
crates/coconutclaw-cli/src/store.rs           DELETED      (1851 lines; split into store/*.rs)
crates/coconutclaw-provider/src/lib.rs        ~rewritten   (trait + shared driver)
crates/coconutclaw-cli/src/markers.rs         +168 → +195  (Effect enum: +ReplyDelta variant; tests)
crates/coconutclaw-cli/src/context.rs         refactored   (consumes &[Effect]; adds sync_managed_context_files)
crates/coconutclaw-cli/src/delivery/mod.rs    +105         (DeliveryTarget::display_id / ::send_placeholder, dispatch_immediate_output)
crates/coconutclaw-cli/src/scheduler.rs       -159         (immediate-vs-scheduled ladder collapsed via dispatch_immediate_output)
crates/coconutclaw-cli/src/turn.rs            simplified   (display_id(), to_effects(), CancelRouter; per-turn watcher thread removed)
crates/coconutclaw-cli/src/webhook.rs        -218 lines   (file-backed queue + fs2 locks removed; replaced with mpsc::Sender<String>)
crates/coconutclaw-cli/Cargo.toml             dep swap     (-fs2  +notify = "8.2.0")
new dirs:  commands/  loops/  recovery/  cancel/  scheduling/  store/
new files: types.rs   util.rs
```

In short: backlog items **1, 2, 3, 4, 5, 6, 7, 8, 10** are landed; **9** is partially done (`Effect::ReplyDelta` variant added, not yet wired to a provider token stream).

---

## Part 1 — Should we adopt `codex app-server`?

### What `codex app-server` offers

- A long‑lived JSON‑RPC process with `create_session` / `send_user_message` RPCs.
- A stable structured event stream (tokens, tool calls, exec start/end) — less
  brittle than scraping the `--json` lines emitted by `codex exec`, which have
  already broken on us (e.g. `--reasoning-effort` removed in v0.118.0, the
  YOLO retry‑without‑`--dangerously-bypass-approvals-and-sandbox` dance).
- Native conversation persistence via Codex rollout files.
- Inline approval round‑trips instead of an all‑or‑nothing dangerous flag.
- First‑class attachments and streaming reply tokens (would enable
  incremental Telegram message edits).

### Current state of the codebase

- Provider abstraction is now a `trait ProviderRunner` with one shared driver
  `run_provider_impl<P>` in `crates/coconutclaw-provider/src/lib.rs`. Six
  runner structs (`CodexRunner`, `PiRunner`, `ClaudeRunner`, `OpenCodeRunner`,
  `GeminiRunner`, `FactoryRunner`) each implement `build_cmd`,
  `progress_parser`, `extract_final`, `prefer_stdout`, `has_dangerous_flag`.
  Public entry point still returns `ProviderOutput { raw_output, success,
  exit_code }`.
- The Codex runner calls `codex exec --output-last-message …` per turn,
  optionally with `--json` for progress, and a YOLO retry‑without‑dangerous‑
  flag fallback that now lives in the shared driver, not in each runner.
- Progress is parsed line‑by‑line in `parse_codex_progress_line`.
- The turn lifecycle (`process_turn_with_status` in `turn.rs`) rebuilds the
  full prompt from instance files every turn via `build_context`
  (SOUL.md + USER.md + MEMORY.md + TASKS/pending.md + recent turns).
- The reply contract is plain‑text markers (`TELEGRAM_REPLY:`, `VOICE_REPLY:`,
  `SEND_PHOTO:`, `MEMORY_APPEND:`, `TASK_APPEND:`, …) parsed by `markers.rs`
  and now compiled into a typed `Vec<Effect>` (`Effect::TelegramReply`,
  `Effect::SendPhoto`, `Effect::MemoryAppend`, …) via `ParsedMarkers::to_effects`.
- Cancellation is `SIGTERM` to the child process group + a `notify`-based
  long-lived `CancelRouter` (`crates/coconutclaw-cli/src/cancel/`) that
  watches `runtime_dir/cancel` and flips registered `Arc<AtomicBool>` flags
  when the file appears.

### Cost analysis specific to CoconutClaw

1. **Breaks the new provider abstraction.** Every other `ProviderRunner` is
   one‑shot subprocess in / text out. `app-server` is a stateful RPC
   channel — it would either need its own parallel path or force a widening
   of the trait (`fn step(&mut self, …) -> Vec<Event>`?) that only one
   provider uses.
2. **Double bookkeeping for conversation state.** Context is already built
   deterministically from `SOUL.md` / `USER.md` / `MEMORY.md` /
   `TASKS/pending.md` / recent `turns` rows. If Codex also maintains a
   session, you either ignore Codex's memory (losing the main upside) or
   ship a half‑Codex/half‑CoconutClaw history that diverges.
3. **The reply contract stays text markers.** Even with structured events
   you still need `markers.rs` to interpret `TELEGRAM_REPLY:` etc., so the
   marker parser, `Effect` enum, and `MEMORY_APPEND` / `TASK_APPEND` flow
   do not simplify.
4. **New operational surface.** Lifecycle, restart, health, backpressure,
   deadlock‑on‑RPC, leaked sessions across instance directories — all
   problems the current "spawn + wait + reap" model avoids.
5. **API churn risk.** The app‑server schema is still moving; today's wins
   on schema stability could flip versus the well‑known `exec` mode.
6. **Process‑spawn cost is not the bottleneck.** Per‑turn wall time is
   dominated by model latency, not `codex` startup.

### Where it *would* pay off

- True streaming replies (live‑edit a Telegram message as tokens arrive) —
  currently only coarse progress strings are exposed. Now that `Effect`
  exists, adding `Effect::ReplyDelta(String)` would be the natural carrier.
- Explicit approval UX (Slack/Telegram "approve this tool call?") — the
  RPC model is built for that, while the CLI forces YOLO‑or‑nothing.
- If Codex CLI schema breakage continues to cause maintenance pain.

### Recommendation

**Not beneficial now.** The `ProviderRunner` trait + shared driver path is
small, uniform across six providers, and the real conversation memory lives
in CoconutClaw's instance files — which neutralises Codex's main app‑server
upside (persistent sessions).

If we do adopt it later, do it as an additive `CodexAppServer` variant
alongside `Codex`, behind the same trait (or a sibling trait with a
streaming method), so the other five providers stay untouched. Trigger
that work only when one of these becomes a goal: live token streaming to
Telegram, structured approvals, or per‑release CLI parsing breakage we no
longer want to chase.

---

## Part 2 — Architecture improvement backlog

Status legend: **[DONE]** landed in working tree • **[PARTIAL]** started • **[OPEN]** not started.

---

### 1. `main.rs` is a god module — **[DONE]**

**Before:** `main.rs` carried ~3.2k lines of production code across 44
functions: CLI parsing, command dispatch, the polling loop, the webhook
loop, Slack socket‑mode handling, scheduled‑task orchestration,
in‑flight recovery, per‑platform command handlers.

**After:** `main.rs` is now ~129 lines of prod code — just clap definitions
and a top‑level `match` on `Commands`. Everything else moved to:

- `commands/` — `once.rs`, `run.rs`, `heartbeat.rs`, `nightly_reflection.rs`, `doctor.rs`, `helpers.rs`
- `loops/` — `poll.rs`, `webhook.rs`, `slack_socket.rs`
- `recovery/` — `pending.rs` (`reconcile_pending_turn_side_effects`, `recover_scheduled_task_output_from_task_run`)
- `cancel/` — `cancel_impl.rs` (marker file paths, watcher, signal helpers)
- `scheduling/` — `mod.rs` (`run_due_scheduled_tasks`, `scheduled_task_context_channel`)
- `types.rs` — shared enums and structs that used to live in `main.rs`
- `util.rs` — small helpers (`iso_now`, `command_exists`, `resolve_instance_path`, etc.)

**Follow‑up:** `loops/webhook.rs` is 983 lines and still does a lot of
parsing + dispatch glue. Worth a second pass once the `Transport` trait
(item 8) exists.

---

### 2. Provider runners were five copies of the same shape — **[DONE]**

**Before:** `run_codex` / `run_claude` / `run_opencode` / `run_gemini` /
`run_factory` / `run_pi` all duplicated "build cmd → optional YOLO retry →
`run_provider_process` → `extract_json_or_fallback` → `finalize_output`".

**After:** `crates/coconutclaw-provider/src/lib.rs` now defines:

```rust
pub struct ProviderCtx<'a> { /* config, attachment, context, cancel_flag, progress_tx, timeout */ }

trait ProviderRunner {
    fn build_cmd(&self, ctx: &ProviderCtx, include_dangerous: bool) -> io::Result<Command>;
    fn progress_parser(&self) -> Option<fn(&str) -> Option<String>> { None }
    fn extract_final(&self, run: &RunResult, ctx: &ProviderCtx) -> Option<String> { None }
    fn prefer_stdout(&self) -> bool { false }
    fn has_dangerous_flag(&self) -> bool { false }
}

fn run_provider_impl<P: ProviderRunner>(runner: P, ctx: ProviderCtx) -> Result<ProviderOutput> { … }
```

Six runner structs implement the trait; the YOLO retry, timeout, child
process management, and output finalisation all live once in the shared
driver. Adding a new provider is now ~30 lines.

---

### 3. Output contract as text markers — **[DONE]**

**Before:** `markers.rs` parsed `TELEGRAM_REPLY:` etc. into a `ParsedMarkers`
struct of `Vec<String>` fields; consumers everywhere read those fields.

**After:** `markers.rs` now defines:

```rust
pub(crate) enum Effect {
    TelegramReply(String),
    VoiceReply(String),
    SendPhoto(String),
    SendDocument(String),
    SendVideo(String),
    SendApproval(String),
    MemoryAppend(String),
    TaskAppend(String),
    SchedulePrompt(String),
}
```

with `ParsedMarkers::to_effects()` and `render_effects(&[Effect])`.

`ParsedMarkers` is now a **parser-internal type** — its only remaining
public uses are inside `markers.rs` itself. All downstream consumers
(`turn.rs::process_turn_with_status`, `loops/webhook.rs`,
`context.rs::append_memory_and_tasks`, `commands/helpers.rs`) take
`&[Effect]`. `render_output(reply, voice_reply, &[Effect])` is the new
signature.

**Open follow-up:** add `Effect::ReplyDelta(String)` once a transport can
render incremental tokens (paves the way for streaming — see item 9).

---

### 4. `Store` god object — **[DONE]**

**Before:** `crates/coconutclaw-cli/src/store.rs` was 1.6k lines wrapping
`kv`, `turns`, `scheduled_tasks`, `task_runs`, and `approvals` in one
struct with ~50 methods.

**After:** the file is deleted. Replaced by:

- `store/mod.rs` — `Store` struct + shared `TurnRecord` / `ScheduledTask` /
  `StoredTurnOutput` / `TaskRun` / `ApprovalRecord` types
- `store/kv.rs` — key/value helpers
- `store/turns.rs` — turn insert / lookup / boundary queries
- `store/scheduled_tasks.rs` — schedule lifecycle
- `store/task_runs.rs` — task‑run state machine
- `store/approvals.rs` — approval queue
- `store/migrations.rs` — schema migrations

Same connection, same SQLite file; just navigable now.

---

### 5. Two concurrency models in one binary — **[DONE]**

**Before:** the webhook HTTP server (tokio + axum) wrote payloads to a
`webhook_updates.jsonl` file guarded by `fs2` advisory locks; the main
blocking loop polled the file via `ensure_webhook_queue_file` /
`with_webhook_lock` / `peek_webhook_queue_line` / `ack_webhook_queue_line`.

**After (phase 1):** the file queue + `fs2` dep are gone. The webhook HTTP server
now takes a `tokio::sync::mpsc::UnboundedSender<String>` and hands payloads
straight into the main loop via `loops/webhook.rs`. The shared types `AckStatus`,
`ensure_webhook_queue_file`, `with_webhook_lock` etc. have been deleted.

```text
[axum POST /webhook]  ──UnboundedSender<String>──▶  [main loop drains via Receiver]
```

**After (phase 2):** the Slack socket-mode channel (`SlackWebhookTurn`) has
been converted from `std::sync::mpsc` to `tokio::sync::mpsc` (unbounded).
All three loop functions (`run_webhook_loop`, `run_poll_loop`,
`drain_slack_socket_turns`) now accept `&mut UnboundedReceiver<T>`.
The webhook/Slack queue plumbing no longer uses `std::sync::mpsc`; remaining
`std::sync::mpsc` usage in production is limited to blocking progress updater
threads (`turn.rs`, `telegram.rs`, `slack.rs`) and the provider progress channel
in `coconutclaw-provider`, with tests using it for local capture helpers.

**Follow-up (deferred):** the main loop is still blocking. If we ever want
a fully async runtime (for streaming replies, async DB, multiple inflight
turns), converting the main loop to async remains — but the file-queue
cross-runtime bridge and the Slack channel type mismatch that motivated
this item are eliminated.

### 6. Cancellation runs two parallel mechanisms — **[DONE]**

**Before:** every turn spawned its own `spawn_cancel_marker_watcher` thread
that polled `runtime_dir/cancel` every 150 ms.

**After:** `crates/coconutclaw-cli/src/cancel/cancel_impl.rs` defines a
single long-lived `CancelRouter` that uses the `notify` crate
(inotify on Linux, FSEvents on macOS, ReadDirectoryChangesW on Windows)
to watch the marker file. Per-turn code obtains an `Arc<AtomicBool>` from
the router; the router holds them as `Weak<AtomicBool>` and flips every
registered flag when a `Create`/`Modify` event fires on the marker path.

```rust
pub(crate) struct CancelRouter {
    registry: Arc<Mutex<Vec<Weak<AtomicBool>>>>,
    _watcher: RecommendedWatcher,
    marker_path: PathBuf,
}
```

The marker file is still the external cancel signal (other processes can
`touch` it), but no more polling threads are spawned per turn.

---

### 7. Conversation memory is duplicated between SQLite and Markdown — **[DONE]**

`MEMORY.md` and `TASKS/pending.md` are both user-editable *and* written by
the agent via `MEMORY_APPEND` / `TASK_APPEND`. The
`COCONUTCLAW:MANAGED:*` markers keep human and agent regions separate.

The flow is now:

1. **Write path:** `append_memory_and_tasks` → `insert_memory_and_tasks` (SQLite) → `sync_managed_context_files` (SQLite → MD).
2. **Read path:** `build_context` reads `MEMORY.md` and `TASKS/pending.md` directly via `read_or_default` — MD is the source of truth for context building.
3. **Bridge:** `managed_memory_entries_from_db()` / `managed_pending_task_entries_from_db()` read from SQLite during the sync step so newly-inserted entries flow through to MD correctly.

No drift: writes go SQLite → MD in one atomic flow; reads always come from MD.

---

### 8. Transport branching repeated across the codebase — **[DONE]**

The big Telegram‑vs‑Slack‑vs‑Stdout `match` ladders have been collapsed
into `DeliveryTarget` methods:

- `DeliveryTarget::send_placeholder()` — builds client + sends progress/placeholder message (~40 lines collapsed into one method in `delivery/mod.rs`)
- `dispatch_immediate_output()` — routes turn output to the right transport (~15 lines)
- `dispatch_scheduled_task_output()` — idempotent scheduled delivery with retry tracking (~60 lines)
- `DeliveryTarget::display_id()` — accessor returning chat_id / channel_id / "local" (~5 lines)
- `DeliveryTarget::transport_name()` — accessor returning "telegram" / "slack" / "local" (pre-existing)

The remaining small match blocks in `turn.rs`, `scheduling/mod.rs`, and
`scheduler.rs` now use these accessors instead of inline matches.

A full `Transport` trait (with `download_attachment`, `send_text`,
`send_photo`, `send_progress`, `parse_inbound_event`) is not yet needed —
the `DeliveryTarget` enum + methods cover the current surface area cleanly.
It can be introduced when a 4th transport channel arrives.

---

### 9. Progress signalling is coarse and per‑turn — **[PARTIAL]**

`parse_codex_progress_line` (and its siblings) still emit short status
strings ("Processing…", "▶ git status") and periodic Telegram/Slack edits.
No token‑streamed reply.

**Done:** `Effect::ReplyDelta(String)` variant added to `markers.rs` with
full support in `label()`, `payload()`, `render_effects()`, and
`render_output()` skip-list. Transports already have `_ => {}` catch-alls
so no breakage. Paves the way for live token streaming once a provider
can emit structured token events.

**Open:** wire a provider token stream (Codex `app-server` or similar)
to populate `ReplyDelta` effects during turn execution.

---

### 10. Tests as huge bottom‑of‑file `mod tests` blocks — **[DONE]**

All inline `mod tests { ... }` blocks in `crates/coconutclaw-cli/src/`
have been extracted into separate `_tests.rs` files using the
`#[cfg(test)] #[path = "..._tests.rs"] mod tests;` pattern:

- `session.rs` → `session_tests.rs`
- `webhook.rs` → `webhook_tests.rs`
- `delivery/mod.rs` → `delivery/mod_tests.rs`
- `slack.rs` → `slack_tests.rs`
- `telegram.rs` → `telegram_tests.rs`
- `service.rs` → `service_tests.rs`

Config tests (`crates/coconutclaw-config/src/lib.rs`) and provider tests
(`crates/coconutclaw-provider/src/lib.rs`) were already extracted in
earlier passes.

**Follow‑up (DONE):** Integration‑flavoured tests kept in `src/` but
named with `_integration_test.rs` suffix for clear separation:

- `scheduler_tests.rs` → `scheduler_integration_test.rs`
- `main_tests.rs` → `main_integration_test.rs`
- `context_tests.rs` split: unit tests stay, Store‑backed integration
tests extracted to `context_integration_test.rs`

Integration tests cannot live in `tests/` (binary crate, no `lib.rs`),
so the naming convention keeps the distinction visible while
preserving access to `pub(crate)` internals.

---

## Updated top‑three priority

With items **1–8 and 10** landed and **9** partially done (`Effect::ReplyDelta`
variant added), the remaining backlog is:

1. **Wire a provider token stream** (item 9 open follow-up) — adopt Codex
   `app-server` or extract token events from an existing provider to
   populate `Effect::ReplyDelta` during turns.
2. **Convert the main loop to async** (item 5 deferred follow-up) — run the
   main loop on the tokio runtime for streaming replies, async DB, or
   multiple inflight turns. Low‑urgency.
