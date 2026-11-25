# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added
- Button-first Telegram Home, scoped immutable operations snapshots, beside-run
  controls, and durable explicit-code memory/preference input using local handlers.
- Explicit local memory import, durable one-off followups and recurring schedules,
  and paired native execution nodes for ordinary Telegram tasks.
- Active, owner-approved facts from an exact Telegram topic can be included as
  bounded data in the signed text request to its assigned runner. Memory remains
  coordinator-owned; it is not a shared index or a replicated database.
- Desktop companion ranks saved runner, run and schedule state by tone, filters
  each page to the rows that need attention, states how old the open export is,
  lists full public runner IDs, and holds its colours and motion classes in one
  token file with an unwired dark set.

### Changed
- Retained runtime is Telegram polling and Codex with a single data directory.
  Earlier release entries describe historical features, not current support.

### Fixed
- Reject quoted TOML keys with trailing whitespace as unknown settings instead
  of panicking, without exposing configuration values in errors.
- Reconcile workspace package metadata with the existing v0.4.1 release tag and
  changelog. That release retained 0.3.1 in Cargo metadata; this is not a new release.
- Document all seven current CLI commands in the architecture overview.
- Desktop companion no longer advises reopening the companion after an
  unacknowledged local runner request; reopening drops control of a runner the
  window started.

### Verification
- The desktop companion changes in this pass await `nsh check desktop`,
  `nsh test desktop` and the native GTK, macOS and Windows gates in
  [docs/desktop/validation.md](docs/desktop/validation.md). No native receipt is
  claimed for them.
- [19 November 2026 fleet record](docs/fleet-verification.md) distinguishes the
  live scoped-memory checks and exact hosted CI on `18c460c` from the earlier
  live routing, scheduling, recovery and cancellation checks on `746c3b8`.

## [0.4.1] - 2026-05-21

### Fixed
- Scheduled task progress updater now uses the delivery target chat ID instead of the
  session ID, eliminating "chat not found" errors when editing progress messages for
  freshly-recovered scheduled tasks.
- OpenCode provider DB contention: runtime now sets `OPENCODE_DB` to a per-instance path
  (`{instance_dir}/opencode.db`) so coconutclaw instances no longer conflict with
  opencode TUI sessions sharing the default SQLite database.

## [0.4.0] - 2026-04-21

### Added
- Slack-only runtime startup support.
- Parallel task scheduling for Slack and Telegram turns.
- Antigravity provider (migrated from Gemini).

### Changed
- Reshaped runtime modules for clearer separation of concerns.
- Instance files now only seeded from the actual CoconutClaw project root.
- Local tool state and captured artifacts excluded from repo tracking.
- Instance-scoped service names now aligned with loaded config.
- Bumped actions/checkout to v6 for Node.js 24 compatibility.

### Fixed
- macOS launchd service environment handling.
- Deterministic webhook end-to-end test logging.
- Windows release lane assertion instead of path patching.
- Windows broken-db cleanup retry until handle release.
- Scheduler cleanup ordering for Windows test stability.
- Scheduled task routing preserved across recovery and upgrades.
- Windows webhook e2e startup stabilization.
- Scheduler task recovery and context persistence improvements.
- Scheduler resume-payload cleanup tests made cross-platform.
- Cross-session task leaks and dropped approval attachments prevented.
- Parallel scheduler tests now use race-free cross-platform markers.
- Parallel scheduler tests now stable on slower Windows CI.

## [0.3.1] - 2026-04-21

### Added
- Runtime confirmation messages when schedules are saved, duplicated, invalid, or disabled.
- `/schedules` Telegram command for listing active scheduled tasks and their last-run state.

### Fixed
- Antigravity provider fallback now prefers stdout over the YOLO stderr banner for plain-text replies.
- Scheduled task inspection and schedule changes are easier to verify from the live bot.

## [0.3.0] - 2026-04-20

### Added
- Implement agent-driven scheduled tasks.
- Improve Slack integration: Socket Mode, Block Kit, Thread Context.
- Support for opencode provider text/reasoning events.
- OPENCODE_SKIP_PERMISSIONS config and --dangerously-skip-permissions flag.

### Fixed
- Slack webhook output routing after ack.
- Context filtering after boundary turns.
- Telegram retry-after integer/string parsing.
