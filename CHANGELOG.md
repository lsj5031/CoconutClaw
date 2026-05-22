# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
- OpenCode provider DB contention: runtime now sets `OPENCODE_DB` to a per-instance path
  (`{instance_dir}/opencode.db`) so coconutclaw instances no longer conflict with
  opencode TUI sessions sharing the default SQLite database.
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
