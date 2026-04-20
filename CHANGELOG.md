# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
