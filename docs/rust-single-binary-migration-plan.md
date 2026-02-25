# CoconutClaw Rust Single-Binary CLI Migration Plan

Updated: 2026-02-25
Target repo: `/home/leo/code/CoconutClaw`

## 1) Objective
Migrate CoconutClaw from Bash scripts to one high-performance Rust CLI binary that runs on Linux, macOS, and Windows with minimal friction.

## 2) Hard Constraints
- Keep current user-visible behavior stable unless explicitly changed.
- Keep output marker contract unchanged:
  - `TELEGRAM_REPLY:`
  - `VOICE_REPLY:`
  - `SEND_PHOTO:`
  - `SEND_DOCUMENT:`
  - `SEND_VIDEO:`
  - `MEMORY_APPEND:`
  - `TASK_APPEND:`
- Provider switching remains non-breaking (`codex`, `pi`, future providers).
- ASR/TTS/media tools stay optional.
- Text-only mode must work with zero external media dependencies.

## 3) Product Shape
Binary name: `coconutclaw`

Subcommands:
- `coconutclaw run` (main agent loop)
- `coconutclaw once --inject-text "..."`
- `coconutclaw doctor` (capability and env checks)
- `coconutclaw webhook` (optional webhook server)

Config precedence:
1. CLI args
2. Env vars
3. Config file

## 4) Cross-Platform Design
- Runtime: async Rust (`tokio`) for responsive I/O and cancellation.
- Data path:
  - Linux/macOS: `$XDG_STATE_HOME/coconutclaw` fallback to `~/.local/state/coconutclaw`
  - Windows: `%LOCALAPPDATA%\\CoconutClaw`
- SQLite: `rusqlite` with migration scripts embedded and run on startup.
- Process execution:
  - Rust `std::process::Command` wrapper with timeout, cancellation, and structured error mapping.
- Signal/cancel behavior:
  - Unified cancellation token in Rust.
  - Platform-aware child termination strategy for Windows and Unix.

## 5) Optional Capability Model
Capabilities are runtime-detected, not hard-required.

Capability table:
- `core_text`: always on.
- `audio_decode`: on if `ffmpeg` exists.
- `asr`: on if ASR backend config + health check pass.
- `tts`: on if TTS backend config + health check pass.

Rules:
- Missing optional capability must never crash core text loop.
- Voice input with missing audio toolchain returns clear fallback guidance.
- `doctor` must print pass/warn/fail and exact missing dependency.

## 6) Performance Targets (MVP)
- Cold start under 300ms on typical dev machine (text-only path).
- Single turn orchestration overhead under 30ms excluding provider latency.
- No busy loops in polling/webhook workflows.
- Memory stable for long-running mode (24h soak without growth spikes).

## 7) Architecture (Simple and Maintainable)
Rust workspace crates:
- `crates/coconutclaw-core`:
  - loop orchestration
  - marker parsing and routing
  - turn state transitions
- `crates/coconutclaw-config`:
  - env/config parsing
  - defaults and validation
- `crates/coconutclaw-provider`:
  - provider trait
  - codex/pi adapters
- `crates/coconutclaw-telegram`:
  - polling + webhook transport
- `crates/coconutclaw-store`:
  - SQLite repository and migrations
- `crates/coconutclaw-media`:
  - optional ffmpeg conversion and media helpers
- `crates/coconutclaw-cli`:
  - clap command entrypoint

Keep modules small and direct. Avoid deep abstraction until needed.

## 8) Migration Plan (Low Risk)
### Phase 0: Baseline Lock (0.5 day)
- Snapshot current Bash behavior and env contract.
- Freeze marker behavior examples for regression checks.
- Document known edge cases (`/cancel`, missing marker fallback, provider errors).

Exit criteria:
- Baseline checklist exists and is reproducible.

### Phase 1: Rust Text Core (1 day)
- Implement `coconutclaw once` and `coconutclaw run` for text-only flow.
- Day-1 multi-instance isolation: support `--instance` + `--data-dir` (env override) and keep legacy `--instance-dir` compatibility.
- Add per-instance lockfile protection to prevent concurrent writers on the same instance path.
- Implement config loader and SQLite access.
- Implement provider trait and codex adapter first.

Exit criteria:
- Text inject flow works end-to-end with same marker contract.
- Running two processes against one instance path fails fast with a clear lock error.

### Phase 2: Provider Parity (1 day)
- Add `pi` adapter with existing env semantics (`PI_*`).
- Keep `AGENT_PROVIDER` switching behavior.
- Add parity checks for cancellation and error mapping.

Exit criteria:
- `AGENT_PROVIDER=codex|pi` both pass text smoke checks.

### Phase 3: Telegram Transport + Webhook (1 day)
- Implement polling loop and optional webhook subcommand.
- Preserve allowlist and command handling behavior.

Exit criteria:
- Rust loop can replace Bash loop in daily use for text traffic.

### Phase 4: Optional Media Layer (1 day)
- Add runtime capability detection for ffmpeg/ASR/TTS.
- Add degraded behavior and clear fallback messages.

Exit criteria:
- Voice path works when tools exist and degrades cleanly when missing.

### Phase 5: Shadow + Cutover (1 day)
- Run Rust in shadow mode against real traffic samples.
- Compare outputs and failure rates.
- Switch default entrypoint once parity is stable.

Exit criteria:
- Stable soak window complete, rollback path validated.

## 9) Rollback Plan
- Keep Bash entrypoint available during migration window.
- One-step rollback by switching service command back to `./agent.sh`.
- No destructive DB changes during MVP phases.

## 10) Deliverables
- Rust workspace skeleton and MVP commands.
- Updated `.env.example` with Rust-compatible keys.
- Migration notes for Linux/macOS/Windows.
- `doctor` output reference and troubleshooting table.

## 11) Immediate Next Actions
1. Create Rust workspace skeleton in `~/code/CoconutClaw`.
2. Implement `coconutclaw once --inject-text` with codex provider.
3. Add day-1 multi-instance options (`--instance`, `--data-dir`) with lockfile safety.
4. Add baseline regression script comparing Bash vs Rust marker outputs.
5. Demo text-only path on Linux first, then run macOS/Windows validation.

## 12) Decision Log (Current)
- Keep ASR/TTS out of core runtime requirements.
- Prefer one binary distribution model across all OSes.
- Prioritize reliability and compatibility before feature expansion.
