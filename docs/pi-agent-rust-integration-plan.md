# CoconutClaw `pi_agent_rust` Integration Plan (Non-Breaking + Easy Switching)

Last updated: 2026-02-25  
Target repo: `/home/leo/code/CoconutClaw`

## 1. Decision

We should add `pi_agent_rust` support, but only as an optional provider.  
Default behavior must stay exactly as today (`codex` path unchanged), so existing scripts keep working.

This is the right path if you need models that are not available through Codex-supported providers.

## 2. Hard Constraints (Must Not Break)

1. Keep current marker contract unchanged:
   - `TELEGRAM_REPLY:`
   - `VOICE_REPLY:`
   - `MEMORY_APPEND:`
   - `TASK_APPEND:`
2. Keep `codex` as the default provider.
3. Keep existing CLI entry points and script flags unchanged.
4. Do not change SQLite schema.
5. Make rollback a single env change (`AGENT_PROVIDER=codex`).

## 3. Current Baseline

### 3.1 CoconutClaw bindings today

- `agent.sh` is centered around `run_codex()`.
- Progress reporting is tied to Codex JSON events.
- Cancel flow is implemented with PID + signals.
- Config is Codex-centric (`CODEX_*`, `EXEC_POLICY`).

### 3.2 `pi_agent_rust` capabilities we can use

- Non-interactive execution via `pi -p`.
- Supports `--mode text|json|rpc`.
- Supports stdin input (good fit for current context-file pipeline).
- Supports provider/model selection (`--provider`, `--model`).

### 3.3 Known risk

- Upstream drop-in certification is currently `NOT_CERTIFIED` (reported on 2026-02-18 UTC in upstream docs).
- So this should be an additive provider, not a forced replacement.

## 4. Target Design (Minimal Changes)

Use a simple provider dispatch layer:

1. Keep `run_codex()` as-is.
2. Add `run_pi()` with the same output contract (marker lines).
3. Add `run_agent()` dispatcher keyed by `AGENT_PROVIDER`.
4. Update only call sites that currently invoke `run_codex()` directly.
5. Keep memory/task extraction and post-processing unchanged.

This keeps risk low and makes provider switching operationally simple.

## 5. Configuration Plan

Add these environment variables:

- `AGENT_PROVIDER=codex` (default)
- `PI_BIN=pi`
- `PI_PROVIDER=` (optional)
- `PI_MODEL=` (optional)
- `PI_MODE=text` (default; safest)
- `PI_EXTRA_ARGS=` (optional passthrough)

Compatibility rules:

- `EXEC_POLICY` remains Codex-only.
- If `AGENT_PROVIDER=pi`, log a warning when Codex-only knobs are set, but do not fail.

## 6. Implementation Phases

### Phase 0: Probe and Guardrails (0.5 day)

Tasks:

- Verify `pi` binary presence and version.
- Run one sample turn using stdin-driven `pi -p`.
- Confirm marker extraction still works end-to-end.
- Verify cancellation does not hang.

Exit criteria:

- Stable `TELEGRAM_REPLY` extraction.
- Cancel path exits cleanly.

### Phase 1: Non-Breaking Provider Switch (1 day)

Tasks:

- Add provider env parsing with strict default to `codex`.
- Implement `run_pi()` in text mode first.
- Implement `run_agent()` dispatcher:
  - `codex` -> existing `run_codex()`
  - `pi` -> new `run_pi()`
- Update runtime validation for provider-specific binaries.
- Keep all existing code paths untouched when `AGENT_PROVIDER=codex`.

Exit criteria:

- Current behavior is unchanged under default config.
- `AGENT_PROVIDER=pi` completes at least one full text turn.

### Phase 2: Streaming + Better Cancel (1 day)

Tasks:

- Add optional `PI_MODE=json` support with a dedicated monitor.
- Map key pi events to current progress updates.
- Provider-specific signal strategy:
  - `pi`: `SIGINT` -> `SIGTERM` -> `SIGKILL`
  - `codex`: keep current behavior

Exit criteria:

- JSON mode shows progress updates.
- `/cancel` is reliable for both providers.

### Phase 3: Docs + Verification + Rollout (0.5 day)

Tasks:

- Update `.env.example` with provider switch examples.
- Update `README.md`:
  - provider switch quick-start
  - using non-Codex models via pi provider/model flags
  - known behavior differences
- Manual verification checklist:
  - `shellcheck agent.sh scripts/*.sh lib/common.sh`
  - `./agent.sh --inject-text "hello" --once` (default codex)
  - `AGENT_PROVIDER=pi ./agent.sh --inject-text "hello" --once`
  - `/cancel` check for both paths
  - marker persistence check for memory/task appends

Exit criteria:

- Both providers run successfully.
- Rollback is confirmed with one env flip.

## 7. File Change Scope

Planned edits:

- `agent.sh` (provider dispatch + pi runner)
- `.env.example` (new provider settings)
- `README.md` (operation and switching guide)
- `docs/pi-agent-rust-integration-plan.md` (this plan)

No schema migrations, no new external service dependencies, no framework rewrite.

## 8. Rollback Plan

If pi path has issues in production:

1. Set `AGENT_PROVIDER=codex`.
2. Restart CoconutClaw process/service.

Because Codex path is preserved, rollback is immediate and low-risk.

## 9. Timeline

- Fast usable path (Phase 0 + 1): 1 to 1.5 days
- Full path (Phase 0 to 3): 2 to 3 days

## 10. Priority Order

1. Phase 0 + Phase 1 first (safe switching with no breakage).
2. Phase 2 second (streaming and better cancellation).
3. Phase 3 last (docs and final operationalization).
