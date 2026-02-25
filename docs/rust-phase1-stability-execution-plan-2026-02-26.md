# CoconutClaw Rust Phase 1 Stability Execution Plan

Date: 2026-02-26
Target repo: `/home/leo/code/CoconutClaw`

## Goal
Complete the first reliability-focused Rust iteration by shipping:
- Stable long-running runtime behavior (single-instance lock, graceful shutdown, loop resilience).
- Key Bash parity for local webhook queue processing and turn persistence edges.

## Scope For This Run
1. Runtime resilience
- Keep lockfile protection enabled for all commands.
- Make `run` a true long-running loop for local runtime usage.
- Add graceful shutdown handling (SIGINT/SIGTERM) and clean stop logs.
- Keep loop alive on per-turn failures instead of crashing the process.

2. Webhook queue parity (local queue file)
- Add queue read/ack loop for `runtime/webhook_updates.jsonl`.
- Add inflight restore from SQLite `kv` so crash/restart can resume safely.
- Add update dedup using `turns.update_id` checks before processing.

3. Persistence and context parity edges
- Persist `update_id` in `turns` for queue-processed updates.
- Include quoted/replied-to context in Rust prompt build path.
- Keep marker output contract unchanged.

4. Verification
- `cargo fmt` + `cargo check`.
- Run text smoke (`once --inject-text`).
- Run webhook queue smoke with sample JSON line and verify ack + turn write.
- Run marker compare script where possible.

## Execution Order
1. Create this plan document.
2. Commit current repository state as baseline checkpoint.
3. Implement runtime and queue parity items.
4. Run validation commands and fix issues.
5. Update this plan with completion status.

## Completion Checklist
- [x] Baseline checkpoint commit created.
- [x] Rust `run` supports long-running resilient local loop with graceful shutdown.
- [x] Webhook queue processing + inflight recovery implemented.
- [x] `update_id` dedup and turn persistence parity implemented.
- [x] Quoted/replied-to context included in prompt context.
- [x] Validation commands passed and documented.

## Validation Notes (2026-02-26)
- `cargo fmt --all` passed.
- `cargo check -p coconutclaw` passed.
- `once` smoke (isolated instance + failing local provider path) produced valid marker output with `TELEGRAM_REPLY` fallback.
- Webhook queue smoke:
  - queued update `900001` was processed and acknowledged (`queue_lines_after=0`).
  - SQLite persisted `turns.update_id=900001` with `status=agent_error` in the fallback-provider smoke path.
- Inflight restore smoke:
  - preloaded `kv.inflight_*` + queue head `900002` was restored, processed, and acknowledged.
  - SQLite persisted `turns.update_id=900002`.
  - `kv` inflight keys were cleared after successful restore (`count=0`).
- Reply context smoke (fake local provider):
  - captured prompt context includes:
    - `## Quoted/replied-to message`
    - `REPLY_FROM: Tester`
    - `REPLY_TEXT: quoted message content`
- Marker compare script:
  - Rust side produced marker output successfully.
  - Bash side returned non-zero in this environment due Telegram/API path requirements, so full Bash-vs-Rust marker parity could not be concluded from this isolated offline run.
