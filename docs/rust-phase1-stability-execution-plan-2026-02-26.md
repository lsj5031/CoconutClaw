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
- [ ] Baseline checkpoint commit created.
- [ ] Rust `run` supports long-running resilient local loop with graceful shutdown.
- [ ] Webhook queue processing + inflight recovery implemented.
- [ ] `update_id` dedup and turn persistence parity implemented.
- [ ] Quoted/replied-to context included in prompt context.
- [ ] Validation commands passed and documented.
