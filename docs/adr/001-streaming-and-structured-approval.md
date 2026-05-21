# ADR 001: Streaming and Structured Approval

**Status:** Accepted  
**Date:** 2026-05-20  
**Deciders:** architecture-review.md (Part 1, Item 9)

---

## Context

Two capabilities are currently scaffolded or analyzed but not wired:

1. **Live token streaming** — `Effect::ReplyDelta(String)` exists in `markers.rs` with full parser/renderer support, but no provider populates it. The provider trait returns a complete `ProviderOutput` after the subprocess exits; there is no streaming channel from provider to turn processing.

2. **Structured approval at the tool-call level** — Task-level approval already works: `SEND_APPROVAL` markers trigger Slack Approve / Reject buttons via Block Kit. What's missing is per-tool-call granularity (e.g., "Approve this specific file write?"). The current provider path is YOLO-or-nothing at the individual-tool-call level: `--dangerously-bypass-approvals-and-sandbox` grants blanket execution, or the provider prompts on its own stdout (which CoconutClaw can't intercept). Codex `app-server` offers inline approval round-trips via JSON-RPC, which would enable intercepting and rendering per-tool-call approval prompts.

Both would require adopting Codex `app-server` (or an equivalent streaming provider interface) as a data source. The architecture review (2026-05-19) analyzed this in depth.

---

## Decision

**Both capabilities are explicitly deferred — not scheduled for any release.** They are not in the v0.4.0 or v0.5.0 scope. However, "deferred" does not mean blocked: if any trigger condition in the "When to Revisit" section fires, the decision is automatically reopened and the work can be planned for the next appropriate release.

### Rationale

1. **Provider abstraction breakage.** Six providers implement `ProviderRunner` as one-shot subprocess → text. `app-server` is a stateful RPC channel. Adding it would either require a parallel code path (doubling the Codex surface) or widening the trait in a way no other provider can satisfy.

2. **Double bookkeeping.** CoconutClaw already owns conversation memory deterministically via `SOUL.md` / `USER.md` / `MEMORY.md` / `TASKS/pending.md` / `turns` rows. Codex `app-server` also maintains its own session state. Reconciling the two would create divergence — you either ignore Codex's memory (losing the main `app-server` upside) or ship a split-brain history.

3. **The reply contract doesn't simplify.** Structured events from `app-server` still need `markers.rs` to interpret `TELEGRAM_REPLY:`, `MEMORY_APPEND:`, `TASK_APPEND:`, etc. The `Effect` enum and marker-parsing flow are not replaced — they're augmented.

4. **New operational surface.** Lifecycle management, health checks, backpressure, RPC deadlocks, and leaked sessions across instance directories — all problems the current "spawn + wait + reap" model avoids.

5. **API churn risk.** The `app-server` schema is still evolving. The `codex exec` CLI is a more stable contract.

6. **Not a bottleneck.** Per-turn wall time is dominated by model latency, not `codex` startup. The coarse progress updates (`"Processing…"`, `"▶ git status"`) are sufficient for current UX.

### What IS done

- `Effect::ReplyDelta(String)` is fully implemented in `markers.rs` — parsing, rendering, and transport skip-lists are in place. When a provider can emit token streams, the carrier type exists.
- Slack approval buttons (Approve / Reject via Block Kit interactive messages) exist and work for `SEND_APPROVAL` markers at the *task* level. What's missing is per-tool-call approval granularity.

---

## Consequences

- **Streaming replies are not available.** Users see a progress indicator (`"⏳ Thinking... (12s)"`) updated periodically, then the full response at once.
- **Provider approval prompts are invisible to CoconutClaw.** If a provider (e.g., Claude Code) prompts "Approve this tool call? (y/n)" on its own stdout, CoconutClaw can't render it as a Slack interactive message. The YOLO flag (`--dangerously-bypass-approvals-and-sandbox`) is the only practical workaround.
- **The `ProviderRunner` trait stays simple.** All six runners remain one-shot subprocess + text output. No widening is needed.
- **`Effect::ReplyDelta` remains dead code.** It is harmless (transports ignore it via `_ => {}` catch-alls) but adds no value until a provider populates it.

---

## When to Revisit

Reopen this decision if any of the following becomes true:

| Trigger | Priority signal |
|---|---|
| Codex CLI schema breakage causes repeated maintenance pain | The `app-server` structured event stream would be more stable than scraping `--json` lines |
| A user-visible streaming UX becomes a requirement | Live token updates to Telegram/Slack are explicitly requested |
| Per-tool-call approval becomes a requirement | The current task-level `SEND_APPROVAL` / YOLO options are insufficient |
| A second provider gains streaming support natively | If Pi, Claude, or another runner offers a streaming interface, a shared streaming path becomes viable |

### If revisited, the implementation approach is:

Add `CodexAppServerRunner` as an *additive* variant alongside `CodexRunner`, behind a sibling trait (e.g., `StreamingProviderRunner` with `fn stream(&mut self, …) -> impl Stream<Item = ProviderEvent>`). The existing six `ProviderRunner` implementations stay untouched. `Effect::ReplyDelta` already exists as the carrier type for token chunks.
