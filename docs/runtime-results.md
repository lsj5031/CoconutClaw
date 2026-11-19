# Verified runtime result bridge

The 16 November 2026 bridge connected the development adapter to the runtime's
durable result inbox and original task outbox. That adapter still uses
disposable Stores. The later [paired-node path](production-nodes.md) admits
ordinary text work separately, then reuses the same result reducer and delivery
owner. The development adapter's receipts are not live fleet evidence.

## Ownership and durable boundaries

- Export reserves the existing source `task_runs.id`, canonical request/session,
  exact signed assignment and origin hash in `runtime_remote_runs`. Source and
  recipient reservations use `waiting_remote`, not a fake approval. A stale
  queued worker rechecks the reservation in the same IMMEDIATE transaction as
  local start, so export cannot race a second provider invocation.
- Existing ScopedPeer history verification supplies exact signed Start/Result
  bytes, role/epoch/assignment authority and original source identity.
  `TaskAdapter::apply_result` commits the exact Result inbox receipt, signed
  custody and adapter return receipt together before success is acknowledged.
  A Result callback need not follow an earlier local Start callback.
- Normal `Store::apply_runtime_results`, startup recovery and polling share one
  IMMEDIATE reducer, at most **32** eligible receipts per transaction. Raw
  pending/applying/Unknown custody fences that conversation's projection, not
  other conversations or native execution slots.
- Projection atomically advances the original run's progress/terminal outcome,
  original payload output (the existing outbox) and consumed receipt state.
  No notification-only task is created. Original run IDs are not runner-local
  IDs, signed domain assignment sequences or approval continuation IDs.
- Before projection and sending, recheck the original request, session, update,
  prompt, channel, sender and chat/topic metadata. Private Telegram source
  senders must match the original private chat; runner requests are normalized
  to isolated Local/Stdout with no sender or Telegram delivery authority.

Exact duplicates compare saved bytes; conflicts refuse. Custody survives
projection failure and restart. Changed source metadata, genuine approval,
existing local outcome/turn, or incompatible task state quarantines the result
as Unknown without overwriting local decisions. Owner-approved continuation is
a distinct existing approval inbox/task link, never a replay of the remote run.

Prior Unknown and cancellation-versus-success ambiguity remain Unknown. Retained
runner evidence is delivered with an explicit uncertainty warning, not a success
claim or retry permission. Remote text never grants core filesystem, memory,
approval or delegation marker authority. The same typed Task delivery key,
rich/chunk/document plans, durable sending/ack flags and single dispatcher apply.
An ambiguous HTTP acknowledgment is not automatically retried. Reapplying
stored result evidence does not invoke Codex again or reset recorded delivery.
There is no supported control for resending an acknowledged Telegram reply.

## Runnable isolated acceptance

From the repository, with the provisioned toolchain/cache:

```sh
cargo build -p coconutclaw --bin coconutclaw --locked --offline
cargo test -p coconutclaw --bin coconutclaw --locked --offline store::task_adapter::tests -- --nocapture
```

Eleven new portable tests reuse the actual scoped adapter and existing fake
Telegram/native-provider fixtures. They cover custody/projection/send/ack cuts,
restart, literal marker injection, cancellation and prior Unknown, legitimate
approval continuation with one native invocation, current owner fence,
post-custody substitution, and an independently executing topic.

The 33-row reducer boundary test keeps its first real scoped transport receipt;
its remaining bulk fixtures use the actual signed Event/Delegation types, one
complete existing application `History::replay` verification and real Store
reservation/custody transactions. It tests 32 then 1 then 0, including the
unmodified thirty-third task before the second drain. It does not claim 33
provider invocations or replace transport acceptance.

The normal-executable case consumes committed real adapter custody, starts and
restarts that exact executable, observes polling and exactly one original outbox
send, and checks unchanged binary bytes. Build the normal executable first for
`--bin`-only runs; `COCONUTCLAW_TEST_BINARY` can select an exact prebuilt binary.
`COCONUTCLAW_RUNTIME_BRIDGE_ARTIFACTS` optionally retains signed public
Start/Result bytes and the isolated acceptance receipt. No real credentials,
live Telegram, authenticated Codex, SSH or repeated cross-Mac acceptance is used.

The existing native CI matrix now explicitly builds the normal executable and
records actual checked-out SHA, OS, suite counts, critical bridge outcomes and
logs. Every source-defined bridge test must execute successfully, including
Windows; a cfg-excluded/ignored critical case fails this subset gate. This is
not hosted evidence for uncommitted local bytes or completion of the mandatory
[whole-architecture formal/native-CI gates](final-verification-gates.md).
At this checkpoint, task graphs, reconciliation, broader independently runnable
work, source-wide models and native Windows acceptance were still follow-up
gates. See [fleet verification](fleet-verification.md) for later dated evidence.

Local final validation: **436 passed, 0 failed, 5 ignored**, including all eleven
new bridge tests; formatting, strict all-target workspace Clippy, doctests and
normal build passed. The exact normal executable independently passed the eight
existing production polling/restart cases plus the new bridge/restart case,
without rebuilding. This is Linux evidence only, not hosted native-CI evidence.
