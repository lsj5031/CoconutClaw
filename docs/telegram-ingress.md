# Durable Telegram ingress

This began as a finite foundation on 16 November 2026. Its local receipts
are not live acceptance. The coordinator is still the sole Telegram ingress
owner when using [paired runners](production-nodes.md).

## Existing owners, not another framework

`run_poll_loop` and `process_webhook_line` now admit canonical full Telegram JSON
to `telegram_ingress` before any command, progress request or model admission.
Numeric update identity is the same identity used by existing Telegram task and
turn receipts. The raw event cannot be represented by `runtime_inbox` alone:
that table references a task run, while `/fresh`, `/memory`, ignored updates and
control replies must not create fake model runs. Its existing request/result/
approval paths remain unchanged.

The core remains the sole ingress owner. SQLite sequence/conditional claims order
accepted events per canonical conversation/topic, not across all conversations.
An earlier pending/applying/Unknown event fences later events and new model
dispatch in that conversation without holding a provider slot. Active models
remain interruptible by controls. Independent conversations retain the existing
five-slot scheduler; persisted approval waits still release slots.
The ingress snapshot and lane reservation share a short SQLite IMMEDIATE gate
with raw admission, so a stale snapshot cannot reserve work after new custody.
The read-only gate is released before worker spawn or any network/provider I/O.

Raw transport custody is distinct from command execution, native execution and
notification acknowledgment. This increment does not introduce a general event
framework, provider loop, dependency, task graph or new network transport.

## Persistence and recovery crosswalk

| Boundary | Existing mechanism reused | Durable result |
| --- | --- | --- |
| Raw receipt | SQLite and Telegram update identity | Canonical full payload, origin, sequence, monotonic cursor |
| Command admission | Existing parser, bot addressing and private topic routing | Conditional conversation-ordered claim |
| `/fresh`, `/memory`, `/schedules`, ignores | Same Store connection and memory transaction/receipt logic | Effect + cached outcome + ack outbox in one transaction |
| `/cancel`, approvals, `/tasks`, normal turns | Existing scheduler/control APIs | Claim before runtime effects; known result stored afterward |
| Normal model turn | Existing request inbox, queued payload and FIFO lane | Same run, no second provider invocation |
| Command reply | Existing rich/chunk/media plans and single bounded dispatcher | Typed `DeliveryKey::Telegram`, separate from task-run delivery |
| Poll acknowledgment | Existing offset protocol | Cursor advances only with durable raw custody, never on failed admission |

`/tasks` uses the scheduler's existing store-opening path; it takes the runtime
claim boundary even though its application operation is read-only. Approval
decisions retain their existing atomic decision/continuation inbox. A caught
failure may return to pending only when the approval is still pending and its
decision receipt is absent, proving transaction rollback before queue/cleanup.
A terminal decision plus an uncommitted ingress outcome is Unknown, not retry.

New control outcomes retain the exact original reply for diagnostic replay, not
permission for direct HTTP dispatch. Poll/recovery never send cached output
themselves. The existing dispatcher drains command acknowledgments, and the same
durable delivery plans record before-I/O and acknowledged-operation state.
There is no second raw HTTP ack sender or fake task result.

Normal-turn progress also belongs to raw custody: before I/O it is marked sending,
with no automatic HTTP retries. A returned message ID is persisted before task
admission; an ambiguous result remains Unknown and is not sent again on replay.
Existing native progress/final delivery continues to use that known ID.

## Crash cuts

* Before raw admission commits: no effect and no cursor advance.
* Admitted, before claim: pending payload is recovered, even after Telegram has
  acknowledged that offset.
* During a SQLite-only control: effect/outcome/outbox transaction rolls back;
  retry is safe, including a failure after memory mutation but before outcome.
* During a runtime control: applying becomes Unknown after restart. Do not repeat
  cancellation, recreate an approval continuation or infer absent effects.
* A normal-turn task receipt proves accepted work in the admission/outcome gap.
  Recovery releases that already queued run without repeating progress or the
  provider. Started/ambiguous native runs retain existing Unknown recovery.
* Known command outcome, before send: the independent outbox sends it once.
* Send/ack crash or ambiguous response: persisted sending/Unknown suspends
  automatic resend or fallback. A committed acknowledgment can finish bookkeeping
  after restart without transmitting again.
* Done event replay: return its cached outcome, do not execute its command or
  provider again. A different canonical payload under that ID refuses without
  replacing the original event or regressing the cursor.

Pending recovery is bounded to32 eligible events (the earliest per bound origin)
per polling wake; earlier
unresolved events are excluded before admission of later events in that origin.
Control delivery is bounded to32 eligible acknowledgments per dispatch pass.
Unroutable/disabled-topic acks do not consume that delivery bound. Telegram
ingress wake latency follows polling/getUpdates; the separate
[calendar wake](inbox-scheduling.md) does not depend on that cadence.

## Authority and legacy custody

Configured private chats, canonical topic IDs, current bot command address and
topic-OFF refusal remain boundary checks. When Telegram supplies a sender ID, it
must match the private chat, including callback sender identity. Approval and
targeted cancellation still require the exact originating session/topic.
Acknowledgment delivery rechecks the saved origin binding, current chat authority
and topic mode; it never falls back from a disabled topic to the root chat.

The old inflight KV is read only for one-way conservative migration. Valid
matching Telegram bytes are copied durably as Unknown before KV retirement.
Malformed, mismatched or non-Telegram checkpoints remain preserved and refuse
startup migration. A historical task/turn/memory receipt without a matching raw
payload is not proof that a new payload or a repeated acknowledgment is safe:
that first raw observation remains Unknown. Stored legacy content and existing
delivery receipts are retained, never reinterpreted as a resend grant.

Raw payloads and provenance stay in the local database, not diagnostics/artifacts.
Logs avoid callback/user bodies. No private memory import or new promotion path
is added; `/memory` remains an explicit authorized incoming command.

## Evidence and remaining gates

Disposable regressions exercise the real poll/webhook/control APIs, SQLite crash
cuts, concurrent replay, canonical/conflicting identities, per-topic ordering,
sender/bot/topic authority, cancellation and approval gaps, slot release, native
admission/restart counters, and actual mock HTTP send/ack uncertainty. They do not
redefine a saved result as proof that its external effects were never delivered.

The 16 November raw-ingress checkpoint preceded the
[runtime result bridge](runtime-results.md), [task graph](runtime-task-graph.md),
and [explicit reconciliation](runtime-reconciliation.md). The
[dated fleet record](fleet-verification.md) separates live cases from these
disposable ingress regressions.
