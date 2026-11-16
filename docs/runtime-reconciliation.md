# Explicit Unknown reconciliation

Finite 16 November 2026 slice, not deployment or complete external-effect
recovery. Reuses the existing authenticated Telegram command, raw inbox,
SQLite control transaction, bounded wait reducer and single delivery outbox.
No framework, dependency, provider retry or separate sender.

## Command

In the original private conversation and exact topic:

```text
/reconcile task 123
/reconcile ack 456
```

`task` identifies a physical task run's delivery, not permission to change its
provider result. `ack` identifies the raw Telegram ingress sequence's command
reply, not proof that an interrupted command executed. Inspection returns an
exact apply command only when sufficient evidence exists:

```text
/reconcile ack 456 token=<returned token>
```

Bare returned tokens are also accepted. Tokens are persisted random lookup
identities, not authority supplied by the operator. An offer freezes complete
original rows, output, route, effect plan and expected prior state. Task offers
also freeze graph ancestry/current head, approval/wait records, inbox/turn
receipts and original remote reservation/custody bytes. Actor and canonical
conversation/topic must match at inspection and apply. Original task payload,
prompt, channel, actor, route and session are cross-checked, not inferred from
remote text. Changed state or assignment, changed head, wrong identity, and
conflicting evidence refuse. Inspect again, never force an old offer.

## Evidence and limits

| Actual state | Permitted conclusion/action |
| --- | --- |
| A single frozen rich text operation, exact markdown matching the committed output, cached `sending` phase, positive attempt count, and the existing sender's durable `telegram:text` acknowledgment | Known acknowledgment of **that text only**. Explicit apply records an immutable cause/audit, updates the aggregate acknowledgment and reconciles its cached text phase. No HTTP or provider call. |
| A SQL reconciliation transaction rolls back, including failure to persist the command outcome | Known nonoccurrence of **that SQL reconciliation**. Projection, audit and command outcome all roll back; replay remains safe through the existing pending SQL control path. |
| No receipt, timeout, zero attempts, `ready`, legacy array, missing plan, different markdown, unacknowledged operation, or unsupported media/chunk/document/multiple-effect plan | No sufficient trusted evidence of external success or safe external nonoccurrence. Refuse, retain Unknown, explain the missing receipt/plan. Neither absence nor an operator assertion grants retry. |
| Failed/Unknown provider outcome, uncertain signed remote result, interrupted runtime control, progress/cleanup ambiguity | Remains unresolved. This slice has no authoritative service nonoccurrence oracle and cannot safely settle these effects. |

The sender writes its completed operation after a successful API acknowledgment;
its rich text plan can still retain a cached `sending` label. Reconciliation
uses that exact operation receipt, not the cached label alone or an inconsistent
aggregate bit. It supports this single rich-text shape deliberately; it does not
reset plans, attempt counts or retry deadlines, rebuild output, switch delivery
representation, or clear another operation's Unknown.
An explicit `unknown` text phase also refuses, even with an inconsistent
completed ID or aggregate bit. This slice does not adjudicate contradictory
per-operation evidence.

Historical provider outcomes, signed custody, task attempts, approvals, raw
control state and progress evidence are not rewritten. The previous complete
delivery snapshot remains in the immutable offer; the immutable audit links it
to the applying raw command. A duplicate apply returns the original disposition
without repeating projection, provider execution or target delivery. A new
command can have its own ordinary reply, sent by the same existing outbox owner.
Failed/Unknown schedules remain blocked, not silently rearmed.

A proven text acknowledgment can unblock the existing dependency reducer only
if **all its other gates already hold**: successful current source, actual result
inbox, no cancellation/approval/remote uncertainty, and no raw custody fence.
That reducer admits one deduplicated continuation for already-requested waiting
work. Reconciliation does not manufacture successful provider evidence.

The authenticated parser atomically marks only `/reconcile` as an exceptional
SQL control. It may pass older **Unknown** raw custody, which it leaves unchanged.
Older pending/applying owners still fence it, including pending ordinary work
behind an Unknown. Ordinary turns and other controls gain no bypass. Restart
retrieval uses the same persisted classification; no payload text grants this
capability without the normal route/actor parser.

## Current Unknown ownership map

- Provider interruption: `task_runs.failed` plus unknown-outcome summary and
  normal result inbox/output, not a generic retryable failure.
- Raw Telegram runtime effect: `telegram_ingress.state=unknown`. A task-admission
  receipt cannot prove an interrupted cancel/approval/control effect.
- Progress: `progress_state=unknown`; lacks a complete per-effect journal.
- Command acknowledgment: `ack_phase=unknown`, separate from control completion.
- Task/command delivery: frozen plan and operation `sending`/`unknown`, completed
  operation IDs and aggregate `delivered`/`ack_phase`, owned by the same sender.
- Signed remote custody: exact original run/reservation/Start/Result and
  `uncertain`/quarantined state. Existing adapter verifies signatures/epoch/
  roles; this slice neither imports new evidence nor interprets remote markers.
- Dependency/approval waits: frozen logical task head, wait identity and
  `waiting`/`unknown` state. Neither reconciliation nor a late callback approves,
  cancels, resumes or replaces a different head.
- Schedules: unresolved failed/started-cancelled/ambiguous delivery blocks later
  occurrences. No new cadence or scheduler retry grant.

The separate [synthetic reconciliation ledger](reconciliation-ledger.md) stays
inert and is not connected as runtime authority.

## Validation and remaining gates

Twelve portable `unknown_reconciliation_*` tests use disposable SQLite,
loopback HTTP and the real parser/inbox/outcome/outbox. They exercise receipts
from actual HTTP delivery, no-evidence refusal, actor/topic/identity guards,
staleness, cancellation/approval/head changes, inner and outer transaction
failures, immutable audits, duplicates/restart, unchanged remote Unknown,
one real wait-reducer continuation, and normal executable polling/restart.
The latter starts the same binary twice, retains a genuinely Unknown control,
sends the target text once and invokes no provider. CI requires each critical
case to execute `ok`; local receipts are not hosted OS evidence.

Next: separate preferences/facts/task progress/notification history and persisted
changed-blocker identity; richer timezone/DST recurrence and earliest-due wake;
then final-source codewide explicit/implicit machine inventory and official
SANY/TLC safety/liveness/fairness, negative controls, bounds/state counts and
source-action mapping. Actual pinned native Ubuntu/macOS/Windows CI and Windows
provider lifecycle remain mandatory. Authenticated Windows Codex is unverified.
[Full final gates](final-verification-gates.md) precede packaging. Independent
review is DEFERRED NOT APPROVED; publishing and activation are not authorized.
