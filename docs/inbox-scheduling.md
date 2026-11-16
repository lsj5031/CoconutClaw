# Durable inbox and calendar foundation

Owner amendment, 16 November2026. Review **DEFERRED NOT APPROVED**; Phase2 remains
incomplete. No deployment or actual user schedule is authorized by these examples.
The previous real Mitori→Mars acceptance is preserved, not rerun.

## Domain and existing owners

* **Event:** one immutable producer identity, kind, origin session and payload.
  `runtime_inbox` consumes a request together with its runnable payload, or a
  result together with its outcome/outbox. Exact duplicate returns the existing
  run, conflicting reuse refuses. Transport progress IDs are not request identity.
* **Session:** canonical conversation/topic authority, history and ordinary FIFO
  coordination. Isolated delegated/scheduled/dependency-continuation attempts use
  task-specific execution lanes without changing that origin or delivery owner.
  All lanes share the existing five-provider limit and conversation raw-event fences.
* **Task:** stable logical requested work in `runtime_tasks`, with current run and
  optional delegation parent. Scheduled occurrences retain their existing identity
  `(schedule_id, revision, due_utc)` and ownership across physical continuations.
* **Run:** existing public `task_runs` execution ID plus `runtime_attempts` ancestry,
  ordinal and triggering inbox event. Approval/dependency resumes advance one task
  to a distinct run atomically. Legacy history backfills as independent roots,
  never guessed ancestry. Public commands still address physical run IDs.

See [persisted waits and the actual task graph](runtime-task-graph.md).

Requests, approval transitions and local provider outcomes use the existing
runtime, not the dev-only iroh adapter. Approval waits persist without an occupied
model slot. A single bounded outbox dispatcher owns delivery, separate from model
lanes. Existing Telegram delivery plans suspend `sending`/`unknown` operations
after ambiguous acknowledgments; neither recovery nor a timer resends them.

User instructions/preferences remain in managed user context, factual memory in
the scoped source-backed memory tables, execution progress/status in task runs/
approvals, and notification operations in the durable result/delivery outbox.
None is silently promoted into another, and no private/legacy memory is imported.
The later [context/notification foundation](context-notifications.md) adds their
distinct authenticated interfaces and persisted unchanged-blocker suppression.

## Dependency choice

Already maintained and lock-pinned: chrono0.4.44, chrono-tz0.10.4 and
rusqlite0.33.0. Existing `SessionScheduler`, request serialization, approval
continuations, delivery plans and restart reconciliation provide the necessary
owners. No new crate, model runtime, executor, queue, cron DSL or service is needed
for one-off, daily and selected-weekday wall-clock rules. The existing notify
dependency supplies filesystem change signaling, not another scheduling framework.

A general cron/async scheduler would still need the SQLite occurrence/inbox/
effect boundary and duplicate the existing admission owner. It is not selected
for this narrow calendar. Broader cron/calendar library selection and fresh
upstream maintenance verification remain open; public internet is unavailable
in this airgapped run. Existing versions and cached interfaces were inspected.

## Explicit policies

* One-off: RFC3339 with explicit offset and whole-second precision. The saved
  instant is UTC; its required IANA zone records the user's chosen timezone.
* Daily or weekly: local `HH:MM`, selected `mon,tue,wed,thu,fri,sat,sun`, IANA
  timezone and date arithmetic, not24-hour durations. Weekday order canonicalizes;
  empty, duplicate or unknown days refuse. Calendar search examines at most32
  dates, independent of downtime, then fails closed.
* `--fold first|second` selects exactly one UTC instant, never both.
  `--gap skip|next-valid` skips a nonexistent wall time or uses the first valid
  minute **on that same local date**, checking at most1439 later minutes.
  An entirely skipped civil date has no occurrence under either policy.
* `--missed coalesce|skip`: coalesce into **one latest due occurrence**, never a
  backlog. Skip advances past missed unaccepted occurrences without allocating
  an occurrence/run; a missed one-off becomes done. A selected occurrence within
  its due UTC whole second remains eligible, even if an older cursor was missed.
  Skip is intentionally lossy under delays, not an execution deadline guarantee.
* Defaults remain first-fold/skip-gap/coalesce. New/edited timing JSON persists
  all three choices. Existing JSON without policies decodes with those defaults
  (read-time migration, no historical outcome or creation-spec rewrite).
  Legacy exact creation-key replay compares normalized timing semantics.
* At most32 eligible schedules per wake. Blocked schedules do not consume that
  batch or starve later eligible ones. Each schedule has at most one unresolved
  run/approval/continuation; failed or ambiguous work, cancellation after start,
  and unknown/inflight notifications block future occurrences until reconciled.
  Only cancellation before start is known not to have effects. No retry is inferred.
* Pause/cancel/edit affect future occurrences only. Accepted payloads are
  immutable; cancellation of accepted work uses existing task cancellation.
  Cancelled/done schedules cannot be resurrected. Create has an explicit
  conversation-scoped idempotency key; editing does not reset that original key.
* An IMMEDIATE SQLite transaction commits occurrence identity, inbox consumption,
  runnable payload, ownership link and next due cursor together. Restart before
  dispatch reloads queued work; restart after start preserves Unknown. A known
  completed turn can reconstruct its outbox, never rerun the provider.
* Historical `scheduled_tasks` and `task_runs.scheduled_task_id` remain untouched.
  Separate runtime ownership prevents numeric IDs from aliasing old schedules.

## Entry points

Local administrative CLI, examples only (use only an explicitly requested task,
cadence, data directory and conversation):

```text
coconutclaw --data-dir /chosen/data schedule --session local:example create \
  --key explicit-followup --prompt "Owner-requested work" \
  --at 2026-12-01T09:00:00+13:00 --timezone Pacific/Auckland
coconutclaw --data-dir /chosen/data schedule --session telegram:321#77 create \
  --key explicit-daily --prompt "Owner-requested daily work" \
  --daily 09:00 --timezone Pacific/Auckland
coconutclaw --data-dir /chosen/data schedule --session telegram:321#77 create \
  --key explicit-weekly --prompt "Owner-requested weekly work" \
  --weekly mon,wed,fri --time 09:00 --timezone Pacific/Auckland \
  --fold second --gap next-valid --missed skip
coconutclaw --data-dir /chosen/data schedule --session telegram:321#77 list
coconutclaw --data-dir /chosen/data schedule --session telegram:321#77 pause 1
coconutclaw --data-dir /chosen/data schedule --session telegram:321#77 resume 1
coconutclaw --data-dir /chosen/data schedule --session telegram:321#77 edit 1 \
  --prompt "Explicitly revised future work" --daily 10:00 --timezone Pacific/Auckland
coconutclaw --data-dir /chosen/data schedule --session telegram:321#77 cancel 1
```

Shell-quote session IDs containing `#` where the shell requires it. Telegram
origins must be configured private chats; existing topic-off admission/delivery
guards remain in effect. `/schedules` lists only the current conversation's new
calendar records. Creation/edit stays explicit local CLI, not model markers or
an automatically inferred natural-language cadence.

Calendar administration uses SQLite transactions without taking core execution
ownership, so it remains available while `run` owns the instance lock. After
recovery the existing SessionScheduler starts one deadline/event intake worker,
with its own persistent Store and bounded one-element signal channel. The
existing notify watcher observes database/WAL/journal changes from separate
administrative processes and outcome/delivery writers. Each pass rechecks durable
eligibility and admits through the same queue lock and atomic occurrence owner.
Earliest-due waiting is independent of Telegram fetches and retry sleeps.
Pause/cancel/edit cannot authorize a stale cached deadline; admission rechecks
them transactionally. Unresolved schedules are excluded from both deadline and
admission selection, so blocked overdue rows neither hot-loop nor starve row33.

Write bursts coalesce with a100ms minimum pass interval; transaction errors back
off1s without creating invocation permission. A30s maximum wait rebases wall-clock
jumps and recovers missed watcher events. Watcher setup failure refuses runtime
startup; later watcher errors are logged and retain bounded rebase recovery.
No hard real-time guarantee is made. Corrupt calendar data fails admission closed
until explicitly repaired. Shutdown signals and joins this worker through the
same scheduler lifecycle. There is no shared mutable Store, second sender,
standalone calendar daemon, resident model tick or new provider owner.
A standalone `schedule create` persists only, never starts a service.

## Finite remaining gates

- [x] [Durable raw Telegram ingress](telegram-ingress.md), including command/control
      dedup and acknowledgment recovery, replaces the single inflight KV checkpoint.
- [x] [Actual runtime remote-runner result inbox bridge](runtime-results.md).
- [x] Persisted approval/dependency waiting/event resumption and task/attempt graph,
      with independent task lanes while conversation coordination stays ordered.
- [x] [Narrow evidence-bound rich-text reconciliation](runtime-reconciliation.md);
      broader provider/effect Unknown remains unresolved, never automatically retried.
- [x] Persisted changed-blocker identities/history and distinct
      [preference/fact/progress/notification interfaces](context-notifications.md).
- [x] Weekly recurrence, selectable missed/DST policies and scheduler-owned
      earliest-due wake independent of Telegram polling latency.
- [ ] Final-source explicit/implicit machine inventory and official SANY/TLC,
      plus actual pinned hosted native Linux/macOS/Windows execution.
- [ ] Authorized product/live acceptance, independent review and final packaging.

Disposable clock/SQLite tests cover DST expectations, catch-up/batch bounds,
concurrent writers, rollback cuts, conflicting replay, future-only edits,
approval continuation, crash recovery and the actual scheduler/provider/outbox
path with existing local fakes. They are not live Telegram or real user jobs.
