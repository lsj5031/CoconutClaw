# Persisted waits and task graph

Finite 16 November 2026 foundation. No deployment, live job, publication,
independent review approval or completion of the always-on architecture.

## Actual owners and identities

`runtime_tasks` owns a stable logical ID (its original run), immutable origin
session/actor/channel, current run and optional independent delegation parent.
`runtime_attempts` links each existing physical `task_runs` ID to that task,
attempt ordinal, predecessor and exact triggering `runtime_inbox` key.
`runtime_waits` records one approval/dependency reason, expected identity,
origin, frozen runnable payload/prompt and terminal wait consumption. A dependency
also retains the exact successful source run and outcome snapshot used to resume.
This is execution progress/evidence, not factual memory or user instructions.

Normal request/schedule/delegation admission creates the graph under the same
SQLite transaction as the existing inbox and runnable payload. Legacy task rows
backfill as separate roots without rewriting outcomes, approvals, deliveries or
guessing historical ancestry. Initialization is transactional and idempotent;
restoring a missing root trigger never invents roots for existing continuations.
Public `/tasks`, approval and cancellation commands retain physical run IDs.
The logical ID and attempt are separately visible in native progress context.

## Waiting and bounded resumption

An ordinary successful local provider outcome may emit:

```text
WAIT_FOR_TASK: 123
```

The positive existing task/run number resolves to its logical task, in the same
canonical session and actor scope (including equivalent private-chat owner
sender representations). Self-dependencies, active dependency cycles, repeated
consumption of an already satisfied dependency, conflicting wait reasons and
approval/delegation combinations refuse. Remote text remains literal and cannot
create any wait, approval, child or memory effect.

The wait outcome and notice persist together; the model exits and its slot is
released. No sleeping provider, model polling loop, new daemon or runtime exists.
At ordinary completion, polling and startup, the existing owner examines at most
32 eligible oldest waits. A known completed current dependency needs its actual
result inbox receipt and output. Cancelled, failed, unresolved, uncertain remote,
Unknown/inflight delivery and raw conversation custody fences do not authorize
resumption. Pending dependencies do not occupy the eligible batch.
The sender's durable aggregate acknowledgment resolves a cached `sending` plan
label; that label alone must not block an already acknowledged outcome. Explicit
`unknown` still blocks, even if an inconsistent aggregate acknowledgment is set.

One IMMEDIATE transaction freezes that completion evidence, admits the
deduplicated `dependency:<waiting-run>` request, creates one new physical attempt,
advances the logical head, inherits schedule ownership and consumes the wait.
The old physical run is completed as **dependency accepted**, not as proof that
the logical task's requested work succeeded. Only its current attempt determines
that work's current outcome. Exact event replay creates no second attempt;
conflicting reuse rolls back. Malformed or substituted frozen payloads do not
run. Stale callbacks cannot overwrite a newer logical head.

The new attempt receives its original requested work and a separate JSON
progress section identifying task/run/ordinal/predecessor/event, its own persisted
pre-wait provider outcome (where present), and the frozen
dependency outcome. This is data, not newly inferred instructions or memory.
New uncertainty in the source is rechecked before native invocation. No
automatic model retry is inferred from recovery, a failed resume or delivery.

Approvals use the existing question/decision API and exact actor/session
authorization. Acceptance binds that decision to the waiting run and links one
new attempt into the same task, rather than creating unrelated logical work.
Cancellation/rejection and acceptance serialize at SQLite; a cancelled wait
cannot resume. Legacy rejection does not need to parse an unusable resume
payload. Owned attachment cleanup happens after committed cancellation, and
wait-notice acknowledgment retains input needed by the later continuation.

Existing signed remote Start/Result/cancel reservations remain authoritative in
`runtime_remote_runs`/`runtime_remote_results`. Their `WaitingRemote` state releases
slots and completes the **same exact original physical run and outbox**. It is not
recast as a dependency wake or a synthetic task. These bindings compose through
the graph's original run ID without weakening existing signature/epoch fences.

## Admission, ordering and delivery

Ordinary conversation work retains its FIFO coordination lane. Isolated
delegated, scheduled and dependency-resumed attempts use task-specific native
contexts and execution lanes, preserving original history/control/delivery scope.
The same admission lock, durable raw-event fences, oldest-ready ordering and
global five-provider limit apply. Waiting work uses no slot; independent work in
the same conversation can run. Cancelled empty lanes and finished task-state
entries are removed; existing owned provider cancellation/shutdown remains.
Oldest-ready orders slot reservation, not OS process scheduling across multiple
independent slots. The legacy recovery FIFO fixture reserves four slots and
verifies that the remaining slot executes older accepted work before a newly
materialized delegation, without relaxing its exact output-order assertion.
The original single bounded outbox dispatcher still sends every visible result.

## Evidence and limits

Twelve new portable `waiting_graph_*` tests cover actual approval ancestry,
cross-scope/cycle/self/repeated-event refusal, transactional cuts/conflicting
events, Unknown/cancel guards, 33-wait draining, approval/cancel races,
legacy backfill/trigger repair, frozen evidence/stale callbacks, attachment and
rejection cleanup, and real disposable provider admission at five slots.
The acknowledged-send regression uses the exact state shape captured from an
intermittent normal-executable restart failure, not a relaxed wait deadline.
The control-ack regression admits independent work during the actual HTTP
acknowledgment. It reproduced SQLite `BUSY_SNAPSHOT` while the old sender held
its query iterator across I/O. The same sender now freezes at most32 authorized
candidates and drops that read before sending/writing, without clearing Unknown
or retrying a possibly sent operation.

The normal executable fixture uses loopback Telegram HTTP and an explicitly
configured disposable Codex-compatible process: a durable dependency wait,
independent same-session work, restart, real approval command, bound successful
dependency event, one new native continuation, persisted outcome/outbox and a
second restart without repeated execution or final send. It checks the native
process actually received structured triggering progress. No authenticated
provider or real Telegram endpoint is used.

The existing native CI matrix now requires all source-defined bridge and graph
critical cases to execute `ok` and retains source/OS/count receipts and normal
fixture logs. Configuration/local execution is not actual hosted native proof.
The finite [explicit receipt reconciliation](runtime-reconciliation.md) can
settle only an exactly acknowledged rich-text operation, preserving provider
Unknown and every other resumption guard.
The finite [context/notification interfaces](context-notifications.md) expose
scoped structured status and persist core-derived changed-blocker decisions
without rewriting this graph or historical results.
Broader wake kinds, logical-task command UX, external-effect reconciliation, richer calendars,
final source-wide formal/native gates and authorized acceptance remain open.
See [verification gates](final-verification-gates.md). No new dependency selected;
the existing Rust/SQLite/provider/inbox/outbox owners suffice for this slice.
