# Context authority and changed-blocker notifications

Finite foundation, 16 November 2026. Same Rust/SQLite Store, inbox, task graph,
provider and sole bounded delivery dispatcher. No import, second memory engine,
notification service, resident model, extra sender or dependency.

## Four distinct sources

| Source | Authority and interface |
| --- | --- |
| User instructions/preferences | Explicit authenticated `/preferences` controls in the exact private conversation/topic. Immutable events retain actor and raw-ingress cause. These are user instructions, never system authority. |
| Factual memory | Existing `/memory` source-backed candidates, promotion, curated recall and FTS5. Facts remain labeled data, not instructions. No preference or notification is inserted into memory. |
| Structured task progress | Existing logical task/run/attempt/event/wait graph and bounded native progress. `/tasks <run-id>` reads scoped structured status, including completed or older attempts and the current head. |
| Notification history | `/notifications [run-id]` reads at most eight scoped decisions, bounded to 3072 JSON bytes, with task/run, core-derived reason, provenance and delivery disposition. It is not injected into model context or stored as facts. |

Minimal preference commands:

```text
/preferences
/preferences set language Speak Finnish
/preferences unset language
```

Keys are 1–32 lowercase ASCII letters/digits/underscore/hyphen. Values contain
1–512 UTF-8 bytes without control characters; at most eight active entries.
Unset appends a tombstone, not historical deletion. An unchanged value leaves
the revision unchanged. Writes and their normal command outcome/outbox share the
existing raw SQL-control transaction. Wrong actor/topic/bot address and raw
identity conflicts do not gain authority. These interfaces are authenticated
Telegram controls, not model markers, a remote API or local `once` commands.
No legacy USER.md/private-memory/configuration import occurs.

Native prompt preferences and their revision are read in the same SQLite
snapshot as existing fact context. A mutation or unset starts a distinct native
session generation; an older in-flight prompt cannot populate the new key.
Isolated task attempts inherit the original conversation's authorized preferences,
not its unrelated factual/history context. Existing isolated task history remains
isolated. SOUL.md/operator configuration stays unchanged. Preference text does not
change runtime scheduling, delivery policy, approval or execution permissions.
Model output and remote text cannot write preferences. Existing memory markers
still propose facts only, with no user-instruction authority.

## Persisted blocker identity

The core observes **current** persisted approval/dependency waits, with exact
logical task/current physical run, frozen wait payload, actor/session/topic and
original delivery route. Dependency identity includes its current physical run
and semantic state: execution pending, approval identity, remote result pending,
dependency wait, cancellation, failure/Unknown, or delivery/coordination blocker.
Queued and running are one execution-pending category. Progress wording, poll
timestamps, notification send retries and replayed input order are not changes.

The initial wait decision links the already-existing task outcome/outbox in its
same transaction; no second initial send is queued. Existing legacy waits get a
silent baseline, not invented historical notifications. A subsequent meaningful
identity change atomically updates the last-blocker projection and appends an
immutable generation/decision with its frozen original route and pending delivery.
Identical repeated observations, restart and concurrent topic observers create no
new unchanged notice. Reordered triggers reread current state, never their old
payload. No model text chooses a blocker identity or notification target.

Observation scans at most 32 waits per transaction. A persisted rotating cursor
reaches later waits even if the first 32 never change. This is bounded polling,
not a per-task timer or claim of instantaneous detection of every transient state.
Changes that occur entirely between observations may coalesce to current state.
This finite automatic notice scope is private Telegram approval/dependency waits,
not all failures, schedules or arbitrary model status text.

## Same delivery owner, no ambiguous resend

Changed notices add a delivery key to the existing native sender, not a new
process or fake task. Its plans, three-attempt cap and retry deadlines are reused.
At the sending-phase commit an IMMEDIATE writer gate rechecks current identity
and original route. A stale/cancelled/changed-head notice not yet sent is
superseded. Cancellation after a request was authorized cannot un-send it; its
actual acknowledgment is retained as historical truth, without repeating it.

Decision fields are immutable; delivery projection changes independently:
`linked`, `pending`, `sent`, `unknown`, `superseded`, `suspended`. Unknown or an
unacknowledged sending phase never grants resend or a fallback representation.
Exhausted known retry plans suspend, so they cannot starve fresh notices behind
the bounded batch. A genuinely new blocker can have its own new notice even when
an earlier different notice is Unknown; it is not a retry of that earlier effect.
An unchanged blocker never bypasses an Unknown by allocating another notice.

History keeps its exact original outcome text. If an existing task outbox later
projects a different cancellation/result message, history does not misattribute
its acknowledgment to the earlier wait notice. No task outcome, signed result,
approval, cancellation, factual memory or raw Unknown control is rewritten.
No service nonoccurrence oracle or operator assertion retry is introduced.

## Evidence and next work

Sixteen portable `context_notifications_*` cases exercise real incoming commands,
context separation and revision invalidation, actual native provider context,
scoped progress/history, duplicate/reorder/restart/concurrent topics, both
initial-outcome and changed-decision transaction cuts, stale routing/cancellation,
acknowledgment during cancellation, ambiguous HTTP without resend, and 33-item
observation and delivery batches. The normal executable starts twice, receives
preferences then their removal in disposable provider contexts, observes one
remote Unknown blocker change, and sends exactly one changed notice. Signed
custody verification remains independently covered by the existing bridge suite;
the new fixture injects only at that already-authenticated boundary.

CI requires every source-defined critical case to execute `ok`, but local checks
are not hosted Linux/macOS/Windows proof. Next: richer timezone/DST recurrence and
earliest-due wake, then final-source repository-wide explicit **and implicit**
machine inventory, official source-linked SANY/TLC safety and justified
liveness/fairness, negative controls/counterexample fixes/regressions, bounds,
state counts/exits/action maps/gaps/residual risks. Actual source-pinned native
Linux/macOS/Windows CI and portable provider lifecycle remain mandatory.
Authenticated Windows Codex is unverified. See the unchanged full mandates in
[final verification gates](final-verification-gates.md).

Packaging waits for those gates and appropriate publication authority. Review
remains DEFERRED NOT APPROVED. No deployment, activation, actual user schedule,
real Telegram/provider use, or Phase 2/architecture/packaging completion claim.
