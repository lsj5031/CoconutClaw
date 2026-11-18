# Telegram operations

`/start` opens Home: **Status, Tasks, Nodes, Scheduled tasks, Memory,
Preferences, Help, New conversation**. Every destination has Home.
No frontend, Mini App, API server, background refresh, probes or pinning is used.

Reports are immutable, timestamped local snapshots. **New snapshot** sends a new
report; opening a detail produces its own timestamp. Only Home and Help, which
contain no observations, may edit a navigation message. Telegram receives UI
text without a parse mode, so user text is literal, not executable HTML/Markdown.
Long views are bounded and explicitly disclose truncation.
Report headers and recorded event times use the configured timezone, with the
zone name and UTC offset shown to distinguish repeated daylight-saving times.
Each report has one snapshot header; refreshing never makes an old report live.

Nodes use the verified enrolled role and a short stable ID suffix. Current
admission cards contain no hostname or friendly-name field, so the UI must not
invent one from an address or the coordinator's host. Full IDs remain internal
to authentication, routes and callback actions. The report distinguishes the
destination of **new text tasks** from existing assignments. “Last reached” is
a saved successful contact, not current online status. Failed probe times were
not recorded by the existing protocol and are explicitly unavailable.

## Scope and authority

Every click and message rechecks the configured private chat allowlist, actual
sender and exact topic. Missing actors are rejected. The bot-authored callback
message is not the clicking actor. Opaque durable button IDs additionally bind
the exact conversation/topic and actor, expire after 24 hours, and cannot become
provider prompts. Mutation controls are single-use. Existing raw ingress custody
and durable outbox receipts prevent repeated update effects. Uncertain runtime
effects/delivery remain Unknown, without automatic retries.
Explicit observation-only controls may inspect a conversation with blocked or
Unknown ingress. This does not release older work, reorder mutations, reconcile
effects, or authorize retries; mutating controls retain the existing lane fence.

There is no existing global operator authority, so all reports are scoped to the
conversation/topic. Configuration is not evidence of runner health. Runtime and
node liveness are unavailable: there is no production heartbeat registry; the
Nodes report instead shows saved contact checks, not continuous monitoring.
Transport counts and last
received time are persisted custody observations, not network/poller health or
a freshness guarantee. Progress means only that progress was recorded, never an
invented percentage. Raw prompts, provider progress/errors, resume payloads,
delivery plans and internal SQL JSON are not part of the status snapshot API.
New runtime timestamps use RFC3339 offsets. Older compact-offset or malformed
timestamps may appear unavailable; existing persisted bytes are not rewritten.
Unknown is displayed as an **unconfirmed outcome**, never as a failed operation
or permission to retry. The next step is to inspect Tasks, the conversation and
local run evidence before repeating work. Unconfirmed delivery similarly asks
the user to check the conversation before sending again.

Tasks are logical work; runs are physical attempts. Beside-run cancellation and
pending approval controls preserve existing runtime ownership/approval semantics.
Cancel is a request, not confirmation of process termination. Schedule next-due
times are persisted cursors, not execution promises. The view shows existing
runtime schedules only, never imports historical schedules or creates jobs.

## Existing commands

| Command | Button destination / meaning |
|---|---|
| `/fresh` | New conversation, adds a context boundary; keeps memory/preferences |
| `/cancel [run ID]` | Tasks, Cancel run; existing scoped cancellation |
| `/tasks [run ID]` | Tasks / current run details, distinguishes task and run |
| `/memory` | Memory; existing list/search/get/remember/promote/correct/undo syntax remains |
| `/preferences` | Preferences; existing set/unset syntax remains |
| `/notifications [run ID]` | Tasks, scoped notification evidence via compatibility command |
| `/schedules` | Scheduled tasks, current schedules and next runs |
| `/approve ID` | Tasks, beside a pending approval |
| `/reject ID` | Tasks, beside a pending approval |
| `/reconcile` | Help, narrow proven acknowledgment recovery; **never generic retry** |

`/help`, `/status` and `/nodes` are direct local views. Unknown slash commands
return helpful local feedback. Commands, callbacks, invalid/stale controls and
guided inputs never ask a provider to formulate their response, including errors.
Approving real work still authorizes its existing durable continuation; that
task execution is not a provider-backed command handler.

## Guided input

Memory **Remember**, **Search**, **Get memory**, **Promote**, **Correct** and
Preferences **Set preference** / **Remove preference** issue an explicit
`/input CODE text` instruction. The durable input is scoped to actor,
conversation and topic, survives restart, expires after 15 minutes, and is
cancelled by **Cancel input** or `/input cancel`. Starting a second guide replaces
the previous one. Old codes and competing/cross-topic input cannot mutate state.
An old guide's Cancel button cannot cancel its replacement. Memory **Undo last
memory change** uses the existing scoped undo semantics and a single-use control.
Ordinary unrelated messages continue as conversation messages; they are not
silently consumed by a pending guide. This explicit-address design avoids
guessing whether arbitrary text is an operational input or provider request.

## Local verification

Tests enter production ingress and deliver through the real durable outbox to
the existing local Telegram mock, with an impossible provider executable.
`COCONUTCLAW_UI_ARTIFACTS=/absolute/path cargo test -p coconutclaw --locked buttons_`
retains ordered readable UI plans and actual mocked wire requests as
`ordered-ui.json`. This is inspectable local evidence, **not** a genuine Telegram
screenshot or live visual acceptance. No live provider/Telegram calls are needed.
Successful callback reports attempt a one-shot spinner acknowledgment after the
durable text receipt. An expired acknowledgment never gates or retries a report.
