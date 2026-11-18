# Private topics and native rich replies

Scope: one owner's authorized **private chat**, currently `321951227`. Keep the
existing configured chat guard. This slice does not enable groups, add chats,
change BotFather settings, import OpenClaw memory, or enable networking features.
Tests use synthetic configuration and local HTTP/process mocks.

## Sessions and mode changes

`SessionKey` is the conversation identity: platform, root chat, optional thread.
Telegram ingress chooses the thread **only** from `message_thread_id`, after
checking the configured chat guard. Channel `direct_messages_topic_id` and
`direct_messages_topic` are rejected, not treated as private bot topics. Positive
private chat IDs and positive integer thread IDs are validated. Numeric chat-ID
strings remain accepted for existing injected/mock updates.

* Topic mode ON: each topic has its own ordered lane, history, `/fresh` boundary,
  Codex resume identity, memory revision, candidates and curated memory.
* Messages without thread metadata always use the independent root session,
  even with topic mode ON.
* Topic mode OFF: ordinary private-chat messages use the root FIFO session.
  Unexpected topic messages and callbacks are diagnosed and ignored.
* OFF does **not** move, merge, erase, or alias topic histories or native Codex
  sessions into root. Queued topic work and topic outbox results remain paused
  until a later startup discovers topic mode ON again. Root work can continue.
  Already-started work interrupted by shutdown is not automatically reexecuted.
* Capability changes take effect at startup. Restart after changing BotFather.
  No setting-changing Telegram method is called.

Startup `getMe` logs `has_topics_enabled` and
`allows_users_to_create_topics`, with a BotFather enablement diagnostic. The
second flag describes whether users may create topics; it is not session
identity and does not override the first. No implicit shared preference or
global-memory scope is introduced. `/fresh` does not erase scoped memory.
Cancellation callbacks and approval commands are scoped to their origin
session; a different topic cannot cancel or approve its neighbor's work.

Root and thread are persisted separately in request/delivery payloads, new turn
transport columns, task source metadata and approval metadata. Existing
historical rows are not rewritten. Internal canonical IDs such as
`telegram:321951227#7` are expanded at the transport boundary: Telegram receives
`chat_id=321951227` and `message_thread_id=7`, including multipart media.
History's historical unprefixed alias remains restricted to that same complete
session identity, never to its root.

### Task context and upgrades

`TASK_APPEND` records retain their persisted `turn_id`. Provider task context is
queried from unfinished managed task rows joined to that source turn's **complete
session and channel**, not read from a shared file. The same rule covers normal
append, side-effect reconciliation, startup, restart and topic-mode transitions.
Tasks with no source turn, another session/channel, manual/unmanaged rows, and
completed tasks are excluded from automatic model context without deleting any
records. Delegated contexts remain task-local; their stored result/task source
and delivery still belong to the originating session.

`TASKS/pending.md` remains an **operator-only aggregate**. Its existing managed
block is rebuilt for human inspection using the existing serialized, atomic
projection; manual prefix/suffix bytes and all database records are preserved.
Neither manual checklist content nor the managed aggregate is authorization for
shared model context. To discuss a manual item in a session, explicitly supply
that item in the session's input; there is no automatic manual-task import or
implicit global preference scope. The prompt tells the provider to use
`TASK_APPEND`, not read/edit that aggregate as conversation memory. This context
boundary is not a filesystem sandbox for Codex's tools.

Pre-correction native transcripts may already contain shared task context.
The correction therefore starts an explicit `context:scoped-tasks-v1` Codex
generation and does not resume unversioned native IDs. Old IDs, historical rows,
scoped memory and task records are preserved, not rewritten or erased.
Subsequent turns resume the new generation normally, including across restart;
memory revision changes and `/fresh` retain their existing generation behavior.
This prevents automatic resumption of pre-fix injected context, but does not
retroactively sanitize historical messages or outputs.

## Execution admission

There are **five fixed global execution slots** per runtime, including delegated
work and approval resumes. Each session has one FIFO lane. Admission chooses the
oldest eligible queued task across lanes and reserves a slot before creating a
worker. Waiting tasks have durable SQLite payloads, not waiting worker threads.
Recovery reconstructs all queues before admitting at most five workers.

The slot covers a whole task worker (provider, persistence and immediate
delivery), conservatively bounding actual providers as well as workers. The
existing delivery-only lock is not a provider mutex. Cancel, approval, success,
error and shutdown all use lane completion or shutdown handling; restarting does
not resurrect interrupted executions. No concurrency tuning knob is added.
Delegated attempts keep isolated task-local Codex/history context; their lane,
authorization and result destination remain the origin topic.

## Rich output and limits

Model marker outcomes are admitted atomically with fixed limits: 512 KiB wire
output, 256 KiB reply, four delegations (16 KiB each), eight media markers
combined, eight memory candidates and sixteen task appends (2048 bytes each).
Paths, approval and wait values are limited to 4096 bytes; at most 48 effects
are accepted. Limits count UTF-8 bytes, not characters. An over-limit outcome
becomes a small durable visible rejection, with no requested effects or partial
delegation admission. Delegated attempts cannot delegate, including after
approval/wait continuation and recovery.

Media markers may name only regular files within `<data-dir>/outbox`, at most
20 MiB each. Generate deliverables there, or explicitly stage intended output
there before emitting a marker. An incoming attachment's download path is not
automatically an approved outgoing attachment. Absolute paths and instance-
relative `outbox/...` paths are supported. Canonical path checks reject traversal,
outside symlink targets and a redirected outbox root. The sender rechecks after
recovery and sends from the checked open handle, not a later pathname reopen.
Generated long-reply Markdown documents use unique temporary files in this
same outbox. This boundary limits marker-driven file disclosure, not the
provider's tool access, and does not authorize copying secrets into the outbox.
Delivery-time rejection is frozen in the durable delivery state. If any prior
delivery was attempted, a separate bounded rejection notice retains the old
receipts and Unknown states; restoring a file does not reactivate the batch.

Official reference inspected **2026-11-15**:

* [Feature overview](https://core.telegram.org/bots/features#rich-messages)
* [Rich formatting and limits](https://core.telegram.org/bots/api/rich-messages.md)
* [InputRichMessage](https://core.telegram.org/bots/api/types/inputrichmessage.md)
* [sendRichMessage](https://core.telegram.org/bots/api/methods/sendrichmessage.md)
* [editMessageText](https://core.telegram.org/bots/api/methods/editmessagetext.md)
* [editMessageMedia](https://core.telegram.org/bots/api/methods/editmessagemedia.md)

The repository already uses reqwest directly and has no maintained Telegram
client abstraction to extend. No dependency or provider framework is added.

Final task text uses `sendRichMessage` with exactly
`rich_message: {"markdown": ...}`. It is **not** MarkdownV2-escaped. Headings,
GFM tables, lists, code, math and details/summary are passed as native Rich
Markdown. Short command/progress/status paths can still use regular messages.
Final rich replies are new messages; progress is removed after acknowledgment.
The assembled provider prompt always requests this native text-rich subset
inside `TELEGRAM_REPLY`, regardless of regular-status Off/Html/MarkdownV2 mode.
It permits multiline reply values and code fences inside the value, not around
the marker protocol, and retains explicit marker/media/approval restrictions.
It neither asks for MarkdownV2 escaping nor promises HTML conversion.
Marker values are native text, not escaped string envelopes: literal `\t`,
`\n` and `\r` stay literal in math and inline code, with no double-escaping
required. Use actual newlines for multiline marker values. Existing JSON
assistant-event recovery decodes JSON string escapes at that envelope boundary
only; marker parsing and saved-output replay do not decode them again.
Reply aliases, multi-block joining and marker whitespace rules are unchanged.
Literal paired or unpaired `<think>` tags in reply values or recovered assistant
text are not removed. The only retained reasoning-envelope syntax is one or
more closed `<think>...</think>` blocks at the start of raw final output,
immediately followed (apart from whitespace) by `TELEGRAM_REPLY:` or `REPLY:`.
No reasoning scan runs inside that reply or inside JSON-decoded assistant text.
An unclosed prefix or a prefix without an explicit final marker is not stripped.
This boundary belongs to the shared marker compiler, not only initial turn
resolution. Interrupted-turn outbox reconstruction, pending side-effect
reconciliation and legacy update replay all compile the same final effects
from saved raw output. Excluded reasoning cannot become an approval, append,
media operation or delegation after restart; legitimate final effects remain.
Literal tags still select plain regular output under the unchanged conservative
markup gate; preserving examples does not grant markup privileges.
The inspected edit API also permits `editMessageText` with `rich_message`
instead of `text`. This slice does not implement rich streaming or rich edits.

This is deliberately a **text-rich subset**: model-supplied embedded media,
arbitrary HTML, buttons, callbacks, Mini Apps, maps, audio and widgets are not
enabled. Images/HTML actions or entity-containing source select literal
plain-text output; only exact details/summary tags pass the HTML gate. Existing
explicit `SEND_PHOTO`, `SEND_DOCUMENT`, and `SEND_VIDEO` markers remain supported
and topic-routed. Rich markup cannot read a local file or authorize a callback.

The API documents 32,768 UTF-8 characters, 500 blocks, 16 nesting levels,
50 attachments and 20 table columns. Source over 32,768 Unicode scalar
characters is selected as a Markdown document before sending, preserving long
code/table/math instead of splitting rich structures. Other format/structural
limit rejections select readable regular text. Regular fallback uses the
existing Unicode-safe 4,096-character chunks, with the saved plan's formatting,
not a potentially changed process configuration. Formatting failure does not
change the stored agent execution result to failure.

## Durable delivery and uncertainty

Before sending, the outbox saves the chosen rich/document/regular representation
and ordered operation plan. It persists each send intent before I/O, each
acknowledgment before the next operation, and skips acknowledged operations
after restart. Existing saved regular/document plans keep their representation.
The immutable saved marker output provides the media operation plan; operation
indices and acknowledgments are persisted. A failed operation stops later
operations until it is acknowledged or a definitive format fallback is chosen.

An explicit API format/unsupported rejection may persist a plain fallback.
Explicit document rejection may select plain chunks. A timeout, invalid
response, 5xx, or crash between intent and acknowledgment is **not** evidence of
non-delivery: that operation remains `unknown`/`sending`, with automatic
resend/fallback suspended. Inspect local `task_payloads.delivery_state` and the
actual chat before deliberately repairing such a receipt. There is no
exactly-once promise: Telegram exposes no caller-supplied idempotency key, and
holding ambiguous outcomes trades automatic liveness for avoiding duplicates.
Input attachments remain owned until acknowledged delivery.

429 responses persist the full `retry_after` deadline; polls do not busy-retry
before it. Rich and regular/media operations conservatively round response time
up to the next epoch second before adding the delay (at most one extra second).
The stored `retry_at` remains an absolute epoch-second value, so existing saved
deadlines are honored unchanged across restart; lost fractional response times
in older receipts cannot be reconstructed. Durable operations have a maximum of
three attempts; no request-level retries run underneath these sends.
Known pre-connection failures are safe to
retry for regular/media operations. Exhausted or ambiguous operations stay
pending for diagnosis rather than failing or rerunning the agent.

## Behavioral evidence

Risk gates are executable in `src/topics_rich_tests.rs` and
`tests/topics_rich.rs`:

| Risk | Observable check |
| --- | --- |
| Context or authorization crosses topics | Root/topic ingress, scoped memory/revisions/boundaries, approval denial, isolated cancellation |
| Managed tasks or manual checklist cross sessions | Actual provider-stdin capture for topic 7 → topic 8/root before/after restart and ON/OFF/ON; startup side-effect replay; manual byte/record preservation; source/session/channel SQL checks |
| Old native transcript retains leaked context | Old IDs preserved but never passed to provider; new scoped generation resumes across restarts |
| Worker explosion or permit leaks | Five distinct live mock provider PIDs; sixth unstarted and durable; same-topic seventh waits; cancellation/shutdown/recovery |
| Recovery bypasses admission | Seven persisted queued tasks recover with only five admitted; error, approval and delegation release slots |
| Rich mistaken for MarkdownV2 | Inspect actual sendRichMessage form, exact Markdown, no parse_mode |
| Generation instructions contradict native output | Actual provider prompt and routed rich form captured for regular Off, Html and MarkdownV2 |
| Native backslashes corrupted by escape guessing | Actual provider output, persisted raw/reply/outbox, native plan and API payloads match single-line math, inline-code and multiline fixtures; JSON-envelope recovery and saved-output restart replay preserve exact content |
| Literal thinking tags deleted as reasoning | Paired/unpaired/closing-only code, plain/JSON assistant recovery and leading-envelope controls preserve exact replies/plans/API values across saved replay; unchanged markup gate selects literal regular text |
| Recovery activates excluded reasoning | Real release restarts from raw-turn windows before/after side-effect completion, approval/delegation controls and repeated restart; identical final output/plans, legitimate appends retained, no reasoning-only effects; direct legacy replay and pending reconciliation checked |
| Duplicate on unknown result | 5xx, accepted-but-timeout and persisted send/ack crash window survive restart without fallback/resend |
| Fractional-second 429 retries too early | Actual polling binary, rich and regular edit responses near `.8` seconds, busy authorized intake, old/new deadline restarts and persisted three-attempt cap; monotonic response-to-next-request intervals each at least `retry_after` |
| Partial output replay | Saved chunk/document regression plus rich text acknowledgment before retrying media |
| Limit/content corruption | Exact 32,768/32,769 Unicode boundary, long code/table/math document, topic multipart fields |
| Model markup gains authority | Embedded HTML action and local-file image remain literal regular text |
| Mode transition merges sessions | Real binary ON/OFF/ON; paused topic queue, ordinary root FIFO and root Codex resume, scoped memory, separate source fields |

The real-binary acceptance can target the release executable with
`COCONUTCLAW_TEST_BINARY` and export its ordered mock Telegram transcript and
actual provider counters using `COCONUTCLAW_ACCEPTANCE_ARTIFACTS`.
This is executable protocol verification, **not live Telegram visual
verification**. No live Codex/Telegram calls or credentials are used.
