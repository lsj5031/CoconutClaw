# Retained runtime

The CLI has four commands: `run`, `once`, `service`, `doctor`. One locked data
directory owns Telegram update ordering and SQLite bookkeeping.

Telegram polling accepts configured chats and downloads image/document/video
attachments. Ordered chat lanes execute Codex through the existing subprocess
driver, with prompts over stdin, JSON progress, an output-last-message file,
timeouts and process-group cancellation. Explicit Codex thread IDs are persisted
per chat and context-boundary generation, never resumed using `--last`.
Delegated attempts use distinct task-scoped session keys. A result is also saved
in the origin conversation history.
Delegated jobs receive their prompt, not the parent transcript or automatic
attachment inheritance. Pass durable local artifact paths explicitly when needed.

`TASK_APPEND` is a local pending-memory item. `DELEGATE_TASK` instead materializes
a durable executable job. It does not recursively delegate. Approval requests
pause the task and can be resolved only in the originating chat. New delegated
jobs, approval state and their parent delivery output are finalized atomically.

SQLite is authoritative for result/outbox state. Delivery is serialized in-process,
records completed Telegram operations and retries only incomplete operations.
Text has a saved representation/chunk plan; document fallback is saved before
chunk delivery, and each successful chunk has its own acknowledgment.
Owned input attachments remain available until result delivery is acknowledged;
pending approvals retain their inputs for an explicit resume.
Crash recovery reconciles final turns, not arbitrary provider execution.
Memory proposals use unique source-turn/append identities, not managed Markdown.
Incoming chat commands authorize promotion, direct remembering, correction and
undo. SQLite transactions own immutable records/history, effective state and
FTS5 indexing. A small scoped curated layer plus bounded snippets enters context;
bulk sources require bounded scoped reads. See [local memory](memory.md).
Task appends still project managed Markdown with in-process read/write serialization.
Replacements are staged and synced beside the target, renamed atomically, and
the parent directory is synced on Unix. Recovery materializes results/children
before reconstructing lanes in durable ID order. It never launches a recovered
child ahead of an older accepted job.

SIGINT/SIGTERM stops acceptance and lane advancement. Execution workers receive
cancellation and are joined within the shutdown budget; owned provider groups
are forcibly killed if needed. Blocking HTTP workers own only requests/response
buffers, never SQLite/projection state, and can be interrupted by their callers.
Provider deadlines cover stdin and output draining even after the leader exits.

Historical database tables/columns do not enable scheduling, voice, Slack or
other providers. No legacy memory import/projection is supported. Historical design documents under
`superpowers/` and past changelog entries are not current feature documentation.
