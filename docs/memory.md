# Local memory

One Rust/rusqlite implementation, using SQLite's already bundled FTS5. No
embeddings, network service, engine interface, plugin or new dependency.

## What the user sees

Commands are incoming chat commands, not model markers:

| Command | Effect |
| --- | --- |
| `/memory remember <text>` | Direct authorized save, active and curated, 1–512 UTF-8 bytes |
| `/memory list [before-id]` | Latest eight scoped record previews, with IDs, state and source turns; optional exclusive pagination cursor |
| `/memory search <text>` | Active-record keyword snippets, ordered by FTS5 rank then ID |
| `/memory get <id>` | Full scoped active record or never-promoted candidate, with provenance |
| `/memory promote <id>` | Activate a candidate/inactive record explicitly, never change its scope |
| `/memory correct <active-id> <text>` | Create an immutable replacement, deactivate old record atomically |
| `/memory undo` | Undo the latest still-effective mutation in this chat, including direct remember |

`recall` is an alias for `search`. Commands also work through isolated local
`once --chat-id ... --inject-text '/memory ...'`; this is a trusted local operator
interface, not a remote API. Memory commands never call Codex.

Polling discovers the bot username with Telegram `getMe` before recovery or
dispatch. `/memory@ActualBotName ...` works only when the address matches that
identity (case-insensitively); commands addressed elsewhere are ignored.
Original command text remains in source provenance. Offline `once` does not
discover a Telegram identity: use plain `/memory`. Addressed memory commands
there return a local rejection, without provider work or memory changes.

`MEMORY_APPEND` now proposes a candidate (1–2048 bytes), linked to its persisted
source turn and append index. It does **not** activate, curate or choose scope.
Invalid/oversize proposals are omitted from memory, with original provider output
still in the source turn. `list` and `get` allow inspection before promotion.
Direct explicit `remember` requires no proposal/promotion round trip. Natural
language alone is not parsed as authorization; use the short explicit command.
This avoids treating quoted or model-paraphrased “remember” text as a command.

Correction inherits the old curated flag; curated corrections stay within 512
bytes. Superseded/undone records remain in immutable history but do not match
search or ordinary `get`. Explicit promotion of an inactive ID is an intentional
restore. Undo is LIFO per chat, not arbitrary historical deletion or redo.

## Compact context, search, then source read

- Curated bootstrap: newest four active curated records, **2048 bytes total**,
  including record labels and JSON-quoted content. Overflow records remain
  searchable; nothing is deleted.
- Automatic pre-reply recall: current input searched as literal keywords,
  **eight snippets maximum, 128 Unicode characters each, 2048 bytes total**
  including labels. Full source bodies stay outside the prompt.
- Explicit search uses the same bounds. `get` returns one source record,
  at most 2048 content bytes (512 for curated), with a total response below
  **8192 bytes**. No arbitrary filesystem path or raw transcript reader exists.
- Query input: at most **512 UTF-8 bytes**, first 16 alphanumeric tokens, first
  64 characters per token. Terms are quoted and OR-combined. Punctuation never
  becomes FTS syntax. Empty/no-match queries return a normal empty-result message;
  oversize queries return an explicit bound message, not an FTS error.
- `list`: eight 128-character previews, 2048 bytes total; `get` is the next step
  when the preview is insufficient.

The memory payload is at most 4096 bytes per ordinary prompt, plus fixed
headings/instructions, independent of corpus size. This is a **memory** bound,
not a bound on user input, recent conversation, SOUL.md or pending tasks.
Memory command source turns are excluded from recent conversation injection.
Every effective memory mutation changes the chat's native Codex session key,
so corrections/undo do not resume a session carrying the old injected memory.
Unchanged revisions resume normally. Prior ordinary conversation can still
mention old facts; memory is not a mechanism to erase transcripts or external
effects.
Prompt data and its session revision are read in one SQLite snapshot. A command
arriving while a lane prepares an older prompt cannot label it as the new
revision; that in-flight turn can finish with old context, but later turns use
the new revision.

## Authority and consistency

Scope is the runtime's canonical Telegram chat identity, never text inside a
memory. Proposals derive scope and source ID from the stored turn. Commands use
the already-authorized incoming chat. Group chats follow the existing chat
allowlist, **not** a new per-human owner identity check; members who can command
an allowed chat share that chat's authority. Local operator `once` is trusted.
No cross-chat promote/get/search/undo operation exists.

Content is JSON-quoted and labeled as data. Content resembling metadata or
commands cannot change its SQL columns, authorize promotion, or create a
different scope. This is a runtime protocol boundary, not a sandbox against a
Codex process with arbitrary local filesystem/SQL access.

Sources and proposal identity are `(turn ID, append index)`. Mutation sources
are persisted incoming command turns. Local record/history IDs are SQLite
AUTOINCREMENT values, never reused by this implementation. Source/candidate
insertion is replay-safe; a saved turn with incomplete side effects is recovered
using the existing turn journal. Candidate and task insertions share a transaction.

Each command mutation, source turn, replacement, effective-set change, FTS
trigger update, immutable history and update-ID receipt commits in **one
immediate transaction**. Failures roll all of them back. Repeated Telegram
updates return their saved receipt without repeating a mutation. Calls without
an update ID are new operator commands, so two explicit remembers create two
records. Repeated promotion of an already-active record is a no-op.
Receipts do not make Telegram delivery atomic: a crash may lose or duplicate
a command reply, but cannot half-apply its memory transition. SQLite errors
propagate for retry rather than reporting success.

## No legacy memory layer

Owner steering on 15 November supersedes Phase 0's compatibility requirements.
There is no legacy memory_entries import, managed MEMORY.md projection, full MEMORY.md
or USER.md prompt injection, or automatic creation of those files. Existing
external files/tables are **not deleted or edited**. They are not supported
automatic memory inputs. The explicit local [OpenClaw Markdown import](openclaw-import.md)
is a separate opt-in operation with selected files, destination scope, preview
and transactional apply. It does not enable any legacy runtime layer.
Fresh data directories are the supported starting point.
If adopting an old installation, stop it, retain its backup, start with a new
data directory and explicitly re-enter desired scoped facts. There is no
automated destructive reset. Existing bounded-memory tables are upgraded
transactionally to support separate import provenance, preserving record/history
IDs and effective search state; legacy external tables are not imported.
Existing task projection
and durable turn/result recovery are separate and remain in place.

## OpenClaw reference, deliberately smaller

Read locally, offline, 15 November 2026:

- `openclaw/docs/concepts/memory.md`: compact USER/MEMORY layer versus detailed
  notes outside bootstrap; search followed by bounded source read.
- `openclaw/docs/concepts/memory-builtin.md`: SQLite FTS5, separate indexed
  provenance and curated/trusted recall.
- `openclaw/docs/concepts/memory-search.md`: keyword BM25, bounded trusted
  pre-reply recall, deliberate FTS-only mode.

Installed reference root:
`/home/leo/.local/share/mise/installs/node/24.20.0/lib/node_modules/openclaw`.
These are design references, not copied implementation or a claim of API parity.
Here small immutable records are already bounded chunks; there is no file
watcher, vector/hybrid search, semantic paraphrase recall, dreaming scheduler,
learned reranking, plugin system or configurable backend.

## Record ownership and later networking gates

| Domain | Stable local reference | Scope and authoritative store |
| --- | --- | --- |
| Messages/source turns | `turns.id`, Telegram update ID | Origin chat, core SQLite |
| Memory | `memory_records.id`, `memory_history.id`, source turn | Origin chat, core SQLite; FTS/effective set are derived |
| Jobs/results | `task_runs.id`, payload/result linkage | Origin session; assigned attempt owns execution result, core owns assignment/delivery |
| Membership | Not implemented | Future owner-authorized peer records, never inferred from transport identity |
| Artifacts | Existing owned local paths and task linkage | Local files; no portable identity or remote access yet |

Future distributed identities must qualify local IDs with a stable core/store
identity; bare integers or filesystem paths are not globally unique. Owner-key
design: an offline-backed-up owner authority authorizes membership and explicit
core transfer; device keys identify peers, not memory promotion authority.
No key creation, copying, backup, rotation, crypto implementation or membership
mutation occurs in this phase.

Before iroh state is called complete, define and test immutable record transport
keys, core/store identity, epoch/fencing for sole-core transfer, membership grants,
revocation/tombstone replay, offline-peer stale grants, owner restore and revoked
device restore, artifact identity/access, and protocol-store/SQLite replay
consistency. Revocation cannot erase data already learned by a peer. Restore
must not resurrect revoked grants or undone memory. Local undo tests do not
prove distributed revocation, recovery or exactly-once execution.

## Verification

New `store/memory_tests.rs` covers proposal/promotion, provenance forgery,
isolation, correction/undo, duplicate replay, reopen, malformed/oversize inputs,
FTS5 availability and injected transactional rollback including the search index.
The existing executable E2E suite now includes fresh-process `once` scenarios
for the full workflow and a 5000-record bulk corpus plus curated records.
It asserts exact SQLite state, context/search/get budgets, source fetch isolation,
native session invalidation and unchanged unscoped files. Incoming Telegram
dispatch is exercised with synthetic local updates and no network/provider.
See the Phase 1 report/evidence for actual check results, not just test intent.
