# Explicit OpenClaw Markdown import

This is a trusted **local operator** command, not a Telegram command, runtime
backend, watcher, or automatic transfer. Never run it against personal sources
without selecting and reviewing them first.

```
coconutclaw --data-dir /private/destination import-openclaw \
  --source-root /selected/workspace --chat-id 321951227 \
  --file USER.md --file memory/selected.md
```

Preview is the default and opens no database, takes no lock, and creates no
destination files. It prints bounded JSON paths, line ranges and excerpts.
Repeat with `--apply` to commit. `--topic-id N` chooses a private-chat topic;
omitting it deliberately selects the root chat. The configured chat allowlist
must authorize the positive private chat ID. File bodies cannot choose scope.
The current deployment authorizes only chat 321951227.

Only explicitly selected `USER.md`, `MEMORY.md`, or `memory/*.md` are accepted.
Nested memory files require `--recursive`; this permits selection, not discovery.
There is no implicit directory scan. Dreams, archives, hidden files and
credential/secret-like filenames are rejected even when explicitly selected.
All path components, including the root, must be non-symlink directories/files.
Traversal, special files and non-Markdown inputs fail closed. Sources are opened
read-only, never renamed, edited or deleted. Local operators must keep the source
tree quiescent while importing.

Bounds per invocation: 32 files, 64 KiB/file, 1 MiB total, 1024 chunks.
UTF-8 content is split at headings/paragraphs and long lines at character
boundaries into at most 2048-byte records, retaining inclusive source line ranges.
Preview returns at most eight 80-character excerpts and 16 KiB of JSON plus
the terminating newline.
Content, including apparent instructions, is quoted memory data, never policy
or executable commands.

Default imports are **unpromoted candidates**, absent from search and bootstrap.
Inspect with `/memory list` and `/memory get`, then `/memory promote <id>` to
enable bounded search/recall. Explicit `--promote-curated` with `--apply` accepts
at most four chunks of at most 512 bytes each and enables compact bootstrap.
This flag affects new versions only; it does not silently curate existing
unchanged candidates. `/memory promote` enables search, not curation.
The existing four-record/2048-byte bootstrap and eight-snippet/2048-byte recall
bounds are unchanged regardless of corpus size.

Import origins are separate from conversation turns. Identity is canonical
source root + relative path + chunk ordinal within destination scope. Versions
retain line ranges and a stable non-cryptographic content fingerprint; full
byte equality, not the fingerprint, decides deduplication. Repeating the latest
source version is a no-op, including after undo. A later deliberate source
reversion is a new version. Changed slots create immutable
replacements, retiring the previous candidate and atomically superseding an
active version with inherited curation. `/memory undo` reverses an active
replacement using ordinary LIFO history. Source version receipts remain, so
rerunning does not silently reverse an undo. A subsequently edited source is a
new version. Missing files/chunks never prune stored memories.

Each apply is one immediate SQLite transaction, including provenance, version
receipts, effective set, FTS and history. A crash/failure leaves none or all.
No original source content is automatically copied into reports or logs.

Roadmap order: bounded built-in memory (done), Telegram topics/parallelism/rich
delivery (done), this explicit import, then iroh **and replicated-state checks**,
remote Codex, packaging. Networking and distributed consistency are not done.
