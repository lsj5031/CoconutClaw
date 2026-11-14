# Phase 0 migration

This version supports Telegram polling and Codex only. There is no automatic
configuration rewrite, credential change, data move, scheduler change or deletion.

1. Stop the old runtime yourself before switching binaries. Back up its complete
   data directory, including SQLite WAL/SHM files when applicable, configuration,
   memories, attachments and histories. Keep the old binary available.
2. Choose the existing directory containing `state.db`, `config.toml`, `MEMORY.md`
   and `TASKS/`. Pass that exact directory to `--data-dir`. The default still
   selects the old `default` directory. Named assistant selection (`--instance`,
   `COCONUTCLAW_INSTANCE`) and `--instance-dir`/`INSTANCE_DIR` are removed.
   The former `--data-dir /parent --instance name` becomes
   `--data-dir /parent/name`. No directories are moved or removed.
3. Make a backed-up, edited `config.toml` using `config.toml.example`.
   Retained values now have strict TOML types and documented numeric domains.
   Invalid supplied values (including environment overrides) are rejected rather
   than silently selecting defaults. Paths must be nonempty and must not place
   state files over config, memory or configured directories. URLs must be
   HTTP(S) without embedded credentials, query or fragment.
   Remove `AGENT_PROVIDER`, all `PI_*`, `CLAUDE_*`, `OPENCODE_*`,
   `ANTIGRAVITY_*`, `FACTORY_*`, `SLACK_*`, `ASR_*`, `TTS_*`, `VOICE_*`,
   `NIGHTLY_*`, `HEARTBEAT_*`, `VISION_*`, `WEBHOOK_*` settings and
   `SCHEDULED_TASKS_ENABLED`, even when empty/off. The loader rejects these
   rather than silently switching behavior. Unset their old environment
   equivalents as well. Host tooling's unrelated `FACTORY_*` variables are
   not runtime settings.
4. Retain Telegram credentials/chat restrictions and `CODEX_BIN`/`CODEX_MODEL`.
   Authenticate Codex separately on the host. Review `EXEC_POLICY`; new configs
   default to `safe`, while an explicitly retained `yolo` still bypasses Codex
   approvals/sandbox. `once` invokes Codex and is not a dry run.
   `PROVIDER_MAX_RETRIES` defaults to zero. A nonzero value explicitly opts into
   retrying transient provider failures and can repeat effects; avoid it for
   consequential work. Timeouts are never counted as successful execution.
5. Legacy `.env` files are left intact. If no TOML file exists, loading stops
   with conversion instructions. Manually convert retained keys to TOML;
   do not copy obsolete keys. If TOML exists, the old `.env` remains untouched.
6. Inspect `doctor`, then explicitly start the runtime when ready. Existing
   service definitions need manual review: old `--instance-dir` arguments no
   longer work. The new service commands manage only `coconutclaw.service` or
   `io.coconutclaw.run`. They never disable/delete old heartbeat/reflection
   timers, launch agents or named services. Those obsolete scheduled invocations
   must be reviewed and disabled manually by the operator. No scheduler operation
   was performed as part of this implementation.

## Data compatibility

SQLite histories, memories, tasks, old schedule rows and approval records remain.
The task payload/result outbox is an additive table. Legacy voice/ASR columns
remain readable as historical data; no voice processing runs. Schedule rows
are inactive and are not backfilled, dispatched or deleted. Historical Slack
records remain in SQLite but cannot route messages or resume approvals.
Legacy non-Telegram inflight input stops recovery with a migration error and
is preserved for manual reconciliation.

Very old databases with duplicate non-null task update IDs are rejected with
an actionable error rather than deduplicated destructively. Back up the DB and
inspect those IDs and their turn/approval relationships before manually assigning
unique identifiers. No task/turn/approval rows are removed automatically.

Queued requests saved by this runtime can start after restart if they were
never marked started. Started attempts without a persisted final turn are
outcome-unknown, not automatically re-executed. Existing legacy queued requests
without the new payload cannot be reconstructed safely and require manual review.
Approvals stay pending across restart; explicit acceptance atomically records a
new attempt. Unknown attempts may have left an orphan Codex process after a hard
runtime kill: inspect the host before explicitly retrying consequential work.

Existing user-edited `SOUL.md`/`USER.md`/`MEMORY.md` files are never overwritten
by templates. Review old voice/scheduling/provider instructions in them manually.
`VOICE_REPLY` and `SCHEDULE_PROMPT` are ignored marker boundaries, not supported
output effects. The retained output contract is listed in `AGENTS.md`.

No iroh, remote runners, replication or new agent runtime is included here.
