use crate::markers::{parse_markers, render_output};
use anyhow::{Context, Result};
use chrono::DateTime;
use coconutclaw_config::RuntimeConfig;
use rusqlite::{Connection, params};
use serde_json::json;
use std::fs;

const SCHEMA_SQL: &str = include_str!("../../../sql/schema.sql");

#[derive(Debug)]
pub(crate) struct TurnRecord {
    pub(crate) ts: String,
    pub(crate) chat_id: String,
    pub(crate) input_type: String,
    pub(crate) user_text: String,
    pub(crate) asr_text: String,
    pub(crate) provider_raw: String,
    pub(crate) telegram_reply: String,
    pub(crate) voice_reply: String,
    pub(crate) status: String,
    pub(crate) update_id: Option<String>,
    pub(crate) duration_ms: Option<i64>,
    pub(crate) channel: String,
    pub(crate) task_run_id: Option<i64>,
    pub(crate) side_effects_applied: bool,
}

#[derive(Debug)]
pub(crate) struct ScheduledTask {
    pub(crate) id: i64,
    pub(crate) ts: String,
    pub(crate) source: String,
    pub(crate) prompt: String,
    pub(crate) schedule_time: String,
    pub(crate) recurring: bool,
    pub(crate) last_run_ts: Option<String>,
    pub(crate) done: bool,
    pub(crate) pending_output: Option<String>,
    pub(crate) delivery_state: Option<String>,
    pub(crate) origin_session: Option<String>,
    pub(crate) delivery_target: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct StoredTurnOutput {
    pub(crate) id: i64,
    pub(crate) ts: String,
    pub(crate) provider_raw: String,
    pub(crate) telegram_reply: String,
    pub(crate) voice_reply: String,
    pub(crate) status: String,
    pub(crate) task_run_id: Option<i64>,
    pub(crate) side_effects_applied: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScheduledTaskInsertResult {
    Inserted,
    Duplicate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TaskRunStatus {
    Queued,
    Running,
    AwaitingApproval,
    CancelRequested,
    Completed,
    Failed,
    Cancelled,
}

impl TaskRunStatus {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::AwaitingApproval => "awaiting_approval",
            Self::CancelRequested => "cancel_requested",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct TaskRun {
    pub(crate) id: i64,
    pub(crate) session_id: String,
    pub(crate) channel: String,
    pub(crate) source_chat_id: Option<String>,
    pub(crate) source_user_id: Option<String>,
    pub(crate) update_id: Option<String>,
    pub(crate) prompt: String,
    pub(crate) status: String,
    pub(crate) created_at: String,
    pub(crate) started_at: Option<String>,
    pub(crate) finished_at: Option<String>,
    pub(crate) cancel_requested_at: Option<String>,
    pub(crate) progress_message_id: Option<String>,
    pub(crate) last_progress: Option<String>,
    pub(crate) error_summary: Option<String>,
    pub(crate) result_summary: Option<String>,
    pub(crate) scheduled_task_id: Option<i64>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct ApprovalRecord {
    pub(crate) id: i64,
    pub(crate) task_run_id: i64,
    pub(crate) session_id: String,
    pub(crate) channel: String,
    pub(crate) source_user_id: Option<String>,
    pub(crate) channel_id: Option<String>,
    pub(crate) thread_ts: Option<String>,
    pub(crate) prompt_text: String,
    pub(crate) status: String,
    pub(crate) request_message_ts: Option<String>,
    pub(crate) resume_payload: String,
    pub(crate) created_at: String,
    pub(crate) resolved_at: Option<String>,
    pub(crate) resolved_by_user_id: Option<String>,
}

#[derive(Debug)]
pub(crate) struct InsertTaskRunParams {
    pub(crate) session_id: String,
    pub(crate) channel: String,
    pub(crate) source_chat_id: Option<String>,
    pub(crate) source_user_id: Option<String>,
    pub(crate) update_id: Option<String>,
    pub(crate) prompt: String,
    pub(crate) created_at: String,
    pub(crate) progress_message_id: Option<String>,
    pub(crate) scheduled_task_id: Option<i64>,
}

#[derive(Debug)]
pub(crate) struct CreateApprovalParams {
    pub(crate) task_run_id: i64,
    pub(crate) session_id: String,
    pub(crate) channel: String,
    pub(crate) source_user_id: Option<String>,
    pub(crate) channel_id: Option<String>,
    pub(crate) thread_ts: Option<String>,
    pub(crate) prompt_text: String,
    pub(crate) request_message_ts: Option<String>,
    pub(crate) resume_payload: String,
    pub(crate) created_at: String,
}

pub(crate) struct Store {
    conn: Connection,
}

impl Store {
    pub(crate) fn open(cfg: &RuntimeConfig) -> Result<Self> {
        if let Some(parent) = cfg.sqlite_db_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        let conn = Connection::open(&cfg.sqlite_db_path)
            .with_context(|| format!("failed to open {}", cfg.sqlite_db_path.display()))?;
        conn.execute_batch(SCHEMA_SQL)
            .context("failed to apply sqlite schema")?;
        let store = Self { conn };
        store.run_migrations()?;
        Ok(store)
    }

    /// Run database migrations tracked by the `schema_version` key in `kv`.
    /// Each migration step is guarded by the version number so it runs only once.
    fn run_migrations(&self) -> Result<()> {
        let current: i64 = self
            .kv_get("schema_version")?
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);

        if current < 1 {
            // Migration 1: add duration_ms column and rename legacy codex_raw → provider_raw.
            // These are idempotent — SQLite errors if the column already exists or has been
            // renamed, which we treat as "already applied". We use a transaction so that
            // either both succeed or neither does, but we commit the version only on full
            // success.
            let _ = self
                .conn
                .execute("ALTER TABLE turns ADD COLUMN duration_ms INTEGER", []);
            let _ = self.conn.execute(
                "ALTER TABLE turns RENAME COLUMN codex_raw TO provider_raw",
                [],
            );
            self.kv_set("schema_version", "1")?;
        }

        if current < 2 {
            // Migration 2: add channel column for multi-platform support.
            // Ignores "duplicate column" errors for idempotency, surfaces all other errors.
            match self.conn.execute(
                "ALTER TABLE turns ADD COLUMN channel TEXT NOT NULL DEFAULT 'telegram'",
                [],
            ) {
                Ok(_) => {}
                Err(err) => {
                    // Column may already exist from a prior partial migration
                    let msg = err.to_string();
                    if !msg.contains("duplicate column") {
                        return Err(err.into());
                    }
                }
            }
            self.kv_set("schema_version", "2")?;
        }

        if current < 3 {
            // Migration 3: add pending_output column for scheduled tasks retries
            match self.conn.execute(
                "ALTER TABLE scheduled_tasks ADD COLUMN pending_output TEXT",
                [],
            ) {
                Ok(_) => {}
                Err(err) => {
                    let msg = err.to_string();
                    if !msg.contains("duplicate column") {
                        return Err(err.into());
                    }
                }
            }
            self.kv_set("schema_version", "3")?;
        }

        if current < 4 {
            self.conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS task_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    source_chat_id TEXT,
                    source_user_id TEXT,
                    update_id TEXT,
                    prompt TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    cancel_requested_at TEXT,
                    progress_message_id TEXT,
                    last_progress TEXT,
                    error_summary TEXT,
                    result_summary TEXT,
                    scheduled_task_id INTEGER
                 );
                 CREATE INDEX IF NOT EXISTS idx_task_runs_session_status ON task_runs(session_id, status, id DESC);
                 CREATE INDEX IF NOT EXISTS idx_task_runs_status ON task_runs(status, id DESC);
                 CREATE TABLE IF NOT EXISTS approvals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_run_id INTEGER NOT NULL,
                    session_id TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    source_user_id TEXT,
                    channel_id TEXT,
                    thread_ts TEXT,
                    prompt_text TEXT NOT NULL,
                    status TEXT NOT NULL,
                    request_message_ts TEXT,
                    resume_payload TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    resolved_at TEXT,
                    resolved_by_user_id TEXT
                 );
                 CREATE INDEX IF NOT EXISTS idx_approvals_task_status ON approvals(task_run_id, status, id DESC);",
            )?;
            self.kv_set("schema_version", "4")?;
        }

        if current < 5 {
            match self.conn.execute(
                "ALTER TABLE task_runs ADD COLUMN scheduled_task_id INTEGER",
                [],
            ) {
                Ok(_) => {}
                Err(err) => {
                    let msg = err.to_string();
                    if !msg.contains("duplicate column") {
                        return Err(err.into());
                    }
                }
            }
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_task_runs_scheduled_task_status
                 ON task_runs(scheduled_task_id, status, id DESC)",
                [],
            )?;
            self.kv_set("schema_version", "5")?;
        }

        if current < 6 {
            for sql in [
                "ALTER TABLE turns ADD COLUMN task_run_id INTEGER",
                "ALTER TABLE turns ADD COLUMN side_effects_applied INTEGER NOT NULL DEFAULT 0",
                "ALTER TABLE scheduled_tasks ADD COLUMN delivery_state TEXT",
            ] {
                match self.conn.execute(sql, []) {
                    Ok(_) => {}
                    Err(err) => {
                        let msg = err.to_string();
                        if !msg.contains("duplicate column") {
                            return Err(err.into());
                        }
                    }
                }
            }
            self.kv_set("schema_version", "6")?;
        }

        if current < 7 {
            self.conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS memory_entries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    source TEXT NOT NULL,
                    content TEXT NOT NULL,
                    turn_id INTEGER NOT NULL,
                    append_index INTEGER NOT NULL
                 );
                 CREATE UNIQUE INDEX IF NOT EXISTS idx_memory_entries_turn_append_unique
                   ON memory_entries(turn_id, append_index);",
            )?;
            for sql in [
                "ALTER TABLE tasks ADD COLUMN turn_id INTEGER",
                "ALTER TABLE tasks ADD COLUMN append_index INTEGER",
                "ALTER TABLE tasks ADD COLUMN managed_file INTEGER NOT NULL DEFAULT 0",
            ] {
                match self.conn.execute(sql, []) {
                    Ok(_) => {}
                    Err(err) => {
                        let msg = err.to_string();
                        if !msg.contains("duplicate column") {
                            return Err(err.into());
                        }
                    }
                }
            }
            self.conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_tasks_turn_append_unique
                 ON tasks(turn_id, append_index)",
                [],
            )?;
            self.kv_set("schema_version", "7")?;
        }

        if current < 8 {
            // Migration 8: add unique index on task_runs.update_id for deduplication.
            // This prevents race conditions where the same update_id could create duplicate task runs.
            // The partial index WHERE update_id IS NOT NULL allows multiple NULL values (no dedup for those).
            self.deduplicate_task_run_update_ids()?;
            self.ensure_task_runs_update_id_unique_index()?;
            self.kv_set("schema_version", "8")?;
        }

        if current < 9 {
            // Migration 9: add origin_session and delivery_target for scheduled task scoping.
            // - origin_session: tracks which session the scheduled task belongs to (for context)
            // - delivery_target: JSON-encoded delivery routing (e.g., {"kind":"telegram","chat_id":"123"})
            for sql in [
                "ALTER TABLE scheduled_tasks ADD COLUMN origin_session TEXT",
                "ALTER TABLE scheduled_tasks ADD COLUMN delivery_target TEXT",
            ] {
                match self.conn.execute(sql, []) {
                    Ok(_) => {}
                    Err(err) => {
                        let msg = err.to_string();
                        if !msg.contains("duplicate column") {
                            return Err(err.into());
                        }
                    }
                }
            }
            self.kv_set("schema_version", "9")?;
        }

        if current < 10 {
            self.backfill_legacy_scheduled_task_routing()?;
            self.kv_set("schema_version", "10")?;
        }

        Ok(())
    }

    fn deduplicate_task_run_update_ids(&self) -> Result<()> {
        let tx = self.conn.unchecked_transaction()?;
        let mut duplicates = tx.prepare(
            "SELECT update_id, MAX(id) AS keep_id
             FROM task_runs
             WHERE update_id IS NOT NULL
             GROUP BY update_id
             HAVING COUNT(*) > 1",
        )?;
        let duplicate_groups = duplicates
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(duplicates);

        for (update_id, keep_id) in duplicate_groups {
            let mut duplicate_rows = tx.prepare(
                "SELECT id
                 FROM task_runs
                 WHERE update_id = ?1
                   AND id != ?2",
            )?;
            let duplicate_ids = duplicate_rows
                .query_map(params![update_id, keep_id], |row| row.get::<_, i64>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            drop(duplicate_rows);

            for duplicate_id in duplicate_ids {
                tx.execute(
                    "UPDATE turns
                     SET task_run_id = ?1
                     WHERE task_run_id = ?2",
                    params![keep_id, duplicate_id],
                )?;
                tx.execute(
                    "UPDATE approvals
                     SET task_run_id = ?1
                     WHERE task_run_id = ?2",
                    params![keep_id, duplicate_id],
                )?;
                tx.execute("DELETE FROM task_runs WHERE id = ?1", params![duplicate_id])?;
            }
        }

        tx.commit()?;
        Ok(())
    }

    fn ensure_task_runs_update_id_unique_index(&self) -> Result<()> {
        self.conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_task_runs_update_id_unique
             ON task_runs(update_id) WHERE update_id IS NOT NULL",
            [],
        )?;
        Ok(())
    }

    fn backfill_legacy_scheduled_task_routing(&self) -> Result<()> {
        let tx = self.conn.unchecked_transaction()?;
        let mut tasks = tx.prepare(
            "SELECT id, ts, prompt, schedule_time, recurring, origin_session, delivery_target
             FROM scheduled_tasks
             WHERE origin_session IS NULL
                OR delivery_target IS NULL
             ORDER BY id ASC",
        )?;
        let scheduled_tasks = tasks
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i32>(4)? != 0,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(tasks);

        for (
            scheduled_task_id,
            ts,
            prompt,
            schedule_time,
            recurring,
            origin_session,
            delivery_target,
        ) in scheduled_tasks
        {
            let Some((matched_origin_session, matched_delivery_target)) =
                find_origin_turn_routing_for_schedule(
                    &tx,
                    &ts,
                    &prompt,
                    &schedule_time,
                    recurring,
                )?
            else {
                continue;
            };

            tx.execute(
                "UPDATE scheduled_tasks
                 SET origin_session = COALESCE(origin_session, ?2),
                     delivery_target = COALESCE(delivery_target, ?3)
                 WHERE id = ?1",
                params![
                    scheduled_task_id,
                    origin_session.or(Some(matched_origin_session)),
                    delivery_target.or(Some(matched_delivery_target))
                ],
            )?;
        }

        tx.commit()?;
        Ok(())
    }

    pub(crate) fn recent_turns_snippet(
        &self,
        limit: u32,
        chat_id: &str,
        channel: &str,
    ) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare(
            "SELECT ts || ' | in=' || COALESCE(REPLACE(user_text, char(10), ' '), '') || ' | out=' || COALESCE(REPLACE(COALESCE(telegram_reply, voice_reply), char(10), ' '), '')
             FROM turns
             WHERE status != 'boundary'
               AND chat_id = ?1
               AND channel = ?2
               AND id > COALESCE((SELECT MAX(id) FROM turns WHERE user_text = '---CONTEXT_BOUNDARY---' AND chat_id = ?1 AND channel = ?2), 0)
             ORDER BY id DESC
             LIMIT ?3",
        )?;

        let mut rows = stmt.query(params![chat_id, channel, limit])?;
        let mut lines = Vec::new();
        while let Some(row) = rows.next()? {
            lines.push(row.get::<_, String>(0)?);
        }
        Ok(lines)
    }

    pub(crate) fn latest_boundary_unix(&self, chat_id: &str, channel: &str) -> Result<Option<i64>> {
        let mut stmt = self.conn.prepare(
            "SELECT ts
             FROM turns
             WHERE status = 'boundary'
               AND chat_id = ?1
               AND channel = ?2
             ORDER BY id DESC
             LIMIT 1",
        )?;

        let mut rows = stmt.query(params![chat_id, channel])?;
        let Some(row) = rows.next()? else {
            return Ok(None);
        };

        let ts: String = row.get(0)?;
        Ok(DateTime::parse_from_str(&ts, "%Y-%m-%dT%H:%M:%S%z")
            .ok()
            .map(|dt| dt.timestamp()))
    }

    pub(crate) fn insert_turn(&self, turn: &TurnRecord) -> Result<Option<i64>> {
        self.conn.execute(
            "INSERT OR IGNORE INTO turns(ts, chat_id, input_type, user_text, asr_text, provider_raw, telegram_reply, voice_reply, status, update_id, duration_ms, channel, task_run_id, side_effects_applied)
             VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            params![
                turn.ts,
                turn.chat_id,
                turn.input_type,
                turn.user_text,
                turn.asr_text,
                turn.provider_raw,
                turn.telegram_reply,
                turn.voice_reply,
                turn.status,
                turn.update_id,
                turn.duration_ms,
                turn.channel,
                turn.task_run_id,
                turn.side_effects_applied as i32,
            ],
        )?;
        if self.conn.changes() > 0 {
            Ok(Some(self.conn.last_insert_rowid()))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn insert_memory_and_tasks(
        &mut self,
        ts: &str,
        source: &str,
        turn_id: Option<i64>,
        memory_lines: &[String],
        task_lines: &[String],
    ) -> Result<()> {
        if memory_lines.is_empty() && task_lines.is_empty() {
            return Ok(());
        }

        let tx = self.conn.transaction()?;

        if let Some(turn_id) = turn_id {
            if !memory_lines.is_empty() {
                let mut stmt = tx.prepare_cached(
                    "INSERT OR IGNORE INTO memory_entries(ts, source, content, turn_id, append_index)
                     VALUES(?1, ?2, ?3, ?4, ?5)",
                )?;
                for (append_index, line) in memory_lines.iter().enumerate() {
                    stmt.execute(params![ts, source, line, turn_id, append_index as i64])?;
                }
            }

            if !task_lines.is_empty() {
                let mut stmt = tx.prepare_cached(
                    "INSERT OR IGNORE INTO tasks(ts, source, content, done, turn_id, append_index, managed_file)
                     VALUES(?1, ?2, ?3, 0, ?4, ?5, 1)",
                )?;

                for (append_index, line) in task_lines.iter().enumerate() {
                    stmt.execute(params![ts, source, line, turn_id, append_index as i64])?;
                }
            }
        } else if !memory_lines.is_empty() || !task_lines.is_empty() {
            anyhow::bail!("memory/task appends require a persisted turn id");
        }

        tx.commit()?;

        Ok(())
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn insert_scheduled_task(
        &self,
        ts: &str,
        source: &str,
        prompt: &str,
        schedule_time: &str,
        recurring: bool,
    ) -> Result<ScheduledTaskInsertResult> {
        self.insert_scheduled_task_with_target(
            ts,
            source,
            prompt,
            schedule_time,
            recurring,
            None,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn insert_scheduled_task_with_target(
        &self,
        ts: &str,
        source: &str,
        prompt: &str,
        schedule_time: &str,
        recurring: bool,
        origin_session: Option<&str>,
        delivery_target: Option<&str>,
    ) -> Result<ScheduledTaskInsertResult> {
        // Deduplicate: reject only when source, schedule shape, origin session, and delivery
        // target all match an existing active row.
        let existing: i64 = self.conn.query_row(
            "SELECT COUNT(*)
             FROM scheduled_tasks
             WHERE source = ?1
               AND prompt = ?2
               AND schedule_time = ?3
               AND done = 0
               AND COALESCE(origin_session, '') = COALESCE(?4, '')
               AND (
                    (delivery_target IS NULL AND ?5 IS NULL)
                    OR delivery_target = ?5
               )",
            params![
                source,
                prompt,
                schedule_time,
                origin_session,
                delivery_target
            ],
            |row| row.get(0),
        )?;
        if existing > 0 {
            return Ok(ScheduledTaskInsertResult::Duplicate);
        }

        self.conn.execute(
            "INSERT INTO scheduled_tasks(ts, source, prompt, schedule_time, recurring, done, pending_output, delivery_state, origin_session, delivery_target)
             VALUES(?1, ?2, ?3, ?4, ?5, 0, NULL, NULL, ?6, ?7)",
            params![ts, source, prompt, schedule_time, recurring as i32, origin_session, delivery_target],
        )?;
        Ok(ScheduledTaskInsertResult::Inserted)
    }

    pub(crate) fn list_active_scheduled_tasks(&self) -> Result<Vec<ScheduledTask>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, ts, source, prompt, schedule_time, recurring, last_run_ts, done, pending_output, delivery_state, origin_session, delivery_target
             FROM scheduled_tasks
             WHERE done = 0
             ORDER BY schedule_time ASC, id ASC",
        )?;
        let mut rows = stmt.query([])?;
        let mut tasks = Vec::new();
        while let Some(row) = rows.next()? {
            tasks.push(ScheduledTask {
                id: row.get(0)?,
                ts: row.get(1)?,
                source: row.get(2)?,
                prompt: row.get(3)?,
                schedule_time: row.get(4)?,
                recurring: row.get::<_, i32>(5)? != 0,
                last_run_ts: row.get(6)?,
                done: row.get::<_, i32>(7)? != 0,
                pending_output: row.get(8)?,
                delivery_state: row.get(9)?,
                origin_session: row.get(10)?,
                delivery_target: row.get(11)?,
            });
        }
        Ok(tasks)
    }

    pub(crate) fn get_scheduled_task(&self, id: i64) -> Result<Option<ScheduledTask>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, ts, source, prompt, schedule_time, recurring, last_run_ts, done, pending_output, delivery_state, origin_session, delivery_target
             FROM scheduled_tasks
             WHERE id = ?1
             LIMIT 1",
        )?;
        let mut rows = stmt.query(params![id])?;
        let Some(row) = rows.next()? else {
            return Ok(None);
        };
        Ok(Some(ScheduledTask {
            id: row.get(0)?,
            ts: row.get(1)?,
            source: row.get(2)?,
            prompt: row.get(3)?,
            schedule_time: row.get(4)?,
            recurring: row.get::<_, i32>(5)? != 0,
            last_run_ts: row.get(6)?,
            done: row.get::<_, i32>(7)? != 0,
            pending_output: row.get(8)?,
            delivery_state: row.get(9)?,
            origin_session: row.get(10)?,
            delivery_target: row.get(11)?,
        }))
    }

    pub(crate) fn get_due_scheduled_tasks(
        &self,
        current_hhmm: &str,
        today: &str,
    ) -> Result<Vec<ScheduledTask>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, ts, source, prompt, schedule_time, recurring, last_run_ts, done, pending_output, delivery_state, origin_session, delivery_target
             FROM scheduled_tasks
             WHERE schedule_time <= ?1
               AND done = 0
               AND (
                 (last_run_ts IS NOT NULL AND substr(last_run_ts, 1, 10) < ?2)
                 OR
                 (last_run_ts IS NULL AND (
                    substr(ts, 1, 10) < ?2 OR substr(ts, 12, 5) <= schedule_time
                 ))
               )
             ORDER BY schedule_time ASC",
        )?;
        let mut rows = stmt.query(params![current_hhmm, today])?;
        let mut tasks = Vec::new();
        while let Some(row) = rows.next()? {
            tasks.push(ScheduledTask {
                id: row.get(0)?,
                ts: row.get(1)?,
                source: row.get(2)?,
                prompt: row.get(3)?,
                schedule_time: row.get(4)?,
                recurring: row.get::<_, i32>(5)? != 0,
                last_run_ts: row.get(6)?,
                done: row.get::<_, i32>(7)? != 0,
                pending_output: row.get(8)?,
                delivery_state: row.get(9)?,
                origin_session: row.get(10)?,
                delivery_target: row.get(11)?,
            });
        }
        Ok(tasks)
    }

    pub(crate) fn mark_scheduled_task_executed(&self, id: i64, ts: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE scheduled_tasks
             SET last_run_ts = ?1,
                 done = CASE WHEN recurring = 0 THEN 1 ELSE 0 END,
                 pending_output = NULL,
                 delivery_state = NULL
             WHERE id = ?2",
            params![ts, id],
        )?;
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn set_scheduled_task_pending_output(&self, id: i64, output: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE scheduled_tasks
             SET pending_output = ?1,
                 delivery_state = NULL
             WHERE id = ?2",
            params![output, id],
        )?;
        Ok(())
    }

    pub(crate) fn set_scheduled_task_delivery_state(
        &self,
        id: i64,
        delivery_state: Option<&str>,
    ) -> Result<()> {
        self.conn.execute(
            "UPDATE scheduled_tasks SET delivery_state = ?1 WHERE id = ?2",
            params![delivery_state, id],
        )?;
        Ok(())
    }

    pub(crate) fn set_scheduled_task_routing(
        &self,
        id: i64,
        origin_session: Option<&str>,
        delivery_target: Option<&str>,
    ) -> Result<()> {
        self.conn.execute(
            "UPDATE scheduled_tasks
             SET origin_session = COALESCE(origin_session, ?2),
                 delivery_target = COALESCE(delivery_target, ?3)
             WHERE id = ?1",
            params![id, origin_session, delivery_target],
        )?;
        Ok(())
    }

    pub(crate) fn insert_boundary_turn(
        &self,
        ts: &str,
        chat_id: &str,
        update_id: Option<&str>,
        channel: &str,
    ) -> Result<bool> {
        self.conn.execute(
            "INSERT OR IGNORE INTO turns(ts, chat_id, input_type, user_text, asr_text, provider_raw, telegram_reply, voice_reply, status, update_id, channel)
             VALUES(?1, ?2, 'system', '---CONTEXT_BOUNDARY---', '', '', '', '', 'boundary', ?3, ?4)",
            params![ts, chat_id, update_id, channel],
        )?;
        Ok(self.conn.changes() > 0)
    }

    pub(crate) fn turn_exists_for_update_id(&self, update_id: &str) -> Result<bool> {
        let mut stmt = self
            .conn
            .prepare("SELECT 1 FROM turns WHERE update_id = ?1 LIMIT 1")?;
        let mut rows = stmt.query(params![update_id])?;
        Ok(rows.next()?.is_some())
    }

    pub(crate) fn rendered_output_for_update_id(&self, update_id: &str) -> Result<Option<String>> {
        let mut stmt = self.conn.prepare(
            "SELECT provider_raw, telegram_reply, voice_reply
             FROM turns
             WHERE update_id = ?1
             ORDER BY id DESC
             LIMIT 1",
        )?;
        let mut rows = stmt.query(params![update_id])?;
        let Some(row) = rows.next()? else {
            return Ok(None);
        };

        let provider_raw: String = row.get(0)?;
        let telegram_reply: String = row.get(1)?;
        let voice_reply: String = row.get(2)?;
        let mut markers = parse_markers(&provider_raw);
        if !telegram_reply.trim().is_empty() {
            markers.telegram_reply = Some(telegram_reply.clone());
        }
        if !voice_reply.trim().is_empty() {
            markers.voice_reply = Some(voice_reply.clone());
        }

        let rendered = render_output(
            markers.telegram_reply.as_deref().unwrap_or_default(),
            markers.voice_reply.as_deref().unwrap_or_default(),
            &markers,
        );
        Ok(Some(rendered.trim_end().to_string()))
    }

    pub(crate) fn turn_output_for_task_run(
        &self,
        task_run_id: i64,
    ) -> Result<Option<StoredTurnOutput>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, ts, provider_raw, telegram_reply, voice_reply, status, task_run_id, side_effects_applied
             FROM turns
             WHERE task_run_id = ?1
             ORDER BY id DESC
             LIMIT 1",
        )?;
        let mut rows = stmt.query(params![task_run_id])?;
        let Some(row) = rows.next()? else {
            return Ok(None);
        };
        Ok(Some(StoredTurnOutput {
            id: row.get(0)?,
            ts: row.get(1)?,
            provider_raw: row.get(2)?,
            telegram_reply: row.get(3)?,
            voice_reply: row.get(4)?,
            status: row.get(5)?,
            task_run_id: row.get(6)?,
            side_effects_applied: row.get::<_, i32>(7)? != 0,
        }))
    }

    pub(crate) fn managed_memory_entries(&self) -> Result<Vec<(String, String)>> {
        let mut stmt = self.conn.prepare(
            "SELECT ts, content
             FROM memory_entries
             ORDER BY id ASC",
        )?;
        let mut rows = stmt.query([])?;
        let mut entries = Vec::new();
        while let Some(row) = rows.next()? {
            entries.push((row.get(0)?, row.get(1)?));
        }
        Ok(entries)
    }

    pub(crate) fn managed_pending_task_entries(&self) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare(
            "SELECT content
             FROM tasks
             WHERE managed_file = 1
               AND done = 0
             ORDER BY id ASC",
        )?;
        let mut rows = stmt.query([])?;
        let mut entries = Vec::new();
        while let Some(row) = rows.next()? {
            entries.push(row.get(0)?);
        }
        Ok(entries)
    }

    pub(crate) fn update_turn_reply_and_side_effects_by_id(
        &self,
        turn_id: i64,
        telegram_reply: &str,
        voice_reply: &str,
    ) -> Result<()> {
        self.conn.execute(
            "UPDATE turns
             SET telegram_reply = ?2,
                 voice_reply = ?3,
                 side_effects_applied = 1
             WHERE id = ?1",
            params![turn_id, telegram_reply, voice_reply],
        )?;
        Ok(())
    }

    pub(crate) fn pending_turn_side_effects(&self) -> Result<Vec<StoredTurnOutput>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, ts, provider_raw, telegram_reply, voice_reply, status, task_run_id, side_effects_applied
             FROM turns
             WHERE side_effects_applied = 0
               AND status NOT IN ('cancelled', 'boundary')
             ORDER BY id ASC",
        )?;
        let mut rows = stmt.query([])?;
        let mut turns = Vec::new();
        while let Some(row) = rows.next()? {
            turns.push(StoredTurnOutput {
                id: row.get(0)?,
                ts: row.get(1)?,
                provider_raw: row.get(2)?,
                telegram_reply: row.get(3)?,
                voice_reply: row.get(4)?,
                status: row.get(5)?,
                task_run_id: row.get(6)?,
                side_effects_applied: row.get::<_, i32>(7)? != 0,
            });
        }
        Ok(turns)
    }

    pub(crate) fn kv_get(&self, key: &str) -> Result<Option<String>> {
        let mut stmt = self
            .conn
            .prepare("SELECT value FROM kv WHERE key = ?1 LIMIT 1")?;
        let mut rows = stmt.query(params![key])?;
        if let Some(row) = rows.next()? {
            return Ok(Some(row.get::<_, String>(0)?));
        }
        Ok(None)
    }

    pub(crate) fn kv_set(&self, key: &str, value: &str) -> Result<()> {
        self.conn.execute(
            "INSERT INTO kv(key, value) VALUES(?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![key, value],
        )?;
        Ok(())
    }

    pub(crate) fn clear_inflight(&self) -> Result<()> {
        self.conn.execute(
            "DELETE FROM kv WHERE key IN ('inflight_update_id', 'inflight_update_json', 'inflight_started_at')",
            [],
        )?;
        Ok(())
    }

    pub(crate) fn max_turn_id(&self) -> Result<i64> {
        let id: i64 = self
            .conn
            .query_row("SELECT COALESCE(MAX(id), 0) FROM turns", [], |row| {
                row.get(0)
            })?;
        Ok(id)
    }

    pub(crate) fn latest_turn_for_prompt_after_id(
        &self,
        after_id: i64,
        prompt: &str,
    ) -> Result<Option<(String, String, String)>> {
        let mut stmt = self.conn.prepare(
            "SELECT ts, COALESCE(NULLIF(telegram_reply, ''), NULLIF(voice_reply, ''), ''), status
             FROM turns
             WHERE id > ?1
               AND user_text = ?2
             ORDER BY id DESC
             LIMIT 1",
        )?;
        let mut rows = stmt.query(params![after_id, prompt])?;
        let Some(row) = rows.next()? else {
            return Ok(None);
        };
        Ok(Some((row.get(0)?, row.get(1)?, row.get(2)?)))
    }

    pub(crate) fn insert_task_run(&self, params: InsertTaskRunParams) -> Result<i64> {
        // Use INSERT OR IGNORE to handle UNIQUE constraint violations gracefully.
        // This prevents race conditions where the same update_id could create duplicate task runs.
        self.conn.execute(
            "INSERT OR IGNORE INTO task_runs(
                session_id, channel, source_chat_id, source_user_id, update_id, prompt, status,
                created_at, progress_message_id, scheduled_task_id
             ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                params.session_id,
                params.channel,
                params.source_chat_id,
                params.source_user_id,
                params.update_id,
                params.prompt,
                TaskRunStatus::Queued.as_str(),
                params.created_at,
                params.progress_message_id,
                params.scheduled_task_id
            ],
        )?;
        if self.conn.changes() == 0 {
            // UNIQUE constraint was violated - duplicate update_id detected
            anyhow::bail!(
                "duplicate update_id rejected by constraint: {:?}",
                params.update_id
            );
        }
        Ok(self.conn.last_insert_rowid())
    }

    pub(crate) fn update_task_run_started(&self, id: i64, started_at: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE task_runs
             SET status = ?2,
                 started_at = ?3
             WHERE id = ?1",
            params![id, TaskRunStatus::Running.as_str(), started_at],
        )?;
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn update_task_run_progress(&self, id: i64, last_progress: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE task_runs SET last_progress = ?2 WHERE id = ?1",
            params![id, last_progress],
        )?;
        Ok(())
    }

    pub(crate) fn mark_task_run_awaiting_approval(&self, id: i64) -> Result<()> {
        self.conn.execute(
            "UPDATE task_runs SET status = ?2 WHERE id = ?1",
            params![id, TaskRunStatus::AwaitingApproval.as_str()],
        )?;
        Ok(())
    }

    pub(crate) fn mark_task_run_cancel_requested(&self, id: i64, ts: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE task_runs
             SET status = CASE
                    WHEN status IN ('queued', 'running', 'awaiting_approval') THEN ?2
                    ELSE status
                 END,
                 cancel_requested_at = COALESCE(cancel_requested_at, ?3)
             WHERE id = ?1",
            params![id, TaskRunStatus::CancelRequested.as_str(), ts],
        )?;
        Ok(())
    }

    pub(crate) fn finish_task_run(
        &self,
        id: i64,
        status: TaskRunStatus,
        finished_at: &str,
        error_summary: Option<&str>,
        result_summary: Option<&str>,
    ) -> Result<()> {
        self.conn.execute(
            "UPDATE task_runs
             SET status = ?2,
                 finished_at = ?3,
                 error_summary = COALESCE(?4, error_summary),
                 result_summary = COALESCE(?5, result_summary)
             WHERE id = ?1",
            params![
                id,
                status.as_str(),
                finished_at,
                error_summary,
                result_summary
            ],
        )?;
        Ok(())
    }

    pub(crate) fn get_task_run(&self, id: i64) -> Result<Option<TaskRun>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, session_id, channel, source_chat_id, source_user_id, update_id, prompt, status,
                    created_at, started_at, finished_at, cancel_requested_at, progress_message_id,
                    last_progress, error_summary, result_summary, scheduled_task_id
             FROM task_runs
             WHERE id = ?1
             LIMIT 1",
        )?;
        let mut rows = stmt.query(params![id])?;
        let Some(row) = rows.next()? else {
            return Ok(None);
        };
        Ok(Some(TaskRun {
            id: row.get(0)?,
            session_id: row.get(1)?,
            channel: row.get(2)?,
            source_chat_id: row.get(3)?,
            source_user_id: row.get(4)?,
            update_id: row.get(5)?,
            prompt: row.get(6)?,
            status: row.get(7)?,
            created_at: row.get(8)?,
            started_at: row.get(9)?,
            finished_at: row.get(10)?,
            cancel_requested_at: row.get(11)?,
            progress_message_id: row.get(12)?,
            last_progress: row.get(13)?,
            error_summary: row.get(14)?,
            result_summary: row.get(15)?,
            scheduled_task_id: row.get(16)?,
        }))
    }

    #[cfg(test)]
    pub(crate) fn list_active_task_runs(&self) -> Result<Vec<TaskRun>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, session_id, channel, source_chat_id, source_user_id, update_id, prompt, status,
                    created_at, started_at, finished_at, cancel_requested_at, progress_message_id,
                    last_progress, error_summary, result_summary, scheduled_task_id
             FROM task_runs
             WHERE status IN ('queued', 'running', 'awaiting_approval', 'cancel_requested')
             ORDER BY id ASC",
        )?;
        let mut rows = stmt.query([])?;
        let mut tasks = Vec::new();
        while let Some(row) = rows.next()? {
            tasks.push(TaskRun {
                id: row.get(0)?,
                session_id: row.get(1)?,
                channel: row.get(2)?,
                source_chat_id: row.get(3)?,
                source_user_id: row.get(4)?,
                update_id: row.get(5)?,
                prompt: row.get(6)?,
                status: row.get(7)?,
                created_at: row.get(8)?,
                started_at: row.get(9)?,
                finished_at: row.get(10)?,
                cancel_requested_at: row.get(11)?,
                progress_message_id: row.get(12)?,
                last_progress: row.get(13)?,
                error_summary: row.get(14)?,
                result_summary: row.get(15)?,
                scheduled_task_id: row.get(16)?,
            });
        }
        Ok(tasks)
    }

    pub(crate) fn list_active_task_runs_for_session(
        &self,
        session_id: &str,
    ) -> Result<Vec<TaskRun>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, session_id, channel, source_chat_id, source_user_id, update_id, prompt, status,
                    created_at, started_at, finished_at, cancel_requested_at, progress_message_id,
                    last_progress, error_summary, result_summary, scheduled_task_id
             FROM task_runs
             WHERE session_id = ?1
               AND status IN ('queued', 'running', 'awaiting_approval', 'cancel_requested')
             ORDER BY id ASC",
        )?;
        let mut rows = stmt.query(params![session_id])?;
        let mut tasks = Vec::new();
        while let Some(row) = rows.next()? {
            tasks.push(TaskRun {
                id: row.get(0)?,
                session_id: row.get(1)?,
                channel: row.get(2)?,
                source_chat_id: row.get(3)?,
                source_user_id: row.get(4)?,
                update_id: row.get(5)?,
                prompt: row.get(6)?,
                status: row.get(7)?,
                created_at: row.get(8)?,
                started_at: row.get(9)?,
                finished_at: row.get(10)?,
                cancel_requested_at: row.get(11)?,
                progress_message_id: row.get(12)?,
                last_progress: row.get(13)?,
                error_summary: row.get(14)?,
                result_summary: row.get(15)?,
                scheduled_task_id: row.get(16)?,
            });
        }
        Ok(tasks)
    }

    pub(crate) fn find_active_task_for_session(&self, session_id: &str) -> Result<Option<TaskRun>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, session_id, channel, source_chat_id, source_user_id, update_id, prompt, status,
                    created_at, started_at, finished_at, cancel_requested_at, progress_message_id,
                    last_progress, error_summary, result_summary, scheduled_task_id
             FROM task_runs
             WHERE session_id = ?1
               AND status IN ('queued', 'running', 'awaiting_approval', 'cancel_requested')
             ORDER BY CASE status
                    WHEN 'running' THEN 0
                    WHEN 'awaiting_approval' THEN 1
                    WHEN 'cancel_requested' THEN 2
                    ELSE 3
               END,
               id ASC
             LIMIT 1",
        )?;
        let mut rows = stmt.query(params![session_id])?;
        let Some(row) = rows.next()? else {
            return Ok(None);
        };
        Ok(Some(TaskRun {
            id: row.get(0)?,
            session_id: row.get(1)?,
            channel: row.get(2)?,
            source_chat_id: row.get(3)?,
            source_user_id: row.get(4)?,
            update_id: row.get(5)?,
            prompt: row.get(6)?,
            status: row.get(7)?,
            created_at: row.get(8)?,
            started_at: row.get(9)?,
            finished_at: row.get(10)?,
            cancel_requested_at: row.get(11)?,
            progress_message_id: row.get(12)?,
            last_progress: row.get(13)?,
            error_summary: row.get(14)?,
            result_summary: row.get(15)?,
            scheduled_task_id: row.get(16)?,
        }))
    }

    pub(crate) fn find_active_task_run_by_update_id(
        &self,
        update_id: &str,
    ) -> Result<Option<TaskRun>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, session_id, channel, source_chat_id, source_user_id, update_id, prompt, status,
                    created_at, started_at, finished_at, cancel_requested_at, progress_message_id,
                    last_progress, error_summary, result_summary, scheduled_task_id
             FROM task_runs
             WHERE update_id = ?1
               AND status IN ('queued', 'running', 'awaiting_approval', 'cancel_requested')
             ORDER BY id DESC
             LIMIT 1",
        )?;
        let mut rows = stmt.query(params![update_id])?;
        let Some(row) = rows.next()? else {
            return Ok(None);
        };
        Ok(Some(TaskRun {
            id: row.get(0)?,
            session_id: row.get(1)?,
            channel: row.get(2)?,
            source_chat_id: row.get(3)?,
            source_user_id: row.get(4)?,
            update_id: row.get(5)?,
            prompt: row.get(6)?,
            status: row.get(7)?,
            created_at: row.get(8)?,
            started_at: row.get(9)?,
            finished_at: row.get(10)?,
            cancel_requested_at: row.get(11)?,
            progress_message_id: row.get(12)?,
            last_progress: row.get(13)?,
            error_summary: row.get(14)?,
            result_summary: row.get(15)?,
            scheduled_task_id: row.get(16)?,
        }))
    }

    pub(crate) fn latest_task_run_for_scheduled_task(
        &self,
        scheduled_task_id: i64,
    ) -> Result<Option<TaskRun>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, session_id, channel, source_chat_id, source_user_id, update_id, prompt, status,
                    created_at, started_at, finished_at, cancel_requested_at, progress_message_id,
                    last_progress, error_summary, result_summary, scheduled_task_id
             FROM task_runs
             WHERE scheduled_task_id = ?1
             ORDER BY id DESC
             LIMIT 1",
        )?;
        let mut rows = stmt.query(params![scheduled_task_id])?;
        let Some(row) = rows.next()? else {
            return Ok(None);
        };
        Ok(Some(TaskRun {
            id: row.get(0)?,
            session_id: row.get(1)?,
            channel: row.get(2)?,
            source_chat_id: row.get(3)?,
            source_user_id: row.get(4)?,
            update_id: row.get(5)?,
            prompt: row.get(6)?,
            status: row.get(7)?,
            created_at: row.get(8)?,
            started_at: row.get(9)?,
            finished_at: row.get(10)?,
            cancel_requested_at: row.get(11)?,
            progress_message_id: row.get(12)?,
            last_progress: row.get(13)?,
            error_summary: row.get(14)?,
            result_summary: row.get(15)?,
            scheduled_task_id: row.get(16)?,
        }))
    }

    pub(crate) fn reconcile_scheduled_tasks_from_completed_runs(
        &self,
        fallback_ts: &str,
    ) -> Result<usize> {
        self.conn.execute(
            "UPDATE scheduled_tasks
             SET last_run_ts = COALESCE(
                    (
                        SELECT finished_at
                        FROM task_runs
                        WHERE scheduled_task_id = scheduled_tasks.id
                          AND status = 'completed'
                          AND finished_at IS NOT NULL
                        ORDER BY id DESC
                        LIMIT 1
                    ),
                    ?1
                 ),
                 done = CASE WHEN recurring = 0 THEN 1 ELSE 0 END,
                 pending_output = NULL
             WHERE done = 0
               AND pending_output IS NOT NULL
               AND EXISTS (
                    SELECT 1
                    FROM task_runs
                    WHERE scheduled_task_id = scheduled_tasks.id
                      AND status = 'completed'
               )",
            params![fallback_ts],
        )?;
        Ok(self.conn.changes() as usize)
    }

    pub(crate) fn create_approval(&self, params: CreateApprovalParams) -> Result<i64> {
        self.conn.execute(
            "INSERT INTO approvals(
                task_run_id, session_id, channel, source_user_id, channel_id, thread_ts, prompt_text,
                status, request_message_ts, resume_payload, created_at
             ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, 'pending', ?8, ?9, ?10)",
            params![
                params.task_run_id,
                params.session_id,
                params.channel,
                params.source_user_id,
                params.channel_id,
                params.thread_ts,
                params.prompt_text,
                params.request_message_ts,
                params.resume_payload,
                params.created_at
            ],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    pub(crate) fn pending_approval_resume_payload_for_task(
        &self,
        task_run_id: i64,
    ) -> Result<Option<String>> {
        let mut stmt = self.conn.prepare(
            "SELECT resume_payload
             FROM approvals
             WHERE task_run_id = ?1
               AND status = 'pending'
             ORDER BY id DESC
             LIMIT 1",
        )?;
        let mut rows = stmt.query(params![task_run_id])?;
        let Some(row) = rows.next()? else {
            return Ok(None);
        };
        Ok(Some(row.get(0)?))
    }

    pub(crate) fn approval_resume_payloads_by_status(&self, status: &str) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare(
            "SELECT resume_payload
             FROM approvals
             WHERE status = ?1
               AND resume_payload != ''
             ORDER BY id ASC",
        )?;
        let mut rows = stmt.query(params![status])?;
        let mut payloads = Vec::new();
        while let Some(row) = rows.next()? {
            payloads.push(row.get(0)?);
        }
        Ok(payloads)
    }

    pub(crate) fn get_pending_approval(&self, approval_id: i64) -> Result<Option<ApprovalRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, task_run_id, session_id, channel, source_user_id, channel_id, thread_ts,
                    prompt_text, status, request_message_ts, resume_payload, created_at,
                    resolved_at, resolved_by_user_id
             FROM approvals
             WHERE id = ?1
             LIMIT 1",
        )?;
        let mut rows = stmt.query(params![approval_id])?;
        let Some(row) = rows.next()? else {
            return Ok(None);
        };
        Ok(Some(ApprovalRecord {
            id: row.get(0)?,
            task_run_id: row.get(1)?,
            session_id: row.get(2)?,
            channel: row.get(3)?,
            source_user_id: row.get(4)?,
            channel_id: row.get(5)?,
            thread_ts: row.get(6)?,
            prompt_text: row.get(7)?,
            status: row.get(8)?,
            request_message_ts: row.get(9)?,
            resume_payload: row.get(10)?,
            created_at: row.get(11)?,
            resolved_at: row.get(12)?,
            resolved_by_user_id: row.get(13)?,
        }))
    }

    pub(crate) fn resolve_approval(
        &self,
        approval_id: i64,
        status: &str,
        resolved_at: &str,
        resolved_by_user_id: Option<&str>,
    ) -> Result<()> {
        self.conn.execute(
            "UPDATE approvals
             SET status = ?2,
                 resolved_at = ?3,
                 resolved_by_user_id = ?4
             WHERE id = ?1",
            params![approval_id, status, resolved_at, resolved_by_user_id],
        )?;
        Ok(())
    }

    pub(crate) fn update_approval_request_message_ts(
        &self,
        approval_id: i64,
        request_message_ts: &str,
    ) -> Result<()> {
        self.conn.execute(
            "UPDATE approvals
             SET request_message_ts = ?2
             WHERE id = ?1",
            params![approval_id, request_message_ts],
        )?;
        Ok(())
    }

    pub(crate) fn expire_pending_approvals_for_task(
        &self,
        task_run_id: i64,
        resolved_at: &str,
    ) -> Result<()> {
        self.conn.execute(
            "UPDATE approvals
             SET status = CASE WHEN status = 'pending' THEN 'expired' ELSE status END,
                 resolved_at = CASE WHEN status = 'pending' THEN COALESCE(resolved_at, ?2) ELSE resolved_at END
             WHERE task_run_id = ?1",
            params![task_run_id, resolved_at],
        )?;
        Ok(())
    }

    pub(crate) fn mark_stale_task_runs_failed(&self, ts: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE task_runs
             SET status = 'failed',
                 finished_at = COALESCE(finished_at, ?1),
                 error_summary = COALESCE(error_summary, 'runtime restarted before task completion')
             WHERE status IN ('queued', 'running', 'awaiting_approval', 'cancel_requested')",
            params![ts],
        )?;
        self.conn.execute(
            "UPDATE approvals
             SET status = CASE WHEN status = 'pending' THEN 'expired' ELSE status END,
                 resolved_at = CASE WHEN status = 'pending' THEN COALESCE(resolved_at, ?1) ELSE resolved_at END
             WHERE status = 'pending'",
            params![ts],
        )?;
        Ok(())
    }
}

fn find_origin_turn_routing_for_schedule(
    tx: &rusqlite::Transaction<'_>,
    ts: &str,
    prompt: &str,
    schedule_time: &str,
    recurring: bool,
) -> Result<Option<(String, String)>> {
    let mut turns = tx.prepare(
        "SELECT chat_id, channel, provider_raw
         FROM turns
         WHERE ts = ?1
           AND status NOT IN ('boundary', 'cancelled')
         ORDER BY id DESC",
    )?;
    let rows = turns
        .query_map(params![ts], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    for (chat_id, channel, provider_raw) in rows {
        let markers = parse_markers(&provider_raw);
        let matched = markers.schedule_prompt.iter().any(|line| {
            parse_schedule_prompt_for_store(line)
                .map(|(line_recurring, line_time, line_prompt)| {
                    line_recurring == recurring
                        && line_time == schedule_time
                        && line_prompt == prompt
                })
                .unwrap_or(false)
        });
        if !matched {
            continue;
        }

        let delivery_target = match channel.as_str() {
            "telegram" => {
                let chat_id = chat_id
                    .strip_prefix("telegram:")
                    .unwrap_or(chat_id.as_str())
                    .split_once('#')
                    .map(|(root, _)| root)
                    .unwrap_or_else(|| {
                        chat_id
                            .strip_prefix("telegram:")
                            .unwrap_or(chat_id.as_str())
                    });
                json!({"kind": "telegram", "chat_id": chat_id}).to_string()
            }
            "slack" => {
                let session = chat_id.strip_prefix("slack:").unwrap_or(chat_id.as_str());
                let (channel_id, thread_ts) = session
                    .split_once('#')
                    .map(|(channel_id, thread_ts)| {
                        (channel_id.to_string(), Some(thread_ts.to_string()))
                    })
                    .unwrap_or_else(|| (session.to_string(), None));
                json!({"kind": "slack", "channel_id": channel_id, "thread_ts": thread_ts})
                    .to_string()
            }
            "local" => json!({"kind": "stdout"}).to_string(),
            _ => continue,
        };

        return Ok(Some((chat_id, delivery_target)));
    }

    Ok(None)
}

fn parse_schedule_prompt_for_store(line: &str) -> Option<(bool, String, String)> {
    let (recurring, rest) = if let Some(stripped) = line.strip_prefix("once ") {
        (false, stripped.trim())
    } else {
        (true, line)
    };

    let (time, prompt) = rest.split_once('|')?;
    let time = time.trim();
    let prompt = prompt.trim();
    let parts: Vec<&str> = time.split(':').collect();
    if parts.len() != 2 {
        return None;
    }
    let h = parts[0].parse::<u8>().ok()?;
    let m = parts[1].parse::<u8>().ok()?;
    if h > 23 || m > 59 || prompt.is_empty() {
        return None;
    }
    Some((recurring, format!("{h:02}:{m:02}"), prompt.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    #[test]
    fn store_open_deduplicates_legacy_task_run_update_ids() {
        let cfg = RuntimeConfig::test_config();
        let conn = Connection::open(&cfg.sqlite_db_path).expect("open legacy db");
        conn.execute_batch(
            "CREATE TABLE kv (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
             );
             INSERT INTO kv(key, value) VALUES ('schema_version', '7');
             INSERT INTO kv(key, value) VALUES ('last_update_id', '0');
             CREATE TABLE task_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                channel TEXT NOT NULL,
                source_chat_id TEXT,
                source_user_id TEXT,
                update_id TEXT,
                prompt TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                started_at TEXT,
                finished_at TEXT,
                cancel_requested_at TEXT,
                progress_message_id TEXT,
                last_progress TEXT,
                error_summary TEXT,
                result_summary TEXT,
                scheduled_task_id INTEGER
             );
             CREATE TABLE turns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                chat_id TEXT NOT NULL,
                input_type TEXT NOT NULL,
                user_text TEXT,
                asr_text TEXT,
                provider_raw TEXT NOT NULL,
                telegram_reply TEXT,
                voice_reply TEXT,
                status TEXT NOT NULL,
                update_id TEXT,
                duration_ms INTEGER,
                task_run_id INTEGER,
                side_effects_applied INTEGER NOT NULL DEFAULT 0,
                channel TEXT NOT NULL DEFAULT 'telegram'
             );
             CREATE TABLE approvals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_run_id INTEGER NOT NULL,
                session_id TEXT NOT NULL,
                channel TEXT NOT NULL,
                source_user_id TEXT,
                channel_id TEXT,
                thread_ts TEXT,
                prompt_text TEXT NOT NULL,
                status TEXT NOT NULL,
                request_message_ts TEXT,
                resume_payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                resolved_at TEXT,
                resolved_by_user_id TEXT
             );",
        )
        .expect("create legacy schema");
        conn.execute(
            "INSERT INTO task_runs(session_id, channel, source_chat_id, update_id, prompt, status, created_at)
             VALUES('telegram:321', 'telegram', '321', 'dup-1', 'first', 'completed', '2026-04-01T00:00:00+0000')",
            [],
        )
        .expect("insert first duplicate");
        let first_id = conn.last_insert_rowid();
        conn.execute(
            "INSERT INTO task_runs(session_id, channel, source_chat_id, update_id, prompt, status, created_at)
             VALUES('telegram:321', 'telegram', '321', 'dup-1', 'second', 'completed', '2026-04-01T00:00:01+0000')",
            [],
        )
        .expect("insert second duplicate");
        let keep_id = conn.last_insert_rowid();
        conn.execute(
            "INSERT INTO turns(ts, chat_id, input_type, user_text, asr_text, provider_raw, telegram_reply, voice_reply, status, update_id, duration_ms, task_run_id, side_effects_applied, channel)
             VALUES('2026-04-01T00:00:02+0000', 'telegram:321', 'text', 'hello', '', 'TELEGRAM_REPLY: hi', 'hi', '', 'ok', 'turn-1', 1, ?1, 1, 'telegram')",
            params![first_id],
        )
        .expect("insert turn linked to duplicate");
        conn.execute(
            "INSERT INTO approvals(task_run_id, session_id, channel, prompt_text, status, resume_payload, created_at)
             VALUES(?1, 'telegram:321', 'telegram', 'approve?', 'pending', '{}', '2026-04-01T00:00:03+0000')",
            params![first_id],
        )
        .expect("insert approval linked to duplicate");
        drop(conn);

        let store = Store::open(&cfg).expect("open migrated store");
        let duplicate_count: i64 = store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM task_runs WHERE update_id = 'dup-1'",
                [],
                |row| row.get(0),
            )
            .expect("count deduped rows");
        assert_eq!(duplicate_count, 1);
        let migrated_turn_task_run_id: i64 = store
            .conn
            .query_row("SELECT task_run_id FROM turns LIMIT 1", [], |row| {
                row.get(0)
            })
            .expect("turn task_run_id");
        assert_eq!(migrated_turn_task_run_id, keep_id);
        let migrated_approval_task_run_id: i64 = store
            .conn
            .query_row("SELECT task_run_id FROM approvals LIMIT 1", [], |row| {
                row.get(0)
            })
            .expect("approval task_run_id");
        assert_eq!(migrated_approval_task_run_id, keep_id);
        let has_unique_index: i64 = store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'index'
                   AND name = 'idx_task_runs_update_id_unique'",
                [],
                |row| row.get(0),
            )
            .expect("check unique index");
        assert_eq!(has_unique_index, 1);
        assert!(
            store
                .conn
                .execute(
                    "INSERT INTO task_runs(session_id, channel, source_chat_id, update_id, prompt, status, created_at)
                     VALUES('telegram:321', 'telegram', '321', 'dup-1', 'third', 'queued', '2026-04-01T00:00:04+0000')",
                    [],
                )
                .is_err()
        );
    }

    #[test]
    fn migration_backfills_legacy_scheduled_task_routing_from_origin_turn() {
        let cfg = RuntimeConfig::test_config();
        let store = Store::open(&cfg).expect("store");
        store
            .conn
            .execute("UPDATE kv SET value = '9' WHERE key = 'schema_version'", [])
            .expect("rewind schema version");
        store
            .conn
            .execute(
                "INSERT INTO turns(ts, chat_id, input_type, user_text, asr_text, provider_raw, telegram_reply, voice_reply, status, update_id, duration_ms, task_run_id, side_effects_applied, channel)
                 VALUES('2026-04-02T09:00:00+0000', 'slack:C123#171.5', 'text', 'schedule it', '', 'TELEGRAM_REPLY: ok\nSCHEDULE_PROMPT: 09:30|Check backups\n', 'ok', '', 'ok', 'turn-2', 1, NULL, 1, 'slack')",
                [],
            )
            .expect("insert origin turn");
        store
            .conn
            .execute(
                "INSERT INTO scheduled_tasks(ts, source, prompt, schedule_time, recurring, done, pending_output, delivery_state, origin_session, delivery_target)
                 VALUES('2026-04-02T09:00:00+0000', 'agent', 'Check backups', '09:30', 1, 0, NULL, NULL, NULL, NULL)",
                [],
            )
            .expect("insert legacy scheduled task");
        drop(store);

        let reopened = Store::open(&cfg).expect("reopen migrated store");
        let task = reopened
            .get_scheduled_task(1)
            .expect("load scheduled task")
            .expect("scheduled task exists");
        assert_eq!(task.origin_session.as_deref(), Some("slack:C123#171.5"));
        assert_eq!(
            task.delivery_target.as_deref(),
            Some(r#"{"channel_id":"C123","kind":"slack","thread_ts":"171.5"}"#)
        );
    }
}
