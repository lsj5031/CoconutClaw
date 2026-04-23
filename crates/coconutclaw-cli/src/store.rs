use crate::markers::{parse_markers, render_output};
use anyhow::{Context, Result};
use chrono::DateTime;
use coconutclaw_config::RuntimeConfig;
use rusqlite::{Connection, params};
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
                    result_summary TEXT
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

    pub(crate) fn insert_turn(&self, turn: &TurnRecord) -> Result<bool> {
        self.conn.execute(
            "INSERT OR IGNORE INTO turns(ts, chat_id, input_type, user_text, asr_text, provider_raw, telegram_reply, voice_reply, status, update_id, duration_ms, channel)
             VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
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
            ],
        )?;
        Ok(self.conn.changes() > 0)
    }

    pub(crate) fn insert_tasks(&mut self, ts: &str, source: &str, lines: &[String]) -> Result<()> {
        if lines.is_empty() {
            return Ok(());
        }

        let tx = self.conn.transaction()?;

        {
            let mut stmt = tx.prepare_cached(
                "INSERT INTO tasks(ts, source, content, done) VALUES(?1, ?2, ?3, 0)",
            )?;

            for line in lines {
                stmt.execute(params![ts, source, line])?;
            }
        }

        tx.commit()?;

        Ok(())
    }

    pub(crate) fn insert_scheduled_task(
        &self,
        ts: &str,
        source: &str,
        prompt: &str,
        schedule_time: &str,
        recurring: bool,
    ) -> Result<ScheduledTaskInsertResult> {
        // Deduplicate: skip if same source, prompt, and schedule_time already exists and is not done.
        let existing: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM scheduled_tasks WHERE source = ?1 AND prompt = ?2 AND schedule_time = ?3 AND done = 0",
            params![source, prompt, schedule_time],
            |row| row.get(0),
        )?;
        if existing > 0 {
            return Ok(ScheduledTaskInsertResult::Duplicate);
        }

        self.conn.execute(
            "INSERT INTO scheduled_tasks(ts, source, prompt, schedule_time, recurring, done, pending_output)
             VALUES(?1, ?2, ?3, ?4, ?5, 0, NULL)",
            params![ts, source, prompt, schedule_time, recurring as i32],
        )?;
        Ok(ScheduledTaskInsertResult::Inserted)
    }

    pub(crate) fn list_active_scheduled_tasks(&self) -> Result<Vec<ScheduledTask>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, ts, source, prompt, schedule_time, recurring, last_run_ts, done, pending_output
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
            });
        }
        Ok(tasks)
    }

    pub(crate) fn get_due_scheduled_tasks(
        &self,
        current_hhmm: &str,
        today: &str,
    ) -> Result<Vec<ScheduledTask>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, ts, source, prompt, schedule_time, recurring, last_run_ts, done, pending_output
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
            });
        }
        Ok(tasks)
    }

    pub(crate) fn mark_scheduled_task_executed(&self, id: i64, ts: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE scheduled_tasks SET last_run_ts = ?1, done = CASE WHEN recurring = 0 THEN 1 ELSE 0 END, pending_output = NULL WHERE id = ?2",
            params![ts, id],
        )?;
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn set_scheduled_task_pending_output(&self, id: i64, output: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE scheduled_tasks SET pending_output = ?1 WHERE id = ?2",
            params![output, id],
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

    pub(crate) fn update_turn_reply_by_update_id(
        &self,
        update_id: &str,
        telegram_reply: &str,
        voice_reply: &str,
    ) -> Result<()> {
        self.conn.execute(
            "UPDATE turns
             SET telegram_reply = ?2,
                 voice_reply = ?3
             WHERE update_id = ?1",
            params![update_id, telegram_reply, voice_reply],
        )?;
        Ok(())
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
        self.conn.execute(
            "INSERT INTO task_runs(
                session_id, channel, source_chat_id, source_user_id, update_id, prompt, status,
                created_at, progress_message_id
             ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                params.session_id,
                params.channel,
                params.source_chat_id,
                params.source_user_id,
                params.update_id,
                params.prompt,
                TaskRunStatus::Queued.as_str(),
                params.created_at,
                params.progress_message_id
            ],
        )?;
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
                    last_progress, error_summary, result_summary
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
        }))
    }

    #[cfg(test)]
    pub(crate) fn list_active_task_runs(&self) -> Result<Vec<TaskRun>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, session_id, channel, source_chat_id, source_user_id, update_id, prompt, status,
                    created_at, started_at, finished_at, cancel_requested_at, progress_message_id,
                    last_progress, error_summary, result_summary
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
                    last_progress, error_summary, result_summary
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
            });
        }
        Ok(tasks)
    }

    pub(crate) fn find_active_task_for_session(&self, session_id: &str) -> Result<Option<TaskRun>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, session_id, channel, source_chat_id, source_user_id, update_id, prompt, status,
                    created_at, started_at, finished_at, cancel_requested_at, progress_message_id,
                    last_progress, error_summary, result_summary
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
        }))
    }

    pub(crate) fn find_active_task_run_by_update_id(
        &self,
        update_id: &str,
    ) -> Result<Option<TaskRun>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, session_id, channel, source_chat_id, source_user_id, update_id, prompt, status,
                    created_at, started_at, finished_at, cancel_requested_at, progress_message_id,
                    last_progress, error_summary, result_summary
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
        }))
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
