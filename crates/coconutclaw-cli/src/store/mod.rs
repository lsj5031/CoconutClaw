use anyhow::{Context, Result};
use coconutclaw_config::RuntimeConfig;
use rusqlite::Connection;
use std::fs;
pub(crate) mod approvals;
pub(crate) mod kv;
pub(crate) mod migrations;
pub(crate) mod scheduled_tasks;
pub(crate) mod task_runs;
pub(crate) mod turns;

const SCHEMA_SQL: &str = include_str!("../../../../sql/schema.sql");

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
    pub(crate) conn: Connection,
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
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
