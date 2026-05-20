use super::*;
use anyhow::Result;
use rusqlite::params;

impl Store {
    pub(crate) fn insert_task_run(&self, params: InsertTaskRunParams) -> Result<i64> {
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
