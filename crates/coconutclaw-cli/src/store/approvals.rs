use super::*;
use anyhow::Result;
use rusqlite::params;

impl Store {
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
}
