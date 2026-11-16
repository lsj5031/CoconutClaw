//! Runtime projection of custody authenticated by the existing scoped adapter.
//! No transport/signing/runner invocation or independent descriptor protocol.
use super::{Store, TaskRunStatus};
use anyhow::{Context, Result, ensure};
use rusqlite::{OptionalExtension, Transaction, TransactionBehavior, params};

impl Store {
    // Only the existing development transport ingress creates reservations.
    // Normal production remote admission remains unavailable.
    #[cfg(test)]
    pub(super) fn reserve_runtime_run(
        &self,
        id: i64,
        origin: &str,
        assignment: &[u8],
    ) -> Result<()> {
        ensure!(
            !self.conn.is_autocommit(),
            "reservation requires assignment transaction"
        );
        let run = self.get_task_run(id)?.context("source run absent")?;
        let request: String = self.conn.query_row(
            "SELECT request FROM task_payloads WHERE task_run_id=?1",
            [id],
            |r| r.get(0),
        )?;
        self.conn.execute(
            "INSERT OR IGNORE INTO runtime_remote_runs VALUES(?1,?2,?3,?4,?5)",
            params![id, origin, assignment, run.session_id, request],
        )?;
        let saved:(String,Vec<u8>,String,String)=self.conn.query_row(
            "SELECT origin,assignment,session_id,request FROM runtime_remote_runs WHERE task_run_id=?1",
            [id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?;
        ensure!(
            saved == (origin.into(), assignment.into(), run.session_id, request),
            "remote reservation conflict"
        );
        self.conn.execute(
            "UPDATE task_runs SET status=?2 WHERE id=?1",
            params![id, TaskRunStatus::WaitingRemote.as_str()],
        )?;
        Ok(())
    }

    /// Caller has already verified signed history, role, assignment and exact
    /// source request with TaskAdapter. Commit this BEFORE acknowledging custody.
    #[cfg(test)]
    pub(super) fn custody_runtime_result(
        &self,
        id: i64,
        start: &[u8],
        result: &[u8],
        outcome: (TaskRunStatus, bool),
        output: &str,
        now: &str,
    ) -> Result<()> {
        let (status, uncertain) = outcome;
        ensure!(!self.conn.is_autocommit(), "custody requires transaction");
        ensure!(
            matches!(
                status,
                TaskRunStatus::Completed | TaskRunStatus::Failed | TaskRunStatus::Cancelled
            ),
            "not a result"
        );
        ensure!(self.runtime_result_is_literal(id)?, "unreserved source run");
        let session: String = self.conn.query_row(
            "SELECT session_id FROM runtime_remote_runs WHERE task_run_id=?1",
            [id],
            |r| r.get(0),
        )?;
        let key = format!("runtime-result:{id}");
        self.record_inbox(
            &key,
            super::inbox::EventKind::Result,
            &session,
            std::str::from_utf8(result)?,
            id,
            now,
        )?;
        self.conn.execute("INSERT OR IGNORE INTO runtime_remote_results(task_run_id,event_key,start,result,status,uncertain,output)
            VALUES(?1,?2,?3,?4,?5,?6,?7)",params![id,key,start,result,status.as_str(),uncertain,output])?;
        let old:(Vec<u8>,Vec<u8>,String,bool,String)=self.conn.query_row(
            "SELECT start,result,status,uncertain,output FROM runtime_remote_results WHERE task_run_id=?1",
            [id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?)))?;
        ensure!(
            old == (
                start.into(),
                result.into(),
                status.as_str().into(),
                uncertain,
                output.into()
            ),
            "remote custody conflict"
        );
        Ok(())
    }

    /// A remote output is inert text, never authority to read core files or
    /// execute marker side effects. Also recheck the durable original route.
    pub(crate) fn runtime_result_is_literal(&self, id: i64) -> Result<bool> {
        let saved:Option<(String,String,String,String)>=self.conn.query_row(
            "SELECT m.session_id,r.session_id,m.request,p.request FROM runtime_remote_runs m
             JOIN task_runs r ON r.id=m.task_run_id JOIN task_payloads p ON p.task_run_id=m.task_run_id
             WHERE m.task_run_id=?1",[id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?;
        if let Some((session, current, request, payload)) = saved {
            ensure!(
                session == current && request == payload,
                "remote source/route changed"
            );
            let request = crate::approval::request_from_payload(&payload, false)?;
            let run = self.get_task_run(id)?.context("remote source absent")?;
            let thread: Option<String> = self.conn.query_row(
                "SELECT source_thread_id FROM task_runs WHERE id=?1",
                [id],
                |r| r.get(0),
            )?;
            ensure!(
                request.session.id() == session
                    && request.update_id == run.update_id
                    && request.input.user_text == run.prompt
                    && request.source.channel_name() == run.channel
                    && request.source_user_id == run.source_user_id,
                "remote source metadata changed"
            );
            match request.delivery {
                crate::delivery::DeliveryTarget::Telegram { chat_id, thread_id } => {
                    ensure!(
                        run.source_chat_id.as_deref() == Some(chat_id.as_str())
                            && thread == thread_id,
                        "remote route metadata changed"
                    );
                }
                crate::delivery::DeliveryTarget::Stdout => {
                    ensure!(
                        run.source_chat_id.is_none() && thread.is_none(),
                        "remote local route metadata changed"
                    );
                }
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// One core orders projection; bounded custody is retained independently of
    /// acknowledgments. No provider, queue admission or HTTP occurs in this tx.
    pub(crate) fn apply_runtime_results(&self, now: &str) -> Result<usize> {
        let tx = Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)?;
        let mut stmt = self.conn.prepare(
            "SELECT c.task_run_id,c.status,c.uncertain,c.output
          FROM runtime_remote_results c JOIN runtime_remote_runs m ON m.task_run_id=c.task_run_id
          WHERE c.state='pending' AND NOT EXISTS(SELECT 1 FROM telegram_ingress t
            WHERE t.session_id=m.session_id AND t.state IN ('pending','applying','unknown'))
          ORDER BY c.seq LIMIT 32",
        )?;
        let rows = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, bool>(2)?,
                    r.get::<_, String>(3)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(stmt);
        let mut applied = 0;
        for (id, status, uncertain, output) in rows {
            let run = self.get_task_run(id)?.context("remote source absent")?;
            let saved: Option<String> = self.conn.query_row(
                "SELECT output FROM task_payloads WHERE task_run_id=?1",
                [id],
                |r| r.get(0),
            )?;
            let approval: bool = self.conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM approvals WHERE task_run_id=?1)
                 OR EXISTS(SELECT 1 FROM turns WHERE task_run_id=?1)
                 OR EXISTS(SELECT 1 FROM runtime_inbox WHERE event_key='result:' || ?1)",
                [id],
                |r| r.get(0),
            )?;
            if self.runtime_result_is_literal(id).is_err()
                || saved.is_some()
                || approval
                || !matches!(
                    run.status.as_str(),
                    "waiting_remote" | "running" | "cancel_requested" | "failed"
                )
            {
                // Preserve local decisions/ack state and raw signed evidence.
                self.conn.execute(
                    "UPDATE runtime_remote_results SET state='unknown' WHERE task_run_id=?1",
                    [id],
                )?;
                continue;
            }
            let unknown = uncertain
                || run
                    .error_summary
                    .as_deref()
                    .is_some_and(|s| s.contains("outcome unknown"))
                || (run.cancel_requested_at.is_some() && status != "cancelled");
            let status = if unknown {
                TaskRunStatus::Failed
            } else {
                match status.as_str() {
                    "completed" => TaskRunStatus::Completed,
                    "cancelled" => TaskRunStatus::Cancelled,
                    _ => TaskRunStatus::Failed,
                }
            };
            if run.started_at.is_none() {
                self.update_task_run_started(id, now)?;
                if let Some(cancelled) = run.cancel_requested_at {
                    self.mark_task_run_cancel_requested(id, &cancelled)?;
                }
            }
            self.finish_task_run(
                id,
                status,
                now,
                unknown.then_some("runtime outcome unknown; preserve evidence; no automatic retry"),
                Some(&output),
            )?;
            self.conn.execute(
                "UPDATE task_payloads SET output=?2 WHERE task_run_id=?1",
                params![id, output],
            )?;
            self.conn.execute(
                "UPDATE runtime_remote_results SET state='applied' WHERE task_run_id=?1",
                [id],
            )?;
            applied += 1;
        }
        tx.commit()?;
        Ok(applied)
    }
}
