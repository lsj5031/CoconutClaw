use super::{Store, inbox::EventKind};
use crate::{approval, scheduler::TaskRequest, scheduling::Timing};
use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Utc};
use rusqlite::{OptionalExtension, Transaction, TransactionBehavior, params};

pub(crate) const WAKE_BATCH: usize = 32;

// Admission and deadline selection share the exact unresolved-effect fence.
const ELIGIBLE: &str = "s.state='active' AND NOT EXISTS(
    SELECT 1 FROM runtime_schedule_runs link JOIN task_runs r ON r.id=link.task_run_id
    WHERE link.schedule_id=s.id AND (
      r.status NOT IN ('completed','cancelled') OR
      (r.status='cancelled' AND r.started_at IS NOT NULL) OR EXISTS(
        SELECT 1 FROM task_payloads p,json_tree(p.delivery_state) op
        WHERE p.task_run_id=r.id AND op.key='phase' AND op.value IN ('sending','unknown'))))";

impl Store {
    pub(crate) fn create_schedule(
        &self,
        key: &str,
        request: &TaskRequest,
        timing: &Timing,
        now: DateTime<Utc>,
    ) -> Result<i64> {
        ensure!(
            !key.is_empty() && key.len() <= 128,
            "creation key must be 1..128 bytes"
        );
        ensure!(
            request.delegated
                && request.media.is_none()
                && request.input.input_type == crate::InputType::Text
                && request.input.attachment_path.is_none()
                && request.input.supplemental_context.is_none()
                && !request.input.user_text.trim().is_empty()
                && request.input.user_text.len() <= 8192,
            "schedules require an isolated text task without transient attachments/context"
        );
        let raw = approval::build_resume_payload(request, &request.input)?;
        let timing_raw = timing.encode();
        let spec = serde_json::json!({"request":raw,"timing":timing_raw}).to_string();
        let tx = Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)?;
        let existing: Option<(i64,String)> = self.conn.query_row(
            "SELECT id,creation_spec FROM runtime_schedules WHERE session_id=?1 AND creation_key=?2",
            params![request.session.id(),key],|r| Ok((r.get(0)?,r.get(1)?))).optional()?;
        if let Some((id, old)) = existing {
            let mut old: serde_json::Value = serde_json::from_str(&old)?;
            old["timing"] = serde_json::Value::String(
                Timing::decode(old["timing"].as_str().context("creation timing missing")?)?
                    .encode(),
            );
            ensure!(
                old == serde_json::from_str::<serde_json::Value>(&spec)?,
                "creation key already belongs to a different schedule"
            );
            return Ok(id);
        }
        self.conn.execute(
            "INSERT INTO runtime_schedules(session_id,creation_key,creation_spec,request,timing,state,next_due)
             VALUES(?1,?2,?3,?4,?5,'active',?6)",
            params![request.session.id(),key,spec,raw,timing_raw,timing.first(now)?.timestamp()])?;
        let id = self.conn.last_insert_rowid();
        tx.commit()?;
        Ok(id)
    }

    pub(crate) fn next_schedule_deadline(&self) -> Result<Option<i64>> {
        Ok(self.conn.query_row(
            &format!("SELECT MIN(s.next_due) FROM runtime_schedules s WHERE {ELIGIBLE}"),
            [],
            |r| r.get(0),
        )?)
    }

    /// Pause/cancel affect FUTURE occurrences, never rewrite accepted work.
    pub(crate) fn set_schedule_state(&self, session: &str, id: i64, state: &str) -> Result<()> {
        ensure!(
            matches!(state, "active" | "paused" | "cancelled"),
            "invalid schedule state"
        );
        let n=self.conn.execute(
            "UPDATE runtime_schedules SET state=?3 WHERE session_id=?1 AND id=?2 AND (state IN ('active','paused') OR state=?3)",
            params![session,id,state])?;
        ensure!(n == 1, "schedule missing or terminal; cannot resurrect");
        Ok(())
    }

    pub(crate) fn edit_schedule(
        &self,
        session: &str,
        id: i64,
        prompt: &str,
        timing: &Timing,
        now: DateTime<Utc>,
    ) -> Result<()> {
        ensure!(
            !prompt.trim().is_empty() && prompt.len() <= 8192,
            "prompt must be 1..8192 bytes"
        );
        let tx = Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)?;
        let raw: String=self.conn.query_row(
            "SELECT request FROM runtime_schedules WHERE session_id=?1 AND id=?2 AND state IN ('active','paused')",
            params![session,id],|r| r.get(0)).optional()?.context("schedule missing or terminal")?;
        let mut request = approval::request_from_payload(&raw, false)?;
        request.input.user_text = prompt.into();
        self.conn.execute(
            "UPDATE runtime_schedules SET request=?3,timing=?4,next_due=?5,revision=revision+1
             WHERE session_id=?1 AND id=?2",
            params![
                session,
                id,
                approval::build_resume_payload(&request, &request.input)?,
                timing.encode(),
                timing.first(now)?.timestamp()
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// One writer atomically allocates occurrence + inbox + runnable payload.
    /// Failed/unknown runs block future occurrences until explicitly reconciled;
    /// neither restart, resume nor edit grants another invocation for that run.
    pub(crate) fn due_schedule_events(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Vec<(i64, TaskRequest)>> {
        let tx = Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)?;
        let mut stmt = self.conn.prepare(&format!(
            "SELECT s.id,s.revision,s.request,s.timing,s.next_due FROM runtime_schedules s
             WHERE {ELIGIBLE} AND s.next_due<=?1 ORDER BY s.next_due,s.id LIMIT ?2"
        ))?;
        let due = stmt
            .query_map(params![now.timestamp(), WAKE_BATCH], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, i64>(4)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(stmt);
        let mut accepted = Vec::new();
        for (id, revision, raw, timing, earliest) in due {
            let timing = Timing::decode(&timing)?;
            let occurrence = timing.occurrence(now)?.timestamp();
            ensure!(
                occurrence >= earliest && occurrence <= now.timestamp(),
                "inconsistent schedule cursor"
            );
            let next = timing.next(now)?;
            self.conn.execute(
                "UPDATE runtime_schedules SET next_due=?2,state=?3 WHERE id=?1",
                params![
                    id,
                    next.map(|t| t.timestamp()),
                    if next.is_some() { "active" } else { "done" }
                ],
            )?;
            if timing.skip_missed(occurrence, now) {
                continue;
            }
            let key = format!("schedule:{id}:{revision}:{occurrence}");
            let mut request = approval::request_from_payload(&raw, false)?;
            request.update_id = Some(key.clone());
            let (run, new) = self.admit_inbox_request(
                EventKind::Schedule,
                Some(&key),
                &request,
                &now.to_rfc3339(),
            )?;
            self.conn.execute(
                "INSERT INTO schedule_occurrences VALUES(?1,?2,?3,?4)",
                params![id, revision, occurrence, key],
            )?;
            self.conn.execute(
                "INSERT INTO runtime_schedule_runs VALUES(?1,?2)",
                params![id, run],
            )?;
            if new {
                accepted.push((run, request));
            }
        }
        tx.commit()?;
        Ok(accepted)
    }

    pub(crate) fn list_schedules(&self, session: &str) -> Result<serde_json::Value> {
        let mut stmt=self.conn.prepare(
            "SELECT id,revision,state,next_due,timing FROM runtime_schedules WHERE session_id=?1 ORDER BY id")?;
        let rows=stmt.query_map([session],|r| Ok(serde_json::json!({
            "id":r.get::<_,i64>(0)?,"revision":r.get::<_,i64>(1)?,"state":r.get::<_,String>(2)?,
            "next_due_utc":r.get::<_,Option<i64>>(3)?,"timing":r.get::<_,String>(4)?
        })))?.collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(serde_json::Value::Array(rows))
    }
}
