use anyhow::{Result, ensure};
use rusqlite::{OptionalExtension, params};

use super::{InsertTaskRunParams, Store};
use crate::scheduler::TaskRequest;

#[derive(Clone, Copy)]
pub(crate) enum EventKind {
    Request,
    Schedule,
    Approval,
    Result,
}

impl EventKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Request => "request",
            Self::Schedule => "schedule",
            Self::Approval => "approval",
            Self::Result => "result",
        }
    }
}

impl Store {
    /// Caller owns an IMMEDIATE transaction, so event consumption and its
    /// runnable payload (or result/approval transition) share one commit.
    pub(crate) fn record_inbox(
        &self,
        key: &str,
        kind: EventKind,
        session: &str,
        payload: &str,
        task: i64,
        now: &str,
    ) -> Result<bool> {
        ensure!(!self.conn.is_autocommit(), "inbox requires a transaction");
        let old: Option<(String, String, String, i64)> = self
            .conn
            .query_row(
                "SELECT kind,session_id,payload,task_run_id FROM runtime_inbox WHERE event_key=?1",
                [key],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .optional()?;
        if let Some(old) = old {
            ensure!(
                old == (kind.as_str().into(), session.into(), payload.into(), task),
                "conflicting inbox replay; original event preserved"
            );
            return Ok(false);
        }
        self.conn.execute(
            "INSERT INTO runtime_inbox VALUES(?1,?2,?3,?4,?5,?6)",
            params![key, kind.as_str(), session, payload, task, now],
        )?;
        Ok(true)
    }

    pub(crate) fn admit_inbox_request(
        &self,
        kind: EventKind,
        key: Option<&str>,
        request: &TaskRequest,
        now: &str,
    ) -> Result<(i64, bool)> {
        ensure!(!self.conn.is_autocommit(), "inbox requires a transaction");
        let raw = crate::approval::build_resume_payload(request, &request.input)?;
        let mut canonical: serde_json::Value = serde_json::from_str(&raw)?;
        // Transport progress IDs may change on redelivery, not the accepted work.
        canonical
            .as_object_mut()
            .expect("request object")
            .remove("progress_message_id");
        let canonical = canonical.to_string();
        if let Some(key) = key {
            let existing: Option<i64> = self
                .conn
                .query_row(
                    "SELECT task_run_id FROM runtime_inbox WHERE event_key=?1",
                    [key],
                    |r| r.get(0),
                )
                .optional()?;
            if let Some(id) = existing {
                self.record_inbox(key, kind, &request.session.id(), &canonical, id, now)?;
                return Ok((id, false));
            }
        }
        let id = self.insert_task_run(InsertTaskRunParams {
            session_id: request.session.id(),
            channel: request.source.channel_name().into(),
            source_chat_id: Some(request.delivery.display_id().into()),
            source_user_id: request.source_user_id.clone(),
            update_id: request.update_id.clone(),
            prompt: request.input.user_text.clone(),
            created_at: now.into(),
            progress_message_id: request.progress_message_id.clone(),
            scheduled_task_id: None,
        })?;
        self.conn.execute(
            "INSERT INTO task_payloads(task_run_id,request) VALUES(?1,?2)",
            params![id, raw],
        )?;
        let generated = format!("request:{id}");
        self.record_inbox(
            key.unwrap_or(&generated),
            kind,
            &request.session.id(),
            &canonical,
            id,
            now,
        )?;
        self.conn.execute(
            "UPDATE runtime_attempts SET cause_event_key=?2 WHERE run_id=?1",
            params![id, key.unwrap_or(&generated)],
        )?;
        Ok((id, true))
    }

    pub(crate) fn inherit_schedule(&self, parent: i64, child: i64) -> Result<()> {
        self.conn.execute(
            "INSERT INTO runtime_schedule_runs(schedule_id,task_run_id)
             SELECT schedule_id,?2 FROM runtime_schedule_runs WHERE task_run_id=?1",
            params![parent, child],
        )?;
        Ok(())
    }
}
