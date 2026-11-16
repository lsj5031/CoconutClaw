//! Isolated development adapter: actual Store rows over the existing scoped
//! transport. Compiled ONLY in tests; no executable admission or executor.
use super::*;
use anyhow::ensure;
use coconutclaw_sync::synthetic::{
    Record,
    membership::{
        ScopedPeer,
        domain::{Action, BoundOrigin, History, SignedEvent},
    },
};
use iroh::{EndpointId, SecretKey};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};

mod driver;
mod execution;
mod proposals;

use crate::{
    approval::{build_resume_payload, request_from_payload},
    delivery::{DeliveryTarget, TaskSource},
    scheduler::TaskRequest,
    session::SessionKey,
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Delegation {
    run: i64,
    parent: i64,
    index: u32,
    payload: String,
}

/// Caller supplies an independently provisioned fixture origin and exact local
/// session mapping. This neither discovers credentials nor grants live authority.
struct TaskAdapter<'a> {
    store: &'a Store,
    origin: &'a BoundOrigin,
    session: &'a SessionKey,
}

fn delegation_id(id: &str) -> Result<(i64, u32)> {
    let parts: Vec<_> = id.split(':').collect();
    ensure!(
        parts.len() == 3 && parts[0] == "delegation",
        "not a delegated task"
    );
    let parent: i64 = parts[1].parse()?;
    let index: u32 = parts[2].parse()?;
    ensure!(
        parent > 0 && id == format!("delegation:{parent}:{index}"),
        "noncanonical delegation identity"
    );
    Ok((parent, index))
}

fn held(run: &TaskRun) -> Result<()> {
    ensure!(
        run.started_at.is_none()
            && run.finished_at.is_none()
            && run.cancel_requested_at.is_none()
            && run.error_summary.is_none()
            && run.result_summary.is_none()
            && run.scheduled_task_id.is_none(),
        "started, cancelled or uncertain task cannot be admitted"
    );
    Ok(())
}

impl<'a> TaskAdapter<'a> {
    fn open(store: &'a Store, origin: &'a BoundOrigin, session: &'a SessionKey) -> Result<Self> {
        origin.verify(origin.origin.owner, origin.origin.namespace)?;
        // No runtime migration: these tables exist only in disposable dev stores.
        let tx = store.conn.unchecked_transaction()?;
        store.conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS adapter_binding(
                singleton INTEGER PRIMARY KEY CHECK(singleton=1), origin TEXT NOT NULL, session TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS adapter_outgoing(
                run INTEGER PRIMARY KEY REFERENCES task_runs(id), event BLOB NOT NULL UNIQUE);
             CREATE TABLE IF NOT EXISTS adapter_incoming(
                assignment INTEGER PRIMARY KEY, source_run INTEGER NOT NULL UNIQUE,
                event BLOB NOT NULL, run INTEGER NOT NULL UNIQUE REFERENCES task_runs(id));
             CREATE TABLE IF NOT EXISTS adapter_execution(
                assignment INTEGER PRIMARY KEY, start BLOB NOT NULL, claimed INTEGER NOT NULL DEFAULT 0,
                result BLOB);
             CREATE TABLE IF NOT EXISTS adapter_returns(
                assignment INTEGER PRIMARY KEY, start BLOB NOT NULL, result BLOB);
             CREATE TABLE IF NOT EXISTS adapter_cancel(
                assignment INTEGER PRIMARY KEY, command TEXT NOT NULL);",
        )?;
        store.conn.execute(
            "INSERT OR IGNORE INTO adapter_binding VALUES(1,?1,?2)",
            params![origin.hash()?, session.id()],
        )?;
        let binding: (String, String) = store.conn.query_row(
            "SELECT origin,session FROM adapter_binding WHERE singleton=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        ensure!(
            binding == (origin.hash()?, session.id()),
            "adapter store binding conflict"
        );
        tx.commit()?;
        Ok(Self {
            store,
            origin,
            session,
        })
    }

    fn request(&self, payload: &str) -> Result<TaskRequest> {
        // Reuse the application's actual request parser/serializer. Refuse
        // omitted, extra or lossy fields instead of silently dropping context.
        let request = request_from_payload(payload, false)?;
        ensure!(
            build_resume_payload(&request, &request.input)? == payload,
            "noncanonical or unsupported application payload"
        );
        let route = match self.session.platform {
            crate::session::SessionPlatform::Local => {
                matches!(request.source, TaskSource::Local)
                    && matches!(request.delivery, DeliveryTarget::Stdout)
                    && request.source_user_id.is_none()
            }
            crate::session::SessionPlatform::Telegram => {
                matches!(request.source, TaskSource::Telegram)
                    && request.delivery.telegram_route().as_deref()
                        == Some(self.session.id().as_str())
                    && crate::delivery::parse_delivery_target(Some(
                        &crate::delivery::serialize_delivery_target(&request.delivery),
                    ))
                    .is_some()
                    && match &request.delivery {
                        DeliveryTarget::Telegram { chat_id, .. } => request
                            .source_user_id
                            .as_ref()
                            .is_none_or(|sender| sender == chat_id),
                        _ => false,
                    }
            }
        };
        ensure!(
            request.session == *self.session
                && route
                && request.input.channel == request.source.channel_name()
                && request.input.input_type == crate::InputType::Text
                && request.delegated
                && request.media.is_none()
                && request.input.attachment_type.is_none()
                && request.input.attachment_path.is_none()
                && !request.input.attachment_owned
                && request.input.supplemental_context.is_none()
                && request.progress_message_id.is_none()
                && request.quoted.reply_from.is_none()
                && request.quoted.reply_text.is_none()
                && request.quoted.reply_ts.is_none(),
            "outside isolated text delegation scope"
        );
        Ok(request)
    }

    fn history(&self, peer: &mut ScopedPeer) -> Result<History> {
        let history = peer.domain_history(self.origin)?;
        ensure!(
            history.pending_fence().is_none(),
            "adapter origin suspended"
        );
        Ok(history)
    }

    async fn export(
        &self,
        peer: &mut ScopedPeer,
        run_id: i64,
        core: &SecretKey,
        runner: EndpointId,
    ) -> Result<Record> {
        let history = self.history(peer)?;
        ensure!(
            core.public() == self.origin.origin.core,
            "not the assigning core"
        );
        let tx = self.store.conn.unchecked_transaction()?;
        let run = self.store.get_task_run(run_id)?.context("task absent")?;
        held(&run)?;
        let (payload, output, delivered, delivery_state): (String, Option<String>, bool, String) =
            self.store.conn.query_row(
                "SELECT request,output,delivered,delivery_state FROM task_payloads WHERE task_run_id=?1",
                [run_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )?;
        ensure!(
            output.is_none() && !delivered && delivery_state == "[]",
            "task already has result/delivery evidence"
        );
        let request = self.request(&payload)?;
        ensure!(
            run.session_id == request.session.id()
                && run.channel == request.source.channel_name()
                && run.prompt == request.input.user_text,
            "task/payload substitution"
        );
        let (parent, index) =
            delegation_id(run.update_id.as_deref().context("delegation absent")?)?;
        ensure!(
            parent < run_id && self.store.get_task_run(parent)?.is_some(),
            "parent absent"
        );
        ensure!(
            request.update_id == run.update_id,
            "delegation payload mismatch"
        );
        let request = serde_json::to_string(&Delegation {
            run: run_id,
            parent,
            index,
            payload,
        })?;
        let prior: Option<Vec<u8>> = self
            .store
            .conn
            .query_row(
                "SELECT event FROM adapter_outgoing WHERE run=?1",
                [run_id],
                |r| r.get(0),
            )
            .optional()?;
        let bytes = if let Some(bytes) = prior {
            ensure!(
                run.status == TaskRunStatus::WaitingRemote.as_str(),
                "reservation no longer held"
            );
            let signed = SignedEvent::parse(&bytes)?;
            ensure!(
                matches!(&signed.event.action, Action::Assign { attempt: 1, runner: r, request: p, .. }
                    if *r == runner && p == &request),
                "export identity/payload conflict"
            );
            bytes
        } else {
            ensure!(
                run.status == TaskRunStatus::Queued.as_str(),
                "task not queued"
            );
            let published = history.export();
            // A rolled-back/lost local mapping cannot turn an already assigned
            // source task into another attempt under a fresh event sequence.
            for bytes in &published {
                if let Action::Assign { request, .. } = SignedEvent::parse(bytes)?.event.action {
                    let previous: Delegation = serde_json::from_str(&request)?;
                    ensure!(
                        previous.run != run_id
                            && (previous.parent, previous.index) != (parent, index),
                        "source delegation already assigned; export mapping unavailable"
                    );
                }
            }
            let mut stmt = self
                .store
                .conn
                .prepare("SELECT event FROM adapter_outgoing")?;
            for event in stmt.query_map([], |r| r.get::<_, Vec<u8>>(0))? {
                ensure!(
                    published.contains(&event?),
                    "earlier publication pending; replay it first"
                );
            }
            let mut event = history.next(
                self.origin,
                core.public(),
                Action::Assign {
                    job: 0,
                    attempt: 1,
                    runner,
                    request,
                },
            )?;
            if let Action::Assign { job, .. } = &mut event.action {
                *job = event.sequence;
            }
            let bytes = SignedEvent::sign(event, core)?.bytes()?;
            let mut events = published;
            events.push(bytes.clone());
            History::replay(
                self.origin,
                self.origin.origin.owner,
                self.origin.origin.namespace,
                events,
            )?;
            self.store.conn.execute(
                "INSERT INTO adapter_outgoing(run,event) VALUES(?1,?2)",
                params![run_id, bytes],
            )?;
            bytes
        };
        self.store
            .reserve_runtime_run(run_id, &self.origin.hash()?, &bytes)?;
        tx.commit()?;
        // Exact inert publication replay is permitted, never task re-execution.
        // A failed publish leaves the durable reservation and original bytes.
        peer.publish_domain(self.origin, &bytes).await
    }

    fn accept(&self, peer: &mut ScopedPeer, assignment: u64, runner: EndpointId) -> Result<i64> {
        let history = self.history(peer)?;
        let job = history.jobs.get(&assignment).context("assignment absent")?;
        ensure!(
            job.runner == runner && job.attempt == 1 && !job.started && job.result.is_none(),
            "not an unstarted assignment for this runner"
        );
        let bytes = history
            .export()
            .into_iter()
            .find(|b| SignedEvent::parse(b).is_ok_and(|s| s.event.sequence == assignment))
            .context("assignment bytes absent")?;
        let signed = SignedEvent::parse(&bytes)?;
        let Action::Assign { request, .. } = &signed.event.action else {
            anyhow::bail!("not an assignment")
        };
        let delegation: Delegation = serde_json::from_str(request)?;
        ensure!(
            serde_json::to_string(&delegation)? == *request,
            "noncanonical delegation"
        );
        ensure!(
            delegation.run > delegation.parent && delegation.parent > 0,
            "invalid source identity"
        );
        let mut request = self.request(&delegation.payload)?;
        ensure!(
            request.update_id.as_deref()
                == Some(format!("delegation:{}:{}", delegation.parent, delegation.index).as_str()),
            "source delegation identity mismatch"
        );
        request.session =
            SessionKey::local(&format!("sync:{}/{}", self.origin.hash()?, assignment));
        request.update_id = Some(format!("sync:{}/{}", self.origin.hash()?, assignment));
        request.source = TaskSource::Local;
        request.source_user_id = None;
        request.delivery = DeliveryTarget::Stdout;
        request.input.channel = "local".into();
        let payload = build_resume_payload(&request, &request.input)?;
        let tx = self.store.conn.unchecked_transaction()?;
        let prior: Option<(i64, Vec<u8>)> = self
            .store
            .conn
            .query_row(
                "SELECT run,event FROM adapter_incoming WHERE assignment=?1",
                [assignment],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        let run_id = if let Some((id, event)) = prior {
            ensure!(event == bytes, "incoming assignment conflict");
            let run = self.store.get_task_run(id)?.context("held task absent")?;
            held(&run)?;
            ensure!(
                run.status == TaskRunStatus::WaitingRemote.as_str()
                    && run.prompt == request.input.user_text
                    && run.session_id == request.session.id()
                    && run.update_id == request.update_id
                    && run.channel == "local",
                "held task conflict"
            );
            let saved: (String, Option<String>, bool, String) = self.store.conn.query_row(
                "SELECT request,output,delivered,delivery_state FROM task_payloads WHERE task_run_id=?1",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )?;
            ensure!(
                saved == (payload, None, false, "[]".into()),
                "held payload conflict"
            );
            id
        } else {
            let id = self.store.insert_task_run(InsertTaskRunParams {
                session_id: request.session.id(),
                channel: "local".into(),
                source_chat_id: None,
                source_user_id: None,
                update_id: request.update_id,
                prompt: request.input.user_text,
                created_at: chrono::Utc::now().to_rfc3339(),
                progress_message_id: None,
                scheduled_task_id: None,
            })?;
            self.store.conn.execute(
                "INSERT INTO task_payloads(task_run_id,request) VALUES(?1,?2)",
                params![id, payload],
            )?;
            self.store.conn.execute(
                "UPDATE task_runs SET status=?2 WHERE id=?1",
                params![id, TaskRunStatus::WaitingRemote.as_str()],
            )?;
            self.store.conn.execute(
                "INSERT INTO adapter_incoming(assignment,source_run,event,run) VALUES(?1,?2,?3,?4)",
                params![assignment, delegation.run, bytes, id],
            )?;
            id
        };
        tx.commit()?;
        Ok(run_id)
    }
}

#[cfg(test)]
mod tests;
