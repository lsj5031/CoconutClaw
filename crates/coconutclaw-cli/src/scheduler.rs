use crate::store::inbox::EventKind;
use rusqlite::{Transaction, TransactionBehavior};
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;

use anyhow::{Context, Result};
use coconutclaw_config::RuntimeConfig;

use crate::approval::{
    cleanup_attachment_from_resume_payload, create_approval_request, request_from_resume_payload,
};
use crate::cancel::CancelRouter;
use crate::delivery::{DeliveryTarget, TaskSource, cleanup_attachment_path};
use crate::session::SessionKey;
use crate::store::{Store, TaskRunStatus};
use crate::telegram::build_telegram_client;
use crate::turn::{hydrate_turn_input, process_turn_with_status};
use crate::{IncomingMedia, QuotedMessage, TurnInput, iso_now, shorten_log_text};

#[path = "scheduler_calendar.rs"]
mod calendar;

#[derive(Debug, Clone)]
pub(crate) enum TaskMedia {
    Telegram(IncomingMedia),
}

#[derive(Debug, Clone)]
pub(crate) struct TaskRequest {
    /// Session key for concurrency control and history.
    pub(crate) session: SessionKey,
    /// Source of the request.
    pub(crate) source: TaskSource,
    /// Input turn data.
    pub(crate) input: TurnInput,
    /// Update/event ID for deduplication.
    pub(crate) update_id: Option<String>,
    /// Media attachment if any.
    pub(crate) media: Option<TaskMedia>,
    /// Quoted message context.
    pub(crate) quoted: QuotedMessage,
    /// Where to deliver the output.
    pub(crate) delivery: DeliveryTarget,
    /// Optional source actor identity.
    pub(crate) source_user_id: Option<String>,
    /// Telegram message ID of the progress indicator.
    pub(crate) progress_message_id: Option<String>,
    /// A delegated attempt has isolated history; delivery remains in the origin chat.
    pub(crate) delegated: bool,
}

#[derive(Debug, Clone)]
struct QueuedTask {
    id: i64,
    request: TaskRequest,
}

#[derive(Debug, Default)]
struct SessionLane {
    active: Option<i64>,
    queue: VecDeque<QueuedTask>,
}

#[derive(Debug, Clone)]
struct RuntimeTaskState {
    cancel_flag: Arc<AtomicBool>,
}

#[derive(Debug)]
struct SchedulerInner {
    cfg: RuntimeConfig,
    lanes: Mutex<HashMap<String, SessionLane>>,
    task_state: Mutex<HashMap<i64, RuntimeTaskState>>,
    cancel_router: Option<Arc<CancelRouter>>,
    delivery_lock: Mutex<()>,
    recovering: AtomicBool,
    stopping: AtomicBool,
    lifecycle: Mutex<()>,
    workers: Mutex<Vec<thread::JoinHandle<()>>>,
    queue_lock: Mutex<()>,
    delivery_wake: mpsc::SyncSender<()>,
    calendar_wake: Mutex<Option<mpsc::SyncSender<()>>>,
    #[cfg(test)]
    reservation_probe: Mutex<Option<(Arc<std::sync::Barrier>, Arc<std::sync::Barrier>)>>,
}

#[derive(Clone, Debug)]
pub(crate) struct SessionScheduler {
    inner: Arc<SchedulerInner>,
}

impl SessionScheduler {
    /// The process entry point must bootstrap Store before starting workers.
    pub(crate) fn new(cfg: RuntimeConfig, cancel_router: Option<Arc<CancelRouter>>) -> Self {
        let (delivery_wake, receive) = mpsc::sync_channel(1);
        let scheduler = Self {
            inner: Arc::new(SchedulerInner {
                cfg,
                lanes: Mutex::new(HashMap::new()),
                task_state: Mutex::new(HashMap::new()),
                cancel_router,
                delivery_lock: Mutex::new(()),
                recovering: AtomicBool::new(false),
                stopping: AtomicBool::new(false),
                lifecycle: Mutex::new(()),
                workers: Mutex::new(Vec::new()),
                queue_lock: Mutex::new(()),
                delivery_wake,
                calendar_wake: Mutex::new(None),
                #[cfg(test)]
                reservation_probe: Mutex::new(None),
            }),
        };
        // One bounded dispatcher, outside model execution lanes. The waiting
        // thread owns only a Weak reference, so dropping the runtime closes it.
        let weak = Arc::downgrade(&scheduler.inner);
        let dispatcher = thread::spawn(move || {
            while receive.recv().is_ok() {
                let Some(inner) = weak.upgrade() else {
                    break;
                };
                let scheduler = Self { inner };
                if scheduler
                    .inner
                    .stopping
                    .load(std::sync::atomic::Ordering::SeqCst)
                {
                    break;
                }
                if let Err(error) = scheduler.retry_deliveries() {
                    tracing::warn!("outbox dispatcher pending: {error:#}");
                }
            }
        });
        scheduler
            .inner
            .workers
            .lock()
            .expect("workers")
            .push(dispatcher);
        scheduler
    }

    pub(crate) fn enqueue(&self, request: TaskRequest) -> Result<i64> {
        let _queue = self.inner.queue_lock.lock().expect("durable enqueue order");
        if self
            .inner
            .stopping
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            anyhow::bail!("runtime is stopping; request was not accepted");
        }
        let created_at = iso_now(&self.inner.cfg.timezone);
        let store = Store::connect(&self.inner.cfg)?;
        let tx = Transaction::new_unchecked(&store.conn, TransactionBehavior::Immediate)?;
        let key = request
            .update_id
            .as_ref()
            .map(|id| format!("request:{}:{id}", request.source.channel_name()));
        let (task_id, new) =
            store.admit_inbox_request(EventKind::Request, key.as_deref(), &request, &created_at)?;
        tx.commit()?;
        if new {
            self.queue_existing(task_id, request);
        }
        Ok(task_id)
    }

    /// Clock-injected admission, shared by the deadline owner and behavior tests.
    /// Only the existing scheduler starts models; the calendar emits inbox work.
    #[cfg(all(test, unix))]
    pub(crate) fn wake_due(&self, now: chrono::DateTime<chrono::Utc>) -> Result<usize> {
        self.wake_due_with_store(&Store::connect(&self.inner.cfg)?, now)
    }

    fn wake_due_with_store(
        &self,
        store: &Store,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<usize> {
        let _queue = self.inner.queue_lock.lock().expect("durable enqueue order");
        if self
            .inner
            .stopping
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Ok(0);
        }
        let accepted = store.due_schedule_events(now)?;
        let count = accepted.len();
        for (id, request) in accepted {
            self.queue_existing(id, request);
        }
        Ok(count)
    }

    pub(crate) fn wake_delivery(&self) {
        let _ = self.inner.delivery_wake.try_send(());
    }

    pub(crate) fn resume_ingress(&self) {
        self.dispatch_ready();
    }

    /// Cancel a task by ID, without session scoping.
    /// Prefer `cancel_task_for_session` to enforce session boundaries.
    pub(crate) fn cancel_task(&self, task_id: i64) -> Result<bool> {
        self.cancel_task_for_session(task_id, None)
    }

    /// Cancel a task by ID, scoped to a specific session.
    /// If `session_id` is None, cancels any task matching the ID.
    pub(crate) fn cancel_task_for_session(
        &self,
        task_id: i64,
        session_id: Option<&str>,
    ) -> Result<bool> {
        let _queue = self.inner.queue_lock.lock().expect("durable enqueue order");
        let now = iso_now(&self.inner.cfg.timezone);
        let store = Store::connect(&self.inner.cfg)?;
        let Some(task) = store.get_task_run(task_id)? else {
            return Ok(false);
        };

        // Enforce session scoping if session_id is provided
        if let Some(expected) = session_id
            && task.session_id != expected
        {
            tracing::debug!(
                "cancel_task_for_session: task {task_id} belongs to {} not {expected}",
                task.session_id
            );
            return Ok(false);
        }

        if matches!(task.status.as_str(), "awaiting_approval" | "waiting_event") {
            let tx = Transaction::new_unchecked(&store.conn, TransactionBehavior::Immediate)?;
            let current = store.get_task_run(task_id)?.expect("task remains");
            if !matches!(
                current.status.as_str(),
                "awaiting_approval" | "waiting_event"
            ) {
                return Ok(false);
            }
            let attachment = store
                .pending_approval_resume_payload_for_task(task_id)?
                .or_else(|| {
                    store
                        .conn
                        .query_row(
                            "SELECT resume_payload FROM runtime_waits WHERE run_id=?1",
                            [task_id],
                            |r| r.get(0),
                        )
                        .ok()
                });
            store.expire_pending_approvals_for_task(task_id, &now)?;
            store.finish_task_run(
                task_id,
                TaskRunStatus::Cancelled,
                &now,
                None,
                Some(if current.status == "awaiting_approval" {
                    "approval cancelled"
                } else {
                    "dependency wait cancelled"
                }),
            )?;
            store.conn.execute(
                "UPDATE runtime_waits SET state='cancelled' WHERE run_id=?1 AND state='waiting'",
                [task_id],
            )?;
            tx.commit()?;
            if let Some(payload) = attachment
                && let Err(error) = cleanup_attachment_from_resume_payload(&payload)
            {
                tracing::warn!("cancelled wait attachment cleanup pending: {error:#}");
            }
            drop(_queue);
            self.complete_task(task_id, &task.session_id)?;
            return Ok(true);
        }
        if task.status == TaskRunStatus::WaitingRemote.as_str() {
            store.mark_task_run_cancel_requested(task_id, &now)?;
            return Ok(true); // Existing transport owner forwards signed intent.
        }
        if task.status == TaskRunStatus::Queued.as_str() {
            let mut removed = false;
            let mut removed_request: Option<TaskRequest> = None;
            {
                let mut lanes = self.inner.lanes.lock().expect("lanes");
                if let Some(lane) = lanes
                    .values_mut()
                    .find(|l| l.queue.iter().any(|q| q.id == task_id))
                    && let Some(index) = lane.queue.iter().position(|queued| queued.id == task_id)
                {
                    removed_request = lane.queue.remove(index).map(|queued| queued.request);
                    removed = true;
                }
                lanes.retain(|_, l| l.active.is_some() || !l.queue.is_empty());
            }
            if removed {
                if let Some(request) = removed_request.as_ref() {
                    Self::cleanup_task_attachment(None, request, &request.input);
                }
                store.finish_task_run(
                    task_id,
                    TaskRunStatus::Cancelled,
                    &now,
                    None,
                    Some("cancelled before execution"),
                )?;
                return Ok(true);
            }
        }

        {
            let mut task_state = self.inner.task_state.lock().expect("task_state");
            if let Some(state) = task_state.get_mut(&task_id) {
                state
                    .cancel_flag
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                store.mark_task_run_cancel_requested(task_id, &now)?;
                return Ok(true);
            }
        }

        Ok(false)
    }

    pub(crate) fn cancel_active_for_session(&self, session_id: &str) -> Result<Option<i64>> {
        let store = Store::connect(&self.inner.cfg)?;
        let Some(task) = store.find_active_task_for_session(session_id)? else {
            return Ok(None);
        };
        if self.cancel_task(task.id)? {
            return Ok(Some(task.id));
        }
        Ok(None)
    }

    // cancel_task_for_session is now merged into cancel_task above

    pub(crate) fn render_active_tasks_for_session(&self, session_id: &str) -> Result<String> {
        let store = Store::connect(&self.inner.cfg)?;
        let tasks = store.list_active_task_runs_for_session(session_id)?;
        Ok(render_active_task_lines(tasks, false))
    }

    pub(crate) fn has_task_for_update_id(&self, update_id: &str) -> Result<bool> {
        let store = Store::connect(&self.inner.cfg)?;
        Ok(store.find_task_run_by_update_id(update_id)?.is_some())
    }

    pub(crate) fn resolve_approval(
        &self,
        approval_id: i64,
        action: &str,
        actor_user_id: &str,
    ) -> Result<Option<String>> {
        let _queue = self.inner.queue_lock.lock().expect("durable enqueue order");
        if self
            .inner
            .stopping
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            anyhow::bail!("runtime is stopping; approval was not resumed");
        }
        let now = iso_now(&self.inner.cfg.timezone);
        let store = Store::connect(&self.inner.cfg)?;
        let tx = Transaction::new_unchecked(&store.conn, TransactionBehavior::Immediate)?;
        let Some(approval) = store.get_pending_approval(approval_id)? else {
            return Ok(Some(
                "Approval request not found. Open Tasks for current requests.".to_string(),
            ));
        };
        if approval.status != "pending" {
            return Ok(Some(format!(
                "Approval request #{} is already {}.",
                approval.id, approval.status
            )));
        }
        if approval.session_id != actor_user_id {
            return Ok(Some(
                "You are not authorized to approve this request.".to_string(),
            ));
        }
        let run = store
            .get_task_run(approval.task_run_id)?
            .context("approval run absent")?;
        if run.status != "awaiting_approval"
            || run.cancel_requested_at.is_some()
            || run
                .error_summary
                .as_deref()
                .is_some_and(|s| s.contains("outcome unknown"))
            || !store.is_current_attempt(run.id)?
        {
            return Ok(Some(
                "This run is no longer waiting for approval. Open Tasks to check its current status.".into(),
            ));
        }
        match action {
            "approve" => {
                store.register_task_wait(
                    run.id,
                    "approval",
                    approval_id,
                    &approval.resume_payload,
                )?;
                store.resolve_approval(approval_id, "approved", &now, Some(actor_user_id))?;
                store.finish_task_run(
                    approval.task_run_id,
                    TaskRunStatus::Completed,
                    &now,
                    None,
                    Some("approval granted"),
                )?;
                let mut request = request_from_resume_payload(&approval.resume_payload)?;
                let key = format!("approval:{approval_id}");
                request.update_id = Some(key.clone());
                let (resume_id, _) =
                    store.admit_inbox_request(EventKind::Approval, Some(&key), &request, &now)?;
                store.link_task_continuation(approval.task_run_id, resume_id, &key)?;
                store.inherit_schedule(approval.task_run_id, resume_id)?;
                tx.commit()?;
                self.queue_existing(resume_id, request);
                Ok(Some(format!(
                    "Approval recorded. Continuation queued as run #{resume_id}."
                )))
            }
            "reject" => {
                store.record_inbox(
                    &format!("approval:{approval_id}"),
                    EventKind::Approval,
                    &approval.session_id,
                    &serde_json::json!({"decision":"reject","actor":actor_user_id}).to_string(),
                    approval.task_run_id,
                    &now,
                )?;
                store.resolve_approval(approval_id, "rejected", &now, Some(actor_user_id))?;
                store.finish_task_run(
                    approval.task_run_id,
                    TaskRunStatus::Cancelled,
                    &now,
                    None,
                    Some("approval rejected"),
                )?;
                store.conn.execute("UPDATE runtime_waits SET state='cancelled' WHERE run_id=?1 AND state='waiting'",[approval.task_run_id])?;
                tx.commit()?;
                if let Err(err) = cleanup_attachment_from_resume_payload(&approval.resume_payload) {
                    tracing::warn!(
                        "failed to cleanup rejected approval attachment #{approval_id}: {err:#}"
                    );
                }
                drop(_queue);
                self.complete_task(approval.task_run_id, &approval.session_id)?;
                Ok(Some("Approval rejected. Task cancelled.".to_string()))
            }
            _ => Ok(Some("Unsupported approval action.".to_string())),
        }
    }

    fn spawn_task(&self, task_id: i64, request: TaskRequest) {
        let _lifecycle = self.inner.lifecycle.lock().expect("scheduler lifecycle");
        if self
            .inner
            .stopping
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return; // Durable queued request remains unstarted for recovery.
        }
        let inner = Arc::clone(&self.inner);
        let scheduler = self.clone();
        let cancel_flag = Arc::new(AtomicBool::new(false));
        {
            let mut task_state = inner.task_state.lock().expect("task_state");
            task_state.insert(
                task_id,
                RuntimeTaskState {
                    cancel_flag: Arc::clone(&cancel_flag),
                },
            );
        }

        let worker = thread::spawn(move || {
            let now = iso_now(&inner.cfg.timezone);
            let task_result =
                scheduler.run_task(task_id, request.clone(), Arc::clone(&cancel_flag));
            let store = match Store::connect(&inner.cfg) {
                Ok(store) => store,
                Err(err) => {
                    tracing::error!("failed to reopen store after task {task_id}: {err:#}");
                    if let Err(advance_err) =
                        scheduler.complete_task(task_id, &request.session.id())
                    {
                        tracing::warn!(
                            "failed to advance scheduler after store reopen failure {task_id}: {advance_err:#}"
                        );
                    }
                    return;
                }
            };

            match task_result {
                Ok(TaskCompletion::WaitingRemote) => {
                    if let Err(err) = scheduler.complete_task(task_id, &request.session.id()) {
                        tracing::warn!("failed to release remote reservation lane: {err:#}");
                    }
                }
                Ok(TaskCompletion::Waiting) => {
                    if let Err(err) = scheduler.complete_task(task_id, &request.session.id()) {
                        tracing::warn!("failed to release approval lane: {err:#}");
                    }
                }
                Ok(TaskCompletion::Finished { status, summary }) => {
                    if let Err(err) =
                        store.finish_task_run(task_id, status, &now, None, Some(&summary))
                    {
                        tracing::warn!("failed to finish task run {task_id}: {err:#}");
                    }
                    if let Err(err) = scheduler.complete_task(task_id, &request.session.id()) {
                        tracing::warn!("failed to advance scheduler after task {task_id}: {err:#}");
                    }
                }
                Err(err) => {
                    if let Err(error) =
                        scheduler.persist_failure(&store, task_id, &request, &err, &now)
                    {
                        tracing::error!("task failure commit pending: {error:#}");
                    }
                    if let Err(advance_err) =
                        scheduler.complete_task(task_id, &request.session.id())
                    {
                        tracing::warn!(
                            "failed to advance scheduler after task failure {task_id}: {advance_err:#}"
                        );
                    }
                }
            }
            scheduler.wake_delivery();
        });
        let mut workers = self.inner.workers.lock().expect("workers");
        // Reap finished workers instead of retaining one handle per historic turn.
        let mut pending = Vec::new();
        for worker in workers.drain(..) {
            if worker.is_finished() {
                let _ = worker.join();
            } else {
                pending.push(worker);
            }
        }
        pending.push(worker);
        *workers = pending;
    }

    fn run_task(
        &self,
        task_id: i64,
        request: TaskRequest,
        cancel_flag: Arc<AtomicBool>,
    ) -> Result<TaskCompletion> {
        let mut store = Store::connect(&self.inner.cfg)?;
        let started_at = iso_now(&self.inner.cfg.timezone);
        {
            let tx = Transaction::new_unchecked(&store.conn, TransactionBehavior::Immediate)?;
            if store.runtime_result_is_literal(task_id)? {
                tx.commit()?;
                return Ok(TaskCompletion::WaitingRemote);
            }
            let current = store.get_task_run(task_id)?.context("queued run absent")?;
            if current.status != "queued"
                || current.cancel_requested_at.is_some()
                || !store.is_current_attempt(task_id)?
            {
                if current.status == "cancel_requested" && current.started_at.is_none() {
                    store.finish_task_run(
                        task_id,
                        TaskRunStatus::Cancelled,
                        &started_at,
                        None,
                        Some("cancelled before execution"),
                    )?;
                }
                tx.commit()?;
                return Ok(TaskCompletion::WaitingRemote);
            }
            store.update_task_run_started(task_id, &started_at)?;
            tx.commit()?;
        }

        if crate::nodes::execute(
            &self.inner.cfg,
            &store,
            task_id,
            &request,
            cancel_flag.clone(),
        )? {
            return Ok(TaskCompletion::WaitingRemote);
        }

        let (input, cleanup_path) = match request.media.clone() {
            Some(TaskMedia::Telegram(media)) => hydrate_turn_input(
                &self.inner.cfg,
                request.update_id.as_deref(),
                request.input.clone(),
                Some(media),
                "telegram",
            )?,
            None => (request.input.clone(), None),
        };

        store.conn.execute(
            "UPDATE task_payloads SET request=?2 WHERE task_run_id=?1",
            rusqlite::params![
                task_id,
                crate::approval::build_resume_payload(&request, &input)?
            ],
        )?;
        let session_id_str = request.session.id();
        let delivery_target_json = crate::delivery::serialize_delivery_target(&request.delivery);
        let processed = match process_turn_with_status(
            &self.inner.cfg,
            &mut store,
            input.clone(),
            &request.source,
            Some(if request.delegated {
                format!("local:task-{task_id}")
            } else {
                request.session.id()
            }),
            request.update_id.clone(),
            Some(task_id),
            request.progress_message_id.as_deref(),
            Some(&request.delivery),
            &request.quoted,
            Some(cancel_flag),
            self.inner.cancel_router.clone(),
            Some(&session_id_str),
            Some(&delivery_target_json),
            request.delegated,
        ) {
            Ok(processed) => processed,
            Err(err) => {
                Self::cleanup_task_attachment(cleanup_path.as_ref(), &request, &input);
                return Err(err);
            }
        };

        let task_result = (|| -> Result<TaskCompletion> {
            let approval = self.persist_result(
                &store,
                task_id,
                &request,
                &input,
                &processed.output,
                processed.status,
            )?;
            if approval {
                return Ok(TaskCompletion::Waiting);
            }
            let status = match processed.status {
                crate::TurnStatus::Cancelled => TaskRunStatus::Cancelled,
                crate::TurnStatus::AgentError | crate::TurnStatus::AgentErrorRecovered => {
                    TaskRunStatus::Failed
                }
                _ => TaskRunStatus::Completed,
            };
            let summary = match status {
                TaskRunStatus::Cancelled => "task cancelled".to_string(),
                TaskRunStatus::Failed => "task failed".to_string(),
                _ => "task completed".to_string(),
            };
            Ok(TaskCompletion::Finished { status, summary })
        })();

        // Successful input cleanup belongs to acknowledged delivery, not execution.
        task_result
    }

    fn task_cleanup_path(
        cleanup_path: Option<&PathBuf>,
        request: &TaskRequest,
        input: &TurnInput,
    ) -> Option<PathBuf> {
        cleanup_path.cloned().or_else(|| {
            (request.media.is_none() && input.attachment_owned)
                .then(|| input.attachment_path.clone())
                .flatten()
        })
    }

    fn cleanup_task_attachment(
        cleanup_path: Option<&PathBuf>,
        request: &TaskRequest,
        input: &TurnInput,
    ) {
        if let Some(path) = Self::task_cleanup_path(cleanup_path, request, input) {
            cleanup_attachment_path(&path);
        }
    }

    fn complete_task(&self, task_id: i64, _session_id: &str) -> Result<()> {
        {
            let mut task_state = self.inner.task_state.lock().expect("task_state");
            task_state.remove(&task_id);
        }

        {
            let mut lanes = self.inner.lanes.lock().expect("lanes");
            if let Some(lane) = lanes.values_mut().find(|l| l.active == Some(task_id)) {
                if lane.active != Some(task_id) {
                    return Ok(());
                }
                lane.active = None;
                if lane.queue.is_empty() {
                    lanes.retain(|_, l| l.active.is_some() || !l.queue.is_empty());
                }
            }
        }
        self.resume_waiting_events()?;
        self.dispatch_ready();
        Ok(())
    }

    fn persist_result(
        &self,
        store: &Store,
        task_id: i64,
        request: &TaskRequest,
        input: &TurnInput,
        raw: &str,
        status: crate::TurnStatus,
    ) -> Result<bool> {
        let _queue = self.inner.queue_lock.lock().expect("durable enqueue order");
        let tx = Transaction::new_unchecked(&store.conn, TransactionBehavior::Immediate)?;
        anyhow::ensure!(store.is_current_attempt(task_id)?, "stale outcome owner");
        let admitted = if status == crate::TurnStatus::Cancelled {
            let saved = crate::markers::CompiledOutput::from_output(raw);
            crate::markers::CompiledOutput::new(
                if saved.reply().trim().is_empty() {
                    crate::wording::CANCELLATION
                } else {
                    saved.reply()
                },
                &[],
            )
            .encode()
        } else {
            crate::delivery::media::admitted_output(&self.inner.cfg, raw, request.delegated)
        };
        let raw = admitted.as_str();
        let fresh = store.record_inbox(
            &format!("result:{task_id}"),
            EventKind::Result,
            &request.session.id(),
            &serde_json::json!({"status":status.as_str(),"output":raw}).to_string(),
            task_id,
            &iso_now(&self.inner.cfg.timezone),
        )?;
        if !fresh {
            return Ok(store.get_task_run(task_id)?.is_some_and(|r| {
                matches!(r.status.as_str(), "awaiting_approval" | "waiting_event")
            }));
        }
        let mut output = crate::markers::CompiledOutput::from_output(raw);
        let effects = output.effects.clone();
        let mut approval = false;
        let mut children = Vec::new();
        if matches!(
            status,
            crate::TurnStatus::Ok
                | crate::TurnStatus::ParseRecovered
                | crate::TurnStatus::ParseFallback
        ) {
            if let Some(dependency) = effects.iter().find_map(|e| match e {
                crate::markers::Effect::WaitForTask(id) => Some(id),
                _ => None,
            }) {
                anyhow::ensure!(
                    effects
                        .iter()
                        .filter(|e| matches!(e, crate::markers::Effect::WaitForTask(_)))
                        .count()
                        == 1
                        && !effects.iter().any(|e| matches!(
                            e,
                            crate::markers::Effect::SendApproval(_)
                                | crate::markers::Effect::DelegateTask(_)
                        )),
                    "one wait reason per outcome"
                );
                let dependency: i64 = dependency
                    .trim()
                    .parse()
                    .context("invalid dependency task number")?;
                store.wait_for_task(
                    task_id,
                    dependency,
                    &crate::approval::build_resume_payload(request, input)?,
                )?;
                output = crate::markers::CompiledOutput::new(
                    &format!("Task #{task_id} is waiting for task #{dependency}."),
                    &[],
                );
                approval = true;
            } else if let Some(prompt) = effects.iter().find_map(|e| match e {
                crate::markers::Effect::SendApproval(p) => Some(p.as_str()),
                _ => None,
            }) {
                let id = create_approval_request(
                    store,
                    request,
                    input,
                    task_id,
                    prompt,
                    &self.inner.cfg.timezone,
                )?;
                store.register_task_wait(
                    task_id,
                    "approval",
                    id,
                    &crate::approval::build_resume_payload(request, input)?,
                )?;
                output = crate::markers::CompiledOutput::new(
                    &format!("{prompt}\nApprove with /approve {id}, or reject with /reject {id}."),
                    &[],
                );
                approval = true;
            } else if !request.delegated {
                for (index, prompt) in effects
                    .iter()
                    .filter_map(|e| match e {
                        crate::markers::Effect::DelegateTask(p) => Some(p),
                        _ => None,
                    })
                    .enumerate()
                {
                    let mut child = request.clone();
                    child.delegated = true;
                    child.media = None;
                    child.input = crate::turn::resolve_turn_input(
                        &self.inner.cfg,
                        store,
                        None,
                        Some(prompt.clone()),
                        None,
                        request.source.channel_name(),
                    )?;
                    child.update_id = Some(format!("delegation:{task_id}:{index}"));
                    child.progress_message_id = None;
                    child.quoted = QuotedMessage {
                        reply_from: None,
                        reply_text: None,
                        reply_ts: None,
                    };
                    let (id, _) = store.admit_inbox_request(
                        EventKind::Request,
                        child.update_id.as_deref(),
                        &child,
                        &iso_now(&self.inner.cfg.timezone),
                    )?;
                    store.link_task_child(task_id, id)?;
                    store.inherit_schedule(task_id, id)?;
                    output.suffix.push_str(&format!("\nDelegated task #{id}."));
                    children.push((id, child));
                }
            }
        }
        if request.delegated && status != crate::TurnStatus::Cancelled {
            output.prefix = format!("Task #{task_id} result\n");
            let reply = format!("{}{}", output.prefix, output.reply());
            store.conn.execute(
                "UPDATE turns SET telegram_reply=?2 WHERE task_run_id=?1",
                rusqlite::params![task_id, reply],
            )?;
        }
        let output = output.encode();
        store.conn.execute(
            "UPDATE task_payloads SET output=?2 WHERE task_run_id=?1",
            rusqlite::params![task_id, output],
        )?;
        if !approval {
            let task_status = match status {
                crate::TurnStatus::Cancelled => TaskRunStatus::Cancelled,
                crate::TurnStatus::AgentError | crate::TurnStatus::AgentErrorRecovered => {
                    TaskRunStatus::Failed
                }
                _ => TaskRunStatus::Completed,
            };
            store.finish_task_run(
                task_id,
                task_status,
                &iso_now(&self.inner.cfg.timezone),
                None,
                Some("result persisted"),
            )?;
        }
        if approval {
            store.record_initial_blocker(task_id, &iso_now(&self.inner.cfg.timezone))?;
        }
        tx.commit()?;
        if !self
            .inner
            .recovering
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            for (id, child) in children {
                self.queue_existing(id, child);
            }
        }
        Ok(approval)
    }

    fn persist_failure(
        &self,
        store: &Store,
        id: i64,
        request: &TaskRequest,
        error: &anyhow::Error,
        now: &str,
    ) -> Result<()> {
        let _queue = self.inner.queue_lock.lock().expect("durable enqueue order");
        let tx = Transaction::new_unchecked(&store.conn, TransactionBehavior::Immediate)?;
        let existing: Option<String> = store.conn.query_row(
            "SELECT output FROM task_payloads WHERE task_run_id=?1",
            [id],
            |r| r.get(0),
        )?;
        if existing.is_some() {
            return Ok(());
        }
        let output = crate::markers::render_output(
            &format!(
                "Run #{id}: outcome unconfirmed (Unknown). Check Tasks and the conversation, then inspect local run evidence before doing this work again. No automatic retry."
            ),
            &[],
        );
        store.record_inbox(
            &format!("result:{id}"),
            EventKind::Result,
            &request.session.id(),
            &serde_json::json!({"outcome":"unknown","output":output}).to_string(),
            id,
            now,
        )?;
        store.finish_task_run(
            id,
            TaskRunStatus::Failed,
            now,
            Some(&format!("outcome unknown; no automatic retry: {error:#}")),
            Some("task execution failed"),
        )?;
        store.conn.execute(
            "UPDATE task_payloads SET output=?2 WHERE task_run_id=?1",
            rusqlite::params![id, output],
        )?;
        tx.commit()?;
        Ok(())
    }

    fn queue_existing(&self, id: i64, request: TaskRequest) {
        {
            let mut lanes = self.inner.lanes.lock().expect("lanes");
            if lanes
                .values()
                .any(|l| l.active == Some(id) || l.queue.iter().any(|q| q.id == id))
            {
                return;
            }
            let key = if request.delegated {
                format!("task:{id}")
            } else {
                request.session.id()
            };
            let lane = lanes.entry(key).or_default();
            lane.queue.push_back(QueuedTask { id, request });
        }
        self.dispatch_ready();
    }

    /// Reserve bounded execution slots before creating threads. Queued work has
    /// no worker. The oldest eligible lane wins, including after recovery;
    /// delegation and approval resumes use this same admission path.
    fn dispatch_ready(&self) {
        let ready = {
            let Ok(store) = Store::connect(&self.inner.cfg) else {
                return;
            };
            // Serialize the ingress snapshot AND lane reservation with raw
            // admission. Release this read-only writer gate before any spawn/I/O.
            let Ok(_reservation) =
                Transaction::new_unchecked(&store.conn, TransactionBehavior::Immediate)
            else {
                return;
            };
            let Ok(blocked) = store.blocked_telegram_sessions() else {
                return;
            };
            #[cfg(test)]
            {
                let probe = self.inner.reservation_probe.lock().unwrap().take();
                if let Some((reached, resume)) = probe {
                    reached.wait();
                    resume.wait();
                }
            }
            let mut lanes = self.inner.lanes.lock().expect("lanes");
            if self
                .inner
                .recovering
                .load(std::sync::atomic::Ordering::SeqCst)
                || self
                    .inner
                    .stopping
                    .load(std::sync::atomic::Ordering::SeqCst)
            {
                return;
            }
            let available =
                5usize.saturating_sub(lanes.values().filter(|lane| lane.active.is_some()).count());
            let mut ready = Vec::new();
            for _ in 0..available {
                let Some(key) = lanes
                    .iter()
                    .filter(|(_, lane)| lane.active.is_none())
                    .filter(|(_, lane)| {
                        !lane
                            .queue
                            .front()
                            .is_some_and(|q| blocked.contains(&q.request.session.id()))
                    })
                    .filter(|(_, lane)| {
                        !lane.queue.front().is_some_and(|q| {
                            q.request.session.thread_id.is_some()
                                && self.inner.cfg.telegram_topics_enabled == Some(false)
                        })
                    })
                    .filter_map(|(key, lane)| lane.queue.front().map(|q| (key.clone(), q.id)))
                    .min_by_key(|(_, id)| *id)
                    .map(|(key, _)| key)
                else {
                    break;
                };
                let lane = lanes.get_mut(&key).expect("eligible lane");
                let next = lane.queue.pop_front().expect("eligible task");
                lane.active = Some(next.id);
                ready.push(next);
            }
            ready
        };
        for next in ready {
            self.spawn_task(next.id, next.request);
        }
    }

    fn deliver(&self, store: &Store, id: i64, request: &TaskRequest, output: &str) -> Result<()> {
        if matches!(
            &request.delivery,
            DeliveryTarget::Telegram {
                thread_id: Some(_),
                ..
            }
        ) && self.inner.cfg.telegram_topics_enabled == Some(false)
        {
            anyhow::bail!(
                "topic delivery paused while BotFather topic mode is OFF; data preserved"
            );
        }
        let _delivery_lock = self.inner.delivery_lock.lock().expect("delivery lock");
        let client = if matches!(request.delivery, DeliveryTarget::Telegram { .. }) {
            Some(build_telegram_client(&self.inner.cfg)?)
        } else {
            None
        };
        let (done, mut state): (bool, String) = store.conn.query_row(
            "SELECT delivered,delivery_state FROM task_payloads WHERE task_run_id=?1",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        if done {
            return Ok(());
        }
        let cancelled: bool = store.conn.query_row(
            "SELECT status='cancelled' FROM task_runs WHERE id=?1",
            [id],
            |r| r.get(0),
        )?;
        let saved_reply = store
            .turn_output_for_task_run(id)?
            .filter(|turn| turn.status == "cancelled")
            .map(|turn| turn.telegram_reply)
            .filter(|reply| !reply.trim().is_empty());
        let cancellation_output = crate::markers::CompiledOutput::new(
            saved_reply
                .as_deref()
                .unwrap_or(crate::wording::CANCELLATION),
            &[],
        )
        .encode();
        let was_wait: bool = store.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM runtime_waits WHERE run_id=?1)",
            [id],
            |r| r.get(0),
        )?;
        let output = if cancelled && !was_wait {
            if output != cancellation_output {
                store.conn.execute(
                    "UPDATE task_payloads SET output=?2,delivery_state='[]' WHERE task_run_id=?1",
                    rusqlite::params![id, cancellation_output],
                )?;
                state = "[]".into();
            }
            cancellation_output.as_str()
        } else {
            output
        };
        if crate::delivery::dispatch_task_output(
            store,
            &self.inner.cfg,
            client.as_ref(),
            &request.delivery,
            crate::delivery::TaskDispatch {
                task_id: crate::delivery::DeliveryKey::Task(id),
                output,
                progress_message_id: request.progress_message_id.as_deref(),
            },
            &state,
        )? {
            store.conn.execute(
                "UPDATE task_payloads SET delivered=1 WHERE task_run_id=?1",
                [id],
            )?;
            let has_approval: bool = store.conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM approvals WHERE task_run_id=?1)
                  OR EXISTS(SELECT 1 FROM runtime_waits WHERE run_id=?1)",
                [id],
                |r| r.get(0),
            )?;
            if !has_approval
                && request.input.attachment_owned
                && let Some(path) = request.input.attachment_path.as_ref()
            {
                cleanup_attachment_path(path);
            }
        }
        Ok(())
    }

    pub(crate) fn retry_deliveries(&self) -> Result<()> {
        if self
            .inner
            .stopping
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Ok(());
        }
        let store = Store::connect(&self.inner.cfg)?;
        let mut stmt = store.conn.prepare(
            "SELECT task_run_id,request,output FROM task_payloads WHERE delivered=0 AND output IS NOT NULL ORDER BY task_run_id")?;
        let rows = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        for (id, raw, output) in rows {
            if self
                .inner
                .stopping
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                break;
            }
            let request = crate::approval::request_from_payload(&raw, false)?;
            if let Err(err) = self.deliver(&store, id, &request, &output) {
                tracing::warn!(task_id = id, "delivery retry pending: {err:#}");
            }
        }
        let _delivery_lock = self.inner.delivery_lock.lock().expect("delivery lock");
        crate::loops::webhook::deliver_telegram_acks(&self.inner.cfg, &store)?;
        crate::delivery::deliver_notifications(&self.inner.cfg, &store)?;
        Ok(())
    }

    pub(crate) fn recover(&self) -> Result<()> {
        self.inner
            .recovering
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let store = Store::connect(&self.inner.cfg)?;
        store.apply_runtime_results(&iso_now(&self.inner.cfg.timezone))?;
        crate::nodes::recover(&store, &iso_now(&self.inner.cfg.timezone))?;
        store.mark_stale_task_runs_failed(&iso_now(&self.inner.cfg.timezone))?;
        let mut stmt = store.conn.prepare(
            "SELECT p.task_run_id,p.request FROM task_payloads p WHERE p.output IS NULL
             AND EXISTS(SELECT 1 FROM turns t WHERE t.task_run_id=p.task_run_id) ORDER BY p.task_run_id")?;
        let rows = stmt
            .query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(stmt);
        for (id, raw) in rows {
            let request = crate::approval::request_from_payload(&raw, false)?;
            if let Some(turn) = store.turn_output_for_task_run(id)? {
                let status = match turn.status.as_str() {
                    "ok" => crate::TurnStatus::Ok,
                    "parse_recovered" => crate::TurnStatus::ParseRecovered,
                    "parse_fallback" => crate::TurnStatus::ParseFallback,
                    "cancelled" => crate::TurnStatus::Cancelled,
                    _ => crate::TurnStatus::AgentError,
                };
                let mut reply = turn.telegram_reply.clone();
                if request.delegated && status != crate::TurnStatus::Cancelled {
                    // Older/current successful materializations retain a labeled
                    // history reply. Remove only that exact proven decoration,
                    // not an arbitrary prefix in literal provider text.
                    let original = crate::turn::resolve_turn_result(
                        &turn.provider_raw,
                        request.source.channel_name(),
                        true,
                        false,
                    )
                    .telegram_reply;
                    if reply == format!("Task #{id} result\n{original}") {
                        reply = original;
                    }
                }
                let output = if status == crate::TurnStatus::Cancelled {
                    crate::markers::CompiledOutput::new(&reply, &[]).encode()
                } else {
                    crate::markers::CompiledOutput::from_saved_turn(&turn.provider_raw, &reply)
                        .encode()
                };
                self.persist_result(&store, id, &request, &request.input, &output, status)?;
            }
        }
        store.conn.execute(
            "UPDATE task_payloads SET output='TELEGRAM_REPLY: Task #' || task_run_id ||
             ' was interrupted. Outcome unknown; inspect local state before retrying.'
             WHERE output IS NULL AND EXISTS(SELECT 1 FROM task_runs r WHERE r.id=task_payloads.task_run_id
             AND r.status='failed' AND r.error_summary LIKE '%outcome unknown%')", [])?;
        self.resume_waiting_events()?;
        let mut stmt = store.conn.prepare(
            "SELECT r.id,p.request FROM task_runs r JOIN task_payloads p ON p.task_run_id=r.id
             WHERE r.status='queued' AND r.started_at IS NULL
             AND NOT EXISTS(SELECT 1 FROM runtime_remote_runs m WHERE m.task_run_id=r.id) ORDER BY r.id",
        )?;
        let rows = stmt
            .query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(stmt);
        for (id, raw) in rows {
            // New delegations materialized above may already be in a lane.
            let in_lane = self
                .inner
                .lanes
                .lock()
                .expect("lanes")
                .values()
                .any(|l| l.active == Some(id) || l.queue.iter().any(|q| q.id == id));
            if !in_lane {
                self.queue_existing(id, crate::approval::request_from_payload(&raw, false)?);
            }
        }
        // Materialization and lane reconstruction finish before any execution.
        // New child IDs sort behind all previously accepted work.
        self.inner
            .recovering
            .store(false, std::sync::atomic::Ordering::SeqCst);
        self.dispatch_ready();
        self.wake_delivery();
        Ok(())
    }

    pub(crate) fn resume_runtime_results(&self) -> Result<()> {
        {
            let _queue = self.inner.queue_lock.lock().expect("durable result order");
            Store::connect(&self.inner.cfg)?
                .apply_runtime_results(&iso_now(&self.inner.cfg.timezone))?;
        }
        self.resume_waiting_events()?;
        self.wake_delivery();
        Ok(())
    }

    pub(crate) fn resume_waiting_events(&self) -> Result<usize> {
        let _queue = self.inner.queue_lock.lock().expect("durable event order");
        if self
            .inner
            .stopping
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Ok(0);
        }
        let store = Store::connect(&self.inner.cfg)?;
        let ready = store.resume_task_events(&iso_now(&self.inner.cfg.timezone))?;
        store.observe_blockers(&iso_now(&self.inner.cfg.timezone))?;
        let count = ready.len();
        for (id, request) in ready {
            self.queue_existing(id, request);
        }
        Ok(count)
    }

    pub(crate) fn request_shutdown(&self) {
        let _lifecycle = self.inner.lifecycle.lock().expect("scheduler lifecycle");
        self.inner
            .stopping
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.wake_delivery();
        if let Some(wake) = self
            .inner
            .calendar_wake
            .lock()
            .expect("calendar wake")
            .as_ref()
        {
            let _ = wake.try_send(());
        }
        for state in self.inner.task_state.lock().expect("task state").values() {
            state
                .cancel_flag
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    /// Stop intake/queue advancement and wait at most ten seconds. Any residual
    /// network-only workers cannot keep owned provider groups alive.
    pub(crate) fn shutdown(&self) -> Result<()> {
        self.request_shutdown();
        let start = std::time::Instant::now();
        loop {
            let remaining = {
                let mut workers = self.inner.workers.lock().expect("workers");
                let mut pending = Vec::new();
                for worker in workers.drain(..) {
                    if worker.is_finished() {
                        let _ = worker.join();
                    } else {
                        pending.push(worker);
                    }
                }
                let count = pending.len();
                *workers = pending;
                count
            };
            if remaining == 0 {
                return Ok(());
            }
            if start.elapsed() >= std::time::Duration::from_secs(8) {
                coconutclaw_provider::kill_active_process_groups();
            }
            if start.elapsed() >= std::time::Duration::from_secs(10) {
                // Do not add an unbounded database wait after the deadline.
                // Startup already treats any unfinished started record as unknown.
                anyhow::bail!(
                    "shutdown deadline reached; provider groups killed; unfinished records require restart reconciliation"
                );
            }
            thread::sleep(std::time::Duration::from_millis(25));
        }
    }
}

// cleanup_attachment_path is defined in delivery module

// parse_delivery_target and serialize_delivery_target are defined in delivery module

fn render_active_task_lines(tasks: Vec<crate::store::TaskRun>, include_session_id: bool) -> String {
    if tasks.is_empty() {
        return "No active tasks.".to_string();
    }

    let mut lines = vec!["Active tasks".to_string()];
    for task in tasks {
        let mut line = format!(
            "#{} [{}] {}",
            task.id,
            crate::wording::run_state(&task.status),
            shorten_log_text(task.prompt.trim(), 80)
        );
        if include_session_id && !task.session_id.trim().is_empty() {
            line.push_str(&format!(" — {}", task.session_id));
        }
        if let Some(progress) = task.last_progress.as_deref()
            && !progress.trim().is_empty()
        {
            line.push_str(&format!(" — progress: {}", shorten_log_text(progress, 40)));
        }
        lines.push(line);
    }

    lines.join("\n")
}

#[derive(Debug)]
enum TaskCompletion {
    WaitingRemote,
    Waiting,
    Finished {
        status: TaskRunStatus,
        summary: String,
    },
}

#[cfg(test)]
#[path = "scheduler_integration_test.rs"]
pub(crate) mod tests;
