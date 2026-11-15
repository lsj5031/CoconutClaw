use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::thread;

use anyhow::Result;
use coconutclaw_config::RuntimeConfig;

use crate::approval::{
    cleanup_attachment_from_resume_payload, create_approval_request, request_from_resume_payload,
};
use crate::cancel::CancelRouter;
use crate::delivery::{DeliveryTarget, TaskSource, cleanup_attachment_path};
use crate::markers::parse_markers;
use crate::session::SessionKey;
use crate::store::{Store, TaskRunStatus};
use crate::telegram::build_telegram_client;
use crate::turn::{hydrate_turn_input, process_turn_with_status};
use crate::{IncomingMedia, QuotedMessage, TurnInput, iso_now, shorten_log_text};

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
}

#[derive(Clone, Debug)]
pub(crate) struct SessionScheduler {
    inner: Arc<SchedulerInner>,
}

impl SessionScheduler {
    pub(crate) fn new(cfg: RuntimeConfig, cancel_router: Option<Arc<CancelRouter>>) -> Self {
        Self {
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
            }),
        }
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
        let store = Store::open(&self.inner.cfg)?;
        let tx = store.conn.unchecked_transaction()?;
        let task_id = store.insert_task_run(crate::store::InsertTaskRunParams {
            session_id: request.session.id(),
            channel: request.source.channel_name().to_string(),
            source_chat_id: Some(self.delivery_chat_id(&request.delivery).to_string()),
            source_user_id: request.source_user_id.clone(),
            update_id: request.update_id.clone(),
            prompt: request.input.user_text.clone(),
            created_at: created_at.clone(),
            progress_message_id: request.progress_message_id.clone(),
            scheduled_task_id: None,
        })?;
        store.conn.execute(
            "INSERT INTO task_payloads(task_run_id,request) VALUES(?1,?2)",
            rusqlite::params![
                task_id,
                crate::approval::build_resume_payload(&request, &request.input)?
            ],
        )?;
        tx.commit()?;
        self.queue_existing(task_id, request);
        Ok(task_id)
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
        let now = iso_now(&self.inner.cfg.timezone);
        let store = Store::open(&self.inner.cfg)?;
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

        if task.status == TaskRunStatus::AwaitingApproval.as_str() {
            self.cleanup_pending_approval_attachment(&store, task_id);
            store.expire_pending_approvals_for_task(task_id, &now)?;
            store.finish_task_run(
                task_id,
                TaskRunStatus::Cancelled,
                &now,
                None,
                Some("approval cancelled"),
            )?;
            self.complete_task(task_id, &task.session_id)?;
            return Ok(true);
        }
        if task.status == TaskRunStatus::Queued.as_str() {
            let mut removed = false;
            let mut removed_request: Option<TaskRequest> = None;
            {
                let mut lanes = self.inner.lanes.lock().expect("lanes");
                if let Some(lane) = lanes.get_mut(&task.session_id)
                    && let Some(index) = lane.queue.iter().position(|queued| queued.id == task_id)
                {
                    removed_request = lane.queue.remove(index).map(|queued| queued.request);
                    removed = true;
                }
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
        let store = Store::open(&self.inner.cfg)?;
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
        let store = Store::open(&self.inner.cfg)?;
        let tasks = store.list_active_task_runs_for_session(session_id)?;
        Ok(render_active_task_lines(tasks, false))
    }

    pub(crate) fn has_task_for_update_id(&self, update_id: &str) -> Result<bool> {
        let store = Store::open(&self.inner.cfg)?;
        Ok(store.find_task_run_by_update_id(update_id)?.is_some())
    }

    fn cleanup_pending_approval_attachment(&self, store: &Store, task_id: i64) {
        match store.pending_approval_resume_payload_for_task(task_id) {
            Ok(Some(payload)) => {
                if let Err(err) = cleanup_attachment_from_resume_payload(&payload) {
                    tracing::warn!(
                        "failed to cleanup pending approval attachment for task {task_id}: {err:#}"
                    );
                }
            }
            Ok(None) => {}
            Err(err) => {
                tracing::warn!(
                    "failed to load pending approval payload for task {task_id}: {err:#}"
                );
            }
        }
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
        let store = Store::open(&self.inner.cfg)?;
        let Some(approval) = store.get_pending_approval(approval_id)? else {
            return Ok(Some("Approval request not found.".to_string()));
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

        match action {
            "approve" => {
                let tx = store.conn.unchecked_transaction()?;
                store.resolve_approval(approval_id, "approved", &now, Some(actor_user_id))?;
                store.finish_task_run(
                    approval.task_run_id,
                    TaskRunStatus::Completed,
                    &now,
                    None,
                    Some("approval granted"),
                )?;
                let request = request_from_resume_payload(&approval.resume_payload)?;
                let resume_id = store.insert_task_run(crate::store::InsertTaskRunParams {
                    session_id: request.session.id(),
                    channel: request.source.channel_name().into(),
                    source_chat_id: Some(request.delivery.display_id().into()),
                    source_user_id: request.source_user_id.clone(),
                    update_id: Some(format!("approval:{approval_id}")),
                    prompt: request.input.user_text.clone(),
                    created_at: now.clone(),
                    progress_message_id: None,
                    scheduled_task_id: None,
                })?;
                store.conn.execute(
                    "INSERT INTO task_payloads(task_run_id,request) VALUES(?1,?2)",
                    rusqlite::params![
                        resume_id,
                        crate::approval::build_resume_payload(&request, &request.input)?
                    ],
                )?;
                tx.commit()?;
                self.queue_existing(resume_id, request);
                Ok(Some(format!(
                    "Approval recorded. Resuming as task #{resume_id}."
                )))
            }
            "reject" => {
                if let Err(err) = cleanup_attachment_from_resume_payload(&approval.resume_payload) {
                    tracing::warn!(
                        "failed to cleanup rejected approval attachment #{approval_id}: {err:#}"
                    );
                }
                store.resolve_approval(approval_id, "rejected", &now, Some(actor_user_id))?;
                store.finish_task_run(
                    approval.task_run_id,
                    TaskRunStatus::Cancelled,
                    &now,
                    None,
                    Some("approval rejected"),
                )?;
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
            let store = match Store::open(&inner.cfg) {
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
                Ok(TaskCompletion::AwaitingApproval) => {
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
                    if let Err(store_err) = store.finish_task_run(
                        task_id,
                        TaskRunStatus::Failed,
                        &now,
                        Some(&format!("{err:#}")),
                        Some("task execution failed"),
                    ) {
                        tracing::warn!("failed to persist task failure {task_id}: {store_err:#}");
                    }
                    let output = crate::markers::render_output(
                        &format!(
                            "Task #{task_id} could not complete. Inspect local state before retrying; execution may have an unknown outcome."
                        ),
                        &[],
                    );
                    if let Err(save_err) = store.conn.execute(
                        "UPDATE task_payloads SET output=COALESCE(output,?2) WHERE task_run_id=?1",
                        rusqlite::params![task_id, output],
                    ) {
                        tracing::warn!("failed to persist failure result {task_id}: {save_err:#}");
                    }
                    if let Err(delivery_err) = scheduler.retry_deliveries() {
                        tracing::warn!(
                            "failed task result delivery pending {task_id}: {delivery_err:#}"
                        );
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
        let mut store = Store::open(&self.inner.cfg)?;
        let started_at = iso_now(&self.inner.cfg.timezone);
        store.update_task_run_started(task_id, &started_at)?;

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
            if let Err(err) = self.retry_deliveries() {
                tracing::warn!(task_id, "result persisted, delivery pending: {err:#}");
            }
            if approval {
                return Ok(TaskCompletion::AwaitingApproval);
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

    fn complete_task(&self, task_id: i64, session_id: &str) -> Result<()> {
        {
            let mut task_state = self.inner.task_state.lock().expect("task_state");
            task_state.remove(&task_id);
        }

        {
            let mut lanes = self.inner.lanes.lock().expect("lanes");
            if let Some(lane) = lanes.get_mut(session_id) {
                if lane.active != Some(task_id) {
                    return Ok(());
                }
                lane.active = None;
                if lane.queue.is_empty() {
                    lanes.remove(session_id);
                }
            }
        }
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
        let tx = store.conn.unchecked_transaction()?;
        let mut output = if status == crate::TurnStatus::Cancelled {
            crate::markers::render_output("❌ Cancelled.", &[])
        } else {
            raw.to_string()
        };
        let effects = parse_markers(&output).to_effects();
        let mut approval = false;
        let mut children = Vec::new();
        if matches!(
            status,
            crate::TurnStatus::Ok
                | crate::TurnStatus::ParseRecovered
                | crate::TurnStatus::ParseFallback
        ) {
            if let Some(prompt) = effects.iter().find_map(|e| match e {
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
                output = crate::markers::render_output(
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
                    let id = store.insert_task_run(crate::store::InsertTaskRunParams {
                        session_id: child.session.id(),
                        channel: child.source.channel_name().into(),
                        source_chat_id: Some(child.delivery.display_id().into()),
                        source_user_id: child.source_user_id.clone(),
                        update_id: child.update_id.clone(),
                        prompt: prompt.clone(),
                        created_at: iso_now(&self.inner.cfg.timezone),
                        progress_message_id: None,
                        scheduled_task_id: None,
                    })?;
                    store.conn.execute(
                        "INSERT INTO task_payloads(task_run_id,request) VALUES(?1,?2)",
                        rusqlite::params![
                            id,
                            crate::approval::build_resume_payload(&child, &child.input)?
                        ],
                    )?;
                    output.push_str(&format!("\nTELEGRAM_REPLY: Delegated task #{id}.\n"));
                    children.push((id, child));
                }
            }
        }
        if request.delegated {
            output = format!("TELEGRAM_REPLY: Task #{task_id} result\n{output}");
            let reply = parse_markers(&output).telegram_reply.unwrap_or_default();
            store.conn.execute(
                "UPDATE turns SET telegram_reply=?2 WHERE task_run_id=?1",
                rusqlite::params![task_id, reply],
            )?;
        }
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

    fn queue_existing(&self, id: i64, request: TaskRequest) {
        {
            let mut lanes = self.inner.lanes.lock().expect("lanes");
            let lane = lanes.entry(request.session.id()).or_default();
            lane.queue.push_back(QueuedTask { id, request });
        }
        self.dispatch_ready();
    }

    /// Reserve bounded execution slots before creating threads. Queued work has
    /// no worker. The oldest eligible lane wins, including after recovery;
    /// delegation and approval resumes use this same admission path.
    fn dispatch_ready(&self) {
        let ready = {
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
        let cancellation_output = crate::markers::render_output("❌ Cancelled.", &[]);
        let output = if cancelled {
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
                task_id: id,
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
                "SELECT EXISTS(SELECT 1 FROM approvals WHERE task_run_id=?1)",
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
        let store = Store::open(&self.inner.cfg)?;
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
        Ok(())
    }

    pub(crate) fn recover(&self) -> Result<()> {
        self.inner
            .recovering
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let store = Store::open(&self.inner.cfg)?;
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
                let effects = parse_markers(&turn.provider_raw).to_effects();
                let output = crate::markers::render_output(&turn.telegram_reply, &effects);
                let status = match turn.status.as_str() {
                    "ok" => crate::TurnStatus::Ok,
                    "parse_recovered" => crate::TurnStatus::ParseRecovered,
                    "parse_fallback" => crate::TurnStatus::ParseFallback,
                    "cancelled" => crate::TurnStatus::Cancelled,
                    _ => crate::TurnStatus::AgentError,
                };
                self.persist_result(&store, id, &request, &request.input, &output, status)?;
            }
        }
        store.conn.execute(
            "UPDATE task_payloads SET output='TELEGRAM_REPLY: Task #' || task_run_id ||
             ' was interrupted. Outcome unknown; inspect local state before retrying.'
             WHERE output IS NULL AND EXISTS(SELECT 1 FROM task_runs r WHERE r.id=task_payloads.task_run_id
             AND r.status='failed' AND r.error_summary LIKE '%outcome unknown%')", [])?;
        let mut stmt = store.conn.prepare(
            "SELECT r.id,p.request FROM task_runs r JOIN task_payloads p ON p.task_run_id=r.id
             WHERE r.status='queued' AND r.started_at IS NULL ORDER BY r.id",
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
        self.retry_deliveries()
    }

    pub(crate) fn request_shutdown(&self) {
        let _lifecycle = self.inner.lifecycle.lock().expect("scheduler lifecycle");
        self.inner
            .stopping
            .store(true, std::sync::atomic::Ordering::SeqCst);
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

    fn delivery_chat_id<'a>(&self, delivery: &'a DeliveryTarget) -> &'a str {
        delivery.display_id()
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
            task.status,
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
    AwaitingApproval,
    Finished {
        status: TaskRunStatus,
        summary: String,
    },
}

#[cfg(test)]
#[path = "scheduler_integration_test.rs"]
mod tests;
