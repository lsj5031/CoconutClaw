use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::thread;

use anyhow::Result;
use coconutclaw_config::RuntimeConfig;

use crate::approval::{
    cleanup_attachment_from_resume_payload, create_slack_approval_request,
    request_from_resume_payload,
};
use crate::cancel::CancelRouter;
use crate::delivery::{
    DeliveryTarget, ScheduledDeliveryResult, ScheduledTaskDispatch, TaskSource,
    cleanup_attachment_path, dispatch_immediate_output, dispatch_scheduled_task_output,
};
use crate::markers::parse_markers;
use crate::session::SessionKey;
use crate::slack::{SlackMedia, build_slack_client};
use crate::store::{Store, TaskRunStatus};
use crate::telegram::{build_telegram_client, valid_telegram_token};
use crate::turn::{hydrate_slack_turn_input, hydrate_turn_input, process_turn_with_status};
use crate::{IncomingMedia, QuotedMessage, TurnInput, iso_now, shorten_log_text};

#[derive(Debug, Clone)]
pub(crate) enum TaskMedia {
    Telegram(IncomingMedia),
    Slack(SlackMedia),
}

#[derive(Debug, Clone)]
pub(crate) struct TaskRequest {
    /// Session key for concurrency control and history.
    pub(crate) session: SessionKey,
    /// Source of the task (telegram/slack/scheduled/local).
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
    /// Scheduled delivery target when persisted or reconstructed for a legacy row.
    pub(crate) persisted_delivery_target: Option<DeliveryTarget>,
    /// User ID of the source actor (for Slack admin checks).
    pub(crate) source_user_id: Option<String>,
    /// Telegram message ID of the progress indicator.
    pub(crate) progress_message_id: Option<String>,
    /// If this task was spawned by a scheduled task, its ID.
    pub(crate) scheduled_task_id: Option<i64>,
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
    session_id: String,
    cancel_flag: Option<Arc<AtomicBool>>,
    awaiting_approval: bool,
}

#[derive(Debug)]
struct SchedulerInner {
    cfg: RuntimeConfig,
    lanes: Mutex<HashMap<String, SessionLane>>,
    task_state: Mutex<HashMap<i64, RuntimeTaskState>>,
    cancel_router: Option<Arc<CancelRouter>>,
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
            }),
        }
    }

    pub(crate) fn enqueue(&self, request: TaskRequest) -> Result<i64> {
        let created_at = iso_now(&self.inner.cfg.timezone);
        let store = Store::open(&self.inner.cfg)?;
        let task_id = store.insert_task_run(crate::store::InsertTaskRunParams {
            session_id: request.session.id(),
            channel: request.source.channel_name().to_string(),
            source_chat_id: Some(self.delivery_chat_id(&request.delivery).to_string()),
            source_user_id: request.source_user_id.clone(),
            update_id: request.update_id.clone(),
            prompt: request.input.user_text.clone(),
            created_at: created_at.clone(),
            progress_message_id: request.progress_message_id.clone(),
            scheduled_task_id: request.scheduled_task_id,
        })?;
        let session_id = request.session.id();
        let mut should_spawn = false;
        {
            let mut lanes = self.inner.lanes.lock().expect("lanes");
            let lane = lanes.entry(session_id.clone()).or_default();
            if lane.active.is_none() {
                lane.active = Some(task_id);
                should_spawn = true;
            } else {
                lane.queue.push_back(QueuedTask {
                    id: task_id,
                    request: request.clone(),
                });
            }
        }
        if should_spawn {
            self.spawn_task(task_id, request);
        }
        Ok(task_id)
    }

    pub(crate) fn enqueue_front(&self, request: TaskRequest) -> Result<i64> {
        let created_at = iso_now(&self.inner.cfg.timezone);
        let store = Store::open(&self.inner.cfg)?;
        let task_id = store.insert_task_run(crate::store::InsertTaskRunParams {
            session_id: request.session.id(),
            channel: request.source.channel_name().to_string(),
            source_chat_id: Some(self.delivery_chat_id(&request.delivery).to_string()),
            source_user_id: request.source_user_id.clone(),
            update_id: request.update_id.clone(),
            prompt: request.input.user_text.clone(),
            created_at: created_at.clone(),
            progress_message_id: request.progress_message_id.clone(),
            scheduled_task_id: request.scheduled_task_id,
        })?;
        let session_id = request.session.id();
        let mut should_spawn = false;
        {
            let mut lanes = self.inner.lanes.lock().expect("lanes");
            let lane = lanes.entry(session_id).or_default();
            if lane.active.is_none() {
                lane.active = Some(task_id);
                should_spawn = true;
            } else {
                lane.queue.push_front(QueuedTask {
                    id: task_id,
                    request: request.clone(),
                });
            }
        }
        if should_spawn {
            self.spawn_task(task_id, request);
        }
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
                if state.awaiting_approval {
                    self.cleanup_pending_approval_attachment(&store, task_id);
                    store.finish_task_run(
                        task_id,
                        TaskRunStatus::Cancelled,
                        &now,
                        None,
                        Some("approval request cancelled"),
                    )?;
                    store.expire_pending_approvals_for_task(task_id, &now)?;
                    let session_id = state.session_id.clone();
                    drop(task_state);
                    self.complete_task(task_id, &session_id)?;
                    return Ok(true);
                }
                if let Some(flag) = state.cancel_flag.as_ref() {
                    flag.store(true, std::sync::atomic::Ordering::SeqCst);
                    store.mark_task_run_cancel_requested(task_id, &now)?;
                    return Ok(true);
                }
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

    pub(crate) fn has_active_task_for_update_id(&self, update_id: &str) -> Result<bool> {
        let store = Store::open(&self.inner.cfg)?;
        Ok(store
            .find_active_task_run_by_update_id(update_id)?
            .is_some())
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
        if !self.is_slack_admin(actor_user_id) {
            return Ok(Some(
                "You are not authorized to approve this request.".to_string(),
            ));
        }

        match action {
            "approve" => {
                store.resolve_approval(approval_id, "approved", &now, Some(actor_user_id))?;
                store.finish_task_run(
                    approval.task_run_id,
                    TaskRunStatus::Completed,
                    &now,
                    None,
                    Some("approval granted"),
                )?;
                let resume_request = request_from_resume_payload(&approval.resume_payload)?;
                let _resume_task_id = self.enqueue_front(resume_request)?;
                self.complete_task(approval.task_run_id, &approval.session_id)?;
                Ok(Some("Approval recorded. Resuming the task.".to_string()))
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
        let inner = Arc::clone(&self.inner);
        let scheduler = self.clone();
        let cancel_flag = Arc::new(AtomicBool::new(false));
        {
            let mut task_state = inner.task_state.lock().expect("task_state");
            task_state.insert(
                task_id,
                RuntimeTaskState {
                    session_id: request.session.id(),
                    cancel_flag: Some(Arc::clone(&cancel_flag)),
                    awaiting_approval: false,
                },
            );
        }

        thread::spawn(move || {
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
                    if let Ok(mut task_state) = inner.task_state.lock()
                        && let Some(state) = task_state.get_mut(&task_id)
                    {
                        state.cancel_flag = None;
                        state.awaiting_approval = true;
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
            Some(TaskMedia::Slack(media)) => hydrate_slack_turn_input(
                &self.inner.cfg,
                request.update_id.as_deref(),
                request.input.clone(),
                Some(media),
            )?,
            None => (request.input.clone(), None),
        };

        let session_id_str = request.session.id();
        let delivery_target_json = crate::delivery::serialize_delivery_target(&request.delivery);
        let processed = match process_turn_with_status(
            &self.inner.cfg,
            &mut store,
            input.clone(),
            &request.source,
            Some(request.session.id()),
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
            if processed.status != crate::TurnStatus::AgentError
                && processed.status != crate::TurnStatus::Cancelled
                && let Some(scheduled_task_id) = request.scheduled_task_id
            {
                store.set_scheduled_task_pending_output(scheduled_task_id, &processed.output)?;
            }

            let effects = parse_markers(&processed.output).to_effects();
            if let Some(scheduled_task_id) = request.scheduled_task_id {
                let telegram_client = match request.persisted_delivery_target.as_ref() {
                    Some(DeliveryTarget::Slack { .. }) | Some(DeliveryTarget::Stdout) => None,
                    Some(DeliveryTarget::Telegram { .. }) | None => {
                        if valid_telegram_token(&self.inner.cfg).is_some() {
                            Some(build_telegram_client(&self.inner.cfg)?)
                        } else {
                            None
                        }
                    }
                };
                let delivered = dispatch_scheduled_task_output(
                    &store,
                    &self.inner.cfg,
                    telegram_client.as_ref(),
                    ScheduledTaskDispatch {
                        scheduled_task_id,
                        delivery_target: request.persisted_delivery_target.as_ref(),
                        output: &processed.output,
                        progress_message_id: request.progress_message_id.as_deref(),
                        delivery_state_raw: None,
                    },
                )?;
                if delivered == ScheduledDeliveryResult::Delivered {
                    let now = iso_now(&self.inner.cfg.timezone);
                    if let Err(err) = store.mark_scheduled_task_executed(scheduled_task_id, &now) {
                        tracing::warn!(
                            "failed to mark scheduled task as executed id={scheduled_task_id}: {err:#}"
                        );
                    }
                }
            } else {
                dispatch_immediate_output(
                    &self.inner.cfg,
                    &request.delivery,
                    &processed.output,
                    request.progress_message_id.as_deref(),
                )?;

                if let DeliveryTarget::Slack {
                    channel_id,
                    thread_ts,
                } = &request.delivery
                    && let Some(prompt) = effects.iter().find_map(|e| match e {
                        crate::markers::Effect::SendApproval(p) => Some(p.as_str()),
                        _ => None,
                    })
                {
                    let client = build_slack_client(&self.inner.cfg)?;
                    create_slack_approval_request(
                        &store,
                        &client,
                        &request,
                        &input,
                        task_id,
                        channel_id,
                        thread_ts.as_deref(),
                        prompt,
                        &self.inner.cfg.timezone,
                    )?;
                    return Ok(TaskCompletion::AwaitingApproval);
                }
            }

            let status = match processed.status {
                crate::TurnStatus::Cancelled => TaskRunStatus::Cancelled,
                crate::TurnStatus::AgentError => TaskRunStatus::Failed,
                _ => TaskRunStatus::Completed,
            };
            let summary = match status {
                TaskRunStatus::Cancelled => "task cancelled".to_string(),
                TaskRunStatus::Failed => "task failed".to_string(),
                _ => "task completed".to_string(),
            };
            Ok(TaskCompletion::Finished { status, summary })
        })();

        let preserve_attachment =
            matches!(task_result.as_ref(), Ok(TaskCompletion::AwaitingApproval));
        if !preserve_attachment {
            Self::cleanup_task_attachment(cleanup_path.as_ref(), &request, &input);
        }

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

    pub(crate) fn cleanup_expired_approval_attachments(&self) -> Result<()> {
        let store = Store::open(&self.inner.cfg)?;
        for payload in store.approval_resume_payloads_by_status("expired")? {
            if let Err(err) = cleanup_attachment_from_resume_payload(&payload) {
                tracing::warn!("failed to cleanup expired approval attachment: {err:#}");
            }
        }
        Ok(())
    }

    fn complete_task(&self, task_id: i64, session_id: &str) -> Result<()> {
        {
            let mut task_state = self.inner.task_state.lock().expect("task_state");
            task_state.remove(&task_id);
        }

        let mut next: Option<QueuedTask> = None;
        {
            let mut lanes = self.inner.lanes.lock().expect("lanes");
            if let Some(lane) = lanes.get_mut(session_id) {
                if lane.active == Some(task_id) {
                    lane.active = None;
                }
                if let Some(queued) = lane.queue.pop_front() {
                    lane.active = Some(queued.id);
                    next = Some(queued);
                } else if lane.active.is_none() {
                    lanes.remove(session_id);
                }
            }
        }

        if let Some(queued) = next {
            self.spawn_task(queued.id, queued.request);
        }
        Ok(())
    }

    fn is_slack_admin(&self, actor_user_id: &str) -> bool {
        if self.inner.cfg.slack_admin_user_ids.is_empty() {
            return true;
        }
        self.inner
            .cfg
            .slack_admin_user_ids
            .iter()
            .any(|candidate| candidate == actor_user_id)
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

// dispatch_scheduled_task_output is imported from crate::delivery above

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
