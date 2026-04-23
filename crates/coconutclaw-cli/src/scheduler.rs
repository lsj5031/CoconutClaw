use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::thread;

use anyhow::{Context, Result};
use coconutclaw_config::RuntimeConfig;
use reqwest::blocking::Client;
use serde_json::{Value, json};

use crate::markers::parse_markers;
use crate::session::SessionKey;
use crate::slack::{
    SlackMedia, build_slack_client, build_slack_user_client, dispatch_slack_output,
    send_slack_approval_request, slack_fetch_thread_context,
};
use crate::store::{Store, TaskRunStatus};
use crate::telegram::{build_telegram_client, dispatch_telegram_output};
use crate::turn::{hydrate_slack_turn_input, hydrate_turn_input, process_turn_with_status};
use crate::{IncomingMedia, QuotedMessage, TurnInput, iso_now, shorten_log_text};

#[derive(Debug, Clone)]
pub(crate) enum TaskMedia {
    Telegram(IncomingMedia),
    Slack(SlackMedia),
}

#[derive(Debug, Clone)]
pub(crate) enum DispatchTarget {
    Telegram {
        chat_id: String,
    },
    Slack {
        channel_id: String,
        thread_ts: Option<String>,
    },
    Stdout,
}

#[derive(Debug, Clone)]
pub(crate) struct TaskRequest {
    pub(crate) session: SessionKey,
    pub(crate) channel: String,
    pub(crate) input: TurnInput,
    pub(crate) update_id: Option<String>,
    pub(crate) media: Option<TaskMedia>,
    pub(crate) quoted: QuotedMessage,
    pub(crate) dispatch: DispatchTarget,
    pub(crate) source_user_id: Option<String>,
    pub(crate) progress_message_id: Option<String>,
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
}

#[derive(Clone, Debug)]
pub(crate) struct SessionScheduler {
    inner: Arc<SchedulerInner>,
}

pub(crate) fn cleanup_attachment_from_resume_payload(payload: &str) -> Result<()> {
    let value: Value = serde_json::from_str(payload).context("parse approval resume payload")?;
    let attachment_owned = value
        .get("attachment_owned")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let attachment_path = value
        .get("attachment_path")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|path| !path.is_empty())
        .map(PathBuf::from);

    if attachment_owned && let Some(path) = attachment_path.as_deref() {
        cleanup_attachment_path(path);
    }

    Ok(())
}

impl SessionScheduler {
    pub(crate) fn new(cfg: RuntimeConfig) -> Self {
        Self {
            inner: Arc::new(SchedulerInner {
                cfg,
                lanes: Mutex::new(HashMap::new()),
                task_state: Mutex::new(HashMap::new()),
            }),
        }
    }

    pub(crate) fn enqueue(&self, request: TaskRequest) -> Result<i64> {
        let created_at = iso_now(&self.inner.cfg.timezone);
        let store = Store::open(&self.inner.cfg)?;
        let task_id = store.insert_task_run(crate::store::InsertTaskRunParams {
            session_id: request.session.id(),
            channel: request.channel.clone(),
            source_chat_id: Some(self.transport_chat_id(&request.dispatch).to_string()),
            source_user_id: request.source_user_id.clone(),
            update_id: request.update_id.clone(),
            prompt: request.input.user_text.clone(),
            created_at: created_at.clone(),
            progress_message_id: request.progress_message_id.clone(),
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
            channel: request.channel.clone(),
            source_chat_id: Some(self.transport_chat_id(&request.dispatch).to_string()),
            source_user_id: request.source_user_id.clone(),
            update_id: request.update_id.clone(),
            prompt: request.input.user_text.clone(),
            created_at: created_at.clone(),
            progress_message_id: request.progress_message_id.clone(),
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

    pub(crate) fn cancel_task(&self, task_id: i64) -> Result<bool> {
        let now = iso_now(&self.inner.cfg.timezone);
        let store = Store::open(&self.inner.cfg)?;
        let Some(task) = store.get_task_run(task_id)? else {
            return Ok(false);
        };

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

    pub(crate) fn cancel_task_for_session(&self, task_id: i64, session_id: &str) -> Result<bool> {
        let store = Store::open(&self.inner.cfg)?;
        let Some(task) = store.get_task_run(task_id)? else {
            return Ok(false);
        };
        if task.session_id != session_id {
            return Ok(false);
        }
        self.cancel_task(task_id)
    }

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
                let resume_request = self.request_from_resume_payload(&approval.resume_payload)?;
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
            Some(TaskMedia::Slack(media)) => {
                let mut input = request.input.clone();
                if let DispatchTarget::Slack {
                    channel_id,
                    thread_ts,
                } = &request.dispatch
                {
                    self.populate_slack_thread_context(
                        &store,
                        channel_id,
                        thread_ts.as_deref(),
                        &request.session.id(),
                        &mut input,
                    );
                }
                hydrate_slack_turn_input(
                    &self.inner.cfg,
                    request.update_id.as_deref(),
                    input,
                    Some(media),
                )?
            }
            None => (request.input.clone(), None),
        };

        let processed = match process_turn_with_status(
            &self.inner.cfg,
            &mut store,
            input.clone(),
            &request.channel,
            Some(request.session.id()),
            request.update_id.clone(),
            request.progress_message_id.as_deref(),
            &request.quoted,
            Some(cancel_flag),
            false,
        ) {
            Ok(processed) => processed,
            Err(err) => {
                Self::cleanup_task_attachment(cleanup_path.as_ref(), &request, &input);
                return Err(err);
            }
        };

        let task_result = (|| -> Result<TaskCompletion> {
            let markers = parse_markers(&processed.output);
            match &request.dispatch {
                DispatchTarget::Telegram { chat_id } => {
                    let client = build_telegram_client(&self.inner.cfg)?;
                    dispatch_telegram_output(
                        &client,
                        &self.inner.cfg,
                        Some(chat_id),
                        &processed.output,
                        request.progress_message_id.as_deref(),
                    )?;
                    if request.channel == "scheduled"
                        && self.inner.cfg.slack_channel_id.as_deref().is_some()
                        && let Ok(slack_client) = build_slack_client(&self.inner.cfg)
                        && let Err(err) = dispatch_slack_output(
                            &slack_client,
                            &self.inner.cfg,
                            self.inner
                                .cfg
                                .slack_channel_id
                                .as_deref()
                                .unwrap_or_default(),
                            &processed.output,
                            None,
                            None,
                        )
                    {
                        tracing::warn!("failed to mirror scheduled task output to slack: {err:#}");
                    }
                }
                DispatchTarget::Slack {
                    channel_id,
                    thread_ts,
                } => {
                    let client = build_slack_client(&self.inner.cfg)?;
                    dispatch_slack_output(
                        &client,
                        &self.inner.cfg,
                        channel_id,
                        &processed.output,
                        request.progress_message_id.as_deref(),
                        thread_ts.as_deref(),
                    )?;

                    if let Some(prompt) = markers.send_approval.first() {
                        let resume_payload = self.build_resume_payload(&request, &input)?;
                        store.mark_task_run_awaiting_approval(task_id)?;
                        let approval_id =
                            store.create_approval(crate::store::CreateApprovalParams {
                                task_run_id: task_id,
                                session_id: request.session.id(),
                                channel: request.channel.clone(),
                                source_user_id: request.source_user_id.clone(),
                                channel_id: Some(channel_id.to_string()),
                                thread_ts: thread_ts.clone(),
                                prompt_text: prompt.clone(),
                                request_message_ts: None,
                                resume_payload,
                                created_at: iso_now(&self.inner.cfg.timezone),
                            })?;
                        let approval_message_ts = match send_slack_approval_request(
                            &client,
                            channel_id,
                            thread_ts.as_deref(),
                            approval_id,
                            prompt,
                        ) {
                            Ok(ts) => ts,
                            Err(err) => {
                                let now = iso_now(&self.inner.cfg.timezone);
                                let _ = store.resolve_approval(approval_id, "expired", &now, None);
                                return Err(err);
                            }
                        };
                        store.update_approval_request_message_ts(
                            approval_id,
                            &approval_message_ts,
                        )?;
                        return Ok(TaskCompletion::AwaitingApproval);
                    }
                }
                DispatchTarget::Stdout => {
                    println!("{}", processed.output);
                }
            }

            if processed.status != crate::TurnStatus::AgentError
                && processed.status != crate::TurnStatus::Cancelled
                && let Some(scheduled_task_id) = request.scheduled_task_id
            {
                let now = iso_now(&self.inner.cfg.timezone);
                if let Err(err) = store.mark_scheduled_task_executed(scheduled_task_id, &now) {
                    tracing::warn!(
                        "failed to mark scheduled task as executed id={scheduled_task_id}: {err:#}"
                    );
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

    fn populate_slack_thread_context(
        &self,
        store: &Store,
        channel_id: &str,
        thread_ts: Option<&str>,
        session_id: &str,
        input: &mut TurnInput,
    ) {
        let Some(thread_ts) = thread_ts else {
            return;
        };
        let boundary_unix = match store.latest_boundary_unix(session_id, "slack") {
            Ok(ts) => ts,
            Err(err) => {
                tracing::warn!("failed to read slack context boundary: {err:#}");
                None
            }
        };
        let history_client = build_slack_user_client(&self.inner.cfg)
            .unwrap_or(None)
            .unwrap_or_else(|| match build_slack_client(&self.inner.cfg) {
                Ok(client) => client,
                Err(err) => {
                    tracing::warn!("failed to build slack history client: {err:#}");
                    Client::new()
                }
            });
        match slack_fetch_thread_context(
            &history_client,
            channel_id,
            Some(thread_ts),
            boundary_unix.map(|ts| ts as f64),
        ) {
            Ok(ctx) if !ctx.trim().is_empty() => input.supplemental_context = Some(ctx),
            Ok(_) => input.supplemental_context = None,
            Err(err) => tracing::warn!("failed to fetch slack thread context: {err:#}"),
        }
    }

    fn build_resume_payload(&self, request: &TaskRequest, input: &TurnInput) -> Result<String> {
        Ok(json!({
            "session_id": request.session.id(),
            "channel": request.channel,
            "input_type": input.input_type.as_str(),
            "user_text": input.user_text,
            "asr_text": input.asr_text,
            "attachment_type": input.attachment_type,
            "attachment_path": input.attachment_path.as_ref().map(|path| path.display().to_string()),
            "attachment_owned": input.attachment_owned,
            "supplemental_context": input.supplemental_context,
            "dispatch": match &request.dispatch {
                DispatchTarget::Telegram { chat_id } => json!({"kind": "telegram", "chat_id": chat_id}),
                DispatchTarget::Slack { channel_id, thread_ts } => json!({"kind": "slack", "channel_id": channel_id, "thread_ts": thread_ts}),
                DispatchTarget::Stdout => json!({"kind": "stdout"}),
            },
            "source_user_id": request.source_user_id,
            "quoted": {
                "reply_from": request.quoted.reply_from,
                "reply_text": request.quoted.reply_text,
                "reply_ts": request.quoted.reply_ts,
            },
        })
        .to_string())
    }

    fn request_from_resume_payload(&self, payload: &str) -> Result<TaskRequest> {
        let value: Value =
            serde_json::from_str(payload).context("invalid approval resume payload")?;
        let session_id = value
            .get("session_id")
            .and_then(Value::as_str)
            .unwrap_or("slack:unknown");
        let dispatch = match value.get("dispatch").and_then(Value::as_object) {
            Some(dispatch) if dispatch.get("kind").and_then(Value::as_str) == Some("telegram") => {
                DispatchTarget::Telegram {
                    chat_id: dispatch
                        .get("chat_id")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                }
            }
            Some(dispatch) if dispatch.get("kind").and_then(Value::as_str) == Some("slack") => {
                DispatchTarget::Slack {
                    channel_id: dispatch
                        .get("channel_id")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    thread_ts: dispatch
                        .get("thread_ts")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned),
                }
            }
            _ => DispatchTarget::Stdout,
        };
        let session = if let Some(stripped) = session_id.strip_prefix("telegram:") {
            SessionKey::telegram(stripped)
        } else if let Some(stripped) = session_id.strip_prefix("slack:") {
            if let Some((channel, thread_id)) = stripped.split_once('#') {
                SessionKey::slack(channel, Some(thread_id))
            } else {
                SessionKey::slack(stripped, None)
            }
        } else if let Some(stripped) = session_id.strip_prefix("scheduled:") {
            SessionKey::scheduled(stripped)
        } else {
            SessionKey::local(session_id)
        };

        let mut input = TurnInput {
            input_type: match value
                .get("input_type")
                .and_then(Value::as_str)
                .unwrap_or("text")
            {
                "voice" => crate::InputType::Voice,
                "photo" => crate::InputType::Photo,
                "video" => crate::InputType::Video,
                "document" => crate::InputType::Document,
                "video_note" => crate::InputType::VideoNote,
                _ => crate::InputType::Text,
            },
            user_text: value
                .get("user_text")
                .and_then(Value::as_str)
                .unwrap_or("(empty message)")
                .to_string(),
            asr_text: value
                .get("asr_text")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            attachment_type: value
                .get("attachment_type")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            attachment_path: value
                .get("attachment_path")
                .and_then(Value::as_str)
                .map(PathBuf::from),
            attachment_owned: value
                .get("attachment_owned")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            supplemental_context: value
                .get("supplemental_context")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            channel: value
                .get("channel")
                .and_then(Value::as_str)
                .unwrap_or("slack")
                .to_string(),
        };
        let approval_note = "Administrator approval granted for the previous pending request. Continue the approved work without asking for approval again.";
        input.supplemental_context = Some(match input.supplemental_context.take() {
            Some(existing) if !existing.trim().is_empty() => format!("{existing}\n{approval_note}"),
            _ => approval_note.to_string(),
        });

        Ok(TaskRequest {
            session,
            channel: value
                .get("channel")
                .and_then(Value::as_str)
                .unwrap_or("slack")
                .to_string(),
            input,
            update_id: None,
            media: None,
            quoted: QuotedMessage {
                reply_from: value
                    .get("quoted")
                    .and_then(|quoted| quoted.get("reply_from"))
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
                reply_text: value
                    .get("quoted")
                    .and_then(|quoted| quoted.get("reply_text"))
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
                reply_ts: value
                    .get("quoted")
                    .and_then(|quoted| quoted.get("reply_ts"))
                    .and_then(Value::as_i64),
            },
            dispatch,
            source_user_id: value
                .get("source_user_id")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            progress_message_id: None,
            scheduled_task_id: None,
        })
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

    fn transport_chat_id<'a>(&self, dispatch: &'a DispatchTarget) -> &'a str {
        match dispatch {
            DispatchTarget::Telegram { chat_id } => chat_id,
            DispatchTarget::Slack { channel_id, .. } => channel_id,
            DispatchTarget::Stdout => "local",
        }
    }
}

fn cleanup_attachment_path(path: &Path) {
    if let Err(err) = std::fs::remove_file(path)
        && err.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!("failed to remove attachment {}: {err:#}", path.display());
    }
}

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
mod tests {
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;
    use std::time::{Duration, Instant};

    use coconutclaw_config::{AgentProvider, RuntimeConfig};
    use serde_json::json;

    use super::{
        DispatchTarget, SessionKey, SessionScheduler, TaskRequest,
        cleanup_attachment_from_resume_payload,
    };
    use crate::store::Store;
    use crate::{InputType, QuotedMessage, TurnInput};

    fn write_fake_provider_script(
        dir: &std::path::Path,
        stem: &str,
        unix_body: &str,
        windows_body: &str,
    ) -> String {
        let (path, command) = if cfg!(windows) {
            let path = dir.join(format!("{stem}.ps1"));
            let command = format!(
                "powershell -NoProfile -ExecutionPolicy Bypass -File \"{}\"",
                path.display()
            );
            (path, command)
        } else {
            let path = dir.join(format!("{stem}.sh"));
            let command = path.display().to_string();
            (path, command)
        };

        fs::write(
            &path,
            if cfg!(windows) {
                windows_body
            } else {
                unix_body
            },
        )
        .expect("write provider script");

        #[cfg(unix)]
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755))
            .expect("chmod provider script");

        command
    }

    fn scheduler_test_config(provider_bin: String) -> RuntimeConfig {
        let mut cfg = RuntimeConfig::test_config();
        cfg.provider = AgentProvider::Codex;
        cfg.codex.bin = provider_bin;
        cfg
    }

    fn make_request(session: SessionKey, channel: &str, prompt: &str) -> TaskRequest {
        TaskRequest {
            session,
            channel: channel.to_string(),
            input: TurnInput {
                input_type: InputType::Text,
                user_text: prompt.to_string(),
                asr_text: String::new(),
                attachment_type: None,
                attachment_path: None,
                attachment_owned: false,
                supplemental_context: None,
                channel: channel.to_string(),
            },
            update_id: None,
            media: None,
            quoted: QuotedMessage {
                reply_from: None,
                reply_text: None,
                reply_ts: None,
            },
            dispatch: DispatchTarget::Stdout,
            source_user_id: None,
            progress_message_id: None,
            scheduled_task_id: None,
        }
    }

    fn make_stdout_request(session: SessionKey, prompt: &str) -> TaskRequest {
        make_request(session, "local", prompt)
    }

    fn wait_for_terminal_tasks(cfg: &RuntimeConfig, task_ids: &[i64]) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let store = crate::store::Store::open(cfg).expect("open store");
            let all_done = task_ids.iter().all(|task_id| {
                store
                    .get_task_run(*task_id)
                    .expect("task lookup")
                    .and_then(|task| match task.status.as_str() {
                        "completed" | "failed" | "cancelled" => Some(task),
                        _ => None,
                    })
                    .is_some()
            });
            if all_done {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "tasks did not reach terminal state"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    #[test]
    fn request_from_resume_payload_restores_owned_attachment() {
        let scheduler = SessionScheduler::new(RuntimeConfig::test_config());
        let payload = r#"{
            "session_id": "slack:C123#171.5",
            "channel": "slack",
            "input_type": "document",
            "user_text": "review this",
            "asr_text": "",
            "attachment_type": "application/pdf",
            "attachment_path": "/tmp/approval.pdf",
            "attachment_owned": true,
            "dispatch": {"kind": "slack", "channel_id": "C123", "thread_ts": "171.5"},
            "quoted": {}
        }"#;

        let request = scheduler
            .request_from_resume_payload(payload)
            .expect("resume request");

        assert!(request.input.attachment_owned);
        assert_eq!(
            request
                .input
                .attachment_path
                .as_deref()
                .map(|path| path.to_string_lossy().to_string()),
            Some("/tmp/approval.pdf".to_string())
        );
    }

    #[test]
    fn build_resume_payload_keeps_attachment_ownership() {
        let scheduler = SessionScheduler::new(RuntimeConfig::test_config());
        let request = TaskRequest {
            session: SessionKey::slack("C123", Some("171.5")),
            channel: "slack".to_string(),
            input: TurnInput {
                input_type: InputType::Document,
                user_text: "ship it".to_string(),
                asr_text: String::new(),
                attachment_type: Some("application/pdf".to_string()),
                attachment_path: Some(PathBuf::from("/tmp/input.pdf")),
                attachment_owned: true,
                supplemental_context: None,
                channel: "slack".to_string(),
            },
            update_id: Some("evt-1".to_string()),
            media: None,
            quoted: QuotedMessage {
                reply_from: None,
                reply_text: None,
                reply_ts: None,
            },
            dispatch: DispatchTarget::Slack {
                channel_id: "C123".to_string(),
                thread_ts: Some("171.5".to_string()),
            },
            source_user_id: Some("U123".to_string()),
            progress_message_id: None,
            scheduled_task_id: None,
        };

        let payload = scheduler
            .build_resume_payload(&request, &request.input)
            .expect("resume payload");

        assert!(payload.contains(r#""attachment_owned":true"#));
    }

    #[test]
    fn different_sessions_can_run_in_parallel() {
        let tmp_dir = tempfile::tempdir().expect("tempdir");
        let markers_dir = tmp_dir.path().join("parallel-markers");
        fs::create_dir_all(&markers_dir).expect("create markers dir");
        let start_one = markers_dir.join("start-one");
        let start_two = markers_dir.join("start-two");
        let saw_one = markers_dir.join("saw-one");
        let saw_two = markers_dir.join("saw-two");
        let provider_bin = write_fake_provider_script(
            tmp_dir.path(),
            "parallel-provider",
            &format!(
                r#"#!/bin/bash
for ((i=1; i<=$#; i++)); do
    if [[ "${{!i}}" == "--output-last-message" ]]; then
        j=$((i + 1))
        OUT_FILE="${{!j}}"
    fi
done
CONTEXT="${{@: -1}}"
if [[ "$CONTEXT" == *"parallel-one"* ]]; then
    touch "{start_one}"
    for _ in $(seq 1 40); do
        if [[ -f "{start_two}" ]]; then
            touch "{saw_one}"
            break
        fi
        sleep 0.1
    done
    printf 'TELEGRAM_REPLY: one\n' > "$OUT_FILE"
elif [[ "$CONTEXT" == *"parallel-two"* ]]; then
    touch "{start_two}"
    for _ in $(seq 1 40); do
        if [[ -f "{start_one}" ]]; then
            touch "{saw_two}"
            break
        fi
        sleep 0.1
    done
    printf 'TELEGRAM_REPLY: two\n' > "$OUT_FILE"
else
    printf 'TELEGRAM_REPLY: unexpected\n' > "$OUT_FILE"
fi
"#,
                start_one = start_one.display(),
                start_two = start_two.display(),
                saw_one = saw_one.display(),
                saw_two = saw_two.display()
            ),
            &format!(
                r#"$outFile = ""
for ($i = 0; $i -lt $args.Count; $i++) {{
    if ($args[$i] -eq "--output-last-message") {{
        $outFile = $args[$i + 1]
        $i++
    }}
}}
$context = $args[$args.Count - 1]
if ($context -match "parallel-one") {{
    New-Item -ItemType File -Path "{start_one}" -Force | Out-Null
    for ($i = 0; $i -lt 40; $i++) {{
        if (Test-Path "{start_two}") {{
            New-Item -ItemType File -Path "{saw_one}" -Force | Out-Null
            break
        }}
        Start-Sleep -Milliseconds 100
    }}
    "TELEGRAM_REPLY: one" | Set-Content -Path $outFile
}} elseif ($context -match "parallel-two") {{
    New-Item -ItemType File -Path "{start_two}" -Force | Out-Null
    for ($i = 0; $i -lt 40; $i++) {{
        if (Test-Path "{start_one}") {{
            New-Item -ItemType File -Path "{saw_two}" -Force | Out-Null
            break
        }}
        Start-Sleep -Milliseconds 100
    }}
    "TELEGRAM_REPLY: two" | Set-Content -Path $outFile
}} else {{
    "TELEGRAM_REPLY: unexpected" | Set-Content -Path $outFile
}}
"#,
                start_one = start_one.display(),
                start_two = start_two.display(),
                saw_one = saw_one.display(),
                saw_two = saw_two.display()
            ),
        );
        let cfg = scheduler_test_config(provider_bin);
        let scheduler = SessionScheduler::new(cfg.clone());

        let task_one = scheduler
            .enqueue(make_stdout_request(
                SessionKey::local("parallel-1"),
                "parallel-one",
            ))
            .expect("enqueue one");
        let task_two = scheduler
            .enqueue(make_stdout_request(
                SessionKey::local("parallel-2"),
                "parallel-two",
            ))
            .expect("enqueue two");
        wait_for_terminal_tasks(&cfg, &[task_one, task_two]);

        assert!(
            saw_one.exists() && saw_two.exists(),
            "expected both sessions to observe the other start marker, markers={:?}",
            [start_one, start_two, saw_one, saw_two]
        );
    }

    #[test]
    fn same_session_stays_fifo() {
        let tmp_dir = tempfile::tempdir().expect("tempdir");
        let events_path = tmp_dir.path().join("fifo-events.log");
        let provider_bin = write_fake_provider_script(
            tmp_dir.path(),
            "fifo-provider",
            &format!(
                r#"#!/bin/bash
for ((i=1; i<=$#; i++)); do
    if [[ "${{!i}}" == "--output-last-message" ]]; then
        j=$((i + 1))
        OUT_FILE="${{!j}}"
    fi
done
CONTEXT="${{@: -1}}"
if [[ "$CONTEXT" == *"fifo-second"* ]]; then
    printf 'start:second\n' >> "{events}"
    printf 'done:second\n' >> "{events}"
    printf 'TELEGRAM_REPLY: second\n' > "$OUT_FILE"
elif [[ "$CONTEXT" == *"fifo-first"* ]]; then
    printf 'start:first\n' >> "{events}"
    sleep 2
    printf 'done:first\n' >> "{events}"
    printf 'TELEGRAM_REPLY: first\n' > "$OUT_FILE"
else
    printf 'TELEGRAM_REPLY: unexpected\n' > "$OUT_FILE"
fi
"#,
                events = events_path.display()
            ),
            &format!(
                r#"$outFile = ""
for ($i = 0; $i -lt $args.Count; $i++) {{
    if ($args[$i] -eq "--output-last-message") {{
        $outFile = $args[$i + 1]
        $i++
    }}
}}
$context = $args[$args.Count - 1]
if ($context -match "fifo-second") {{
    "start:second" | Add-Content -Path "{events}"
    "done:second" | Add-Content -Path "{events}"
    "TELEGRAM_REPLY: second" | Set-Content -Path $outFile
}} elseif ($context -match "fifo-first") {{
    "start:first" | Add-Content -Path "{events}"
    Start-Sleep -Seconds 2
    "done:first" | Add-Content -Path "{events}"
    "TELEGRAM_REPLY: first" | Set-Content -Path $outFile
}} else {{
    "TELEGRAM_REPLY: unexpected" | Set-Content -Path $outFile
}}
"#,
                events = events_path.display()
            ),
        );
        let cfg = scheduler_test_config(provider_bin);
        let scheduler = SessionScheduler::new(cfg.clone());
        let session = SessionKey::local("shared-session");

        let first = scheduler
            .enqueue(make_stdout_request(session.clone(), "fifo-first"))
            .expect("enqueue first");
        let second = scheduler
            .enqueue(make_stdout_request(session, "fifo-second"))
            .expect("enqueue second");
        wait_for_terminal_tasks(&cfg, &[first, second]);

        let events = fs::read_to_string(&events_path).expect("read events");
        let lines: Vec<&str> = events.lines().collect();
        assert_eq!(
            lines,
            vec!["start:first", "done:first", "start:second", "done:second"],
            "same-session work should remain FIFO"
        );
    }

    #[test]
    fn telegram_chats_can_run_in_parallel() {
        let tmp_dir = tempfile::tempdir().expect("tempdir");
        let markers_dir = tmp_dir.path().join("telegram-parallel-markers");
        fs::create_dir_all(&markers_dir).expect("create markers dir");
        let start_one = markers_dir.join("start-321");
        let start_two = markers_dir.join("start-999");
        let saw_one = markers_dir.join("saw-321");
        let saw_two = markers_dir.join("saw-999");
        let provider_bin = write_fake_provider_script(
            tmp_dir.path(),
            "telegram-parallel-provider",
            &format!(
                r#"#!/bin/bash
for ((i=1; i<=$#; i++)); do
    if [[ "${{!i}}" == "--output-last-message" ]]; then
        j=$((i + 1))
        OUT_FILE="${{!j}}"
    fi
done
CONTEXT="${{@: -1}}"
if [[ "$CONTEXT" == *"telegram-parallel-one"* ]]; then
    touch "{start_one}"
    for _ in $(seq 1 40); do
        if [[ -f "{start_two}" ]]; then
            touch "{saw_one}"
            break
        fi
        sleep 0.1
    done
    printf 'TELEGRAM_REPLY: one\n' > "$OUT_FILE"
elif [[ "$CONTEXT" == *"telegram-parallel-two"* ]]; then
    touch "{start_two}"
    for _ in $(seq 1 40); do
        if [[ -f "{start_one}" ]]; then
            touch "{saw_two}"
            break
        fi
        sleep 0.1
    done
    printf 'TELEGRAM_REPLY: two\n' > "$OUT_FILE"
else
    printf 'TELEGRAM_REPLY: unexpected\n' > "$OUT_FILE"
fi
"#,
                start_one = start_one.display(),
                start_two = start_two.display(),
                saw_one = saw_one.display(),
                saw_two = saw_two.display()
            ),
            &format!(
                r#"$outFile = ""
for ($i = 0; $i -lt $args.Count; $i++) {{
    if ($args[$i] -eq "--output-last-message") {{
        $outFile = $args[$i + 1]
        $i++
    }}
}}
$context = $args[$args.Count - 1]
if ($context -match "telegram-parallel-one") {{
    New-Item -ItemType File -Path "{start_one}" -Force | Out-Null
    for ($i = 0; $i -lt 40; $i++) {{
        if (Test-Path "{start_two}") {{
            New-Item -ItemType File -Path "{saw_one}" -Force | Out-Null
            break
        }}
        Start-Sleep -Milliseconds 100
    }}
    "TELEGRAM_REPLY: one" | Set-Content -Path $outFile
}} elseif ($context -match "telegram-parallel-two") {{
    New-Item -ItemType File -Path "{start_two}" -Force | Out-Null
    for ($i = 0; $i -lt 40; $i++) {{
        if (Test-Path "{start_one}") {{
            New-Item -ItemType File -Path "{saw_two}" -Force | Out-Null
            break
        }}
        Start-Sleep -Milliseconds 100
    }}
    "TELEGRAM_REPLY: two" | Set-Content -Path $outFile
}} else {{
    "TELEGRAM_REPLY: unexpected" | Set-Content -Path $outFile
}}
"#,
                start_one = start_one.display(),
                start_two = start_two.display(),
                saw_one = saw_one.display(),
                saw_two = saw_two.display()
            ),
        );
        let cfg = scheduler_test_config(provider_bin);
        let scheduler = SessionScheduler::new(cfg.clone());

        let task_one = scheduler
            .enqueue(make_request(
                SessionKey::telegram("321"),
                "telegram",
                "telegram-parallel-one",
            ))
            .expect("enqueue one");
        let task_two = scheduler
            .enqueue(make_request(
                SessionKey::telegram("999"),
                "telegram",
                "telegram-parallel-two",
            ))
            .expect("enqueue two");
        wait_for_terminal_tasks(&cfg, &[task_one, task_two]);

        assert!(
            saw_one.exists() && saw_two.exists(),
            "expected both telegram chats to observe the other start marker, markers={:?}",
            [start_one, start_two, saw_one, saw_two]
        );
    }

    #[test]
    fn telegram_same_chat_stays_fifo() {
        let tmp_dir = tempfile::tempdir().expect("tempdir");
        let events_path = tmp_dir.path().join("telegram-fifo-events.log");
        let provider_bin = write_fake_provider_script(
            tmp_dir.path(),
            "telegram-fifo-provider",
            &format!(
                r#"#!/bin/bash
for ((i=1; i<=$#; i++)); do
    if [[ "${{!i}}" == "--output-last-message" ]]; then
        j=$((i + 1))
        OUT_FILE="${{!j}}"
    fi
done
CONTEXT="${{@: -1}}"
if [[ "$CONTEXT" == *"telegram-fifo-second"* ]]; then
    printf 'start:second\n' >> "{events}"
    printf 'done:second\n' >> "{events}"
    printf 'TELEGRAM_REPLY: second\n' > "$OUT_FILE"
elif [[ "$CONTEXT" == *"telegram-fifo-first"* ]]; then
    printf 'start:first\n' >> "{events}"
    sleep 2
    printf 'done:first\n' >> "{events}"
    printf 'TELEGRAM_REPLY: first\n' > "$OUT_FILE"
else
    printf 'TELEGRAM_REPLY: unexpected\n' > "$OUT_FILE"
fi
"#,
                events = events_path.display()
            ),
            &format!(
                r#"$outFile = ""
for ($i = 0; $i -lt $args.Count; $i++) {{
    if ($args[$i] -eq "--output-last-message") {{
        $outFile = $args[$i + 1]
        $i++
    }}
}}
$context = $args[$args.Count - 1]
if ($context -match "telegram-fifo-second") {{
    "start:second" | Add-Content -Path "{events}"
    "done:second" | Add-Content -Path "{events}"
    "TELEGRAM_REPLY: second" | Set-Content -Path $outFile
}} elseif ($context -match "telegram-fifo-first") {{
    "start:first" | Add-Content -Path "{events}"
    Start-Sleep -Seconds 2
    "done:first" | Add-Content -Path "{events}"
    "TELEGRAM_REPLY: first" | Set-Content -Path $outFile
}} else {{
    "TELEGRAM_REPLY: unexpected" | Set-Content -Path $outFile
}}
"#,
                events = events_path.display()
            ),
        );
        let cfg = scheduler_test_config(provider_bin);
        let scheduler = SessionScheduler::new(cfg.clone());
        let session = SessionKey::telegram("321");

        let first = scheduler
            .enqueue(make_request(
                session.clone(),
                "telegram",
                "telegram-fifo-first",
            ))
            .expect("enqueue first");
        let second = scheduler
            .enqueue(make_request(session, "telegram", "telegram-fifo-second"))
            .expect("enqueue second");
        wait_for_terminal_tasks(&cfg, &[first, second]);

        let events = fs::read_to_string(&events_path).expect("read events");
        let lines: Vec<&str> = events.lines().collect();
        assert_eq!(
            lines,
            vec!["start:first", "done:first", "start:second", "done:second"],
            "same telegram chat should remain FIFO"
        );
    }

    #[test]
    fn cleanup_attachment_from_resume_payload_removes_owned_file() {
        let tmp_dir = tempfile::tempdir().expect("tempdir");
        let attachment_path = tmp_dir.path().join("approval.txt");
        fs::write(&attachment_path, "pending").expect("write attachment");
        let payload = json!({
            "attachment_owned": true,
            "attachment_path": attachment_path.display().to_string(),
        })
        .to_string();

        cleanup_attachment_from_resume_payload(&payload).expect("cleanup payload");

        assert!(!attachment_path.exists());
    }

    #[test]
    fn render_active_tasks_for_session_hides_other_sessions() {
        let cfg = RuntimeConfig::test_config();
        let store = Store::open(&cfg).expect("store");
        store
            .insert_task_run(crate::store::InsertTaskRunParams {
                session_id: "telegram:321".to_string(),
                channel: "telegram".to_string(),
                source_chat_id: Some("321".to_string()),
                source_user_id: None,
                update_id: Some("evt-1".to_string()),
                prompt: "session one".to_string(),
                created_at: "2026-04-22T10:00:00+0000".to_string(),
                progress_message_id: None,
            })
            .expect("insert task one");
        store
            .insert_task_run(crate::store::InsertTaskRunParams {
                session_id: "telegram:999".to_string(),
                channel: "telegram".to_string(),
                source_chat_id: Some("999".to_string()),
                source_user_id: None,
                update_id: Some("evt-2".to_string()),
                prompt: "session two".to_string(),
                created_at: "2026-04-22T10:00:01+0000".to_string(),
                progress_message_id: None,
            })
            .expect("insert task two");

        let scheduler = SessionScheduler::new(cfg);
        let output = scheduler
            .render_active_tasks_for_session("telegram:321")
            .expect("render tasks");

        assert!(output.contains("session one"));
        assert!(!output.contains("session two"));
        assert!(!output.contains("telegram:999"));
    }
}
