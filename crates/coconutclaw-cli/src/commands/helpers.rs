use anyhow::Result;
use coconutclaw_config::RuntimeConfig;

use crate::delivery::{DeliveryTarget, TaskSource};
use crate::markers::render_output;
use crate::scheduler::SessionScheduler;
use crate::session::SessionKey;
use crate::store::Store;
use crate::types::{ProcessOutcome, WebhookTurn};

fn parse_command_parts<'a>(
    text: &'a str,
    bot_username: Option<&str>,
) -> Option<(&'a str, Option<&'a str>)> {
    let trimmed = text.trim();
    if !trimmed.starts_with('/') {
        return None;
    }

    let mut parts = trimmed.splitn(2, char::is_whitespace);
    let command = crate::telegram::command_token(parts.next()?, bot_username)?;
    let arg = parts
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    Some((command, arg))
}

fn parse_task_id_arg(arg: &str) -> Option<i64> {
    arg.parse::<i64>().ok().filter(|id| *id > 0)
}

pub(crate) fn render_command_output(reply: &str) -> String {
    render_output(reply, &[]).trim_end().to_string()
}

fn enqueue_telegram_turn(
    scheduler: &SessionScheduler,
    turn: WebhookTurn,
    progress_message_id: Option<String>,
) -> Result<i64> {
    let session = turn.session;
    let chat_id = session.root_id.clone();
    let thread_id = session.thread_id.clone();
    // Private-chat admission verified the actual sender equals this root.
    let source_user_id = Some(session.root_id.clone());
    scheduler.enqueue(crate::scheduler::TaskRequest {
        session,
        source: TaskSource::Telegram,
        delegated: false,
        input: turn.input,
        update_id: turn.update_id,
        media: turn.media.map(crate::scheduler::TaskMedia::Telegram),
        quoted: turn.quoted,
        delivery: DeliveryTarget::Telegram { chat_id, thread_id },
        source_user_id,
        progress_message_id,
    })
}

pub(crate) fn maybe_handle_telegram_command(
    cfg: &RuntimeConfig,
    store: &Store,
    scheduler: &SessionScheduler,
    update_id: Option<String>,
    chat_id: &str,
    text: &str,
) -> Result<Option<ProcessOutcome>> {
    let Some((command, arg)) = parse_command_parts(text, cfg.telegram_bot_username.as_deref())
    else {
        return Ok(None);
    };

    let session = SessionKey::telegram(chat_id);
    if let Some(output) =
        crate::telegram_ui::handle(cfg, store, scheduler, &session, update_id.as_deref(), text)?
    {
        return Ok(Some(ProcessOutcome {
            should_ack: true,
            update_id,
            chat_id: Some(chat_id.to_string()),
            output: Some(output),
            cleanup_path: None,
            progress_message_id: None,
        }));
    }
    let reply = match command {
        "/memory" => Some(store.memory_command(
            &session.id(),
            text,
            update_id.as_deref(),
            &crate::iso_now(&cfg.timezone),
        )?),
        "/fresh" => {
            let ts = crate::util::iso_now(&cfg.timezone);
            match store.insert_boundary_turn(&ts, &session.id(), update_id.as_deref(), "telegram") {
                Ok(true) => {
                    tracing::info!("inserted context boundary for session={}", session.id())
                }
                Ok(false) => {}
                Err(err) => return Err(err),
            }
            Some("New conversation started. Earlier messages are no longer included; memory and preferences are kept.".to_string())
        }
        "/cancel" => {
            let reply = if let Some(arg) = arg {
                if let Some(task_id) = parse_task_id_arg(arg) {
                    if scheduler.cancel_task_for_session(task_id, Some(&session.id()))? {
                        format!(
                            "Cancel requested for run #{task_id}, not yet confirmed. Open Tasks to check its status."
                        )
                    } else {
                        format!("Run #{task_id} is not active for this conversation/topic.")
                    }
                } else {
                    "Use /cancel [run ID] with one positive integer ID, or /cancel to cancel the active run.".to_string()
                }
            } else if let Some(task_id) = scheduler.cancel_active_for_session(&session.id())? {
                format!(
                    "Cancel requested for run #{task_id}, not yet confirmed. Open Tasks to check its status."
                )
            } else {
                "No active run for this conversation/topic.".to_string()
            };
            Some(reply)
        }
        "/tasks" => Some(if let Some(id) = arg {
            store.task_status_command(&session, id, update_id.as_deref())?
        } else {
            scheduler.render_active_tasks_for_session(&session.id())?
        }),
        "/preferences" => Some(store.preferences_command(
            &session,
            arg.unwrap_or(""),
            update_id.as_deref(),
            &crate::iso_now(&cfg.timezone),
        )?),
        "/notifications" => {
            Some(store.notifications_command(&session, arg.unwrap_or(""), update_id.as_deref())?)
        }
        "/schedules" => Some(store.list_schedules(&session.id())?.to_string()),
        "/reconcile" => Some(store.reconciliation_command(
            &session,
            arg.unwrap_or(""),
            update_id.as_deref(),
            &crate::iso_now(&cfg.timezone),
        )?),
        "/approve" | "/reject" => {
            if let Some(id) = arg.and_then(parse_task_id_arg) {
                Some(
                    scheduler
                        .resolve_approval(
                            id,
                            if command == "/approve" {
                                "approve"
                            } else {
                                "reject"
                            },
                            &session.id(),
                        )?
                        .unwrap_or_else(|| {
                            "Approval unavailable or already resolved in this conversation/topic."
                                .into()
                        }),
                )
            } else {
                Some(format!(
                    "Use {command} <approval id> with one positive integer ID."
                ))
            }
        }
        _ => Some("Command not recognized. Open /start or /help. No task was started.".into()),
    };

    Ok(reply.map(|reply_text| ProcessOutcome {
        should_ack: true,
        update_id,
        chat_id: Some(chat_id.to_string()),
        output: Some(render_command_output(&reply_text)),
        cleanup_path: None,
        progress_message_id: None,
    }))
}

// Telegram update enqueue entry point.
pub(crate) fn enqueue_telegram(
    scheduler: &SessionScheduler,
    turn: WebhookTurn,
    progress_message_id: Option<String>,
) -> Result<i64> {
    enqueue_telegram_turn(scheduler, turn, progress_message_id)
}
