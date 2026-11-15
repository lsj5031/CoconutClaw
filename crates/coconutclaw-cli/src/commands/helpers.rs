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
    scheduler.enqueue(crate::scheduler::TaskRequest {
        session,
        source: TaskSource::Telegram,
        delegated: false,
        input: turn.input,
        update_id: turn.update_id,
        media: turn.media.map(crate::scheduler::TaskMedia::Telegram),
        quoted: turn.quoted,
        delivery: DeliveryTarget::Telegram { chat_id, thread_id },
        source_user_id: None,
        progress_message_id,
    })
}

pub(crate) fn maybe_handle_telegram_command(
    cfg: &RuntimeConfig,
    store: &mut Store,
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
                Err(err) => tracing::warn!("failed to insert context boundary: {err:#}"),
            }
            Some("Context cleared. Fresh start!".to_string())
        }
        "/cancel" => {
            let reply = if let Some(arg) = arg {
                if let Some(task_id) = parse_task_id_arg(arg) {
                    if scheduler.cancel_task_for_session(task_id, Some(&session.id()))? {
                        format!("Cancel requested for task #{task_id}.")
                    } else {
                        format!("Task #{task_id} is not active for this chat.")
                    }
                } else {
                    "Use /cancel [task id] with one positive integer ID, or /cancel to cancel the active task.".to_string()
                }
            } else if let Some(task_id) = scheduler.cancel_active_for_session(&session.id())? {
                format!("Cancel requested for task #{task_id}.")
            } else {
                "No active task for this chat.".to_string()
            };
            Some(reply)
        }
        "/tasks" => Some(scheduler.render_active_tasks_for_session(&session.id())?),
        "/schedules" => Some(
            "Scheduling was removed; existing schedule data is preserved but inactive.".to_string(),
        ),
        "/approve" | "/reject" => {
            if let Some(id) = arg.and_then(parse_task_id_arg) {
                scheduler.resolve_approval(
                    id,
                    if command == "/approve" {
                        "approve"
                    } else {
                        "reject"
                    },
                    &session.id(),
                )?
            } else {
                Some(format!(
                    "Use {command} <approval id> with one positive integer ID."
                ))
            }
        }
        _ => None,
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

pub(crate) fn dispatch_process_outcome(
    cfg: &RuntimeConfig,
    telegram_client: Option<&reqwest::blocking::Client>,
    outcome: &ProcessOutcome,
    output: &str,
) -> Result<()> {
    let client =
        telegram_client.ok_or_else(|| anyhow::anyhow!("Telegram client not configured"))?;
    crate::telegram::dispatch_telegram_output(
        client,
        cfg,
        outcome.chat_id.as_deref(),
        output,
        outcome.progress_message_id.as_deref(),
    )
}
