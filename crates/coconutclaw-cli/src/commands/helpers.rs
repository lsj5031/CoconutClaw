use anyhow::{Context, Result};
use coconutclaw_config::RuntimeConfig;

use crate::delivery::{DeliveryTarget, TaskSource};
use crate::markers::render_output;
use crate::scheduler::SessionScheduler;
use crate::session::SessionKey;
use crate::slack::{
    SlackWebhookTurn, build_slack_client, dispatch_slack_output, send_slack_progress_message,
    valid_slack_channel_id, valid_slack_token,
};
use crate::store::Store;
use crate::types::{ProcessOutcome, QuotedMessage, WebhookTurn};
use crate::util::shorten_log_text;
use std::io::Write;

fn parse_command_parts(text: &str) -> Option<(&str, Option<&str>)> {
    let trimmed = text.trim();
    if !trimmed.starts_with('/') {
        return None;
    }

    let mut parts = trimmed.splitn(2, char::is_whitespace);
    let command = parts.next()?;
    let arg = parts
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    Some((command, arg))
}

fn parse_task_id_arg(arg: Option<&str>) -> Result<Option<i64>> {
    let Some(arg) = arg else {
        return Ok(None);
    };
    let task_id = arg
        .split_whitespace()
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing task id"))?
        .parse::<i64>()
        .with_context(|| format!("invalid task id: {arg}"))?;
    Ok(Some(task_id))
}

fn slack_actor_is_admin(cfg: &RuntimeConfig, actor_user_id: Option<&str>) -> bool {
    if cfg.slack_admin_user_ids.is_empty() {
        return true;
    }
    let Some(actor_user_id) = actor_user_id else {
        return false;
    };
    cfg.slack_admin_user_ids
        .iter()
        .any(|candidate| candidate == actor_user_id)
}

pub(crate) fn render_command_output(reply: &str) -> String {
    render_output(reply, "", &[]).trim_end().to_string()
}

fn enqueue_telegram_turn(
    scheduler: &SessionScheduler,
    turn: WebhookTurn,
    progress_message_id: Option<String>,
) -> Result<i64> {
    let session = SessionKey::telegram(&turn.chat_id);
    let chat_id = turn.chat_id.clone();
    scheduler.enqueue(crate::scheduler::TaskRequest {
        session,
        source: TaskSource::Telegram,
        input: turn.input,
        update_id: turn.update_id,
        media: turn.media.map(crate::scheduler::TaskMedia::Telegram),
        quoted: turn.quoted,
        delivery: DeliveryTarget::Telegram { chat_id },
        persisted_delivery_target: None,
        source_user_id: None,
        progress_message_id,
        scheduled_task_id: None,
    })
}

fn enqueue_slack_turn(
    cfg: &RuntimeConfig,
    store: &Store,
    scheduler: &SessionScheduler,
    mut turn: SlackWebhookTurn,
    progress_message_id: Option<String>,
) -> Result<i64> {
    let session = SessionKey::slack(&turn.channel_id, turn.thread_ts.as_deref());
    if turn.media.is_some() {
        turn.input.supplemental_context = crate::delivery::slack::hydrate_thread_context(
            cfg,
            store,
            &turn.channel_id,
            turn.thread_ts.as_deref(),
            &session.id(),
        );
    }
    let channel_id = turn.channel_id.clone();
    let thread_ts = turn.thread_ts.clone();
    scheduler.enqueue(crate::scheduler::TaskRequest {
        session,
        source: TaskSource::Slack {
            channel_id: channel_id.clone(),
            thread_ts: thread_ts.clone(),
        },
        input: turn.input,
        update_id: turn.event_id,
        media: turn.media.map(crate::scheduler::TaskMedia::Slack),
        quoted: QuotedMessage {
            reply_from: None,
            reply_text: None,
            reply_ts: None,
        },
        delivery: DeliveryTarget::Slack {
            channel_id,
            thread_ts,
        },
        persisted_delivery_target: None,
        source_user_id: turn.source_user_id,
        progress_message_id,
        scheduled_task_id: None,
    })
}

fn render_active_schedules_reply(cfg: &RuntimeConfig, store: &Store) -> Result<String> {
    let tasks = store.list_active_scheduled_tasks()?;
    if tasks.is_empty() {
        return Ok(format!(
            "No active scheduled tasks. Timezone: {}.",
            cfg.timezone
        ));
    }

    let mut lines = vec![format!("Active scheduled tasks ({})", cfg.timezone)];
    for (idx, task) in tasks.iter().enumerate() {
        lines.push(format!(
            "{}. {} at {} — {}",
            idx + 1,
            if task.recurring { "Daily" } else { "Once" },
            task.schedule_time,
            shorten_log_text(task.prompt.trim(), 120)
        ));
        if let Some(last_run_ts) = task.last_run_ts.as_deref() {
            lines.push(format!("Last run: {last_run_ts}"));
        }
        if task.pending_output.is_some() {
            lines.push("Pending delivery retry queued.".to_string());
        }
    }

    Ok(lines.join("\n"))
}

pub(crate) fn maybe_handle_telegram_command(
    cfg: &RuntimeConfig,
    store: &mut Store,
    scheduler: &SessionScheduler,
    update_id: Option<String>,
    chat_id: &str,
    text: &str,
) -> Result<Option<ProcessOutcome>> {
    let Some((command, arg)) = parse_command_parts(text) else {
        return Ok(None);
    };

    let session = SessionKey::telegram(chat_id);
    let reply = match command {
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
            let task_id = parse_task_id_arg(arg)?;
            let reply = if let Some(task_id) = task_id {
                if scheduler.cancel_task_for_session(task_id, Some(&session.id()))? {
                    format!("Cancel requested for task #{task_id}.")
                } else {
                    format!("Task #{task_id} is not active for this chat.")
                }
            } else if let Some(task_id) = scheduler.cancel_active_for_session(&session.id())? {
                format!("Cancel requested for task #{task_id}.")
            } else {
                "No active task for this chat.".to_string()
            };
            Some(reply)
        }
        "/tasks" => Some(scheduler.render_active_tasks_for_session(&session.id())?),
        "/schedules" => Some(render_active_schedules_reply(cfg, store)?),
        _ => None,
    };

    Ok(reply.map(|reply_text| ProcessOutcome {
        should_ack: true,
        update_id,
        chat_id: Some(chat_id.to_string()),
        output_channel: Some("telegram".to_string()),
        output_thread_ts: None,
        output: Some(render_command_output(&reply_text)),
        cleanup_path: None,
        progress_message_id: None,
    }))
}

pub(crate) fn maybe_handle_slack_command(
    cfg: &RuntimeConfig,
    store: &mut Store,
    scheduler: &SessionScheduler,
    channel_id: &str,
    thread_ts: Option<&str>,
    source_user_id: Option<&str>,
    text: &str,
) -> Result<Option<ProcessOutcome>> {
    let Some((command, arg)) = parse_command_parts(text) else {
        return Ok(None);
    };

    let session = SessionKey::slack(channel_id, thread_ts);
    let reply = match command {
        "/fresh" => {
            if !slack_actor_is_admin(cfg, source_user_id) {
                Some("Admin role required to clear Slack thread context.".to_string())
            } else {
                let ts = crate::util::iso_now(&cfg.timezone);
                match store.insert_boundary_turn(&ts, &session.id(), None, "slack") {
                    Ok(true) => tracing::info!(
                        "inserted context boundary for slack session={}",
                        session.id()
                    ),
                    Ok(false) => {}
                    Err(err) => tracing::warn!("failed to insert slack context boundary: {err:#}"),
                }
                Some("Context cleared. Fresh start!".to_string())
            }
        }
        "/cancel" => {
            let task_id = parse_task_id_arg(arg)?;
            if let Some(task_id) = task_id {
                if !slack_actor_is_admin(cfg, source_user_id) {
                    Some("Admin role required to cancel a specific task by id.".to_string())
                } else if scheduler.cancel_task_for_session(task_id, Some(&session.id()))? {
                    Some(format!("Cancel requested for task #{task_id}."))
                } else {
                    let store = Store::open(cfg)?;
                    let task = store.get_task_run(task_id)?;
                    match task {
                        Some(t) if t.session_id != session.id() => {
                            Some("Task belongs to a different session.".to_string())
                        }
                        _ => Some(format!("Task #{task_id} is not active.")),
                    }
                }
            } else if let Some(task_id) = scheduler.cancel_active_for_session(&session.id())? {
                Some(format!("Cancel requested for task #{task_id}."))
            } else {
                Some("No active task for this Slack session.".to_string())
            }
        }
        "/tasks" => Some(scheduler.render_active_tasks_for_session(&session.id())?),
        "/schedules" => Some(render_active_schedules_reply(cfg, store)?),
        _ => None,
    };

    Ok(reply.map(|reply_text| ProcessOutcome {
        should_ack: true,
        update_id: None,
        chat_id: Some(channel_id.to_string()),
        output_channel: Some("slack".to_string()),
        output_thread_ts: thread_ts.map(ToOwned::to_owned),
        output: Some(render_command_output(&reply_text)),
        cleanup_path: None,
        progress_message_id: None,
    }))
}

pub(crate) fn dispatch_slack_if_configured(cfg: &RuntimeConfig, output: &str) {
    if valid_slack_token(cfg).is_none() {
        return;
    }
    let Ok(slack_client) = build_slack_client(cfg) else {
        return;
    };
    let Some(ch) = valid_slack_channel_id(cfg) else {
        return;
    };
    if let Err(err) = dispatch_slack_output(&slack_client, cfg, ch, output, None, None) {
        tracing::warn!("slack dispatch failed: {err:#}");
    }
}

// Re-export the private helpers needed by loops/webhook.rs and loops/slack_socket.rs
pub(crate) fn enqueue_telegram(
    scheduler: &SessionScheduler,
    turn: WebhookTurn,
    progress_message_id: Option<String>,
) -> Result<i64> {
    enqueue_telegram_turn(scheduler, turn, progress_message_id)
}

pub(crate) fn enqueue_slack(
    cfg: &RuntimeConfig,
    store: &Store,
    scheduler: &SessionScheduler,
    turn: SlackWebhookTurn,
    progress_message_id: Option<String>,
) -> Result<i64> {
    enqueue_slack_turn(cfg, store, scheduler, turn, progress_message_id)
}

pub(crate) fn render_schedules(cfg: &RuntimeConfig, store: &Store) -> Result<String> {
    render_active_schedules_reply(cfg, store)
}

pub(crate) fn process_slack_socket_turn(
    cfg: &RuntimeConfig,
    store: &mut Store,
    scheduler: &SessionScheduler,
    turn: SlackWebhookTurn,
) -> Result<()> {
    if let Some(outcome) = maybe_handle_slack_command(
        cfg,
        store,
        scheduler,
        &turn.channel_id,
        turn.thread_ts.as_deref(),
        turn.source_user_id.as_deref(),
        &turn.input.user_text,
    )? {
        if let Some(output) = outcome.output.as_deref() {
            let slack_client = build_slack_client(cfg)?;
            dispatch_slack_output(
                &slack_client,
                cfg,
                &turn.channel_id,
                output,
                None,
                turn.thread_ts.as_deref(),
            )?;
            print!("{output}");
            std::io::stdout().flush().ok();
        }
        return Ok(());
    }

    if let Some(event_id) = turn.event_id.as_deref()
        && store.turn_exists_for_update_id(event_id)?
    {
        return Ok(());
    }

    let slack_client = build_slack_client(cfg)?;
    let progress_message_id =
        send_slack_progress_message(&slack_client, &turn.channel_id, turn.thread_ts.as_deref())
            .map_err(|err| {
                tracing::warn!("slack progress message failed: {err:#}");
                err
            })
            .ok();

    let task_id = enqueue_slack_turn(
        cfg,
        store,
        scheduler,
        turn.clone(),
        progress_message_id.clone(),
    )?;
    tracing::info!(task_id, session = %SessionKey::slack(&turn.channel_id, turn.thread_ts.as_deref()).id(), "queued slack socket task");
    Ok(())
}

pub(crate) fn dispatch_process_outcome(
    cfg: &RuntimeConfig,
    telegram_client: Option<&reqwest::blocking::Client>,
    outcome: &ProcessOutcome,
    output: &str,
) -> Result<()> {
    match outcome.output_channel.as_deref() {
        Some("slack") => {
            let slack_client = build_slack_client(cfg)?;
            let channel_id = outcome.chat_id.as_deref().ok_or_else(|| {
                anyhow::anyhow!("missing slack channel id for webhook output dispatch")
            })?;
            dispatch_slack_output(
                &slack_client,
                cfg,
                channel_id,
                output,
                outcome.progress_message_id.as_deref(),
                outcome.output_thread_ts.as_deref(),
            )
        }
        _ => {
            let Some(client) = telegram_client else {
                tracing::warn!("cannot dispatch telegram output: telegram client not configured");
                return Ok(());
            };
            crate::telegram::dispatch_telegram_output(
                client,
                cfg,
                outcome.chat_id.as_deref(),
                output,
                outcome.progress_message_id.as_deref(),
            )
        }
    }
}
