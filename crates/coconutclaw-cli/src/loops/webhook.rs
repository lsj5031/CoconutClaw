use anyhow::{Context, Result};
use coconutclaw_config::RuntimeConfig;
use reqwest::blocking::Client;
use serde_json::Value;
use std::fs;
use std::io::Write;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::Duration;
use tokio::sync::mpsc;

use crate::commands::helpers::{
    dispatch_process_outcome, enqueue_slack, enqueue_telegram, maybe_handle_slack_command,
    maybe_handle_telegram_command, render_command_output, render_schedules,
};
use crate::markers::render_output;
use crate::scheduler::SessionScheduler;
use crate::session::SessionKey;
use crate::slack::{SlackMedia, SlackWebhookTurn, build_slack_client, send_slack_progress_message};
use crate::store::Store;
use crate::telegram::{register_telegram_webhook, send_progress_message};
use crate::types::{
    IncomingMedia, InputType, ProcessOutcome, QuotedMessage, TurnInput, WebhookAction, WebhookTurn,
};
use crate::util::{
    configured_telegram_chat_scope, extract_incoming_media, is_allowed_chat, iso_now,
    shorten_log_text,
};
use crate::webhook::{extract_update_id_from_json, extract_update_id_from_value, value_to_string};

use super::slack_socket::drain_slack_socket_turns;

#[allow(clippy::too_many_arguments)]
pub(crate) fn run_webhook_loop(
    cfg: &RuntimeConfig,
    store: &mut Store,
    scheduler: &SessionScheduler,
    telegram_client: Option<&Client>,
    shutdown: &Arc<AtomicBool>,
    mut webhook_rx: mpsc::UnboundedReceiver<String>,
    webhook_tx: &mpsc::UnboundedSender<String>,
    mut slack_rx: Option<&mut mpsc::UnboundedReceiver<SlackWebhookTurn>>,
) -> Result<()> {
    if let Some(client) = telegram_client {
        register_telegram_webhook(client, cfg)?;
    } else {
        tracing::info!("telegram webhook disabled: TELEGRAM_BOT_TOKEN not configured");
    }

    // A tokio mpsc sender is created before this call and passed to spawn_webhook_http_server.
    // The receiver is passed here so the main loop can drain it.

    while !shutdown.load(Ordering::SeqCst) {
        // Drain Slack socket mode turns first
        if let Some(ref mut rx) = slack_rx {
            drain_slack_socket_turns(cfg, store, scheduler, rx);
        }

        let progressed = drain_webhook_channel(
            cfg,
            store,
            scheduler,
            telegram_client,
            shutdown,
            &mut webhook_rx,
            webhook_tx,
        );

        // Run any due scheduled tasks
        if let Err(err) =
            crate::scheduling::run_due_scheduled_tasks(cfg, store, scheduler, telegram_client)
        {
            tracing::warn!("scheduled task execution failed: {err:#}");
        }

        if !progressed {
            // Webhook mode is channel-driven: keep the idle sleep short so newly
            // accepted HTTP updates are processed promptly instead of waiting for
            // the poll-loop interval used by long polling mode.
            thread::sleep(Duration::from_millis(100));
        }
    }

    tracing::info!("shutdown signal received, stopping webhook loop");
    Ok(())
}

pub(crate) fn restore_inflight_update(
    cfg: &RuntimeConfig,
    store: &mut Store,
    scheduler: &SessionScheduler,
    telegram_client: Option<&Client>,
) -> Result<()> {
    let Some(inflight_json) = store.kv_get("inflight_update_json")? else {
        return Ok(());
    };

    // Check if this is a Slack inflight record (JSON with "channel": "slack")
    if let Ok(v) = serde_json::from_str::<Value>(&inflight_json)
        && v.get("channel").and_then(|c| c.as_str()) == Some("slack")
    {
        restore_inflight_slack(cfg, store, scheduler, &v)?;
        return Ok(());
    }

    let inflight_update_id = store.kv_get("inflight_update_id")?;
    let resolved_id = if let Some(ref id) = inflight_update_id
        && !id.trim().is_empty()
    {
        Some(id.clone())
    } else {
        match extract_update_id_from_json(&inflight_json) {
            Ok(id) => id,
            Err(err) => {
                tracing::warn!("inflight JSON is malformed, clearing inflight record: {err:#}");
                let _ = store.clear_inflight();
                return Ok(());
            }
        }
    };

    if let Some(update_id) = resolved_id.as_deref()
        && store.turn_exists_for_update_id(update_id)?
    {
        store.clear_inflight()?;
        tracing::info!("restored inflight update_id={update_id} (dedup)");
        return Ok(());
    }

    let outcome = match process_webhook_line(cfg, store, scheduler, &inflight_json) {
        Ok(outcome) => outcome,
        Err(err) => {
            tracing::warn!("failed to process inflight update, clearing: {err:#}");
            let _ = store.clear_inflight();
            return Ok(());
        }
    };

    store.clear_inflight()?;
    if let Some(output) = outcome.output.as_deref() {
        if let Err(err) = dispatch_process_outcome(cfg, telegram_client, &outcome, output) {
            tracing::warn!("failed to dispatch restored inflight output: {err:#}");
        } else {
            println!("{output}");
            std::io::stdout().flush().ok();
        }
    }
    if let Some(path) = outcome.cleanup_path.as_deref() {
        let _ = fs::remove_file(path);
    }

    Ok(())
}

/// Restore an inflight Slack Socket Mode turn from a previous crash.
fn restore_inflight_slack(
    cfg: &RuntimeConfig,
    store: &mut Store,
    scheduler: &SessionScheduler,
    payload: &Value,
) -> Result<()> {
    let event_id = payload
        .get("event_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let channel_id = payload
        .get("channel_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let thread_ts = payload.get("thread_ts").and_then(|v| v.as_str());
    let user_text = payload
        .get("user_text")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    if !event_id.is_empty() && store.turn_exists_for_update_id(event_id)? {
        tracing::info!("restored slack inflight event_id={event_id} (dedup, already processed)");
        store.clear_inflight()?;
        return Ok(());
    }

    tracing::info!("restoring inflight slack turn: event_id={event_id} channel={channel_id}");

    let turn = SlackWebhookTurn {
        event_id: if event_id.is_empty() {
            None
        } else {
            Some(event_id.to_string())
        },
        channel_id: channel_id.to_string(),
        thread_ts: thread_ts.map(|s| s.to_string()),
        source_user_id: payload
            .get("source_user_id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        input: TurnInput {
            input_type: InputType::Text,
            user_text: if user_text.is_empty() {
                "(empty message)".to_string()
            } else {
                user_text.to_string()
            },
            asr_text: String::new(),
            attachment_type: None,
            attachment_path: None,
            attachment_owned: false,
            supplemental_context: None,
            channel: "slack".to_string(),
        },
        media: None,
    };

    if let Err(err) =
        crate::commands::helpers::process_slack_socket_turn(cfg, store, scheduler, turn)
    {
        tracing::warn!("failed to restore inflight slack turn, clearing: {err:#}");
        let _ = store.clear_inflight();
    }

    Ok(())
}

/// Drain webhook updates from the in-process mpsc channel (replaces the file-based JSONL queue).
pub(crate) fn drain_webhook_channel(
    cfg: &RuntimeConfig,
    store: &mut Store,
    scheduler: &SessionScheduler,
    telegram_client: Option<&Client>,
    shutdown: &Arc<AtomicBool>,
    rx: &mut mpsc::UnboundedReceiver<String>,
    tx: &mpsc::UnboundedSender<String>,
) -> bool {
    let mut progressed = false;

    loop {
        if shutdown.load(Ordering::SeqCst) {
            break;
        }

        // Non-blocking: try_recv drains all available updates, then returns Err when empty.
        let line = match rx.try_recv() {
            Ok(line) => line,
            Err(mpsc::error::TryRecvError::Empty) => break,
            Err(mpsc::error::TryRecvError::Disconnected) => {
                tracing::warn!("webhook channel disconnected");
                break;
            }
        };

        if let Err(err) = extract_update_id_from_json(&line) {
            tracing::warn!("dropping malformed webhook channel entry: {err:#}");
            progressed = true;
            continue;
        }

        let outcome = match process_webhook_line(cfg, store, scheduler, &line) {
            Ok(outcome) => outcome,
            Err(err) => {
                // Transient processing error — re-send to channel for retry.
                tracing::warn!("webhook processing failed, re-queuing: {err:#}");
                if let Err(send_err) = tx.send(line) {
                    tracing::warn!("failed to re-queue webhook message: {send_err:#}");
                }
                return false;
            }
        };

        if let Err(err) = store.clear_inflight() {
            tracing::warn!("failed to clear inflight after webhook processing: {err:#}");
        }
        progressed = true;

        if let Some(output) = outcome.output.as_deref() {
            if let Err(err) = dispatch_process_outcome(cfg, telegram_client, &outcome, output) {
                tracing::warn!("failed to dispatch webhook output: {err:#}");
            } else {
                println!("{output}");
                std::io::stdout().flush().ok();
            }
        }
        if let Some(path) = outcome.cleanup_path.as_deref() {
            let _ = fs::remove_file(path);
        }
    }

    progressed
}

fn is_slack_interactive_payload(value: &Value) -> bool {
    value
        .get("type")
        .and_then(Value::as_str)
        .map(|t| t == "block_actions" || t == "interactive_message" || t == "slash_commands")
        .unwrap_or(false)
}

fn is_slack_event(payload: &str) -> bool {
    serde_json::from_str::<Value>(payload)
        .ok()
        .and_then(|v| {
            v.get("type")
                .and_then(|t| t.as_str())
                .map(|s| s == "event_callback")
        })
        .unwrap_or(false)
}

fn parse_slack_webhook_event(_cfg: &RuntimeConfig, payload: &str) -> Option<WebhookAction> {
    let value: Value = serde_json::from_str(payload).ok()?;

    if value.get("type").and_then(|v| v.as_str()) != Some("event_callback") {
        return None;
    }

    let event = value.get("event")?;
    let event_type = event.get("type")?.as_str()?;

    if event_type != "message" {
        return None;
    }

    // Skip bot messages (avoid echo loops)
    if event.get("bot_id").is_some()
        || event.get("subtype").and_then(|v| v.as_str()) == Some("bot_message")
    {
        return None;
    }

    let event_id = value
        .get("event_id")
        .and_then(|v| v.as_str())
        .map(String::from);
    let channel_id = event.get("channel")?.as_str()?.to_string();
    let text = event
        .get("text")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let thread_ts = event
        .get("thread_ts")
        .and_then(|v| v.as_str())
        .map(String::from);

    // Check for files
    let media = event.get("files").and_then(|files| {
        files.as_array()?.first().and_then(|file| {
            Some(SlackMedia::File {
                url_private: file.get("url_private")?.as_str()?.to_string(),
                filetype: file
                    .get("filetype")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                filename: file
                    .get("filename")
                    .and_then(|v| v.as_str())
                    .unwrap_or("file")
                    .to_string(),
                size: file.get("size").and_then(|v| v.as_u64()),
            })
        })
    });

    Some(WebhookAction::SlackTurn(Box::new(SlackWebhookTurn {
        event_id,
        channel_id,
        thread_ts,
        source_user_id: event
            .get("user")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        input: TurnInput {
            input_type: InputType::Text,
            user_text: text,
            asr_text: String::new(),
            attachment_type: None,
            attachment_path: None,
            attachment_owned: false,
            supplemental_context: None,
            channel: "slack".to_string(),
        },
        media,
    })))
}

pub(crate) fn parse_webhook_action(cfg: &RuntimeConfig, line: &str) -> Result<WebhookAction> {
    let value: Value = serde_json::from_str(line).context("invalid webhook JSON payload")?;
    let update_id = extract_update_id_from_value(&value);
    let configured_chat = configured_telegram_chat_scope(cfg);

    if let Some(callback_query) = value.get("callback_query") {
        let data = callback_query
            .get("data")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if data.eq_ignore_ascii_case("cancel") {
            let chat_id = callback_query
                .get("message")
                .and_then(|node| node.get("chat"))
                .and_then(|node| node.get("id"))
                .map(value_to_string);
            if is_allowed_chat(cfg, chat_id.as_deref()) {
                return Ok(WebhookAction::Cancel {
                    update_id,
                    chat_id: chat_id.unwrap_or_default(),
                });
            }
            let actual_chat = chat_id.unwrap_or_else(|| "<missing>".to_string());
            return Ok(WebhookAction::Ignore {
                update_id,
                reason: format!(
                    "callback_cancel_chat_id_mismatch actual={actual_chat} configured={configured_chat}"
                ),
            });
        }
        return Ok(WebhookAction::Ignore {
            update_id,
            reason: format!(
                "callback_query_ignored data={}",
                shorten_log_text(data.trim(), 64)
            ),
        });
    }

    let Some(message) = value.get("message") else {
        return Ok(WebhookAction::Ignore {
            update_id,
            reason: "missing_message_payload".to_string(),
        });
    };

    let chat_id = message
        .get("chat")
        .and_then(|node| node.get("id"))
        .map(value_to_string)
        .unwrap_or_default();
    if chat_id.trim().is_empty() {
        return Ok(WebhookAction::Ignore {
            update_id,
            reason: "missing_chat_id".to_string(),
        });
    }
    if !is_allowed_chat(cfg, Some(&chat_id)) {
        return Ok(WebhookAction::Ignore {
            update_id,
            reason: format!("chat_id_mismatch actual={chat_id} configured={configured_chat}"),
        });
    }

    let message_text = message
        .get("text")
        .and_then(Value::as_str)
        .or_else(|| message.get("caption").and_then(Value::as_str))
        .unwrap_or_default();
    let media = extract_incoming_media(message);

    if message_text.trim().eq_ignore_ascii_case("/fresh") {
        return Ok(WebhookAction::Fresh { update_id, chat_id });
    }
    if message_text.trim().eq_ignore_ascii_case("/cancel") {
        return Ok(WebhookAction::Cancel { update_id, chat_id });
    }
    if message_text.trim().eq_ignore_ascii_case("/schedules") {
        return Ok(WebhookAction::Schedules { update_id, chat_id });
    }

    let reply_from = message
        .get("reply_to_message")
        .and_then(|node| node.get("from"))
        .and_then(|node| node.get("first_name"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let reply_text = message
        .get("reply_to_message")
        .and_then(|node| node.get("text"))
        .and_then(Value::as_str)
        .or_else(|| {
            message
                .get("reply_to_message")
                .and_then(|node| node.get("caption"))
                .and_then(Value::as_str)
        })
        .map(ToOwned::to_owned);
    let reply_ts = message
        .get("reply_to_message")
        .and_then(|node| node.get("date"))
        .and_then(Value::as_i64);

    let (input_type, attachment_type) = match media.as_ref() {
        Some(IncomingMedia::Voice { .. }) => (InputType::Voice, None),
        Some(IncomingMedia::Photo { .. }) => (InputType::Photo, Some("photo".to_string())),
        Some(IncomingMedia::Document { .. }) => (InputType::Document, Some("document".to_string())),
        Some(IncomingMedia::Video { .. }) => (InputType::Video, Some("video".to_string())),
        Some(IncomingMedia::VideoNote { .. }) => {
            (InputType::VideoNote, Some("video_note".to_string()))
        }
        None => (InputType::Text, None),
    };

    let input = TurnInput {
        input_type,
        user_text: {
            let trimmed = message_text.trim();
            if trimmed.is_empty() {
                "(empty message)".to_string()
            } else {
                trimmed.to_string()
            }
        },
        asr_text: String::new(),
        attachment_type,
        attachment_path: None,
        attachment_owned: false,
        supplemental_context: None,
        channel: "telegram".to_string(),
    };

    Ok(WebhookAction::Turn(Box::new(WebhookTurn {
        update_id,
        chat_id,
        input,
        media,
        quoted: QuotedMessage {
            reply_from,
            reply_text,
            reply_ts,
        },
    })))
}

pub(crate) fn process_webhook_line(
    cfg: &RuntimeConfig,
    store: &mut Store,
    scheduler: &SessionScheduler,
    line: &str,
) -> Result<ProcessOutcome> {
    // Resolve the webhook action: try Slack event/payload detection first, then Telegram
    let action = if is_slack_event(line) {
        parse_slack_webhook_event(cfg, line).unwrap_or_else(|| WebhookAction::Ignore {
            update_id: None,
            reason: "unhandled_slack_event".to_string(),
        })
    } else if let Ok(value) = serde_json::from_str::<Value>(line) {
        if is_slack_interactive_payload(&value) {
            let payload_type = value.get("type").and_then(Value::as_str).unwrap_or("");
            let channel_id = value
                .get("channel")
                .and_then(|node| node.get("id"))
                .and_then(Value::as_str)
                .or_else(|| value.get("channel_id").and_then(Value::as_str))
                .unwrap_or_default()
                .to_string();
            let thread_ts = value
                .get("container")
                .and_then(|node| node.get("thread_ts"))
                .and_then(Value::as_str)
                .or_else(|| {
                    value
                        .get("message")
                        .and_then(|node| node.get("thread_ts"))
                        .and_then(Value::as_str)
                })
                .or_else(|| {
                    value
                        .get("message")
                        .and_then(|node| node.get("ts"))
                        .and_then(Value::as_str)
                })
                .map(ToOwned::to_owned);
            let actor_user_id = value
                .get("user")
                .and_then(|node| node.get("id"))
                .and_then(Value::as_str)
                .or_else(|| value.get("user_id").and_then(Value::as_str));

            if payload_type == "slash_commands" {
                let command = value.get("command").and_then(Value::as_str).unwrap_or("");
                let extra = value
                    .get("text")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .trim();
                let mut text = command.to_string();
                if !extra.is_empty() {
                    text.push(' ');
                    text.push_str(extra);
                }
                if let Some(outcome) = maybe_handle_slack_command(
                    cfg,
                    store,
                    scheduler,
                    &channel_id,
                    thread_ts.as_deref(),
                    actor_user_id,
                    &text,
                )? {
                    return Ok(outcome);
                }
                WebhookAction::Ignore {
                    update_id: None,
                    reason: "unhandled_slack_interactive_payload".to_string(),
                }
            } else if let Some(actions) = value.get("actions").and_then(Value::as_array) {
                if let Some(action) = actions.first() {
                    let action_id = action
                        .get("action_id")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let action_value = action.get("value").and_then(Value::as_str).unwrap_or("");
                    if let Some((approval_id, approval_action)) = action_value
                        .strip_prefix("approval:")
                        .and_then(|value| value.split_once(':'))
                    {
                        let reply = approval_id
                            .parse::<i64>()
                            .with_context(|| format!("invalid approval id: {approval_id}"))
                            .and_then(|approval_id| {
                                scheduler.resolve_approval(
                                    approval_id,
                                    approval_action,
                                    actor_user_id.unwrap_or_default(),
                                )
                            })?
                            .unwrap_or_else(|| "Approval action processed.".to_string());
                        return Ok(ProcessOutcome {
                            should_ack: true,
                            update_id: None,
                            chat_id: Some(channel_id),
                            output_channel: Some("slack".to_string()),
                            output_thread_ts: thread_ts,
                            output: Some(render_command_output(&reply)),
                            cleanup_path: None,
                            progress_message_id: None,
                        });
                    }
                    if action_id == "cancel" {
                        let session = SessionKey::slack(&channel_id, thread_ts.as_deref());
                        let reply = if let Some(task_id) =
                            scheduler.cancel_active_for_session(&session.id())?
                        {
                            format!("Cancel requested for task #{task_id}.")
                        } else {
                            "No active task for this Slack session.".to_string()
                        };
                        return Ok(ProcessOutcome {
                            should_ack: true,
                            update_id: None,
                            chat_id: Some(channel_id),
                            output_channel: Some("slack".to_string()),
                            output_thread_ts: thread_ts,
                            output: Some(render_command_output(&reply)),
                            cleanup_path: None,
                            progress_message_id: None,
                        });
                    }
                }
                WebhookAction::Ignore {
                    update_id: None,
                    reason: "unhandled_slack_interactive_payload".to_string(),
                }
            } else {
                WebhookAction::Ignore {
                    update_id: None,
                    reason: "unhandled_slack_interactive_payload".to_string(),
                }
            }
        } else {
            parse_webhook_action(cfg, line)?
        }
    } else {
        parse_webhook_action(cfg, line)?
    };

    match action {
        WebhookAction::Ignore { update_id, reason } => {
            let update_id_text = update_id.as_deref().unwrap_or_default().to_string();
            let ignored_at = iso_now(&cfg.timezone);
            let _ = store.kv_set("last_ignored_update_id", &update_id_text);
            let _ = store.kv_set("last_ignored_reason", &reason);
            let _ = store.kv_set("last_ignored_at", &ignored_at);
            tracing::info!(
                "ignored telegram update_id={} reason={reason}",
                if update_id_text.trim().is_empty() {
                    "unknown"
                } else {
                    update_id_text.as_str()
                }
            );
            if let Some(update_id) = update_id.as_ref() {
                let _ = store.kv_set("last_update_id", update_id);
            }
            Ok(ProcessOutcome {
                should_ack: true,
                update_id,
                chat_id: None,
                output_channel: None,
                output_thread_ts: None,
                output: None,
                cleanup_path: None,
                progress_message_id: None,
            })
        }
        WebhookAction::Cancel { update_id, chat_id } => {
            let session_id = SessionKey::telegram(&chat_id).id();
            let reply = if let Some(task_id) = scheduler.cancel_active_for_session(&session_id)? {
                format!("Cancel requested for task #{task_id}.")
            } else {
                "No active task for this chat.".to_string()
            };
            if let Some(update_id) = update_id.as_ref() {
                let _ = store.kv_set("last_update_id", update_id);
            }
            Ok(ProcessOutcome {
                should_ack: true,
                update_id,
                chat_id: Some(chat_id),
                output_channel: Some("telegram".to_string()),
                output_thread_ts: None,
                output: Some(render_command_output(&reply)),
                cleanup_path: None,
                progress_message_id: None,
            })
        }
        WebhookAction::Fresh { update_id, chat_id } => {
            let ts = iso_now(&cfg.timezone);
            let session = SessionKey::telegram(&chat_id);
            match store.insert_boundary_turn(&ts, &session.id(), update_id.as_deref(), "telegram") {
                Ok(true) => {
                    tracing::info!("inserted context boundary for session={}", session.id())
                }
                Ok(false) => {}
                Err(err) => tracing::warn!("failed to insert context boundary: {err:#}"),
            }
            if let Some(update_id) = update_id.as_ref() {
                let _ = store.kv_set("last_update_id", update_id);
            }
            Ok(ProcessOutcome {
                should_ack: true,
                update_id,
                chat_id: Some(chat_id),
                output_channel: Some("telegram".to_string()),
                output_thread_ts: None,
                output: Some(
                    render_output("Context cleared. Fresh start!", "", &[])
                        .trim_end()
                        .to_string(),
                ),
                cleanup_path: None,
                progress_message_id: None,
            })
        }
        WebhookAction::Schedules { update_id, chat_id } => {
            if let Some(update_id) = update_id.as_ref() {
                let _ = store.kv_set("last_update_id", update_id);
            }
            let reply = render_schedules(cfg, store)?;
            Ok(ProcessOutcome {
                should_ack: true,
                update_id,
                chat_id: Some(chat_id),
                output_channel: Some("telegram".to_string()),
                output_thread_ts: None,
                output: Some(render_output(&reply, "", &[]).trim_end().to_string()),
                cleanup_path: None,
                progress_message_id: None,
            })
        }
        WebhookAction::Turn(turn) => {
            let chat_id = turn.chat_id.clone();
            if let Some(outcome) = maybe_handle_telegram_command(
                cfg,
                store,
                scheduler,
                turn.update_id.clone(),
                &chat_id,
                &turn.input.user_text,
            )? {
                if let Some(update_id) = outcome.update_id.as_ref() {
                    let _ = store.kv_set("last_update_id", update_id);
                }
                return Ok(outcome);
            }

            if let Some(update_id) = turn.update_id.as_deref()
                && store.turn_exists_for_update_id(update_id)?
            {
                let _ = store.kv_set("last_update_id", update_id);
                let replay_output = store.rendered_output_for_update_id(update_id)?;
                return Ok(ProcessOutcome {
                    should_ack: true,
                    update_id: Some(update_id.to_string()),
                    chat_id: Some(chat_id),
                    output_channel: Some("telegram".to_string()),
                    output_thread_ts: None,
                    output: replay_output,
                    cleanup_path: None,
                    progress_message_id: None,
                });
            }
            if let Some(update_id) = turn.update_id.as_deref()
                && scheduler.has_active_task_for_update_id(update_id)?
            {
                let _ = store.kv_set("last_update_id", update_id);
                return Ok(ProcessOutcome {
                    should_ack: true,
                    update_id: Some(update_id.to_string()),
                    chat_id: Some(chat_id),
                    output_channel: None,
                    output_thread_ts: None,
                    output: None,
                    cleanup_path: None,
                    progress_message_id: None,
                });
            }

            let progress_message_id = send_progress_message(cfg, &chat_id)
                .map_err(|err| {
                    tracing::warn!("failed to send progress message: {err:#}");
                    err
                })
                .ok()
                .flatten();

            let turn = *turn;
            let update_id = turn.update_id.clone();
            let task_id = enqueue_telegram(scheduler, turn, progress_message_id.clone())?;
            tracing::info!(task_id, session = %SessionKey::telegram(&chat_id).id(), "queued telegram task");

            if let Some(update_id) = update_id.as_ref() {
                let _ = store.kv_set("last_update_id", update_id);
            }

            Ok(ProcessOutcome {
                should_ack: true,
                update_id,
                chat_id: Some(chat_id),
                output_channel: Some("telegram".to_string()),
                output_thread_ts: None,
                output: None,
                cleanup_path: None,
                progress_message_id,
            })
        }
        WebhookAction::SlackTurn(turn) => {
            if let Some(outcome) = maybe_handle_slack_command(
                cfg,
                store,
                scheduler,
                &turn.channel_id,
                turn.thread_ts.as_deref(),
                turn.source_user_id.as_deref(),
                &turn.input.user_text,
            )? {
                return Ok(outcome);
            }

            if let Some(event_id) = turn.event_id.as_deref()
                && store.turn_exists_for_update_id(event_id)?
            {
                let replay_output = store.rendered_output_for_update_id(event_id)?;
                return Ok(ProcessOutcome {
                    should_ack: true,
                    update_id: Some(event_id.to_string()),
                    chat_id: Some(turn.channel_id.clone()),
                    output_channel: Some("slack".to_string()),
                    output_thread_ts: turn.thread_ts.clone(),
                    output: replay_output,
                    cleanup_path: None,
                    progress_message_id: None,
                });
            }
            if let Some(event_id) = turn.event_id.as_deref()
                && scheduler.has_active_task_for_update_id(event_id)?
            {
                return Ok(ProcessOutcome {
                    should_ack: true,
                    update_id: Some(event_id.to_string()),
                    chat_id: Some(turn.channel_id.clone()),
                    output_channel: None,
                    output_thread_ts: turn.thread_ts.clone(),
                    output: None,
                    cleanup_path: None,
                    progress_message_id: None,
                });
            }

            let slack_client = build_slack_client(cfg)?;
            let progress_message_id = send_slack_progress_message(
                &slack_client,
                &turn.channel_id,
                turn.thread_ts.as_deref(),
            )
            .map_err(|err| {
                tracing::warn!("slack progress message failed: {err:#}");
                err
            })
            .ok();

            let update_id = turn.event_id.clone();
            let channel_id = turn.channel_id.clone();
            let thread_ts = turn.thread_ts.clone();
            let task_id = enqueue_slack(cfg, store, scheduler, *turn, progress_message_id.clone())?;
            tracing::info!(task_id, session = %SessionKey::slack(&channel_id, thread_ts.as_deref()).id(), "queued slack task");

            Ok(ProcessOutcome {
                should_ack: true,
                update_id,
                chat_id: Some(channel_id),
                output_channel: Some("slack".to_string()),
                output_thread_ts: thread_ts,
                output: None,
                cleanup_path: None,
                progress_message_id,
            })
        }
    }
}
