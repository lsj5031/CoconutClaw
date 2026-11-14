use anyhow::{Context, Result};
use coconutclaw_config::RuntimeConfig;
use reqwest::blocking::Client;
use serde_json::Value;
use std::fs;
use std::io::Write;

use crate::commands::helpers::{
    dispatch_process_outcome, enqueue_telegram, maybe_handle_telegram_command,
    render_command_output,
};
use crate::markers::render_output;
use crate::scheduler::SessionScheduler;
use crate::session::SessionKey;
use crate::store::Store;
use crate::telegram::send_progress_message;
use crate::types::{
    IncomingMedia, InputType, ProcessOutcome, QuotedMessage, TurnInput, WebhookAction, WebhookTurn,
};
use crate::util::{
    configured_telegram_chat_scope, extract_incoming_media, is_allowed_chat, iso_now,
    shorten_log_text,
};
use crate::util::{extract_update_id_from_json, extract_update_id_from_value, value_to_string};

pub(crate) fn restore_inflight_update(
    cfg: &RuntimeConfig,
    store: &mut Store,
    scheduler: &SessionScheduler,
    telegram_client: Option<&Client>,
) -> Result<()> {
    let Some(inflight_json) = store.kv_get("inflight_update_json")? else {
        return Ok(());
    };

    if serde_json::from_str::<Value>(&inflight_json)?
        .get("channel")
        .and_then(Value::as_str)
        == Some("slack")
    {
        anyhow::bail!(
            "legacy non-Telegram inflight update preserved; see docs/migration-phase-0.md"
        );
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

/// Parse a Telegram update received through polling.
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
    if message.get("voice").is_some() || message.get("audio").is_some() {
        return Ok(WebhookAction::Ignore {
            update_id,
            reason: "audio unsupported; send text, image or document".to_string(),
        });
    }
    let media = extract_incoming_media(message);

    if let Some(token) = message_text.split_whitespace().next()
        && token.starts_with('/')
        && crate::telegram::command_token(token, cfg.telegram_bot_username.as_deref()).is_none()
    {
        return Ok(WebhookAction::Ignore {
            update_id,
            reason: "command_address_not_this_bot".to_string(),
        });
    }

    if message_text.trim().eq_ignore_ascii_case("/fresh") {
        return Ok(WebhookAction::Fresh { update_id, chat_id });
    }
    if message_text.trim().eq_ignore_ascii_case("/cancel") {
        return Ok(WebhookAction::Cancel { update_id, chat_id });
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
    let action = parse_webhook_action(cfg, line)?;

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
                output: Some(
                    render_output("Context cleared. Fresh start!", &[])
                        .trim_end()
                        .to_string(),
                ),
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
                let managed: bool = store.conn.query_row(
                    "SELECT EXISTS(SELECT 1 FROM task_payloads p JOIN task_runs r ON r.id=p.task_run_id WHERE r.update_id=?1)",
                    [update_id], |r| r.get(0))?;
                let replay_output = if managed {
                    scheduler.retry_deliveries()?;
                    None
                } else {
                    store.rendered_output_for_update_id(update_id)?
                };
                return Ok(ProcessOutcome {
                    should_ack: true,
                    update_id: Some(update_id.to_string()),
                    chat_id: Some(chat_id),
                    output: replay_output,
                    cleanup_path: None,
                    progress_message_id: None,
                });
            }
            if let Some(update_id) = turn.update_id.as_deref()
                && scheduler.has_task_for_update_id(update_id)?
            {
                let _ = store.kv_set("last_update_id", update_id);
                return Ok(ProcessOutcome {
                    should_ack: true,
                    update_id: Some(update_id.to_string()),
                    chat_id: Some(chat_id),
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
                output: None,
                cleanup_path: None,
                progress_message_id,
            })
        }
    }
}
