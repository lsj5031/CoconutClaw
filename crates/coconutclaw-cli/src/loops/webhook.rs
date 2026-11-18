use anyhow::{Context, Result};
use coconutclaw_config::RuntimeConfig;
use reqwest::blocking::Client;
use rusqlite::{OptionalExtension, Transaction, TransactionBehavior, params};
use serde_json::Value;

use crate::commands::helpers::{
    enqueue_telegram, maybe_handle_telegram_command, render_command_output,
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
};
use crate::util::{extract_update_id_from_value, value_to_string};

pub(crate) fn restore_inflight_update(
    cfg: &RuntimeConfig,
    store: &mut Store,
    scheduler: &SessionScheduler,
    _telegram_client: Option<&Client>,
) -> Result<()> {
    if let Some(raw) = store.kv_get("inflight_update_json")? {
        let value: Value = serde_json::from_str(&raw)?;
        anyhow::ensure!(
            value.get("channel").and_then(Value::as_str) != Some("slack"),
            "legacy non-Telegram checkpoint preserved; manual migration required"
        );
        let (id, payload, session, _) = telegram_identity(cfg, store, &raw)?;
        if let Some(old_id) = store
            .kv_get("inflight_update_id")?
            .filter(|v| !v.is_empty())
        {
            anyhow::ensure!(
                old_id == id,
                "legacy checkpoint identity mismatch; KV preserved"
            );
        }
        let tx = Transaction::new_unchecked(&store.conn, TransactionBehavior::Immediate)?;
        let seq =
            store.admit_telegram(&id, &payload, session.as_deref(), &iso_now(&cfg.timezone))?;
        // The old KV has no effect/ack boundary. Never infer that it is safe to
        // replay a control or resend an old result from its presence alone.
        store.conn.execute("UPDATE telegram_ingress SET state='unknown',ack_phase='unknown' WHERE seq=?1 AND state='pending'",[seq])?;
        store.clear_inflight()?;
        tx.commit()?;
    }
    store.recover_telegram_claims()?;
    drain_telegram_ingress(cfg, store, scheduler)?;
    scheduler.resume_ingress();
    scheduler.wake_delivery();
    Ok(())
}

fn telegram_identity(
    cfg: &RuntimeConfig,
    store: &Store,
    line: &str,
) -> Result<(String, String, Option<String>, WebhookAction)> {
    let value: Value = serde_json::from_str(line).context("invalid Telegram update")?;
    let id = value
        .get("update_id")
        .and_then(Value::as_u64)
        .context("numeric Telegram update_id required")?
        .to_string();
    let mut action = parse_webhook_value(cfg, &value)?;
    // A callback's bot-authored message may omit the topic metadata. The
    // persisted control binds the original topic, but never overrides an
    // explicit topic or the source chat/sender checks performed by the parser.
    if let WebhookAction::Turn(turn) = &mut action
        && let Some(callback) = value.get("callback_query")
        && callback["message"].get("message_thread_id").is_none()
        && let Some(token) = callback["data"]
            .as_str()
            .and_then(|s| s.strip_prefix("ui:"))
    {
        let origin: Option<String> = store
            .conn
            .query_row(
                "SELECT session_id FROM telegram_ui_controls WHERE token=?1 AND actor=?2",
                params![token, turn.session.root_id],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(origin) = origin {
            let bound = SessionKey::from_id(origin);
            if bound.platform == turn.session.platform && bound.root_id == turn.session.root_id {
                turn.session = bound;
                if turn.session.thread_id.is_some() && cfg.telegram_topics_enabled == Some(false) {
                    action = WebhookAction::Ignore {
                        update_id: Some(id.clone()),
                        reason: "topic route received with topic mode OFF; history preserved"
                            .into(),
                    };
                }
            }
        }
    }
    let session = match &action {
        WebhookAction::Fresh { session, .. } | WebhookAction::Cancel { session, .. } => {
            Some(session.id())
        }
        WebhookAction::Turn(turn) => Some(turn.session.id()),
        WebhookAction::Ignore { .. } => None,
    };
    Ok((id, value.to_string(), session, action))
}

pub(crate) fn drain_telegram_ingress(
    cfg: &RuntimeConfig,
    store: &mut Store,
    scheduler: &SessionScheduler,
) -> Result<()> {
    for raw in store.pending_telegram()? {
        if process_webhook_line(cfg, store, scheduler, &raw).is_err() {
            tracing::warn!(
                "Telegram event retained pending or Unknown; effect not blindly repeated"
            );
        }
    }
    scheduler.resume_ingress();
    Ok(())
}

/// Called by the existing bounded dispatcher under its delivery owner lock.
/// Cached ProcessOutcome.output is diagnostic data, not permission to resend.
pub(crate) fn deliver_telegram_acks(cfg: &RuntimeConfig, store: &Store) -> Result<()> {
    // Finish the bounded read before HTTP or writes. A live row iterator keeps
    // a WAL snapshot that cannot be promoted after another task commits.
    let candidates = {
        let mut stmt = store.conn.prepare(
            "SELECT seq,output,route,session_id,delivery_state FROM telegram_ingress
         WHERE state='done' AND output IS NOT NULL AND ack_phase='ready' ORDER BY seq",
        )?;
        let mut candidates = Vec::new();
        for row in stmt.query_map([], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
            ))
        })? {
            let (seq, output, route, origin, state) = row?;
            let session = SessionKey::telegram(&route);
            anyhow::ensure!(
                session.id() == origin,
                "ack route differs from admitted origin"
            );
            if !is_allowed_chat(cfg, Some(&session.root_id))
                || (session.thread_id.is_some() && cfg.telegram_topics_enabled == Some(false))
            {
                continue;
            }
            candidates.push((seq, output, session, state));
            if candidates.len() == 32 {
                break;
            }
        }
        candidates
    };
    let mut client = None;
    for (seq, output, session, state) in candidates {
        if client.is_none() {
            client = Some(crate::telegram::build_telegram_client(cfg)?);
        }
        let target = crate::delivery::DeliveryTarget::Telegram {
            chat_id: session.root_id,
            thread_id: session.thread_id,
        };
        let result = crate::delivery::dispatch_task_output(
            store,
            cfg,
            client.as_ref(),
            &target,
            crate::delivery::TaskDispatch {
                task_id: crate::delivery::DeliveryKey::Telegram(seq),
                output: &output,
                progress_message_id: None,
            },
            &state,
        );
        if result.is_ok_and(|done| done) {
            store.conn.execute(
                "UPDATE telegram_ingress SET ack_phase='sent' WHERE seq=?1",
                [seq],
            )?;
        } else {
            // Including a crash after request transmission but before its ack:
            // existing plans suspend the operation; never reconstruct/resend it.
            store.conn.execute(
                "UPDATE telegram_ingress SET ack_phase='unknown' WHERE seq=?1 AND EXISTS(
                  SELECT 1 FROM json_tree(delivery_state) WHERE key='phase' AND value IN ('sending','unknown'))",[seq])?;
        }
    }
    Ok(())
}

/// Parse a Telegram update received through polling.
#[cfg(test)]
pub(crate) fn parse_webhook_action(cfg: &RuntimeConfig, line: &str) -> Result<WebhookAction> {
    let value: Value = serde_json::from_str(line).context("invalid webhook JSON payload")?;
    parse_webhook_value(cfg, &value)
}

fn parse_webhook_value(cfg: &RuntimeConfig, value: &Value) -> Result<WebhookAction> {
    let update_id = extract_update_id_from_value(value);
    let configured_chat = configured_telegram_chat_scope(cfg);

    if let Some(callback_query) = value.get("callback_query") {
        let data = callback_query
            .get("data")
            .and_then(Value::as_str)
            .unwrap_or_default();
        {
            let chat_id = callback_query
                .get("message")
                .and_then(|node| node.get("chat"))
                .and_then(|node| node.get("id"))
                .map(value_to_string);
            if is_allowed_chat(cfg, chat_id.as_deref()) {
                if callback_query
                    .get("from")
                    .and_then(|v| v.get("id"))
                    .map(value_to_string)
                    .is_none_or(|actor| Some(actor.as_str()) != chat_id.as_deref())
                {
                    return Ok(WebhookAction::Ignore {
                        update_id,
                        reason: "callback_sender_mismatch".into(),
                    });
                }
                let session = match SessionKey::from_telegram_message(&callback_query["message"]) {
                    Ok(session) => session,
                    Err(error) => {
                        return Ok(WebhookAction::Ignore {
                            update_id,
                            reason: error.to_string(),
                        });
                    }
                };
                if session.thread_id.is_some() && cfg.telegram_topics_enabled == Some(false) {
                    return Ok(WebhookAction::Ignore { update_id, reason: "topic route received with topic mode OFF; enable in BotFather, history preserved".into() });
                }
                if data.eq_ignore_ascii_case("cancel") {
                    return Ok(WebhookAction::Cancel { update_id, session });
                }
                let input = TurnInput {
                    input_type: InputType::Text,
                    user_text: format!("/button {}", data.strip_prefix("ui:").unwrap_or("invalid")),
                    attachment_type: None,
                    attachment_path: None,
                    attachment_owned: false,
                    supplemental_context: None,
                    channel: "telegram".into(),
                };
                return Ok(WebhookAction::Turn(Box::new(WebhookTurn {
                    update_id,
                    session,
                    input,
                    media: None,
                    quoted: QuotedMessage {
                        reply_from: None,
                        reply_text: None,
                        reply_ts: None,
                    },
                })));
            }
            let actual_chat = chat_id.unwrap_or_else(|| "<missing>".to_string());
            return Ok(WebhookAction::Ignore {
                update_id,
                reason: format!(
                    "callback_cancel_chat_id_mismatch actual={actual_chat} configured={configured_chat}"
                ),
            });
        }
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
    if message
        .get("from")
        .and_then(|v| v.get("id"))
        .map(value_to_string)
        .is_none_or(|actor| actor != chat_id)
    {
        return Ok(WebhookAction::Ignore {
            update_id,
            reason: "message_sender_mismatch".into(),
        });
    }
    let session = match SessionKey::from_telegram_message(message) {
        Ok(session) => session,
        Err(error) => {
            return Ok(WebhookAction::Ignore {
                update_id,
                reason: error.to_string(),
            });
        }
    };
    if session.thread_id.is_some() && cfg.telegram_topics_enabled == Some(false) {
        return Ok(WebhookAction::Ignore {
            update_id,
            reason:
                "topic route received with topic mode OFF; enable in BotFather, history preserved"
                    .into(),
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
    if message_text.trim().is_empty() && media.is_none() {
        return Ok(WebhookAction::Ignore {
            update_id,
            reason: "message_has_no_supported_task_content".into(),
        });
    }

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
        return Ok(WebhookAction::Fresh { update_id, session });
    }
    if message_text.trim().eq_ignore_ascii_case("/cancel") {
        return Ok(WebhookAction::Cancel { update_id, session });
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
        session,
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
    let (id, payload, session, action) = telegram_identity(cfg, store, line)?;
    let reconciliation = matches!(&action, WebhookAction::Turn(turn)
        if turn.input.user_text.split_whitespace().next()
            .and_then(|t| crate::telegram::command_token(t, cfg.telegram_bot_username.as_deref()))
            == Some("/reconcile"));
    let custody = Transaction::new_unchecked(&store.conn, TransactionBehavior::Immediate)?;
    let observation = if let WebhookAction::Turn(turn) = &action {
        let mut parts = turn.input.user_text.splitn(2, char::is_whitespace);
        let command = parts
            .next()
            .and_then(|t| crate::telegram::command_token(t, cfg.telegram_bot_username.as_deref()))
            .unwrap_or("");
        crate::telegram_ui::observation_control(
            store,
            &turn.session,
            command,
            parts.next().unwrap_or("").trim(),
        )?
    } else {
        false
    };
    let seq = store.admit_telegram(&id, &payload, session.as_deref(), &iso_now(&cfg.timezone))?;
    if reconciliation {
        store.conn.execute(
            "INSERT OR IGNORE INTO telegram_reconciliation_controls VALUES(?1)",
            [seq],
        )?;
    }
    if observation {
        store.conn.execute(
            "INSERT OR IGNORE INTO telegram_ui_observations VALUES(?1)",
            [seq],
        )?;
    }
    custody.commit()?;
    let sql_only = match &action {
        WebhookAction::Ignore { .. } | WebhookAction::Fresh { .. } => true,
        WebhookAction::Turn(turn) => {
            turn.input
                .user_text
                .split_whitespace()
                .next()
                .and_then(|t| {
                    crate::telegram::command_token(t, cfg.telegram_bot_username.as_deref())
                })
                .is_some_and(|c| {
                    c.starts_with('/') && !matches!(c, "/cancel" | "/approve" | "/reject")
                })
                && !crate::telegram_ui::runtime_control(
                    store,
                    &turn.session,
                    &turn.input.user_text,
                )?
        }
        _ => false,
    };
    if sql_only {
        let tx = Transaction::new_unchecked(&store.conn, TransactionBehavior::Immediate)?;
        if store.claim_telegram(seq)? {
            let outcome = process_webhook_action(cfg, store, scheduler, action, None)?;
            store.finish_telegram(seq, &outcome)?;
        }
        tx.commit()?;
    } else if store.claim_telegram(seq)? {
        let rollback_candidate = approval_command_id(cfg, &action);
        let normal_turn = matches!(&action,WebhookAction::Turn(turn)
            if !turn.input.user_text.split_whitespace().next()
                .and_then(|t|crate::telegram::command_token(t,cfg.telegram_bot_username.as_deref()))
                    .is_some_and(|c|c.starts_with('/')));
        let result = (|| {
            let mut progress = None;
            if let WebhookAction::Turn(turn) = &action {
                let command = turn
                    .input
                    .user_text
                    .split_whitespace()
                    .next()
                    .and_then(|t| {
                        crate::telegram::command_token(t, cfg.telegram_bot_username.as_deref())
                    });
                let control = command.is_some_and(|c| c.starts_with('/'));
                if !control
                    && !scheduler.has_task_for_update_id(&id)?
                    && !store.turn_exists_for_update_id(&id)?
                {
                    store.conn.execute(
                        "UPDATE telegram_ingress SET progress_state='sending' WHERE seq=?1",
                        [seq],
                    )?;
                    let mut once = cfg.clone();
                    once.telegram_api_retry_attempts = 0;
                    progress = send_progress_message(&once, &turn.session.telegram_route_id())
                        .ok()
                        .flatten();
                    store.conn.execute("UPDATE telegram_ingress SET progress_state=?2,progress_message_id=?3 WHERE seq=?1",
                        params![seq,if progress.is_some(){"sent"}else{"unknown"},progress])?;
                }
            }
            let legacy =
                store.turn_exists_for_update_id(&id)? && !scheduler.has_task_for_update_id(&id)?;
            let outcome = process_webhook_action(cfg, store, scheduler, action, progress)?;
            let tx = Transaction::new_unchecked(&store.conn, TransactionBehavior::Immediate)?;
            store.finish_telegram(seq, &outcome)?;
            if legacy {
                store.conn.execute(
                    "UPDATE telegram_ingress SET ack_phase='unknown' WHERE seq=?1",
                    [seq],
                )?;
            }
            tx.commit()?;
            Ok::<_, anyhow::Error>(())
        })();
        if let Err(error) = result {
            // Approval's existing transaction queues/cleans up only after commit.
            // Pending + no decision receipt proves rollback, unlike cancellation
            // flags, process effects or a committed approval/outcome gap.
            let rolled_back=rollback_candidate.is_some_and(|approval|store.conn.query_row(
                "SELECT status='pending' AND NOT EXISTS(SELECT 1 FROM runtime_inbox WHERE event_key=?2)
                 FROM approvals WHERE id=?1",
                params![approval,format!("approval:{approval}")],|r|r.get::<_,bool>(0)).unwrap_or(false));
            let admitted = normal_turn && scheduler.has_task_for_update_id(&id).unwrap_or(false);
            store.conn.execute(
                "UPDATE telegram_ingress SET state=?2 WHERE seq=?1 AND state='applying'",
                params![
                    seq,
                    if rolled_back {
                        "pending"
                    } else if admitted {
                        "applying"
                    } else {
                        "unknown"
                    }
                ],
            )?;
            return Err(error);
        }
    }
    scheduler.resume_ingress();
    store.telegram_outcome(seq)
}

fn approval_command_id(cfg: &RuntimeConfig, action: &WebhookAction) -> Option<i64> {
    let WebhookAction::Turn(turn) = action else {
        return None;
    };
    let mut parts = turn.input.user_text.split_whitespace();
    let command =
        crate::telegram::command_token(parts.next()?, cfg.telegram_bot_username.as_deref())?;
    if !matches!(command, "/approve" | "/reject") {
        return None;
    }
    let id = parts.next()?.parse::<i64>().ok().filter(|id| *id > 0)?;
    parts.next().is_none().then_some(id)
}

fn process_webhook_action(
    cfg: &RuntimeConfig,
    store: &Store,
    scheduler: &SessionScheduler,
    action: WebhookAction,
    progress_message_id: Option<String>,
) -> Result<ProcessOutcome> {
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
            Ok(ProcessOutcome {
                should_ack: true,
                update_id,
                chat_id: None,
                output: None,
                cleanup_path: None,
                progress_message_id: None,
            })
        }
        WebhookAction::Cancel { update_id, session } => {
            let chat_id = session.telegram_route_id();
            let session_id = session.id();
            let reply = if let Some(task_id) = scheduler.cancel_active_for_session(&session_id)? {
                format!(
                    "Cancel requested for run #{task_id}, not yet confirmed. Open Tasks to check its status."
                )
            } else {
                "No active run for this conversation/topic.".to_string()
            };
            Ok(ProcessOutcome {
                should_ack: true,
                update_id,
                chat_id: Some(chat_id),
                output: Some(render_command_output(&reply)),
                cleanup_path: None,
                progress_message_id: None,
            })
        }
        WebhookAction::Fresh { update_id, session } => {
            let chat_id = session.telegram_route_id();
            let ts = iso_now(&cfg.timezone);
            match store.insert_boundary_turn(&ts, &session.id(), update_id.as_deref(), "telegram") {
                Ok(true) => {
                    tracing::info!("inserted context boundary for session={}", session.id())
                }
                Ok(false) => {}
                Err(err) => return Err(err),
            }
            Ok(ProcessOutcome {
                should_ack: true,
                update_id,
                chat_id: Some(chat_id),
                output: Some(
                    render_output("New conversation started. Earlier messages are no longer included; memory and preferences are kept.", &[])
                        .trim_end()
                        .to_string(),
                ),
                cleanup_path: None,
                progress_message_id: None,
            })
        }
        WebhookAction::Turn(turn) => {
            let chat_id = turn.session.telegram_route_id();
            if let Some(outcome) = maybe_handle_telegram_command(
                cfg,
                store,
                scheduler,
                turn.update_id.clone(),
                &chat_id,
                &turn.input.user_text,
            )? {
                return Ok(outcome);
            }

            if let Some(update_id) = turn.update_id.as_deref()
                && store.turn_exists_for_update_id(update_id)?
            {
                let managed: bool = store.conn.query_row(
                    "SELECT EXISTS(SELECT 1 FROM task_payloads p JOIN task_runs r ON r.id=p.task_run_id WHERE r.update_id=?1)",
                    [update_id], |r| r.get(0))?;
                let replay_output = if managed {
                    scheduler.wake_delivery();
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
                return Ok(ProcessOutcome {
                    should_ack: true,
                    update_id: Some(update_id.to_string()),
                    chat_id: Some(chat_id),
                    output: None,
                    cleanup_path: None,
                    progress_message_id: None,
                });
            }

            let turn = *turn;
            let update_id = turn.update_id.clone();
            let task_id = enqueue_telegram(scheduler, turn, progress_message_id.clone())?;
            tracing::info!(task_id, session = %SessionKey::telegram(&chat_id).id(), "queued telegram task");

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
