use std::path::PathBuf;

use anyhow::Result;
use coconutclaw_config::{RuntimeConfig, TelegramParseFallback, TelegramParseMode};
use reqwest::blocking::Client;

use crate::delivery::{
    DeliveryKey, DeliveryState, TaskDispatch, persist_delivery_state, reserve_telegram_delivery,
};
use crate::markers::CompiledOutput;
use crate::store::Store;
use crate::telegram::{
    TELEGRAM_TEXT_CHAR_LIMIT, render_telegram_reply_text, send_markdown_reply_document,
    send_or_edit_text, split_text_chunks, telegram_delete_message, telegram_remove_keyboard,
    telegram_send_media_file,
};

fn chunk_plan(cfg: &RuntimeConfig, reply: &str) -> serde_json::Value {
    let rendered = render_telegram_reply_text(cfg, reply);
    serde_json::json!({
        "kind":"chunks",
        "chunks":split_text_chunks(&rendered, TELEGRAM_TEXT_CHAR_LIMIT, cfg.telegram_parse_mode),
        "parse_mode":cfg.telegram_parse_mode.as_api_value().unwrap_or("off"),
        "fallback_plain":cfg.telegram_parse_fallback == TelegramParseFallback::Plain,
    })
}

/// Text-rich subset only. Never pass model-supplied HTML actions or embedded
/// media to Telegram. Existing explicit media markers retain their own path.
/// Details/summary are the only raw HTML accepted in Rich Markdown.
fn rich_text_safe(reply: &str) -> bool {
    if reply.contains("![") || reply.contains('&') {
        return false;
    }
    let mut rest = reply.to_string();
    for tag in [
        "<details>",
        "<details open>",
        "</details>",
        "<summary>",
        "</summary>",
    ] {
        rest = rest.replace(tag, "");
    }
    !rest.contains('<')
}

fn now_seconds() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn retry_deadline_seconds(delay: u64) -> u64 {
    // Keep existing epoch-second receipts readable, but round response time
    // upward: truncation would permit a retry up to one second too early.
    now_seconds().saturating_add(1).saturating_add(delay)
}

/// Persist the operation before I/O and its acknowledgment before proceeding.
/// A crash in the send/ack window is deliberately held for operator inspection:
/// Telegram has no caller-supplied idempotency key.
fn durable_op(
    store: &Store,
    task_id: crate::delivery::DeliveryKey,
    state: &mut DeliveryState,
    op: &str,
    send: impl FnOnce() -> Result<()>,
) -> Result<bool> {
    if state.has_telegram_op(op) {
        return Ok(true);
    }
    let saved = state
        .operations
        .entry(op.to_string())
        .or_insert_with(|| serde_json::json!({"phase":"ready","attempts":0}));
    if saved["phase"] != "ready"
        || saved["attempts"].as_u64().unwrap_or(0) >= 3
        || saved["retry_at"].as_u64().unwrap_or(0) > now_seconds()
    {
        return Ok(false);
    }
    saved["attempts"] = (saved["attempts"].as_u64().unwrap_or(0) + 1).into();
    saved["phase"] = "sending".into();
    if !reserve_telegram_delivery(store, task_id, state)? {
        return Ok(false);
    }
    match send() {
        Ok(()) => {
            state.mark_telegram_op(op);
            if matches!(task_id, crate::delivery::DeliveryKey::Telegram(_)) && op == "telegram:text"
            {
                // UI uses one text operation, not a rich/chunk plan. Preserve
                // its positive receipt rather than a misleading sending phase.
                state.operations.get_mut(op).expect("saved operation")["phase"] =
                    "acknowledged".into();
            }
            persist_delivery_state(store, task_id, state)?;
            Ok(true)
        }
        Err(error) => {
            let saved = state.operations.get_mut(op).expect("saved operation");
            if error
                .downcast_ref::<reqwest::Error>()
                .is_some_and(|error| error.is_connect())
            {
                saved["phase"] = "ready".into();
                persist_delivery_state(store, task_id, state)?;
                return Err(error);
            }
            if let Some(api) = error.downcast_ref::<crate::telegram::TelegramApiFailure>()
                && (400..500).contains(&api.code)
            {
                saved["phase"] = "ready".into();
                if api.code == 429 {
                    saved["retry_at"] =
                        retry_deadline_seconds(api.retry_after.unwrap_or(1).max(1)).into();
                }
                persist_delivery_state(store, task_id, state)?;
                return Err(error);
            }
            saved["phase"] = "unknown".into();
            persist_delivery_state(store, task_id, state)?;
            tracing::warn!(?task_id, op, "delivery outcome unknown; resend suspended");
            Ok(false)
        }
    }
}

fn optional_op(
    store: &Store,
    key: DeliveryKey,
    state: &mut DeliveryState,
    op: &str,
    send: impl FnOnce() -> Result<()>,
) -> Result<bool> {
    if state.operations.contains_key(op) || state.has_telegram_op(op) {
        return Ok(true);
    }
    state
        .operations
        .insert(op.into(), serde_json::json!({"phase":"attempted"}));
    if !reserve_telegram_delivery(store, key, state)? {
        state.operations.remove(op);
        return Ok(false);
    }
    Ok(send().is_ok())
}

fn clean_progress(
    store: &Store,
    key: DeliveryKey,
    state: &mut DeliveryState,
    client: &Client,
    cfg: &RuntimeConfig,
    chat_id: &str,
    message_id: &str,
) -> Result<()> {
    if !optional_op(store, key, state, "telegram:progress-delete", || {
        telegram_delete_message(client, cfg, chat_id, message_id)
    })? {
        optional_op(store, key, state, "telegram:progress-keyboard", || {
            telegram_remove_keyboard(client, cfg, chat_id, message_id)
        })?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn deliver_text(
    store: &Store,
    cfg: &RuntimeConfig,
    client: &Client,
    request: TaskDispatch<'_>,
    state: &mut DeliveryState,
    chat_id: &str,
    reply: &str,
) -> Result<bool> {
    if matches!(request.task_id, crate::delivery::DeliveryKey::Telegram(_))
        && let Some(message) = crate::telegram_ui::decode(reply)
    {
        let sent = durable_op(store, request.task_id, state, "telegram:text", || {
            crate::telegram::telegram_send_ui(client, cfg, chat_id, &message)
        })?;
        if sent && let Some(id) = message.callback_id.as_deref() {
            optional_op(
                store,
                request.task_id,
                state,
                "telegram:callback-attempted",
                || crate::telegram::telegram_answer_ui_callback(client, cfg, id),
            )?;
        }
        return Ok(sent);
    }
    if state.has_telegram_op("telegram:text") {
        return Ok(true);
    }
    if state.text_plan.is_none() {
        state.text_plan = Some(if reply.chars().count() > 32768 {
            serde_json::json!({"kind":"document","markdown":reply})
        } else if rich_text_safe(reply) {
            serde_json::json!({"kind":"rich","markdown":reply,"phase":"ready","attempts":0})
        } else {
            let mut plain = cfg.clone();
            plain.telegram_parse_mode = TelegramParseMode::Off;
            chunk_plan(&plain, reply)
        });
        persist_delivery_state(store, request.task_id, state)?;
    }
    if state
        .text_plan
        .as_ref()
        .is_some_and(|plan| plan["kind"] == "rich")
    {
        let plan = state.text_plan.as_mut().expect("saved rich plan");
        if plan["phase"] == "sending"
            || plan["phase"] == "unknown"
            || plan["attempts"].as_u64().unwrap_or(0) >= 3
            || plan["retry_at"].as_u64().unwrap_or(0) > now_seconds()
        {
            return Ok(false);
        }
        let markdown = plan["markdown"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("invalid rich plan"))?
            .to_string();
        let attempts = plan["attempts"].as_u64().unwrap_or(0) + 1;
        plan["phase"] = "sending".into();
        plan["attempts"] = attempts.into();
        if !reserve_telegram_delivery(store, request.task_id, state)? {
            return Ok(false);
        }
        match crate::telegram::telegram_send_rich(client, cfg, chat_id, &markdown)? {
            crate::telegram::RichSendOutcome::Sent => {
                state.mark_telegram_op("telegram:text");
                persist_delivery_state(store, request.task_id, state)?;
                if let Some(message_id) = request.progress_message_id
                    && !store.legacy_admission_frozen()?
                {
                    let _ = telegram_delete_message(client, cfg, chat_id, message_id);
                }
                return Ok(true);
            }
            crate::telegram::RichSendOutcome::Rejected => {
                let mut plain = cfg.clone();
                plain.telegram_parse_mode = TelegramParseMode::Off;
                state.text_plan = Some(chunk_plan(&plain, &markdown));
            }
            crate::telegram::RichSendOutcome::RateLimited(delay) => {
                let plan = state.text_plan.as_mut().expect("saved rich plan");
                plan["phase"] = "ready".into();
                plan["retry_at"] = retry_deadline_seconds(delay).into();
                persist_delivery_state(store, request.task_id, state)?;
                return Ok(false);
            }
            crate::telegram::RichSendOutcome::Unknown => {
                state.text_plan.as_mut().expect("saved rich plan")["phase"] = "unknown".into();
                persist_delivery_state(store, request.task_id, state)?;
                tracing::warn!(
                    task_id = ?request.task_id,
                    "rich delivery outcome unknown; automatic resend/fallback suspended"
                );
                return Ok(false);
            }
        }
        persist_delivery_state(store, request.task_id, state)?;
    }
    let plan = state
        .text_plan
        .as_ref()
        .expect("persisted text plan")
        .clone();
    if plan["kind"] == "document" {
        let markdown = plan["markdown"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("invalid saved document plan"))?;
        let mut send_cfg = cfg.clone();
        send_cfg.telegram_api_retry_attempts = 0;
        match durable_op(
            store,
            request.task_id,
            state,
            "telegram:text:document",
            || send_markdown_reply_document(client, &send_cfg, chat_id, markdown, None),
        ) {
            Ok(true) => {
                state.mark_telegram_op("telegram:text");
                persist_delivery_state(store, request.task_id, state)?;
                if let Some(message_id) = request.progress_message_id
                    && !store.legacy_admission_frozen()?
                {
                    let _ = telegram_delete_message(client, cfg, chat_id, message_id);
                }
                return Ok(true);
            }
            Ok(false) => return Ok(false),
            Err(error)
                if error
                    .downcast_ref::<crate::telegram::TelegramApiFailure>()
                    .is_some_and(|error| error.code == 400) => {}
            Err(error) => return Err(error),
        };
        // This choice is durable before the first fallback chunk is sent.
        let mut plain = cfg.clone();
        plain.telegram_parse_mode = TelegramParseMode::Off;
        state.text_plan = Some(chunk_plan(&plain, markdown));
        persist_delivery_state(store, request.task_id, state)?;
    }
    let plan = state.text_plan.as_ref().expect("saved text plan");
    let chunks = plan["chunks"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("invalid saved chunk plan"))?
        .clone();
    let mut send_cfg = cfg.clone();
    send_cfg.telegram_api_retry_attempts = 0;
    send_cfg.telegram_parse_mode = match plan["parse_mode"].as_str() {
        Some("off") => TelegramParseMode::Off,
        Some("HTML") => TelegramParseMode::Html,
        Some("MarkdownV2") => TelegramParseMode::MarkdownV2,
        _ => anyhow::bail!("invalid saved parse-mode plan"),
    };
    let fallback_plain = plan["fallback_plain"] == true;
    // Durable delivery, unlike ephemeral status helpers, must persist a format
    // change before its next HTTP request.
    send_cfg.telegram_parse_fallback = TelegramParseFallback::None;
    for (index, chunk) in chunks.iter().enumerate() {
        let op = format!("telegram:text:chunk:{index}");
        if state.has_telegram_op(&op) {
            continue;
        }
        let chunk = chunk
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("invalid saved text chunk"))?;
        let mut op_cfg = send_cfg.clone();
        if state
            .operations
            .get(&op)
            .is_some_and(|saved| saved["plain"] == true)
        {
            op_cfg.telegram_parse_mode = TelegramParseMode::Off;
        }
        let sent = durable_op(store, request.task_id, state, &op, || {
            send_or_edit_text(
                client,
                &op_cfg,
                chat_id,
                chunk,
                if index == 0 {
                    request.progress_message_id
                } else {
                    None
                },
            )
        });
        match sent {
            Ok(true) => {}
            Ok(false) => return Ok(false),
            Err(error) => {
                if fallback_plain && crate::telegram::should_fallback_plain_for_error(&error) {
                    state.operations.get_mut(&op).expect("saved operation")["plain"] = true.into();
                    persist_delivery_state(store, request.task_id, state)?;
                }
                tracing::warn!("task text chunk pending: {error:#}");
                return Ok(false);
            }
        }
        state.mark_telegram_op(op);
        persist_delivery_state(store, request.task_id, state)?;
    }
    state.mark_telegram_op("telegram:text");
    persist_delivery_state(store, request.task_id, state)?;
    Ok(true)
}

/// Walk Telegram-bound effects and dispatch each op idempotently.
/// Returns true if every attempted op succeeded; false if any retryable failure occurred.
fn output_effects(
    store: &Store,
    key: crate::delivery::DeliveryKey,
    output: &str,
) -> Result<CompiledOutput> {
    if matches!(key, crate::delivery::DeliveryKey::Telegram(_))
        && crate::telegram_ui::decode(output).is_some()
    {
        return Ok(CompiledOutput::new(output, &[]));
    }
    let literal = match key {
        crate::delivery::DeliveryKey::Task(id) => store.runtime_result_is_literal(id)?,
        crate::delivery::DeliveryKey::Telegram(_)
        | crate::delivery::DeliveryKey::Notification(_) => false,
    };
    Ok(if literal {
        let uncertain = if let crate::delivery::DeliveryKey::Task(id) = key {
            store.get_task_run(id)?.is_some_and(|r| {
                r.error_summary
                    .as_deref()
                    .is_some_and(|s| s.contains("outcome unknown"))
            })
        } else {
            false
        };
        let text = if uncertain {
            format!(
                "Outcome unknown; do not automatically retry. Retained runner evidence:\n{output}"
            )
        } else {
            output.into()
        };
        CompiledOutput::new(&text, &[])
    } else {
        CompiledOutput::from_output(output)
    })
}

/// Existing sender receipts prove this text operation only, not provider
/// success, control effects, progress cleanup or any other external operation.
pub(crate) fn acknowledged_text(
    store: &Store,
    key: crate::delivery::DeliveryKey,
    output: &str,
    state: &mut DeliveryState,
) -> Result<()> {
    anyhow::ensure!(
        state.has_telegram_op("telegram:text"),
        "Unknown retained: no durable text acknowledgment; timeout or operator assertion cannot prove nonoccurrence"
    );
    let effects = output_effects(store, key, output)?.delivery_effects();
    let [crate::markers::Effect::TelegramReply(reply)] = effects.as_slice() else {
        anyhow::bail!(
            "Unknown retained: this finite reconciliation supports one rich text operation only"
        );
    };
    let plan = state
        .text_plan
        .as_mut()
        .ok_or_else(|| anyhow::anyhow!("Unknown retained: missing frozen text plan"))?;
    anyhow::ensure!(
        !reply.trim().is_empty()
            && plan["kind"] == "rich"
            && plan["phase"] == "sending"
            && plan["markdown"].as_str() == Some(reply.trim())
            && plan["attempts"]
                .as_u64()
                .is_some_and(|n| (1..=3).contains(&n))
            && state.completed_ops.len() == 1
            && state.operations.is_empty(),
        "Unknown retained: unsupported or conflicting delivery plan/operation evidence"
    );
    // The sender saves completed_ops after Ok, while its cached rich phase
    // may still say sending. Explicit Unknown is contradictory evidence, not
    // a cached success label that this slice can adjudicate.
    plan["phase"] = "acknowledged".into();
    Ok(())
}

pub(crate) fn dispatch_task_output(
    store: &Store,
    cfg: &RuntimeConfig,
    client: &Client,
    request: TaskDispatch<'_>,
    state: &mut DeliveryState,
    chat_id: Option<&str>,
) -> Result<bool> {
    let mut single_attempt = cfg.clone();
    single_attempt.telegram_api_retry_attempts = 0;
    let cfg = &single_attempt;
    let chat_id = chat_id.map(str::trim).filter(|value| !value.is_empty());
    let policy_key = "telegram:output-policy";
    let output = if let Some(output) = state
        .operations
        .get(policy_key)
        .and_then(|policy| policy["rejected_output"].as_str())
    {
        let mut saved = CompiledOutput::new(
            output
                .strip_prefix("TELEGRAM_REPLY: ")
                .unwrap_or(output)
                .trim_end(),
            &[],
        );
        saved.rejected = true;
        saved
    } else {
        output_effects(store, request.task_id, request.output)?
    };
    // Literal remote evidence is text, never reinterpret it as marker protocol.
    // For ordinary outputs, validate the complete batch before the first send.
    let output = if output
        .effects
        .iter()
        .any(|e| !matches!(e, crate::markers::Effect::TelegramReply(_)))
    {
        super::media::admit_compiled(cfg, output, false)
    } else {
        output
    };
    let rejected = output.rejected;
    let effects = output.delivery_effects();
    if rejected && !state.operations.contains_key(policy_key) {
        let partial = state.text_plan.is_some()
            || !state.completed_ops.is_empty()
            || !state.operations.is_empty();
        state.operations.insert(
            policy_key.into(),
            serde_json::json!({"phase":"rejected","partial":partial,"rejected_output":crate::markers::render_effects(&effects)}),
        );
        persist_delivery_state(store, request.task_id, state)?;
    }
    if rejected
        && state.operations[policy_key]["partial"].is_null()
        && !state.operations.contains_key("telegram:output-rejection")
        && (state.text_plan.is_some()
            || !state.completed_ops.is_empty()
            || state.operations.keys().any(|key| key != policy_key))
    {
        return Ok(false);
    }
    if rejected
        && (state.operations[policy_key]["partial"] == true
            || state.operations.contains_key("telegram:output-rejection"))
    {
        // A file can leave the outbox between an acknowledged text send and
        // a later media retry. Do not silently reuse that text receipt for a
        // rejection, reset receipts, or erase a prior Unknown.
        let uncertain = state.operations.iter().any(|(op, saved)| {
            !state.has_telegram_op(op)
                && matches!(saved["phase"].as_str(), Some("sending" | "unknown"))
        }) || (!state.has_telegram_op("telegram:text")
            && state
                .text_plan
                .as_ref()
                .is_some_and(|plan| matches!(plan["phase"].as_str(), Some("sending" | "unknown"))));
        let Some(chat_id) = chat_id else {
            return Ok(false);
        };
        let notice = "Remaining output rejected by runtime limits or media outbox policy. Previously acknowledged effects are unchanged; unknown effects remain unknown.";
        let op = "telegram:output-rejection";
        state
            .operations
            .entry(op)
            .or_insert_with(|| serde_json::json!({"phase":"ready","attempts":0,"message":notice}));
        let mut plain = cfg.clone();
        plain.telegram_parse_mode = TelegramParseMode::Off;
        plain.telegram_parse_fallback = TelegramParseFallback::None;
        return durable_op(store, request.task_id, state, op, || {
            crate::telegram::telegram_send_message(client, &plain, chat_id, notice).map(|_| ())
        })
        .map(|sent| sent && !uncertain);
    }

    // Index counters for idempotent media tracking
    let mut photo_idx = 0usize;
    let mut doc_idx = 0usize;
    let mut video_idx = 0usize;

    for effect in &effects {
        match effect {
            crate::markers::Effect::TelegramReply(reply) => {
                let reply = reply.trim();
                if !reply.is_empty() {
                    if !state.has_telegram_op("telegram:text") {
                        if let Some(chat_id) = chat_id {
                            let send_result =
                                deliver_text(store, cfg, client, request, state, chat_id, reply);
                            match send_result {
                                Ok(true) => {
                                    state.mark_telegram_op("telegram:text");
                                    persist_delivery_state(store, request.task_id, state)?;
                                }
                                Ok(false) => return Ok(false),
                                Err(err) => {
                                    tracing::warn!(
                                        "task telegram text dispatch failed (will retry): {err:#}"
                                    );
                                    return Ok(false);
                                }
                            }
                        } else {
                            tracing::warn!(
                                "cannot dispatch task telegram text output: no chat_id available"
                            );
                            state.mark_telegram_op("telegram:text");
                            persist_delivery_state(store, request.task_id, state)?;
                        }
                    }
                } else if request.progress_message_id.is_some()
                    && !state.has_telegram_op("telegram:text")
                {
                    if let (Some(chat_id), Some(message_id)) =
                        (chat_id, request.progress_message_id)
                    {
                        clean_progress(
                            store,
                            request.task_id,
                            state,
                            client,
                            cfg,
                            chat_id,
                            message_id,
                        )?;
                    }
                    state.mark_telegram_op("telegram:text");
                    persist_delivery_state(store, request.task_id, state)?;
                }
            }
            crate::markers::Effect::SendPhoto(item) => {
                let op_id = format!("telegram:photo:{photo_idx}");
                photo_idx += 1;
                if state.has_telegram_op(&op_id) {
                    continue;
                }
                let path = crate::resolve_instance_path(&cfg.instance_dir, PathBuf::from(item));
                if let Some(chat_id) = chat_id {
                    match durable_op(store, request.task_id, state, &op_id, || {
                        telegram_send_media_file(client, cfg, chat_id, "sendPhoto", "photo", &path)
                    }) {
                        Ok(true) => {
                            state.mark_telegram_op(op_id);
                            persist_delivery_state(store, request.task_id, state)?;
                        }
                        Ok(false) => return Ok(false),
                        Err(err) => {
                            tracing::warn!(
                                "task telegram photo dispatch failed (will retry): {err:#}"
                            );
                            return Ok(false);
                        }
                    }
                } else {
                    tracing::warn!(
                        "cannot dispatch task photo {}: no chat_id available",
                        path.display()
                    );
                    state.mark_telegram_op(op_id);
                    persist_delivery_state(store, request.task_id, state)?;
                }
            }
            crate::markers::Effect::SendDocument(item) => {
                let op_id = format!("telegram:document:{doc_idx}");
                doc_idx += 1;
                if state.has_telegram_op(&op_id) {
                    continue;
                }
                let path = crate::resolve_instance_path(&cfg.instance_dir, PathBuf::from(item));
                if let Some(chat_id) = chat_id {
                    match durable_op(store, request.task_id, state, &op_id, || {
                        telegram_send_media_file(
                            client,
                            cfg,
                            chat_id,
                            "sendDocument",
                            "document",
                            &path,
                        )
                    }) {
                        Ok(true) => {
                            state.mark_telegram_op(op_id);
                            persist_delivery_state(store, request.task_id, state)?;
                        }
                        Ok(false) => return Ok(false),
                        Err(err) => {
                            tracing::warn!(
                                "task telegram document dispatch failed (will retry): {err:#}"
                            );
                            return Ok(false);
                        }
                    }
                } else {
                    tracing::warn!(
                        "cannot dispatch task document {}: no chat_id available",
                        path.display()
                    );
                    state.mark_telegram_op(op_id);
                    persist_delivery_state(store, request.task_id, state)?;
                }
            }
            crate::markers::Effect::SendVideo(item) => {
                let op_id = format!("telegram:video:{video_idx}");
                video_idx += 1;
                if state.has_telegram_op(&op_id) {
                    continue;
                }
                let path = crate::resolve_instance_path(&cfg.instance_dir, PathBuf::from(item));
                if let Some(chat_id) = chat_id {
                    match durable_op(store, request.task_id, state, &op_id, || {
                        telegram_send_media_file(client, cfg, chat_id, "sendVideo", "video", &path)
                    }) {
                        Ok(true) => {
                            state.mark_telegram_op(op_id);
                            persist_delivery_state(store, request.task_id, state)?;
                        }
                        Ok(false) => return Ok(false),
                        Err(err) => {
                            tracing::warn!(
                                "task telegram video dispatch failed (will retry): {err:#}"
                            );
                            return Ok(false);
                        }
                    }
                } else {
                    tracing::warn!(
                        "cannot dispatch task video {}: no chat_id available",
                        path.display()
                    );
                    state.mark_telegram_op(op_id);
                    persist_delivery_state(store, request.task_id, state)?;
                }
            }
            _ => {}
        }
    }

    // If no text reply at all but we have a progress message, clean it up
    let has_text_reply = effects
        .iter()
        .any(|e| matches!(e, crate::markers::Effect::TelegramReply(_)));
    if !has_text_reply
        && request.progress_message_id.is_some()
        && !state.has_telegram_op("telegram:text")
    {
        if let (Some(chat_id), Some(message_id)) = (chat_id, request.progress_message_id) {
            clean_progress(
                store,
                request.task_id,
                state,
                client,
                cfg,
                chat_id,
                message_id,
            )?;
        }
        state.mark_telegram_op("telegram:text");
        persist_delivery_state(store, request.task_id, state)?;
    }

    Ok(true)
}
