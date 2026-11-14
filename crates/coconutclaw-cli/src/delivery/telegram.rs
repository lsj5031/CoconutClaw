use std::path::PathBuf;

use anyhow::Result;
use coconutclaw_config::{RuntimeConfig, TelegramParseFallback, TelegramParseMode};
use reqwest::blocking::Client;

use crate::delivery::{DeliveryState, TaskDispatch, persist_delivery_state};
use crate::markers::parse_markers;
use crate::store::Store;
use crate::telegram::{
    TELEGRAM_TEXT_CHAR_LIMIT, render_telegram_reply_text, send_markdown_reply_document,
    send_or_edit_text, should_send_reply_as_document, split_text_chunks, telegram_delete_message,
    telegram_remove_keyboard, telegram_send_media_file,
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
    if state.has_telegram_op("telegram:text") {
        return Ok(true);
    }
    if state.text_plan.is_none() {
        state.text_plan = Some(
            if should_send_reply_as_document(&render_telegram_reply_text(cfg, reply)) {
                serde_json::json!({"kind":"document","markdown":reply})
            } else {
                chunk_plan(cfg, reply)
            },
        );
        persist_delivery_state(store, request.task_id, state)?;
    }
    let plan = state.text_plan.as_ref().expect("persisted text plan");
    if plan["kind"] == "document" {
        let markdown = plan["markdown"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("invalid saved document plan"))?;
        if send_markdown_reply_document(client, cfg, chat_id, markdown, request.progress_message_id)
            .is_ok()
        {
            state.mark_telegram_op("telegram:text");
            persist_delivery_state(store, request.task_id, state)?;
            return Ok(true);
        }
        // This choice is durable before the first fallback chunk is sent.
        state.text_plan = Some(chunk_plan(cfg, markdown));
        persist_delivery_state(store, request.task_id, state)?;
    }
    let plan = state.text_plan.as_ref().expect("saved text plan");
    let chunks = plan["chunks"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("invalid saved chunk plan"))?
        .clone();
    let mut send_cfg = cfg.clone();
    send_cfg.telegram_parse_mode = match plan["parse_mode"].as_str() {
        Some("off") => TelegramParseMode::Off,
        Some("HTML") => TelegramParseMode::Html,
        Some("MarkdownV2") => TelegramParseMode::MarkdownV2,
        _ => anyhow::bail!("invalid saved parse-mode plan"),
    };
    send_cfg.telegram_parse_fallback = if plan["fallback_plain"] == true {
        TelegramParseFallback::Plain
    } else {
        TelegramParseFallback::None
    };
    for (index, chunk) in chunks.iter().enumerate() {
        let op = format!("telegram:text:chunk:{index}");
        if state.has_telegram_op(&op) {
            continue;
        }
        let chunk = chunk
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("invalid saved text chunk"))?;
        if let Err(error) = send_or_edit_text(
            client,
            &send_cfg,
            chat_id,
            chunk,
            if index == 0 {
                request.progress_message_id
            } else {
                None
            },
        ) {
            tracing::warn!("task text chunk pending: {error:#}");
            return Ok(false);
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
pub(crate) fn dispatch_task_output(
    store: &Store,
    cfg: &RuntimeConfig,
    client: &Client,
    request: TaskDispatch<'_>,
    state: &mut DeliveryState,
    chat_id: Option<&str>,
) -> Result<bool> {
    let chat_id = chat_id.map(str::trim).filter(|value| !value.is_empty());
    let effects = parse_markers(request.output).to_effects();
    let mut all_ok = true;

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
                                Ok(false) => all_ok = false,
                                Err(err) => {
                                    tracing::warn!(
                                        "task telegram text dispatch failed (will retry): {err:#}"
                                    );
                                    all_ok = false;
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
                        let _ = telegram_delete_message(client, cfg, chat_id, message_id).or_else(
                            |_| telegram_remove_keyboard(client, cfg, chat_id, message_id),
                        );
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
                    match telegram_send_media_file(
                        client,
                        cfg,
                        chat_id,
                        "sendPhoto",
                        "photo",
                        &path,
                    ) {
                        Ok(()) => {
                            state.mark_telegram_op(op_id);
                            persist_delivery_state(store, request.task_id, state)?;
                        }
                        Err(err) => {
                            tracing::warn!(
                                "task telegram photo dispatch failed (will retry): {err:#}"
                            );
                            all_ok = false;
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
                    match telegram_send_media_file(
                        client,
                        cfg,
                        chat_id,
                        "sendDocument",
                        "document",
                        &path,
                    ) {
                        Ok(()) => {
                            state.mark_telegram_op(op_id);
                            persist_delivery_state(store, request.task_id, state)?;
                        }
                        Err(err) => {
                            tracing::warn!(
                                "task telegram document dispatch failed (will retry): {err:#}"
                            );
                            all_ok = false;
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
                    match telegram_send_media_file(
                        client,
                        cfg,
                        chat_id,
                        "sendVideo",
                        "video",
                        &path,
                    ) {
                        Ok(()) => {
                            state.mark_telegram_op(op_id);
                            persist_delivery_state(store, request.task_id, state)?;
                        }
                        Err(err) => {
                            tracing::warn!(
                                "task telegram video dispatch failed (will retry): {err:#}"
                            );
                            all_ok = false;
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
            let _ = telegram_delete_message(client, cfg, chat_id, message_id)
                .or_else(|_| telegram_remove_keyboard(client, cfg, chat_id, message_id));
        }
        state.mark_telegram_op("telegram:text");
        persist_delivery_state(store, request.task_id, state)?;
    }

    Ok(all_ok)
}
