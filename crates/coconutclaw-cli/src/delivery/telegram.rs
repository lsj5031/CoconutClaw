use std::path::PathBuf;

use anyhow::Result;
use coconutclaw_config::RuntimeConfig;
use reqwest::blocking::Client;

use crate::delivery::{
    ScheduledDeliveryState, ScheduledTaskDispatch, persist_scheduled_delivery_state,
};
use crate::markers::parse_markers;
use crate::store::Store;
use crate::telegram::{
    dispatch_telegram_output, render_telegram_reply_text, send_markdown_reply_document,
    send_or_edit_text, send_voice_reply, should_send_reply_as_document, telegram_delete_message,
    telegram_remove_keyboard, telegram_send_media_file,
};

pub(crate) fn dispatch_output(
    client: &Client,
    cfg: &RuntimeConfig,
    chat_id: Option<&str>,
    output: &str,
    progress_message_id: Option<&str>,
) -> Result<()> {
    dispatch_telegram_output(client, cfg, chat_id, output, progress_message_id)
}

pub(crate) fn scheduled_delivery_has_expected_ops(
    cfg: &RuntimeConfig,
    output: &str,
    progress_message_id: Option<&str>,
) -> bool {
    let markers = parse_markers(output);
    markers.telegram_reply.is_some()
        || progress_message_id.is_some()
        || (cfg.tts_cmd_template.is_some()
            && markers
                .voice_reply
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .is_some())
        || !markers.send_photo.is_empty()
        || !markers.send_document.is_empty()
        || !markers.send_video.is_empty()
}

/// Walk Telegram-bound markers and dispatch each op idempotently.
/// Returns true if every attempted op succeeded; false if any retryable failure occurred.
pub(crate) fn dispatch_scheduled_output(
    store: &Store,
    cfg: &RuntimeConfig,
    client: &Client,
    request: ScheduledTaskDispatch<'_>,
    state: &mut ScheduledDeliveryState,
    chat_id: Option<&str>,
) -> Result<bool> {
    let chat_id = chat_id.map(str::trim).filter(|value| !value.is_empty());
    let markers = parse_markers(request.output);
    let mut all_ok = true;

    if let Some(reply) = markers.telegram_reply.as_deref() {
        let reply = reply.trim();
        if !reply.is_empty() {
            if !state.has_telegram_op("telegram:text") {
                if let Some(chat_id) = chat_id {
                    let rendered_reply = render_telegram_reply_text(cfg, reply);
                    let send_result = if should_send_reply_as_document(&rendered_reply) {
                        match send_markdown_reply_document(
                            client,
                            cfg,
                            chat_id,
                            reply,
                            request.progress_message_id,
                        ) {
                            Ok(()) => Ok(()),
                            Err(err) => {
                                tracing::warn!(
                                    "failed to send long reply as markdown document, falling back to text: {err:#}"
                                );
                                send_or_edit_text(
                                    client,
                                    cfg,
                                    chat_id,
                                    &rendered_reply,
                                    request.progress_message_id,
                                )
                            }
                        }
                    } else {
                        send_or_edit_text(
                            client,
                            cfg,
                            chat_id,
                            &rendered_reply,
                            request.progress_message_id,
                        )
                    };
                    match send_result {
                        Ok(()) => {
                            state.mark_telegram_op("telegram:text");
                            persist_scheduled_delivery_state(
                                store,
                                request.scheduled_task_id,
                                state,
                            )?;
                        }
                        Err(err) => {
                            tracing::warn!(
                                "scheduled telegram text dispatch failed (will retry): {err:#}"
                            );
                            all_ok = false;
                        }
                    }
                } else {
                    tracing::warn!(
                        "cannot dispatch scheduled telegram text output: no chat_id available"
                    );
                    state.mark_telegram_op("telegram:text");
                    persist_scheduled_delivery_state(store, request.scheduled_task_id, state)?;
                }
            }
        } else if request.progress_message_id.is_some() && !state.has_telegram_op("telegram:text") {
            if let (Some(chat_id), Some(message_id)) = (chat_id, request.progress_message_id) {
                let _ = telegram_delete_message(client, cfg, chat_id, message_id)
                    .or_else(|_| telegram_remove_keyboard(client, cfg, chat_id, message_id));
            }
            state.mark_telegram_op("telegram:text");
            persist_scheduled_delivery_state(store, request.scheduled_task_id, state)?;
        }
    } else if request.progress_message_id.is_some() && !state.has_telegram_op("telegram:text") {
        if let (Some(chat_id), Some(message_id)) = (chat_id, request.progress_message_id) {
            let _ = telegram_delete_message(client, cfg, chat_id, message_id)
                .or_else(|_| telegram_remove_keyboard(client, cfg, chat_id, message_id));
        }
        state.mark_telegram_op("telegram:text");
        persist_scheduled_delivery_state(store, request.scheduled_task_id, state)?;
    }

    if cfg.tts_cmd_template.is_some()
        && let Some(voice_reply) = markers.voice_reply.as_deref()
    {
        let voice_reply = voice_reply.trim();
        if !voice_reply.is_empty() && !state.has_telegram_op("telegram:voice") {
            if let Some(chat_id) = chat_id {
                match send_voice_reply(client, cfg, chat_id, voice_reply) {
                    Ok(()) => {
                        state.mark_telegram_op("telegram:voice");
                        persist_scheduled_delivery_state(store, request.scheduled_task_id, state)?;
                    }
                    Err(err) => {
                        tracing::warn!(
                            "scheduled telegram voice dispatch failed (will retry): {err:#}"
                        );
                        all_ok = false;
                    }
                }
            } else {
                tracing::warn!(
                    "cannot dispatch scheduled telegram voice output: no chat_id available"
                );
                state.mark_telegram_op("telegram:voice");
                persist_scheduled_delivery_state(store, request.scheduled_task_id, state)?;
            }
        }
    }

    let media_groups: [(&[String], &str, &str, &str); 3] = [
        (&markers.send_photo, "telegram:photo", "sendPhoto", "photo"),
        (
            &markers.send_document,
            "telegram:document",
            "sendDocument",
            "document",
        ),
        (&markers.send_video, "telegram:video", "sendVideo", "video"),
    ];

    for (items, op_prefix, method, field) in media_groups {
        for (idx, item) in items.iter().enumerate() {
            let op_id = format!("{op_prefix}:{idx}");
            if state.has_telegram_op(&op_id) {
                continue;
            }
            let path = crate::resolve_instance_path(&cfg.instance_dir, PathBuf::from(item));
            if let Some(chat_id) = chat_id {
                match telegram_send_media_file(client, cfg, chat_id, method, field, &path) {
                    Ok(()) => {
                        state.mark_telegram_op(op_id);
                        persist_scheduled_delivery_state(store, request.scheduled_task_id, state)?;
                    }
                    Err(err) => {
                        tracing::warn!(
                            "scheduled telegram {field} dispatch failed (will retry): {err:#}"
                        );
                        all_ok = false;
                    }
                }
            } else {
                tracing::warn!(
                    "cannot dispatch scheduled {field} {}: no chat_id available",
                    path.display()
                );
                state.mark_telegram_op(op_id);
                persist_scheduled_delivery_state(store, request.scheduled_task_id, state)?;
            }
        }
    }

    Ok(all_ok)
}
