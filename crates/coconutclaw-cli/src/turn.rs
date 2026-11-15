//! Turn processing: running the AI provider and resolving output.
//!
//! Handles the full lifecycle of a single conversational turn:
//! building context, invoking the provider, handling cancellation
//! and progress updates, resolving the output, and persisting the turn.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::time::Instant;

use anyhow::{Result, bail};
use coconutclaw_config::RuntimeConfig;
use coconutclaw_provider::run_provider_session;

use crate::cancel::CancelRouter;
use crate::context::{append_memory_and_tasks, build_context};
use crate::delivery::{DeliveryTarget, TaskSource};
use crate::markers::{
    extract_assistant_text_from_json_stream, extract_error_summary, final_marker_payload,
    parse_markers, recover_unstructured_reply, render_output, should_retry_provider_failure,
};
use crate::store::{Store, TurnRecord};
use crate::telegram::{
    build_telegram_client, spawn_progress_updater, telegram_download_file, valid_telegram_chat_id,
};
use crate::{
    IncomingMedia, InputType, QuotedMessage, TurnInput, TurnResult, TurnStatus,
    clear_cancel_marker, iso_now, resolve_instance_path,
};

pub(crate) struct ProcessedTurn {
    pub(crate) output: String,
    pub(crate) status: TurnStatus,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn process_turn(
    cfg: &RuntimeConfig,
    store: &mut Store,
    input: TurnInput,
    source: &TaskSource,
    context_chat_id: Option<String>,
    update_id: Option<String>,
    progress_message_id: Option<&str>,
    quoted: &QuotedMessage,
    cancel_router: Option<Arc<CancelRouter>>,
) -> Result<String> {
    Ok(process_turn_with_status(
        cfg,
        store,
        input,
        source,
        context_chat_id,
        update_id,
        None,
        progress_message_id,
        None,
        quoted,
        None,
        cancel_router,
        None,
        None,
    )?
    .output)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn process_turn_with_status(
    cfg: &RuntimeConfig,
    store: &mut Store,
    input: TurnInput,
    source: &TaskSource,
    context_chat_id: Option<String>,
    update_id: Option<String>,
    task_run_id: Option<i64>,
    progress_message_id: Option<&str>,
    progress_target: Option<&DeliveryTarget>,
    quoted: &QuotedMessage,
    external_cancel_flag: Option<Arc<AtomicBool>>,
    cancel_router: Option<Arc<CancelRouter>>,
    origin_session: Option<&str>,
    delivery_target_json: Option<&str>,
) -> Result<ProcessedTurn> {
    let turn_start = Instant::now();
    let _span = tracing::info_span!(
        "process_turn",
        input_type = %input.input_type,
        provider = "codex",
    )
    .entered();

    clear_cancel_marker(cfg);
    let ts = iso_now(&cfg.timezone);
    let context_channel = turn_context_channel(source, &input).to_string();
    let chat_id = context_chat_id
        .or_else(|| progress_target.map(|target| target.display_id().to_owned()))
        .or_else(|| match source {
            TaskSource::Telegram => valid_telegram_chat_id(cfg).map(ToOwned::to_owned),
            TaskSource::Local => Some("local".to_string()),
        })
        .unwrap_or_else(|| "local".to_string());
    let (chat_id, _) = crate::session::history_ids(&chat_id, &context_channel);
    let progress_chat_id = progress_target
        .and_then(|target| target.telegram_route())
        .or_else(|| match source {
            TaskSource::Telegram => Some(chat_id.clone()),
            TaskSource::Local => None,
        })
        .unwrap_or_else(|| chat_id.clone());

    // Actual incoming user text only. Delegated prompts must never acquire core
    // memory authority by spelling a command.
    let token = input.user_text.split_whitespace().next().unwrap_or("");
    if task_run_id.is_none()
        && origin_session.is_none()
        && token.split_once('@').map_or(token, |(command, _)| command) == "/memory"
    {
        if crate::telegram::command_token(token, cfg.telegram_bot_username.as_deref()).is_none() {
            return Ok(ProcessedTurn {
                output: render_output(
                    "Memory command address is not verified for this bot. Use plain /memory with local once.",
                    &[],
                ),
                status: TurnStatus::Ok,
            });
        }
        let reply = store.memory_command(&chat_id, &input.user_text, update_id.as_deref(), &ts)?;
        return Ok(ProcessedTurn {
            output: render_output(&reply, &[]),
            status: TurnStatus::Ok,
        });
    }

    // Polling commands can change memory while a lane prepares its prompt.
    // Bind the prompt and native-session generation to one SQLite snapshot,
    // so an older in-flight prompt never populates a newer memory generation.
    let (context, revision) = {
        let snapshot = store.conn.unchecked_transaction()?;
        let revision = store.memory_revision(&chat_id)?;
        let context = build_context(cfg, store, &input, &ts, &chat_id, quoted)?;
        snapshot.commit()?;
        (context, revision)
    };

    let cancel_flag = external_cancel_flag.unwrap_or_else(|| Arc::new(AtomicBool::new(false)));
    if let Some(router) = &cancel_router {
        router.register(Arc::clone(&cancel_flag));
    }
    let progress_updater_stop = Arc::new(AtomicBool::new(false));
    let (presentation_sender, progress_updater) = if let Some(message_id) = progress_message_id {
        let (progress_tx, progress_rx) = mpsc::sync_channel::<String>(32);
        let updater = spawn_progress_updater(
            cfg.clone(),
            telegram_progress_chat_id(&progress_chat_id),
            message_id.to_string(),
            progress_rx,
            Arc::clone(&progress_updater_stop),
        );
        (Some(progress_tx), Some(updater))
    } else {
        (None, None)
    };
    // Durable latest status is independent of best-effort Telegram presentation.
    // Coalesce bursts and bound both the persisted value and the UI queue.
    let (progress_sender, progress_rx) = mpsc::channel::<String>();
    let progress_cfg = cfg.clone();
    let collector_stop = Arc::new(AtomicBool::new(false));
    let collector_stopped = collector_stop.clone();
    let progress_collector = std::thread::spawn(move || {
        let store = Store::open(&progress_cfg).ok();
        loop {
            let first = match progress_rx.recv_timeout(std::time::Duration::from_millis(50)) {
                Ok(first) => first,
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
                Err(mpsc::RecvTimeoutError::Timeout)
                    if collector_stopped.load(Ordering::SeqCst) =>
                {
                    break;
                }
                Err(_) => continue,
            };
            let latest = progress_rx.try_iter().take(128).last().unwrap_or(first);
            let latest: String = latest.chars().take(512).collect();
            if let (Some(store), Some(id)) = (&store, task_run_id)
                && let Err(err) = store.update_task_run_progress(id, &latest)
            {
                tracing::warn!("failed to persist provider progress: {err:#}");
            }
            if let Some(tx) = &presentation_sender {
                let _ = tx.try_send(latest);
            }
            if collector_stopped.load(Ordering::SeqCst) {
                break;
            }
        }
    });

    let boundary_id = store.latest_boundary_id(&chat_id, &context_channel)?;
    let base_session_key = if revision == 0 {
        format!("codex_session:{chat_id}:{boundary_id}")
    } else {
        format!("codex_session:{chat_id}:{boundary_id}:memory:{revision}")
    };
    // Pre-isolation native transcripts may contain the old shared task file.
    // Start an explicitly new context generation; retain every old ID/history
    // row, but never fall back to resuming those unscoped transcripts.
    let session_key = format!("{base_session_key}:context:scoped-tasks-v1");
    let native_session = store.kv_get(&session_key)?;
    let mut provider_result = run_provider_session(
        input.attachment_path.as_ref(),
        cfg,
        &context,
        Some(&cancel_flag),
        Some(&progress_sender),
        None,
        native_session.as_deref(),
    );
    let mut retries = 0u32;
    while retries < cfg.provider_max_retries {
        let should_retry = match &provider_result {
            Ok(result) => {
                !result.success
                    && !cancel_flag.load(Ordering::SeqCst)
                    && result.exit_code != 124
                    && should_retry_provider_failure(&result.raw_output)
            }
            _ => false,
        };
        if !should_retry {
            break;
        }
        retries += 1;
        tracing::warn!(
            attempt = retries + 1,
            "provider failed with retryable error, retrying"
        );
        provider_result = run_provider_session(
            input.attachment_path.as_ref(),
            cfg,
            &context,
            Some(&cancel_flag),
            Some(&progress_sender),
            None,
            native_session.as_deref(),
        );
    }
    drop(progress_sender);
    collector_stop.store(true, Ordering::SeqCst);
    let _ = progress_collector.join();
    progress_updater_stop.store(true, Ordering::SeqCst);
    if let Some(handle) = progress_updater {
        let _ = handle.join();
    }

    if let Ok(result) = &provider_result
        && let Some(session_id) = &result.session_id
    {
        store.kv_set(&session_key, session_id)?;
    }
    let (raw_output, provider_success, exit_code) = match provider_result {
        Ok(result) => (result.raw_output, result.success, result.exit_code),
        Err(err) => (format!("{err:#}"), false, 1),
    };
    let duration_ms = turn_start.elapsed().as_millis() as i64;
    let cancelled = cancel_flag.load(Ordering::SeqCst) || exit_code == 130;
    let turn_result =
        resolve_turn_result(&raw_output, &context_channel, provider_success, cancelled);
    let effects = turn_result.effects;
    let telegram_reply = turn_result.telegram_reply;
    let status = turn_result.status;

    tracing::info!(
        duration_ms,
        status = %status,
        retries,
        "turn completed"
    );

    let inserted_turn_id = store.insert_turn(&TurnRecord {
        ts: ts.clone(),
        chat_id: origin_session.unwrap_or(&chat_id).to_string(),
        input_type: input.input_type.to_string(),
        user_text: input.user_text,
        provider_raw: raw_output,
        telegram_reply: telegram_reply.clone(),
        status: status.to_string(),
        update_id: update_id.clone(),
        duration_ms: Some(duration_ms),
        channel: context_channel.clone(),
        task_run_id,
        side_effects_applied: false,
    })?;

    if let Some(inserted_turn_id) = inserted_turn_id
        && status != TurnStatus::Cancelled
    {
        append_memory_and_tasks(
            cfg,
            store,
            &ts,
            Some(inserted_turn_id),
            &effects,
            origin_session,
            delivery_target_json,
        )?;
        let persist_result =
            store.update_turn_reply_and_side_effects_by_id(inserted_turn_id, &telegram_reply);
        if let Err(err) = persist_result {
            tracing::warn!(
                "failed to update stored turn reply/side-effect state after turn processing: {err:#}"
            );
        }
    } else if let Some(inserted_turn_id) = inserted_turn_id
        && status == TurnStatus::Cancelled
    {
        let persist_result =
            store.update_turn_reply_and_side_effects_by_id(inserted_turn_id, &telegram_reply);
        if let Err(err) = persist_result {
            tracing::warn!("failed to mark cancelled turn side-effect state as persisted: {err:#}");
        }
    }

    if cancel_router.is_some() {
        clear_cancel_marker(cfg);
    }
    Ok(ProcessedTurn {
        output: render_output(&telegram_reply, &effects),
        status,
    })
}

fn turn_context_channel<'a>(source: &TaskSource, input: &'a TurnInput) -> &'a str {
    let channel = input.channel.trim();
    if channel.is_empty() {
        source.channel_name()
    } else {
        input.channel.as_str()
    }
}

fn telegram_progress_chat_id(chat_id: &str) -> String {
    chat_id
        .strip_prefix("telegram:")
        .unwrap_or(chat_id)
        .to_string()
}

pub(crate) fn resolve_turn_result(
    raw_output: &str,
    channel: &str,
    provider_success: bool,
    cancelled: bool,
) -> TurnResult {
    let channel = channel.to_owned();

    if cancelled {
        return TurnResult {
            effects: vec![],
            telegram_reply: "❌ Cancelled.".to_string(),
            status: TurnStatus::Cancelled,
            channel,
        };
    }

    let cleaned = final_marker_payload(raw_output).trim().to_string();
    let raw_output = &cleaned;
    let markers = parse_markers(raw_output);
    let telegram_reply = markers.reply().cloned().unwrap_or_default();
    let effects = markers.to_effects();
    if !telegram_reply.trim().is_empty() {
        return TurnResult {
            effects,
            telegram_reply,
            status: if provider_success {
                TurnStatus::Ok
            } else {
                TurnStatus::AgentError
            },
            channel: channel.clone(),
        };
    }

    if provider_success {
        if let Some(recovered) = recover_unstructured_reply(&cleaned) {
            return TurnResult {
                effects,
                telegram_reply: recovered,
                status: TurnStatus::ParseRecovered,
                channel: channel.clone(),
            };
        }
        // If it's just raw text without markers, and no specific unstructured JSON format
        // was found, return the text directly instead of an error message.
        return TurnResult {
            effects,
            telegram_reply: if cleaned.is_empty() {
                "I could not parse structured markers from the model output.".to_string()
            } else {
                cleaned.clone()
            },
            status: if cleaned.is_empty() {
                TurnStatus::ParseFallback
            } else {
                TurnStatus::ParseRecovered
            },
            channel: channel.clone(),
        };
    }

    if let Some(recovered) = extract_assistant_text_from_json_stream(&cleaned) {
        return TurnResult {
            effects,
            telegram_reply: recovered,
            status: TurnStatus::AgentErrorRecovered,
            channel: channel.clone(),
        };
    }

    // Provider failed but output is plain natural-language text (not a JSON error
    // stream).  Deliver it to the user instead of wrapping it in a generic error
    // prefix — a partial response is more useful than "Agent execution failed locally."
    if let Some(recovered) = recover_unstructured_reply(&cleaned) {
        return TurnResult {
            effects,
            telegram_reply: recovered,
            status: TurnStatus::AgentErrorRecovered,
            channel: channel.clone(),
        };
    }

    let err_line = extract_error_summary(raw_output)
        .or_else(|| {
            raw_output
                .lines()
                .find(|line| !line.trim().is_empty())
                .map(|line| line.to_string())
        })
        .unwrap_or_else(|| "Please check local logs and retry.".to_string());
    TurnResult {
        effects,
        telegram_reply: format!("Agent execution failed locally. {err_line}"),
        status: TurnStatus::AgentError,
        channel,
    }
}

pub(crate) fn resolve_turn_input(
    cfg: &RuntimeConfig,
    _store: &Store,
    _update_id: Option<String>,
    text: Option<String>,
    file: Option<PathBuf>,
    channel: &str,
) -> Result<TurnInput> {
    if let Some(path) = file {
        let resolved = resolve_instance_path(&cfg.instance_dir, path);
        if !resolved.exists() {
            bail!("inject file not found: {}", resolved.display());
        }

        let lower = resolved
            .extension()
            .and_then(|ext| ext.to_str())
            .unwrap_or_default()
            .to_ascii_lowercase();
        let (input_type, attachment_type) = match lower.as_str() {
            "jpg" | "jpeg" | "png" | "gif" | "webp" | "bmp" => {
                (InputType::Photo, Some("photo".to_string()))
            }
            "mp4" | "mkv" | "avi" | "mov" | "webm" => (InputType::Video, Some("video".to_string())),
            _ => (InputType::Document, Some("document".to_string())),
        };

        let user_text = text
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "(empty message)".to_string());

        return Ok(TurnInput {
            input_type,
            user_text,
            attachment_type,
            attachment_path: Some(resolved),
            attachment_owned: false,
            supplemental_context: None,
            channel: channel.to_string(),
        });
    }

    let user_text = text
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "(empty message)".to_string());

    Ok(TurnInput {
        input_type: InputType::Text,
        user_text,
        attachment_type: None,
        attachment_path: None,
        attachment_owned: false,
        supplemental_context: None,
        channel: channel.to_string(),
    })
}

pub(crate) fn hydrate_turn_input(
    cfg: &RuntimeConfig,
    update_id: Option<&str>,
    mut input: TurnInput,
    media: Option<IncomingMedia>,
    channel: &str,
) -> Result<(TurnInput, Option<PathBuf>)> {
    let Some(media) = media else {
        return Ok((input, None));
    };

    // Only Telegram attachments are downloaded.
    if channel != "telegram" {
        tracing::warn!(
            "Media hydration for channel '{}' not yet implemented, ignoring attachment",
            channel
        );
        return Ok((input, None));
    }

    let client = build_telegram_client(cfg)?;

    let suffix = update_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("0")
        .to_string();

    match media {
        IncomingMedia::Photo { file_id } => {
            let path = cfg.tmp_dir.join(format!("photo_{suffix}.jpg"));
            telegram_download_file(&client, cfg, &file_id, &path)?;
            input.input_type = InputType::Photo;
            input.attachment_type = Some("photo".to_string());
            input.attachment_path = Some(path.clone());
            input.attachment_owned = true;
            Ok((input, Some(path)))
        }
        IncomingMedia::Document { file_id, file_name } => {
            let ext = file_name
                .as_deref()
                .and_then(|name| Path::new(name).extension().and_then(|ext| ext.to_str()))
                .unwrap_or("bin");
            let path = cfg.tmp_dir.join(format!("doc_{suffix}.{ext}"));
            telegram_download_file(&client, cfg, &file_id, &path)?;
            input.input_type = InputType::Document;
            input.attachment_type = Some("document".to_string());
            input.attachment_path = Some(path.clone());
            input.attachment_owned = true;
            Ok((input, Some(path)))
        }
        IncomingMedia::Video { file_id } => {
            let path = cfg.tmp_dir.join(format!("video_{suffix}.mp4"));
            telegram_download_file(&client, cfg, &file_id, &path)?;
            input.input_type = InputType::Video;
            input.attachment_type = Some("video".to_string());
            input.attachment_path = Some(path.clone());
            input.attachment_owned = true;
            Ok((input, Some(path)))
        }
        IncomingMedia::VideoNote { file_id } => {
            let path = cfg.tmp_dir.join(format!("video_note_{suffix}.mp4"));
            telegram_download_file(&client, cfg, &file_id, &path)?;
            input.input_type = InputType::VideoNote;
            input.attachment_type = Some("video_note".to_string());
            input.attachment_path = Some(path.clone());
            input.attachment_owned = true;
            Ok((input, Some(path)))
        }
    }
}

#[cfg(test)]
#[path = "turn_tests.rs"]
mod tests;
