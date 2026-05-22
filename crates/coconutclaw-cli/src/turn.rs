//! Turn processing: running the AI provider and resolving output.
//!
//! Handles the full lifecycle of a single conversational turn:
//! building context, invoking the provider, handling cancellation
//! and progress updates, resolving the output, and persisting the turn.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::time::Instant;

use anyhow::{Context, Result, bail};
use coconutclaw_config::RuntimeConfig;
use coconutclaw_provider::run_provider;

use crate::cancel::CancelRouter;
use crate::context::{append_memory_and_tasks, build_context};
use crate::delivery::{DeliveryTarget, TaskSource};
use crate::markers::{
    extract_assistant_text_from_json_stream, extract_error_summary, parse_markers,
    recover_unstructured_reply, render_output, should_retry_provider_failure,
};
use crate::slack::{
    SlackMedia, build_slack_client, slack_download_file, spawn_slack_progress_updater,
};
use crate::store::{Store, TurnRecord};
use crate::telegram::{
    build_telegram_client, spawn_progress_updater, telegram_download_file, valid_telegram_chat_id,
};
use crate::{
    IncomingMedia, InputType, QuotedMessage, TurnInput, TurnResult, TurnStatus,
    asr_feature_enabled, clear_cancel_marker, command_exists, iso_now, resolve_instance_path,
};

fn run_asr_script(cfg: &RuntimeConfig, audio_path: &Path) -> Result<String> {
    let _span = tracing::info_span!("asr", path = %audio_path.display()).entered();
    let script = cfg.root_dir.join("scripts/asr.sh");
    if !script.is_file() {
        bail!("ASR script not found: {}", script.display());
    }
    if !command_exists("bash") {
        bail!("bash not found; cannot run ASR script");
    }

    let mut cmd = Command::new("bash");
    cmd.arg(script)
        .arg(audio_path)
        .current_dir(&cfg.root_dir)
        .env("INSTANCE_DIR", &cfg.instance_dir);
    if let Some(value) = cfg.asr_url.as_deref() {
        cmd.env("ASR_URL", value);
    }
    if let Some(value) = cfg.asr_cmd_template.as_deref() {
        cmd.env("ASR_CMD_TEMPLATE", value);
    }
    if let Some(value) = cfg.asr_file_field.as_deref() {
        cmd.env("ASR_FILE_FIELD", value);
    }
    if let Some(value) = cfg.asr_text_jq.as_deref() {
        cmd.env("ASR_TEXT_JQ", value);
    }
    if let Some(value) = cfg.asr_preprocess.as_deref() {
        cmd.env("ASR_PREPROCESS", value);
    }
    if let Some(value) = cfg.asr_sample_rate.as_deref() {
        cmd.env("ASR_SAMPLE_RATE", value);
    }

    let output = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .context("failed to execute ASR script")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        bail!("ASR script failed: {stderr}");
    }

    let text = String::from_utf8_lossy(&output.stdout).to_string();
    Ok(text.trim().to_string())
}

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
        provider = %cfg.provider.as_str(),
    )
    .entered();

    clear_cancel_marker(cfg);
    let ts = iso_now(&cfg.timezone);
    let context_channel = turn_context_channel(source, &input).to_string();
    let progress_channel = progress_target
        .map(DeliveryTarget::transport_name)
        .unwrap_or(&context_channel);
    let chat_id = context_chat_id
        .or_else(|| progress_target.map(|target| target.display_id().to_owned()))
        .or_else(|| match source {
            TaskSource::Slack { .. } => cfg.slack_channel_id.clone(),
            TaskSource::Telegram => valid_telegram_chat_id(cfg).map(ToOwned::to_owned),
            TaskSource::Scheduled | TaskSource::Local => Some("local".to_string()),
        })
        .unwrap_or_else(|| "local".to_string());
    let progress_chat_id = progress_target
        .map(|target| target.display_id().to_owned())
        .or_else(|| match source {
            TaskSource::Slack { channel_id, .. } => Some(channel_id.clone()),
            TaskSource::Telegram => Some(chat_id.clone()),
            TaskSource::Scheduled | TaskSource::Local => None,
        })
        .unwrap_or_else(|| chat_id.clone());

    let context = build_context(cfg, store, &input, &ts, &chat_id, quoted)?;

    let cancel_flag = external_cancel_flag.unwrap_or_else(|| Arc::new(AtomicBool::new(false)));
    if let Some(router) = &cancel_router {
        router.register(Arc::clone(&cancel_flag));
    }
    let progress_updater_stop = Arc::new(AtomicBool::new(false));
    let (progress_sender, progress_updater) = if let Some(message_id) = progress_message_id {
        let (progress_tx, progress_rx) = mpsc::channel::<String>();
        let updater = if progress_channel == "slack" {
            match build_slack_client(cfg) {
                Ok(slack_client) => spawn_slack_progress_updater(
                    slack_client,
                    progress_chat_id,
                    message_id.to_string(),
                    progress_rx,
                    Arc::clone(&progress_updater_stop),
                    cfg.progress_update_interval_secs,
                ),
                Err(err) => {
                    tracing::warn!("failed to build slack client for progress updater: {err:#}");
                    drop(progress_rx);
                    return Err(err);
                }
            }
        } else {
            spawn_progress_updater(
                cfg.clone(),
                telegram_progress_chat_id(&progress_chat_id),
                message_id.to_string(),
                progress_rx,
                Arc::clone(&progress_updater_stop),
            )
        };
        (Some(progress_tx), Some(updater))
    } else {
        (None, None)
    };

    let mut provider_result = run_provider(
        input.attachment_path.as_ref(),
        cfg,
        &context,
        Some(&cancel_flag),
        progress_sender.as_ref(),
        None,
    );
    let mut retries = 0u32;
    while retries < cfg.provider_max_retries {
        let should_retry = match &provider_result {
            Ok(result) => {
                !result.success
                    && !cancel_flag.load(Ordering::SeqCst)
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
        provider_result = run_provider(
            input.attachment_path.as_ref(),
            cfg,
            &context,
            Some(&cancel_flag),
            progress_sender.as_ref(),
            None,
        );
    }
    drop(progress_sender);
    progress_updater_stop.store(true, Ordering::SeqCst);
    if let Some(handle) = progress_updater {
        let _ = handle.join();
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
    let voice_reply = turn_result.voice_reply;
    let status = turn_result.status;

    tracing::info!(
        duration_ms,
        status = %status,
        retries,
        "turn completed"
    );

    let inserted_turn_id = store.insert_turn(&TurnRecord {
        ts: ts.clone(),
        chat_id,
        input_type: input.input_type.to_string(),
        user_text: input.user_text,
        asr_text: input.asr_text,
        provider_raw: raw_output,
        telegram_reply: telegram_reply.clone(),
        voice_reply: voice_reply.clone(),
        status: status.to_string(),
        update_id: update_id.clone(),
        duration_ms: Some(duration_ms),
        channel: context_channel.clone(),
        task_run_id,
        side_effects_applied: false,
    })?;

    let mut telegram_reply = telegram_reply;
    if let Some(inserted_turn_id) = inserted_turn_id
        && status != TurnStatus::Cancelled
    {
        let append_outcome = append_memory_and_tasks(
            cfg,
            store,
            &ts,
            Some(inserted_turn_id),
            &effects,
            origin_session,
            delivery_target_json,
        )?;
        if !append_outcome.schedule_feedback.is_empty() {
            if !telegram_reply.trim().is_empty() {
                telegram_reply.push_str(
                    "

",
                );
            }
            telegram_reply.push_str(&append_outcome.schedule_feedback.join(
                "
",
            ));
        }

        let persist_result = store.update_turn_reply_and_side_effects_by_id(
            inserted_turn_id,
            &telegram_reply,
            &voice_reply,
        );
        if let Err(err) = persist_result {
            tracing::warn!(
                "failed to update stored turn reply/side-effect state after turn processing: {err:#}"
            );
        }
    } else if let Some(inserted_turn_id) = inserted_turn_id
        && status == TurnStatus::Cancelled
    {
        let persist_result = store.update_turn_reply_and_side_effects_by_id(
            inserted_turn_id,
            &telegram_reply,
            &voice_reply,
        );
        if let Err(err) = persist_result {
            tracing::warn!("failed to mark cancelled turn side-effect state as persisted: {err:#}");
        }
    }

    if cancel_router.is_some() {
        clear_cancel_marker(cfg);
    }
    Ok(ProcessedTurn {
        output: render_output(&telegram_reply, &voice_reply, &effects),
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
        .split_once('#')
        .map(|(root, _)| root)
        .unwrap_or_else(|| chat_id.strip_prefix("telegram:").unwrap_or(chat_id))
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
            voice_reply: String::new(),
            status: TurnStatus::Cancelled,
            channel,
        };
    }

    // Strip<think>...</think> blocks emitted by reasoning models (e.g. Qwen3.5).
    let mut cleaned = String::new();
    let mut current = raw_output;
    while let Some(start) = current.find("<think>") {
        cleaned.push_str(&current[..start]);
        if let Some(end) = current[start + "<think>".len()..].find("</think>") {
            let skip_index = start + "<think>".len() + end + "</think>".len();
            current = &current[skip_index..];
        } else {
            // Unclosed think tag: assume the rest of the output is thought process.
            current = "";
            break;
        }
    }
    cleaned.push_str(current);
    let cleaned = cleaned.trim().to_string();
    let raw_output = if cleaned.is_empty() && !raw_output.trim().is_empty() {
        // If the entire output was inside <think>, fallback to original text.
        raw_output
    } else {
        &cleaned
    };
    let markers = parse_markers(raw_output);
    let telegram_reply = markers.reply().cloned().unwrap_or_default();
    let voice_reply = markers.voice_reply.clone().unwrap_or_default();
    let effects = markers.to_effects();
    if !telegram_reply.trim().is_empty() || !voice_reply.trim().is_empty() {
        return TurnResult {
            effects,
            telegram_reply,
            voice_reply,
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
                voice_reply: String::new(),
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
            voice_reply: String::new(),
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
            voice_reply: String::new(),
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
            voice_reply: String::new(),
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
        voice_reply: String::new(),
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
            asr_text: String::new(),
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
        asr_text: String::new(),
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

    // Only Telegram media is currently supported for download.
    // Slack media download will be handled by the slack module.
    if channel != "telegram" {
        tracing::warn!(
            "Media hydration for channel '{}' not yet implemented, ignoring attachment",
            channel
        );
        return Ok((input, None));
    }

    let client = match build_telegram_client(cfg) {
        Ok(client) => client,
        Err(err) => {
            tracing::warn!("telegram media fetch disabled: {err:#}");
            return Ok((input, None));
        }
    };

    let suffix = update_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("0")
        .to_string();

    match media {
        IncomingMedia::Voice { file_id } => {
            let voice_path = cfg.tmp_dir.join(format!("in_{suffix}.oga"));
            if let Err(err) = telegram_download_file(&client, cfg, &file_id, &voice_path) {
                tracing::warn!("failed to download voice attachment: {err:#}");
                return Ok((input, None));
            }

            input.input_type = InputType::Voice;
            input.attachment_type = None;
            input.attachment_path = None;
            input.attachment_owned = false;

            if asr_feature_enabled(cfg) {
                match run_asr_script(cfg, &voice_path) {
                    Ok(asr_text) => {
                        let asr_text = asr_text.trim().to_string();
                        if !asr_text.is_empty() {
                            input.asr_text = asr_text.clone();
                            input.user_text = asr_text;
                        }
                    }
                    Err(err) => {
                        tracing::warn!("ASR failed for voice attachment: {err:#}");
                    }
                }
            }

            let _ = fs::remove_file(voice_path);
            Ok((input, None))
        }
        IncomingMedia::Photo { file_id } => {
            let path = cfg.tmp_dir.join(format!("photo_{suffix}.jpg"));
            if let Err(err) = telegram_download_file(&client, cfg, &file_id, &path) {
                tracing::warn!("failed to download photo attachment: {err:#}");
                return Ok((input, None));
            }
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
            if let Err(err) = telegram_download_file(&client, cfg, &file_id, &path) {
                tracing::warn!("failed to download document attachment: {err:#}");
                return Ok((input, None));
            }
            input.input_type = InputType::Document;
            input.attachment_type = Some("document".to_string());
            input.attachment_path = Some(path.clone());
            input.attachment_owned = true;
            Ok((input, Some(path)))
        }
        IncomingMedia::Video { file_id } => {
            let path = cfg.tmp_dir.join(format!("video_{suffix}.mp4"));
            if let Err(err) = telegram_download_file(&client, cfg, &file_id, &path) {
                tracing::warn!("failed to download video attachment: {err:#}");
                return Ok((input, None));
            }
            input.input_type = InputType::Video;
            input.attachment_type = Some("video".to_string());
            input.attachment_path = Some(path.clone());
            input.attachment_owned = true;
            Ok((input, Some(path)))
        }
        IncomingMedia::VideoNote { file_id } => {
            let path = cfg.tmp_dir.join(format!("video_note_{suffix}.mp4"));
            if let Err(err) = telegram_download_file(&client, cfg, &file_id, &path) {
                tracing::warn!("failed to download video_note attachment: {err:#}");
                return Ok((input, None));
            }
            input.input_type = InputType::VideoNote;
            input.attachment_type = Some("video_note".to_string());
            input.attachment_path = Some(path.clone());
            input.attachment_owned = true;
            Ok((input, Some(path)))
        }
    }
}

pub(crate) fn hydrate_slack_turn_input(
    cfg: &RuntimeConfig,
    update_id: Option<&str>,
    mut input: TurnInput,
    media: Option<SlackMedia>,
) -> Result<(TurnInput, Option<PathBuf>)> {
    let Some(media) = media else {
        return Ok((input, None));
    };

    let client = match build_slack_client(cfg) {
        Ok(c) => c,
        Err(err) => {
            tracing::warn!("slack client build failed for media download: {err:#}");
            return Ok((input, None));
        }
    };

    let suffix = update_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("0")
        .to_string();

    match media {
        SlackMedia::File {
            url_private,
            filetype,
            filename,
            size: _,
        } => {
            let ext = Path::new(&filename)
                .extension()
                .and_then(|e| e.to_str())
                .unwrap_or("bin");
            let path = cfg.tmp_dir.join(format!("slack_{suffix}.{ext}"));

            if let Err(err) = slack_download_file(&client, &url_private, &path) {
                tracing::warn!("failed to download slack file attachment: {err:#}");
                return Ok((input, None));
            }

            let (input_type, attachment_type) = match filetype.as_deref().unwrap_or("") {
                "jpg" | "jpeg" | "png" | "gif" | "webp" | "bmp" => {
                    (InputType::Photo, Some("photo".to_string()))
                }
                "mp4" | "mkv" | "avi" | "mov" | "webm" => {
                    (InputType::Video, Some("video".to_string()))
                }
                _ => (InputType::Document, Some("document".to_string())),
            };

            input.input_type = input_type;
            input.attachment_type = attachment_type;
            input.attachment_path = Some(path.clone());
            input.attachment_owned = true;
            Ok((input, Some(path)))
        }
    }
}

#[cfg(test)]
#[path = "turn_tests.rs"]
mod tests;
