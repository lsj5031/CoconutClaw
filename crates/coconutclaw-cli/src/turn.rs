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


use crate::context::{append_memory_and_tasks, build_context};
use crate::markers::{
    ParsedMarkers, extract_assistant_text_from_json_stream, extract_error_summary, parse_markers,
    recover_unstructured_reply, render_output, should_retry_provider_failure,
};
use crate::store::{Store, TurnRecord};
use crate::telegram::{
    build_telegram_client, spawn_progress_updater, telegram_download_file,
};
use crate::{
    InputType, TurnInput, QuotedMessage, TurnResult, TurnStatus, IncomingMedia,
    clear_cancel_marker, command_exists, iso_now,
    maybe_spawn_cancel_watcher, resolve_instance_path, asr_feature_enabled,
};

pub(crate) fn process_turn(
    cfg: &RuntimeConfig,
    store: &Store,
    input: TurnInput,
    chat_id_override: Option<String>,
    update_id: Option<String>,
    progress_message_id: Option<&str>,
    quoted: &QuotedMessage,
) -> Result<String> {
    let turn_start = Instant::now();
    let _span = tracing::info_span!(
        "process_turn",
        input_type = %input.input_type,
        provider = %cfg.provider.as_str(),
    )
    .entered();

    clear_cancel_marker(cfg);
    let ts = iso_now(&cfg.timezone);
    let chat_id = chat_id_override
        .or_else(|| cfg.telegram_chat_id.clone())
        .unwrap_or_else(|| "local".to_string());

    let context = build_context(cfg, store, &input, &ts, quoted)?;

    let cancel_flag = Arc::new(AtomicBool::new(false));
    let cancel_watcher_stop = Arc::new(AtomicBool::new(false));
    let cancel_watcher = if update_id.is_some() {
        maybe_spawn_cancel_watcher(
            cfg,
            store,
            update_id.clone(),
            Arc::clone(&cancel_flag),
            Arc::clone(&cancel_watcher_stop),
        )
        .ok()
        .flatten()
    } else {
        None
    };
    let progress_updater_stop = Arc::new(AtomicBool::new(false));
    let (progress_sender, progress_updater) = if let Some(message_id) = progress_message_id {
        let (progress_tx, progress_rx) = mpsc::channel::<String>();
        (
            Some(progress_tx),
            Some(spawn_progress_updater(
                cfg.clone(),
                chat_id.clone(),
                message_id.to_string(),
                progress_rx,
                Arc::clone(&progress_updater_stop),
            )),
        )
    } else {
        (None, None)
    };

    let mut provider_result =
        run_provider(cfg, &context, Some(&cancel_flag), progress_sender.as_ref());
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
        provider_result = run_provider(cfg, &context, Some(&cancel_flag), progress_sender.as_ref());
    }
    cancel_watcher_stop.store(true, Ordering::SeqCst);
    if let Some(handle) = cancel_watcher {
        let _ = handle.join();
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
    let turn_result = resolve_turn_result(&raw_output, provider_success, cancelled);
    let markers = turn_result.markers;
    let telegram_reply = turn_result.telegram_reply;
    let voice_reply = turn_result.voice_reply;
    let status = turn_result.status;

    tracing::info!(
        duration_ms,
        status = %status,
        retries,
        "turn completed"
    );

    let inserted = store.insert_turn(&TurnRecord {
        ts: ts.clone(),
        chat_id,
        input_type: input.input_type.to_string(),
        user_text: input.user_text,
        asr_text: input.asr_text,
        provider_raw: raw_output,
        telegram_reply: telegram_reply.clone(),
        voice_reply: voice_reply.clone(),
        status: status.to_string(),
        update_id,
        duration_ms: Some(duration_ms),
    })?;

    if inserted && status != TurnStatus::Cancelled {
        append_memory_and_tasks(cfg, store, &ts, &markers)?;
    }

    clear_cancel_marker(cfg);
    Ok(render_output(&telegram_reply, &voice_reply, &markers))
}

pub(crate) fn resolve_turn_result(raw_output: &str, provider_success: bool, cancelled: bool) -> TurnResult {
    if cancelled {
        return TurnResult {
            markers: ParsedMarkers::default(),
            telegram_reply: "❌ Cancelled.".to_string(),
            voice_reply: String::new(),
            status: TurnStatus::Cancelled,
        };
    }

    let markers = parse_markers(raw_output);
    let telegram_reply = markers.telegram_reply.clone().unwrap_or_default();
    let voice_reply = markers.voice_reply.clone().unwrap_or_default();
    if !telegram_reply.trim().is_empty() || !voice_reply.trim().is_empty() {
        return TurnResult {
            markers,
            telegram_reply,
            voice_reply,
            status: if provider_success {
                TurnStatus::Ok
            } else {
                TurnStatus::AgentError
            },
        };
    }

    if provider_success {
        if let Some(recovered) = recover_unstructured_reply(raw_output) {
            return TurnResult {
                markers,
                telegram_reply: recovered,
                voice_reply: String::new(),
                status: TurnStatus::ParseRecovered,
            };
        }
        return TurnResult {
            markers,
            telegram_reply: "I could not parse structured markers from the model output."
                .to_string(),
            voice_reply: String::new(),
            status: TurnStatus::ParseFallback,
        };
    }

    if let Some(recovered) = extract_assistant_text_from_json_stream(raw_output) {
        return TurnResult {
            markers,
            telegram_reply: recovered,
            voice_reply: String::new(),
            status: TurnStatus::AgentErrorRecovered,
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
        markers,
        telegram_reply: format!("Agent execution failed locally. {err_line}"),
        voice_reply: String::new(),
        status: TurnStatus::AgentError,
    }
}

pub(crate) fn resolve_turn_input(
    inject_text: Option<String>,
    inject_file: Option<PathBuf>,
    cfg: &RuntimeConfig,
) -> Result<TurnInput> {
    let user_text = inject_text
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "(empty message)".to_string());

    if let Some(path) = inject_file {
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
                "mp4" | "mkv" | "avi" | "mov" | "webm" => {
                    (InputType::Video, Some("video".to_string()))
                }
                _ => (InputType::Document, Some("document".to_string())),
            };

        return Ok(TurnInput {
            input_type,
            user_text,
            asr_text: String::new(),
            attachment_type,
            attachment_path: Some(resolved),
            attachment_owned: false,
        });
    }

    Ok(TurnInput {
        input_type: InputType::Text,
        user_text,
        asr_text: String::new(),
        attachment_type: None,
        attachment_path: None,
        attachment_owned: false,
    })
}

pub(crate) fn hydrate_turn_input(
    cfg: &RuntimeConfig,
    update_id: Option<&str>,
    mut input: TurnInput,
    media: Option<IncomingMedia>,
) -> Result<(TurnInput, Option<PathBuf>)> {
    let Some(media) = media else {
        return Ok((input, None));
    };

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
            } else {
                tracing::info!("voice attachment received but ASR is disabled in config.toml");
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

pub(crate) fn run_asr_script(cfg: &RuntimeConfig, audio_path: &Path) -> Result<String> {
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

