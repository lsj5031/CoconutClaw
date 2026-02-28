//! Telegram Bot API interaction layer.
//!
//! Contains all functions that directly call the Telegram HTTP API:
//! client construction, message send/edit, file upload/download,
//! progress updates, markdown rendering, webhook registration, and rate-limit retry.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use coconutclaw_config::RuntimeConfig;
use coconutclaw_config::{TelegramParseFallback, TelegramParseMode};
use reqwest::blocking::{Client, multipart};
use serde_json::Value;
use telegram_markdown_v2::{UnsupportedTagsStrategy, convert_with_strategy};

use crate::markers::parse_markers;
use crate::{command_exists, resolve_instance_path};
use crate::webhook::{
    value_to_string, webhook_public_endpoint,
};

pub(crate) fn valid_telegram_token(cfg: &RuntimeConfig) -> Option<&str> {
    cfg.telegram_bot_token
        .as_deref()
        .map(str::trim)
        .filter(|token| !token.is_empty() && *token != "replace_me")
}

pub(crate) fn valid_telegram_chat_id(cfg: &RuntimeConfig) -> Option<&str> {
    cfg.telegram_chat_id
        .as_deref()
        .map(str::trim)
        .filter(|chat_id| !chat_id.is_empty() && *chat_id != "replace_me")
}

pub(crate) fn telegram_api_base(cfg: &RuntimeConfig) -> Result<String> {
    let token = valid_telegram_token(cfg).ok_or_else(|| {
        anyhow::anyhow!("TELEGRAM_BOT_TOKEN is missing; set it in instance config.toml")
    })?;
    Ok(format!("https://api.telegram.org/bot{token}"))
}

pub(crate) fn telegram_file_base(cfg: &RuntimeConfig) -> Result<String> {
    let token = valid_telegram_token(cfg).ok_or_else(|| {
        anyhow::anyhow!("TELEGRAM_BOT_TOKEN is missing; set it in instance config.toml")
    })?;
    Ok(format!("https://api.telegram.org/file/bot{token}"))
}

pub(crate) fn build_telegram_client(cfg: &RuntimeConfig) -> Result<Client> {
    let _ = telegram_api_base(cfg)?;
    let _ = valid_telegram_chat_id(cfg).ok_or_else(|| {
        anyhow::anyhow!("TELEGRAM_CHAT_ID is missing; set it in instance config.toml")
    })?;
    Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .context("failed to build telegram HTTP client")
}

pub(crate) fn telegram_download_file(
    client: &Client,
    cfg: &RuntimeConfig,
    file_id: &str,
    out_path: &Path,
) -> Result<()> {
    let base = telegram_api_base(cfg)?;
    let file_base = telegram_file_base(cfg)?;
    let response = client
        .post(format!("{base}/getFile"))
        .form(&[("file_id", file_id)])
        .send()
        .context("failed to call telegram getFile")?;
    let value = parse_telegram_response(response, "getFile")?;
    let file_path = value
        .get("result")
        .and_then(|node| node.get("file_path"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow::anyhow!("telegram getFile returned empty file_path"))?;

    if let Some(parent) = out_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    let response = client
        .get(format!("{file_base}/{file_path}"))
        .send()
        .context("failed to download telegram file")?;
    if !response.status().is_success() {
        bail!("telegram file download HTTP {}", response.status().as_u16());
    }
    let bytes = response
        .bytes()
        .context("failed to read downloaded telegram file")?;
    fs::write(out_path, &bytes)
        .with_context(|| format!("failed to write {}", out_path.display()))?;
    Ok(())
}

pub(crate) fn fetch_poll_updates(
    client: &Client,
    cfg: &RuntimeConfig,
    offset: Option<u64>,
) -> Result<Vec<Value>> {
    fetch_updates(client, cfg, offset, 25, r#"["message","callback_query"]"#)
}

pub(crate) fn fetch_cancel_updates(
    client: &Client,
    cfg: &RuntimeConfig,
    offset: Option<u64>,
) -> Result<Vec<Value>> {
    fetch_updates(client, cfg, offset, 0, r#"["message","callback_query"]"#)
}

pub(crate) fn fetch_updates(
    client: &Client,
    cfg: &RuntimeConfig,
    offset: Option<u64>,
    timeout_seconds: u64,
    allowed_updates: &str,
) -> Result<Vec<Value>> {
    let base = telegram_api_base(cfg)?;
    let url = format!("{base}/getUpdates");

    let mut query: Vec<(&str, String)> = vec![
        ("timeout", timeout_seconds.to_string()),
        ("allowed_updates", allowed_updates.to_string()),
    ];
    if let Some(offset) = offset {
        query.push(("offset", offset.to_string()));
    }

    let response = client
        .get(url)
        .query(&query)
        .send()
        .context("failed to call telegram getUpdates")?;
    let value = parse_telegram_response(response, "getUpdates")?;
    let updates = value
        .get("result")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    Ok(updates)
}

pub(crate) fn register_bot_commands(client: &Client, cfg: &RuntimeConfig) -> Result<()> {
    let base = telegram_api_base(cfg)?;
    let commands = serde_json::json!([
        {"command": "fresh", "description": "Start a fresh conversation"},
        {"command": "cancel", "description": "Cancel the current task"},
    ]);
    let params = vec![("commands".to_string(), commands.to_string())];
    telegram_post_form(
        client,
        &format!("{base}/setMyCommands"),
        &params,
        "setMyCommands",
    )?;
    Ok(())
}

pub(crate) fn register_telegram_webhook(client: &Client, cfg: &RuntimeConfig) -> Result<()> {
    let webhook_url = webhook_public_endpoint(cfg)?;
    let base = telegram_api_base(cfg)?;

    let mut params: Vec<(String, String)> = vec![
        ("url".to_string(), webhook_url),
        (
            "allowed_updates".to_string(),
            r#"["message","callback_query"]"#.to_string(),
        ),
        ("drop_pending_updates".to_string(), "false".to_string()),
    ];
    if let Some(secret) = cfg.webhook_secret.as_deref().map(str::trim)
        && !secret.is_empty()
    {
        params.push(("secret_token".to_string(), secret.to_string()));
    }

    let response = client
        .post(format!("{base}/setWebhook"))
        .form(&params)
        .send()
        .context("failed to call telegram setWebhook")?;
    parse_telegram_response(response, "setWebhook")?;
    Ok(())
}

pub(crate) fn dispatch_telegram_output(
    client: &Client,
    cfg: &RuntimeConfig,
    chat_id_override: Option<&str>,
    output: &str,
    progress_message_id: Option<&str>,
) -> Result<()> {
    let _span = tracing::info_span!("dispatch_telegram").entered();
    let Some(chat_id) = chat_id_override
        .or(cfg.telegram_chat_id.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        tracing::warn!("cannot dispatch telegram output: no chat_id available");
        return Ok(());
    };

    let markers = parse_markers(output);
    if let Some(reply) = markers.telegram_reply.as_deref() {
        let reply = reply.trim();
        if !reply.is_empty() {
            let rendered_reply = render_telegram_reply_text(cfg, reply);
            send_or_edit_text(client, cfg, chat_id, &rendered_reply, progress_message_id)?;
        } else if let Some(message_id) = progress_message_id {
            let _ = telegram_remove_keyboard(client, cfg, chat_id, message_id);
        }
    } else if let Some(message_id) = progress_message_id {
        let _ = telegram_remove_keyboard(client, cfg, chat_id, message_id);
    }

    if let Some(voice_reply) = markers.voice_reply.as_deref() {
        let voice_reply = voice_reply.trim();
        if !voice_reply.is_empty()
            && let Err(err) = send_voice_reply(client, cfg, chat_id, voice_reply)
        {
            tracing::warn!("failed to send voice reply: {err:#}");
        }
    }

    for item in markers.send_photo {
        let path = resolve_instance_path(&cfg.instance_dir, PathBuf::from(item));
        if let Err(err) =
            telegram_send_media_file(client, cfg, chat_id, "sendPhoto", "photo", &path)
        {
            tracing::warn!("failed to send photo {}: {err:#}", path.display());
        }
    }
    for item in markers.send_document {
        let path = resolve_instance_path(&cfg.instance_dir, PathBuf::from(item));
        if let Err(err) =
            telegram_send_media_file(client, cfg, chat_id, "sendDocument", "document", &path)
        {
            tracing::warn!("failed to send document {}: {err:#}", path.display());
        }
    }
    for item in markers.send_video {
        let path = resolve_instance_path(&cfg.instance_dir, PathBuf::from(item));
        if let Err(err) =
            telegram_send_media_file(client, cfg, chat_id, "sendVideo", "video", &path)
        {
            tracing::warn!("failed to send video {}: {err:#}", path.display());
        }
    }

    Ok(())
}

pub(crate) fn send_progress_message(cfg: &RuntimeConfig, chat_id: &str) -> Result<Option<String>> {
    let client = build_telegram_client(cfg)?;
    let base = telegram_api_base(cfg)?;
    let reply_markup = progress_reply_markup();
    let params = [
        ("chat_id", chat_id.to_string()),
        ("text", "Thinking...".to_string()),
        ("reply_markup", reply_markup.to_string()),
    ];
    let response = client
        .post(format!("{base}/sendMessage"))
        .form(&params)
        .send()
        .context("failed to send progress message")?;
    let value = parse_telegram_response(response, "sendMessage")?;
    let message_id = value
        .get("result")
        .and_then(|node| node.get("message_id"))
        .map(value_to_string)
        .map(|id| id.trim().to_string())
        .filter(|id| !id.is_empty());
    Ok(message_id)
}

pub(crate) fn progress_reply_markup() -> &'static str {
    r#"{"inline_keyboard":[[{"text":"Cancel","callback_data":"cancel"}]]}"#
}

pub(crate) fn progress_status_text(elapsed_secs: u64) -> String {
    format!("Thinking...\nElapsed: {elapsed_secs}s\nTap Cancel to stop.")
}

pub(crate) fn progress_status_with_events(elapsed_secs: u64, statuses: &[String]) -> String {
    let mut text = progress_status_text(elapsed_secs);
    if statuses.is_empty() {
        return text;
    }
    text.push_str("\n\n");
    for status in statuses {
        text.push_str("- ");
        text.push_str(status);
        text.push('\n');
    }
    text.trim_end().to_string()
}

pub(crate) fn spawn_progress_updater(
    cfg: RuntimeConfig,
    chat_id: String,
    message_id: String,
    progress_rx: mpsc::Receiver<String>,
    stop_flag: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    let interval_secs = cfg.progress_update_interval_secs.max(1);
    thread::spawn(move || {
        let client = match build_telegram_client(&cfg) {
            Ok(client) => client,
            Err(_) => return,
        };
        let started = Instant::now();
        let mut last_bucket = 0u64;
        let mut last_edit = Instant::now()
            .checked_sub(Duration::from_secs(5))
            .unwrap_or_else(Instant::now);
        let mut statuses: Vec<String> = Vec::new();
        let mut saw_event = false;
        let mut channel_closed = false;

        loop {
            match progress_rx.recv_timeout(Duration::from_millis(400)) {
                Ok(status) => {
                    let status = status.trim().to_string();
                    if !status.is_empty() {
                        if let Some(existing) = statuses.iter().position(|item| item == &status) {
                            statuses.remove(existing);
                        }
                        statuses.push(status);
                        if statuses.len() > 5 {
                            statuses.remove(0);
                        }
                        saw_event = true;
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    channel_closed = true;
                }
            }

            let elapsed = started.elapsed().as_secs();
            let bucket = elapsed / interval_secs;
            let elapsed_tick = bucket > last_bucket;
            if elapsed_tick {
                last_bucket = bucket;
            }

            if (elapsed_tick || saw_event) && last_edit.elapsed() >= Duration::from_secs(1) {
                let text = progress_status_with_events(elapsed, &statuses);
                let rendered = render_telegram_reply_text(&cfg, &text);
                let _ = telegram_edit_message_text(
                    &client,
                    &cfg,
                    &chat_id,
                    &message_id,
                    &rendered,
                    true,
                );
                saw_event = false;
                last_edit = Instant::now();
            }

            if stop_flag.load(Ordering::SeqCst) && channel_closed {
                break;
            }
        }
    })
}

pub(crate) fn send_or_edit_text(
    client: &Client,
    cfg: &RuntimeConfig,
    chat_id: &str,
    text: &str,
    progress_message_id: Option<&str>,
) -> Result<()> {
    let chunks = split_text_chunks(text, 4096);
    if chunks.is_empty() {
        if let Some(message_id) = progress_message_id {
            let _ = telegram_remove_keyboard(client, cfg, chat_id, message_id);
        }
        return Ok(());
    }

    let mut chunks_iter = chunks.into_iter();
    let first_chunk = chunks_iter.next().unwrap();

    if let Some(message_id) = progress_message_id {
        if let Err(err) =
            telegram_edit_message_text(client, cfg, chat_id, message_id, &first_chunk, false)
        {
            tracing::warn!("failed to edit progress message with reply, sending new: {err:#}");
            let _ = telegram_remove_keyboard(client, cfg, chat_id, message_id);
            if let Err(err) = telegram_send_message(client, cfg, chat_id, &first_chunk) {
                tracing::warn!("failed to send text chunk: {err:#}");
                return Err(err);
            }
        }
    } else if let Err(err) = telegram_send_message(client, cfg, chat_id, &first_chunk) {
        tracing::warn!("failed to send text chunk: {err:#}");
        return Err(err);
    }

    let mut last_err: Option<anyhow::Error> = None;
    for chunk in chunks_iter {
        if let Err(err) = telegram_send_message(client, cfg, chat_id, &chunk) {
            tracing::warn!("failed to send text chunk: {err:#}");
            last_err = Some(err);
        }
    }

    match last_err {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

pub(crate) fn split_text_chunks(text: &str, max_chars: usize) -> Vec<String> {
    if max_chars == 0 {
        return vec![text.to_string()];
    }
    if text.is_empty() {
        return Vec::new();
    }
    if text.chars().count() <= max_chars {
        return vec![text.to_string()];
    }

    let mut chunks = Vec::new();
    let mut current = String::new();

    for line in text.split('\n') {
        let line_len = line.chars().count();
        if line_len > max_chars {
            // Finalize current chunk before handling the oversized line.
            if !current.is_empty() {
                chunks.push(current);
                current = String::new();
            }
            // Fall back to character-boundary splitting for this single line.
            let chars: Vec<char> = line.chars().collect();
            let mut start = 0usize;
            while start < chars.len() {
                let end = (start + max_chars).min(chars.len());
                chunks.push(chars[start..end].iter().collect());
                start = end;
            }
            continue;
        }

        let sep = if current.is_empty() { 0 } else { 1 }; // for '\n'
        if current.chars().count() + sep + line_len > max_chars {
            // Adding this line would exceed the limit; finalize current chunk.
            if !current.is_empty() {
                chunks.push(current);
            }
            current = line.to_string();
        } else {
            if !current.is_empty() {
                current.push('\n');
            }
            current.push_str(line);
        }
    }

    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

pub(crate) fn render_markdown_v2_reply(text: &str) -> String {
    match convert_with_strategy(text, UnsupportedTagsStrategy::Escape) {
        Ok(rendered) => rendered.trim_end_matches('\n').to_string(),
        Err(err) => {
            tracing::warn!("markdown conversion failed, sending original text: {err:#}");
            text.to_string()
        }
    }
}

pub(crate) fn render_telegram_reply_text(cfg: &RuntimeConfig, text: &str) -> String {
    if matches!(cfg.telegram_parse_mode, TelegramParseMode::MarkdownV2) {
        render_markdown_v2_reply(text)
    } else {
        text.to_string()
    }
}

pub(crate) fn telegram_edit_message_text(
    client: &Client,
    cfg: &RuntimeConfig,
    chat_id: &str,
    message_id: &str,
    text: &str,
    keep_cancel_button: bool,
) -> Result<()> {
    let base = telegram_api_base(cfg)?;
    let reply_markup = if keep_cancel_button {
        progress_reply_markup()
    } else {
        r#"{"inline_keyboard":[]}"#
    };
    let params =
        telegram_text_form_params(cfg, chat_id, Some(message_id), text, Some(reply_markup));
    let url = format!("{base}/editMessageText");
    match telegram_post_form(client, &url, &params, "editMessageText") {
        Ok(_) => Ok(()),
        Err(err) if should_retry_plain_text(cfg) && should_fallback_plain_for_error(&err) => {
            tracing::warn!("editMessageText markdown parse failed, retrying plain text: {err:#}");
            let retry = strip_parse_mode_param(&params);
            telegram_post_form(client, &url, &retry, "editMessageText").map(|_| ())
        }
        Err(err) => Err(err),
    }
}

pub(crate) fn telegram_remove_keyboard(
    client: &Client,
    cfg: &RuntimeConfig,
    chat_id: &str,
    message_id: &str,
) -> Result<()> {
    let base = telegram_api_base(cfg)?;
    let reply_markup = r#"{"inline_keyboard":[]}"#;
    let params = [
        ("chat_id", chat_id.to_string()),
        ("message_id", message_id.to_string()),
        ("reply_markup", reply_markup.to_string()),
    ];
    let response = client
        .post(format!("{base}/editMessageReplyMarkup"))
        .form(&params)
        .send()
        .context("failed to call telegram editMessageReplyMarkup")?;
    parse_telegram_response(response, "editMessageReplyMarkup")?;
    Ok(())
}

pub(crate) fn telegram_answer_callback(
    client: &Client,
    cfg: &RuntimeConfig,
    callback_query_id: &str,
) -> Result<()> {
    let base = telegram_api_base(cfg)?;
    let params = [("callback_query_id", callback_query_id.to_string())];
    let response = client
        .post(format!("{base}/answerCallbackQuery"))
        .form(&params)
        .send()
        .context("failed to call telegram answerCallbackQuery")?;
    parse_telegram_response(response, "answerCallbackQuery")?;
    Ok(())
}

pub(crate) fn send_voice_reply(client: &Client, cfg: &RuntimeConfig, chat_id: &str, text: &str) -> Result<()> {
    let _span = tracing::info_span!("tts").entered();
    let script = cfg.root_dir.join("scripts/tts.sh");
    if !script.is_file() {
        bail!("TTS script not found: {}", script.display());
    }
    if !command_exists("bash") {
        bail!("bash not found; cannot run TTS script");
    }

    let output_voice = cfg.tmp_dir.join(format!(
        "reply_{}.ogg",
        chrono::Utc::now().timestamp_millis()
    ));
    if let Some(parent) = output_voice.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    let mut cmd = Command::new("bash");
    cmd.arg(script)
        .arg(text)
        .arg(&output_voice)
        .current_dir(&cfg.root_dir)
        .env("INSTANCE_DIR", &cfg.instance_dir);
    if let Some(value) = cfg.tts_cmd_template.as_deref() {
        cmd.env("TTS_CMD_TEMPLATE", value);
    }
    if let Some(value) = cfg.voice_bitrate.as_deref() {
        cmd.env("VOICE_BITRATE", value);
    }
    if let Some(value) = cfg.tts_max_chars.as_deref() {
        cmd.env("TTS_MAX_CHARS", value);
    }

    let output = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .context("failed to execute TTS script")?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        let _ = fs::remove_file(&output_voice);
        bail!("TTS script failed: {stderr}");
    }

    let result =
        telegram_send_media_file(client, cfg, chat_id, "sendVoice", "voice", &output_voice);
    let _ = fs::remove_file(&output_voice);
    result
}

pub(crate) fn telegram_send_message(
    client: &Client,
    cfg: &RuntimeConfig,
    chat_id: &str,
    text: &str,
) -> Result<Option<String>> {
    let base = telegram_api_base(cfg)?;
    let params = telegram_text_form_params(cfg, chat_id, None, text, None);
    let url = format!("{base}/sendMessage");
    let value = match telegram_post_form(client, &url, &params, "sendMessage") {
        Ok(value) => value,
        Err(err) if should_retry_plain_text(cfg) && should_fallback_plain_for_error(&err) => {
            tracing::warn!("sendMessage markdown parse failed, retrying plain text: {err:#}");
            let retry = strip_parse_mode_param(&params);
            telegram_post_form(client, &url, &retry, "sendMessage")?
        }
        Err(err) => return Err(err),
    };
    let message_id = value
        .get("result")
        .and_then(|node| node.get("message_id"))
        .map(value_to_string)
        .map(|id| id.trim().to_string())
        .filter(|id| !id.is_empty());
    Ok(message_id)
}

pub(crate) fn telegram_text_form_params(
    cfg: &RuntimeConfig,
    chat_id: &str,
    message_id: Option<&str>,
    text: &str,
    reply_markup: Option<&str>,
) -> Vec<(String, String)> {
    let mut params = Vec::new();
    params.push(("chat_id".to_string(), chat_id.to_string()));
    if let Some(message_id) = message_id {
        params.push(("message_id".to_string(), message_id.to_string()));
    }
    params.push(("text".to_string(), text.to_string()));
    if let Some(reply_markup) = reply_markup {
        params.push(("reply_markup".to_string(), reply_markup.to_string()));
    }
    if let Some(parse_mode) = cfg.telegram_parse_mode.as_api_value() {
        params.push(("parse_mode".to_string(), parse_mode.to_string()));
    }
    params
}

pub(crate) fn strip_parse_mode_param(params: &[(String, String)]) -> Vec<(String, String)> {
    params
        .iter()
        .filter(|(key, _)| key != "parse_mode")
        .cloned()
        .collect()
}

pub(crate) fn should_retry_plain_text(cfg: &RuntimeConfig) -> bool {
    matches!(cfg.telegram_parse_mode, TelegramParseMode::MarkdownV2)
        && matches!(cfg.telegram_parse_fallback, TelegramParseFallback::Plain)
}

pub(crate) fn should_fallback_plain_for_error(err: &anyhow::Error) -> bool {
    let text = format!("{err:#}").to_ascii_lowercase();
    text.contains("can't parse entities")
        || text.contains("can't find end of")
        || text.contains("character '-' is reserved")
        || text.contains("character '.' is reserved")
        || text.contains("character '!' is reserved")
}

pub(crate) fn telegram_post_form(
    client: &Client,
    url: &str,
    params: &[(String, String)],
    action: &str,
) -> Result<Value> {
    const MAX_429_RETRY_ATTEMPTS: usize = 1;
    const MAX_429_SLEEP_SECS: u64 = 60;

    let mut attempts = 0usize;
    loop {
        let response = client
            .post(url)
            .form(params)
            .send()
            .with_context(|| format!("failed to call telegram {action}"))?;

        if response.status().as_u16() == 429 {
            let body = response
                .text()
                .with_context(|| format!("failed to read telegram {action} response body"))?;
            if attempts < MAX_429_RETRY_ATTEMPTS
                && let Some(retry_after) = telegram_retry_after_seconds(&body)
            {
                let sleep_secs = retry_after.clamp(1, MAX_429_SLEEP_SECS);
                attempts += 1;
                tracing::warn!("telegram {action} rate limited, retrying in {sleep_secs}s");
                thread::sleep(Duration::from_secs(sleep_secs));
                continue;
            }
            bail!("telegram {action} HTTP 429: {body}");
        }

        return parse_telegram_response(response, action);
    }
}

pub(crate) fn telegram_retry_after_seconds(body: &str) -> Option<u64> {
    let value: Value = serde_json::from_str(body).ok()?;
    value
        .get("parameters")
        .and_then(|node| node.get("retry_after"))
        .and_then(|node| {
            node.as_u64()
                .or_else(|| node.as_i64().and_then(|value| u64::try_from(value).ok()))
                .or_else(|| node.as_str().and_then(|value| value.parse::<u64>().ok()))
        })
}

pub(crate) fn telegram_send_media_file(
    client: &Client,
    cfg: &RuntimeConfig,
    chat_id: &str,
    method: &str,
    field: &str,
    path: &Path,
) -> Result<()> {
    if !path.is_file() {
        bail!(
            "{} marker path not found: {}",
            field.to_ascii_uppercase(),
            path.display()
        );
    }

    let base = telegram_api_base(cfg)?;
    let form = multipart::Form::new()
        .text("chat_id", chat_id.to_string())
        .file(field.to_string(), path)
        .with_context(|| format!("failed to prepare multipart upload for {}", path.display()))?;

    let response = client
        .post(format!("{base}/{method}"))
        .multipart(form)
        .send()
        .with_context(|| format!("failed to call telegram {method}"))?;
    parse_telegram_response(response, method)?;
    Ok(())
}

pub(crate) fn parse_telegram_response(response: reqwest::blocking::Response, action: &str) -> Result<Value> {
    let status = response.status();
    let body = response
        .text()
        .with_context(|| format!("failed to read telegram {action} response body"))?;
    if !status.is_success() {
        bail!("telegram {action} HTTP {}: {body}", status.as_u16());
    }

    let value: Value = serde_json::from_str(&body)
        .with_context(|| format!("telegram {action} returned invalid JSON: {body}"))?;
    let ok = value.get("ok").and_then(Value::as_bool).unwrap_or(false);
    if !ok {
        let description = value
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or("unknown telegram error");
        bail!("telegram {action} failed: {description}");
    }

    Ok(value)
}

