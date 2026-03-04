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
use pulldown_cmark::{CodeBlockKind, Event, Options, Parser, Tag, TagEnd};
use reqwest::blocking::{Client, multipart};
use serde_json::Value;

use crate::markers::parse_markers;
use crate::webhook::{value_to_string, webhook_public_endpoint};
use crate::{command_exists, resolve_instance_path};

const TELEGRAM_TEXT_CHAR_LIMIT: usize = 4096;

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
    let timeout_secs = cfg.telegram_api_timeout_secs.max(1);
    Client::builder()
        .timeout(Duration::from_secs(timeout_secs))
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
            if should_send_reply_as_document(&rendered_reply) {
                if let Err(err) =
                    send_markdown_reply_document(client, cfg, chat_id, reply, progress_message_id)
                {
                    tracing::warn!(
                        "failed to send long reply as markdown document, falling back to text: {err:#}"
                    );
                    send_or_edit_text(client, cfg, chat_id, &rendered_reply, progress_message_id)?;
                }
            } else {
                send_or_edit_text(client, cfg, chat_id, &rendered_reply, progress_message_id)?;
            }
        } else if let Some(message_id) = progress_message_id {
            let _ = telegram_remove_keyboard(client, cfg, chat_id, message_id);
        }
    } else if let Some(message_id) = progress_message_id {
        let _ = telegram_remove_keyboard(client, cfg, chat_id, message_id);
    }

    // Only attempt voice reply if TTS is configured (tts_cmd_template is set)
    if cfg.tts_cmd_template.is_some()
        && let Some(voice_reply) = markers.voice_reply.as_deref()
    {
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
                        // Phase labels are transient: each replaces the previous one
                        const PHASE_LABELS: &[&str] = &[
                            "Processing...",
                            "Reasoning...",
                            "Preparing tool call...",
                            "Compacting context...",
                            "Context compacted",
                        ];
                        let is_phase = PHASE_LABELS.contains(&status.as_str())
                            || status.starts_with("turn ")
                            || status.starts_with("Retrying ");
                        if is_phase {
                            statuses.retain(|s| {
                                !PHASE_LABELS.contains(&s.as_str())
                                    && !s.starts_with("turn ")
                                    && !s.starts_with("Retrying ")
                            });
                        }
                        // For completion markers (✓/✗), replace matching start (▶)
                        // and carry forward the detail from the start entry
                        let status = if status.starts_with("✓ ") || status.starts_with("✗ ") {
                            let marker = if status.starts_with("✓ ") {
                                "✓"
                            } else {
                                "✗"
                            };
                            let tool_prefix = status
                                .trim_start_matches("✓ ")
                                .trim_start_matches("✗ ")
                                .split(':')
                                .next()
                                .unwrap_or("");
                            let start_prefix = format!("▶ {tool_prefix}");
                            // If completion has no detail, inherit from start entry
                            let has_detail = status.contains(':');
                            let inherited = if !has_detail {
                                statuses
                                    .iter()
                                    .find(|s| s.starts_with(&start_prefix))
                                    .and_then(|s| s.split_once(':'))
                                    .map(|(_, detail)| format!("{marker} {tool_prefix}:{detail}"))
                            } else {
                                None
                            };
                            statuses.retain(|s| !s.starts_with(&start_prefix));
                            inherited.unwrap_or(status)
                        } else {
                            status
                        };
                        if let Some(existing) = statuses.iter().position(|item| item == &status) {
                            statuses.remove(existing);
                        }
                        statuses.push(status);
                        if statuses.len() > 8 {
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
    let chunks = split_text_chunks(text, TELEGRAM_TEXT_CHAR_LIMIT);
    if chunks.is_empty() {
        if let Some(message_id) = progress_message_id {
            let _ = telegram_remove_keyboard(client, cfg, chat_id, message_id);
        }
        return Ok(());
    }

    let mut chunks_iter = chunks.into_iter();
    let first_chunk = chunks_iter.next().expect("chunks verified non-empty above");

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

pub(crate) fn should_send_reply_as_document(text: &str) -> bool {
    text.chars().count() > TELEGRAM_TEXT_CHAR_LIMIT
}

fn send_markdown_reply_document(
    client: &Client,
    cfg: &RuntimeConfig,
    chat_id: &str,
    markdown_text: &str,
    progress_message_id: Option<&str>,
) -> Result<()> {
    let path = cfg.tmp_dir.join(format!(
        "telegram_reply_{}.md",
        chrono::Utc::now().timestamp_millis()
    ));
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    let mut content = markdown_text.to_string();
    if !content.ends_with('\n') {
        content.push('\n');
    }
    fs::write(&path, content).with_context(|| format!("failed to write {}", path.display()))?;

    if let Some(message_id) = progress_message_id {
        let notice = "Reply is long; sent as markdown document.";
        if let Err(err) =
            telegram_edit_message_text(client, cfg, chat_id, message_id, notice, false)
        {
            tracing::warn!("failed to edit progress message for long reply: {err:#}");
            let _ = telegram_remove_keyboard(client, cfg, chat_id, message_id);
        }
    }

    let send_result =
        telegram_send_media_file(client, cfg, chat_id, "sendDocument", "document", &path);
    let _ = fs::remove_file(&path);
    send_result
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

/// Simple MarkdownV2 escaper for backward compatibility.
/// Escapes all Telegram MarkdownV2 special characters.
pub(crate) fn render_markdown_v2_reply(text: &str) -> String {
    const SPECIAL: &[char] = &[
        '_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!',
    ];
    let mut out = String::with_capacity(text.len() * 2);
    for ch in text.chars() {
        if SPECIAL.contains(&ch) {
            out.push('\\');
        }
        out.push(ch);
    }
    out
}

pub(crate) fn render_html_reply(text: &str) -> String {
    let options = Options::empty();
    let parser = Parser::new_ext(text, options);
    let mut html = String::new();
    let mut list_index: Option<u64> = None;

    for event in parser {
        match event {
            Event::Start(tag) => match tag {
                Tag::Paragraph => {}
                Tag::Heading { .. } => html.push_str("<b>"),
                Tag::Strong => html.push_str("<b>"),
                Tag::Emphasis => html.push_str("<i>"),
                Tag::Strikethrough => html.push_str("<s>"),
                Tag::CodeBlock(kind) => match kind {
                    CodeBlockKind::Fenced(lang) if !lang.is_empty() => {
                        html.push_str(&format!(
                            "<pre><code class=\"language-{}\">",
                            html_escape(&lang)
                        ));
                    }
                    _ => html.push_str("<pre><code>"),
                },
                Tag::Link { dest_url, .. } => {
                    html.push_str(&format!("<a href=\"{}\">", html_escape(&dest_url)));
                }
                Tag::List(start) => {
                    list_index = start;
                }
                Tag::Item => {
                    if let Some(idx) = list_index.as_mut() {
                        html.push_str(&format!("{}. ", idx));
                        *idx += 1;
                    } else {
                        html.push_str("• ");
                    }
                }
                Tag::BlockQuote(_) => html.push_str("<blockquote>"),
                _ => {}
            },
            Event::End(tag_end) => match tag_end {
                TagEnd::Paragraph => html.push_str("\n\n"),
                TagEnd::Heading(_) => html.push_str("</b>\n\n"),
                TagEnd::Strong => html.push_str("</b>"),
                TagEnd::Emphasis => html.push_str("</i>"),
                TagEnd::Strikethrough => html.push_str("</s>"),
                TagEnd::CodeBlock => {
                    html.push_str("</code></pre>\n");
                }
                TagEnd::Link => html.push_str("</a>"),
                TagEnd::Item => html.push('\n'),
                TagEnd::List(_) => {
                    list_index = None;
                    html.push('\n');
                }
                TagEnd::BlockQuote(_) => html.push_str("</blockquote>\n"),
                _ => {}
            },
            Event::Text(text) => {
                html.push_str(&html_escape(&text));
            }
            Event::Html(raw) | Event::InlineHtml(raw) => {
                html.push_str(&html_escape(&raw));
            }
            Event::Code(code) => {
                html.push_str("<code>");
                html.push_str(&html_escape(&code));
                html.push_str("</code>");
            }
            Event::SoftBreak => html.push('\n'),
            Event::HardBreak => html.push('\n'),
            Event::Rule => html.push_str("---\n"),
            _ => {}
        }
    }

    html.trim_end().to_string()
}

fn html_escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

pub(crate) fn render_telegram_reply_text(cfg: &RuntimeConfig, text: &str) -> String {
    match cfg.telegram_parse_mode {
        TelegramParseMode::Html => render_html_reply(text),
        TelegramParseMode::MarkdownV2 => render_markdown_v2_reply(text),
        TelegramParseMode::Off => text.to_string(),
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

pub(crate) fn send_voice_reply(
    client: &Client,
    cfg: &RuntimeConfig,
    chat_id: &str,
    text: &str,
) -> Result<()> {
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
    matches!(
        cfg.telegram_parse_mode,
        TelegramParseMode::MarkdownV2 | TelegramParseMode::Html
    ) && matches!(cfg.telegram_parse_fallback, TelegramParseFallback::Plain)
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
    const TRANSIENT_RETRY_BASE_DELAY_MS: u64 = 500;

    let mut attempts = 0usize;
    loop {
        let response = match client.post(url).form(params).send() {
            Ok(resp) => resp,
            Err(err) => {
                // Network-level error (connection timeout, DNS failure, etc.)
                if attempts < TRANSIENT_RETRY_BASE_DELAY_MS.count_ones() as usize {
                    let delay_ms = TRANSIENT_RETRY_BASE_DELAY_MS * (1 << attempts);
                    attempts += 1;
                    tracing::warn!(
                        "telegram {action} network error, retrying in {delay_ms}ms: {err:#}"
                    );
                    thread::sleep(Duration::from_millis(delay_ms));
                    continue;
                }
                bail!("telegram {action} network error after {attempts} retries: {err:#}");
            }
        };

        let status = response.status().as_u16();

        // Rate limiting - honor retry-after header
        if status == 429 {
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

        // Server errors (5xx) - retry with exponential backoff
        if (500..600).contains(&status) {
            let body = response.text().unwrap_or_default();
            if attempts < 3 {
                let delay_ms = TRANSIENT_RETRY_BASE_DELAY_MS * (1 << attempts);
                attempts += 1;
                tracing::warn!(
                    "telegram {action} server error HTTP {status}, retrying in {delay_ms}ms"
                );
                thread::sleep(Duration::from_millis(delay_ms));
                continue;
            }
            bail!("telegram {action} HTTP {status} after {attempts} retries: {body}");
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

pub(crate) fn parse_telegram_response(
    response: reqwest::blocking::Response,
    action: &str,
) -> Result<Value> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn html_bold_and_italic() {
        let text = "This is **bold** and *italic* text.";
        let rendered = render_html_reply(text);
        assert!(rendered.contains("<b>bold</b>"));
        assert!(rendered.contains("<i>italic</i>"));
    }

    #[test]
    fn html_inline_code() {
        let text = "Run `echo hello` in the terminal.";
        let rendered = render_html_reply(text);
        assert!(rendered.contains("<code>echo hello</code>"));
    }

    #[test]
    fn html_code_block_with_language() {
        let text = "```rust\nfn main() {}\n```";
        let rendered = render_html_reply(text);
        assert!(rendered.contains("<pre><code class=\"language-rust\">"));
        assert!(rendered.contains("fn main() {}"));
        assert!(rendered.contains("</code></pre>"));
    }

    #[test]
    fn html_escapes_special_chars() {
        let text = "x < 10 & y > 5";
        let rendered = render_html_reply(text);
        assert!(rendered.contains("x &lt; 10 &amp; y &gt; 5"));
    }

    #[test]
    fn html_unclosed_bold_does_not_crash() {
        let text = "This is **unclosed bold";
        let rendered = render_html_reply(text);
        // pulldown-cmark handles unclosed tags gracefully
        assert!(!rendered.is_empty());
    }

    #[test]
    fn html_link() {
        let text = "Visit [example](https://example.com) now.";
        let rendered = render_html_reply(text);
        assert!(rendered.contains("<a href=\"https://example.com\">example</a>"));
    }

    #[test]
    fn html_unordered_list() {
        let text = "Items:\n- apple\n- banana";
        let rendered = render_html_reply(text);
        assert!(rendered.contains("• apple"));
        assert!(rendered.contains("• banana"));
    }

    #[test]
    fn html_ordered_list() {
        let text = "Steps:\n1. first\n2. second";
        let rendered = render_html_reply(text);
        assert!(rendered.contains("1. first"));
        assert!(rendered.contains("2. second"));
    }

    #[test]
    fn html_heading_renders_as_bold() {
        let text = "# Hello World";
        let rendered = render_html_reply(text);
        assert!(rendered.contains("<b>Hello World</b>"));
    }

    #[test]
    fn html_cjk_with_markdown() {
        let text = "使用 **gemini** 的 `cli` 工具";
        let rendered = render_html_reply(text);
        assert!(rendered.contains("<b>gemini</b>"));
        assert!(rendered.contains("<code>cli</code>"));
    }

    #[test]
    fn html_preserves_xml_tags() {
        let text =
            "Here is some <boltArtifact>content</boltArtifact> and <thinking>stuff</thinking>";
        let rendered = render_html_reply(text);
        assert!(rendered.contains("&lt;boltArtifact&gt;"));
        assert!(rendered.contains("&lt;/boltArtifact&gt;"));
        assert!(rendered.contains("&lt;thinking&gt;"));
        assert!(rendered.contains("&lt;/thinking&gt;"));
    }
}
