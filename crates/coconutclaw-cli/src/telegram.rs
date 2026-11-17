//! Telegram Bot API interaction layer.
//!
//! Contains all functions that directly call the Telegram HTTP API:
//! client construction, message send/edit, file upload/download,
//! progress updates, markdown rendering, and rate-limit retry.

use std::fs;
use std::path::Path;
#[cfg(test)]
use std::path::PathBuf;
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

use crate::util::value_to_string;
#[cfg(test)]
use crate::{markers::parse_markers, resolve_instance_path};

pub(crate) const TELEGRAM_TEXT_CHAR_LIMIT: usize = 4096;

pub(crate) enum RichSendOutcome {
    Sent,
    Rejected,
    RateLimited(u64),
    Unknown,
}

#[derive(Debug)]
pub(crate) struct TelegramApiFailure {
    pub(crate) code: u64,
    pub(crate) retry_after: Option<u64>,
    description: String,
}
impl std::fmt::Display for TelegramApiFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "telegram HTTP {}: {}", self.code, self.description)
    }
}
impl std::error::Error for TelegramApiFailure {}

/// A single attempt: transport errors and 5xx are not evidence of non-delivery.
/// No request-level retry may run underneath the durable rich operation.
pub(crate) fn telegram_send_rich(
    client: &Client,
    cfg: &RuntimeConfig,
    route: &str,
    markdown: &str,
) -> Result<RichSendOutcome> {
    let base = telegram_api_base(cfg)?;
    let mut params = telegram_route_params(route)?;
    params.push((
        "rich_message".into(),
        serde_json::json!({"markdown":markdown}).to_string(),
    ));
    let request = client
        .post(format!("{base}/sendRichMessage"))
        .form(&params)
        .build()
        .map_err(reqwest::Error::without_url)?;
    let response = match execute_request(client, request) {
        Ok(response) => response,
        Err(_) => return Ok(RichSendOutcome::Unknown),
    };
    let status = response.0.as_u16();
    if response.0.is_server_error() {
        return Ok(RichSendOutcome::Unknown);
    }
    let value: Value = serde_json::from_slice(&response.1).unwrap_or(Value::Null);
    let code = value["error_code"].as_u64().unwrap_or(status as u64);
    if code == 429 {
        return Ok(RichSendOutcome::RateLimited(
            value["parameters"]["retry_after"]
                .as_u64()
                .unwrap_or(1)
                .max(1),
        ));
    }
    if response.0.is_success()
        && value["ok"] == true
        && value["result"]["message_id"].as_i64().is_some()
    {
        return Ok(RichSendOutcome::Sent);
    }
    let description = value["description"]
        .as_str()
        .unwrap_or("")
        .to_ascii_lowercase();
    // Only explicit API-level format/unsupported rejection permits a different
    // representation. Authentication, routing and unknown errors are not format errors.
    if value["ok"] == false
        && matches!(code, 400 | 404)
        && [
            "parse",
            "format",
            "rich",
            "too long",
            "too many",
            "method not found",
        ]
        .iter()
        .any(|needle| description.contains(needle))
    {
        return Ok(RichSendOutcome::Rejected);
    }
    Ok(RichSendOutcome::Unknown)
}

/// Only a verified runtime identity can authorize an addressed command.
pub(crate) fn command_token<'a>(token: &'a str, username: Option<&str>) -> Option<&'a str> {
    match token.split_once('@') {
        Some((command, target)) => username
            .filter(|name| !name.is_empty() && name.eq_ignore_ascii_case(target))
            .map(|_| command),
        None => Some(token),
    }
}

pub(crate) fn discover_bot_username(client: &Client, cfg: &mut RuntimeConfig) -> Result<String> {
    let base = telegram_api_base(cfg)?;
    let response = telegram_request(client, cfg, "getMe", || {
        Ok(client
            .post(format!("{base}/getMe"))
            .build()
            .map_err(reqwest::Error::without_url)?)
    })?;
    let value = parse_telegram_response(response, "getMe")?;
    let bot = &value["result"];
    cfg.telegram_topics_enabled = Some(bot["has_topics_enabled"].as_bool().unwrap_or(false));
    tracing::info!(
        has_topics_enabled = bot["has_topics_enabled"].as_bool().unwrap_or(false),
        allows_users_to_create_topics = bot["allows_users_to_create_topics"]
            .as_bool()
            .unwrap_or(false),
        "private chat topic capabilities; enable/disable topic mode manually in BotFather; root and topic sessions always remain separate"
    );
    value
        .get("result")
        .filter(|bot| bot.get("is_bot").and_then(Value::as_bool) == Some(true))
        .and_then(|bot| bot.get("username"))
        .and_then(Value::as_str)
        .filter(|name| {
            !name.is_empty()
                && name
                    .bytes()
                    .all(|ch| ch.is_ascii_alphanumeric() || ch == b'_')
        })
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow::anyhow!("telegram getMe returned no valid bot username"))
}

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
        .or_else(|| {
            cfg.telegram_chat_ids.iter().find_map(|chat_id| {
                let trimmed = chat_id.trim();
                (!trimmed.is_empty() && trimmed != "replace_me").then_some(trimmed)
            })
        })
}

pub(crate) fn telegram_api_base(cfg: &RuntimeConfig) -> Result<String> {
    let token = valid_telegram_token(cfg).ok_or_else(|| {
        anyhow::anyhow!("TELEGRAM_BOT_TOKEN is missing; set it in instance config.toml")
    })?;
    if let Some(base) = &cfg.telegram_api_base {
        Ok(format!("{}/bot{}", base.trim_end_matches('/'), token))
    } else {
        Ok(format!("https://api.telegram.org/bot{token}"))
    }
}

pub(crate) fn telegram_file_base(cfg: &RuntimeConfig) -> Result<String> {
    let token = valid_telegram_token(cfg).ok_or_else(|| {
        anyhow::anyhow!("TELEGRAM_BOT_TOKEN is missing; set it in instance config.toml")
    })?;
    if let Some(base) = &cfg.telegram_file_base {
        Ok(format!("{}/bot{}", base.trim_end_matches('/'), token))
    } else {
        Ok(format!("https://api.telegram.org/file/bot{token}"))
    }
}

pub(crate) fn build_telegram_client(cfg: &RuntimeConfig) -> Result<Client> {
    let _ = telegram_api_base(cfg)?;
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
    let response = telegram_request(client, cfg, "getFile", || {
        Ok(client
            .post(format!("{base}/getFile"))
            .form(&[("file_id", file_id)])
            .build()
            .map_err(reqwest::Error::without_url)?)
    })?;
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

    let response = telegram_request(client, cfg, "file download", || {
        Ok(client
            .get(format!("{file_base}/{file_path}"))
            .build()
            .map_err(reqwest::Error::without_url)?)
    })?;
    if !response.0.is_success() {
        bail!("telegram file download HTTP {}", response.0.as_u16());
    }
    let bytes = response.1;
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

    let response = telegram_request(client, cfg, "getUpdates", || {
        Ok(client
            .get(&url)
            .query(&query)
            .build()
            .map_err(reqwest::Error::without_url)?)
    })?;
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
        {"command": "start", "description": "Open Home"},
        {"command": "help", "description": "Operations help"},
        {"command": "fresh", "description": "Start a fresh conversation"},
        {"command": "cancel", "description": "Cancel the current task"},
        {"command": "tasks", "description": "List active tasks"},
        {"command": "memory", "description": "Remember, search, get, promote, correct or undo chat memory"},
        {"command": "preferences", "description": "List, set or unset your conversation instructions/preferences"},
        {"command": "notifications", "description": "Inspect scoped task notification decisions and delivery"},
        {"command": "approve", "description": "Approve a request by id"},
        {"command": "reject", "description": "Reject a request by id"},
        {"command": "schedules", "description": "Scheduled tasks and next runs"},
        {"command": "reconcile", "description": "Narrow acknowledgment recovery, not retry"},
    ]);
    let params = vec![("commands".to_string(), commands.to_string())];
    telegram_post_form(
        client,
        cfg,
        &format!("{base}/setMyCommands"),
        &params,
        "setMyCommands",
    )?;
    Ok(())
}

/// One durable outbox attempt; no hidden retries or format fallbacks.
pub(crate) fn telegram_send_ui(
    client: &Client,
    cfg: &RuntimeConfig,
    route: &str,
    message: &crate::telegram_ui::Message,
) -> Result<()> {
    let base = telegram_api_base(cfg)?;
    let mut params = telegram_route_params(route)?;
    params.push(("text".into(), message.text.clone()));
    params.push(("reply_markup".into(), message.keyboard.to_string()));
    let method = if let Some(id) = &message.edit {
        params.push(("message_id".into(), id.clone()));
        "editMessageText"
    } else {
        "sendMessage"
    };
    let mut once = cfg.clone();
    once.telegram_api_retry_attempts = 0;
    match telegram_post_form(client, &once, &format!("{base}/{method}"), &params, method) {
        Ok(_) => Ok(()),
        Err(e) if message.edit.is_some() && telegram_error_is_not_modified(&e) => Ok(()),
        Err(e) => Err(e),
    }
}

/// Ephemeral spinner acknowledgment. Never retries an expired callback.
pub(crate) fn telegram_answer_ui_callback(
    client: &Client,
    cfg: &RuntimeConfig,
    id: &str,
) -> Result<()> {
    let base = telegram_api_base(cfg)?;
    let mut once = cfg.clone();
    once.telegram_api_retry_attempts = 0;
    telegram_post_form(
        client,
        &once,
        &format!("{base}/answerCallbackQuery"),
        &[("callback_query_id".into(), id.into())],
        "answerCallbackQuery",
    )
    .map(|_| ())
}

// Legacy wire-format/media compatibility test helper only. Runtime input and
// result delivery now exclusively use the durable outbox owner.
#[cfg(test)]
pub(crate) fn dispatch_telegram_output(
    client: &Client,
    cfg: &RuntimeConfig,
    chat_id_override: Option<&str>,
    output: &str,
    progress_message_id: Option<&str>,
) -> Result<()> {
    let _span = tracing::info_span!("dispatch_telegram").entered();
    let Some(chat_id) = chat_id_override
        .or(valid_telegram_chat_id(cfg))
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        tracing::warn!("cannot dispatch telegram output: no chat_id available");
        return Ok(());
    };

    let effects = parse_markers(output).to_effects();
    let mut has_text_reply = false;

    for effect in &effects {
        match effect {
            crate::markers::Effect::TelegramReply(reply) => {
                has_text_reply = true;
                let reply = reply.trim();
                if !reply.is_empty() {
                    let rendered_reply = render_telegram_reply_text(cfg, reply);
                    if should_send_reply_as_document(&rendered_reply) {
                        if let Err(err) = send_markdown_reply_document(
                            client,
                            cfg,
                            chat_id,
                            reply,
                            progress_message_id,
                        ) {
                            tracing::warn!(
                                "failed to send long reply as markdown document, falling back to text: {err:#}"
                            );
                            send_or_edit_text(
                                client,
                                cfg,
                                chat_id,
                                &rendered_reply,
                                progress_message_id,
                            )?;
                        }
                    } else {
                        send_or_edit_text(
                            client,
                            cfg,
                            chat_id,
                            &rendered_reply,
                            progress_message_id,
                        )?;
                    }
                }
            }
            crate::markers::Effect::SendPhoto(item) => {
                let path = resolve_instance_path(&cfg.instance_dir, PathBuf::from(item));
                if let Err(err) =
                    telegram_send_media_file(client, cfg, chat_id, "sendPhoto", "photo", &path)
                {
                    tracing::warn!("failed to send photo {}: {err:#}", path.display());
                }
            }
            crate::markers::Effect::SendDocument(item) => {
                let path = resolve_instance_path(&cfg.instance_dir, PathBuf::from(item));
                if let Err(err) = telegram_send_media_file(
                    client,
                    cfg,
                    chat_id,
                    "sendDocument",
                    "document",
                    &path,
                ) {
                    tracing::warn!("failed to send document {}: {err:#}", path.display());
                }
            }
            crate::markers::Effect::SendVideo(item) => {
                let path = resolve_instance_path(&cfg.instance_dir, PathBuf::from(item));
                if let Err(err) =
                    telegram_send_media_file(client, cfg, chat_id, "sendVideo", "video", &path)
                {
                    tracing::warn!("failed to send video {}: {err:#}", path.display());
                }
            }
            _ => {}
        }
    }

    // If no text reply but we had a progress message, clean it up
    if !has_text_reply {
        if let Some(message_id) = progress_message_id {
            let _ = telegram_delete_message(client, cfg, chat_id, message_id)
                .or_else(|_| telegram_remove_keyboard(client, cfg, chat_id, message_id));
        }
    } else {
        // If reply was empty but we had a progress message, clean it up
        let has_empty_reply = effects
            .iter()
            .any(|e| matches!(e, crate::markers::Effect::TelegramReply(r) if r.trim().is_empty()));
        if has_empty_reply && let Some(message_id) = progress_message_id {
            let _ = telegram_delete_message(client, cfg, chat_id, message_id)
                .or_else(|_| telegram_remove_keyboard(client, cfg, chat_id, message_id));
        }
    }

    Ok(())
}

pub(crate) fn send_progress_message(cfg: &RuntimeConfig, chat_id: &str) -> Result<Option<String>> {
    let client = build_telegram_client(cfg)?;
    let base = telegram_api_base(cfg)?;
    let reply_markup = progress_reply_markup();
    let mut params = telegram_route_params(chat_id)?;
    params.push(("text".into(), "Thinking...".into()));
    params.push(("reply_markup".into(), reply_markup.into()));
    let response = telegram_request(&client, cfg, "progress message", || {
        Ok(client
            .post(format!("{base}/sendMessage"))
            .form(&params)
            .build()
            .map_err(reqwest::Error::without_url)?)
    })?;
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
    mut cfg: RuntimeConfig,
    chat_id: String,
    message_id: String,
    progress_rx: mpsc::Receiver<String>,
    stop_flag: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    // Presentation must never hold provider completion/shutdown hostage.
    cfg.telegram_api_timeout_secs = cfg.telegram_api_timeout_secs.min(1);
    cfg.telegram_api_retry_attempts = 0;
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
            if stop_flag.load(Ordering::SeqCst) {
                break;
            }
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
                if let Err(err) = telegram_edit_message_text(
                    &client,
                    &cfg,
                    &chat_id,
                    &message_id,
                    &rendered,
                    true,
                ) {
                    tracing::warn!(
                        "failed to update telegram progress message chat_id={} message_id={}: {err:#}",
                        chat_id,
                        message_id
                    );
                }
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
    let chunks = split_text_chunks(text, TELEGRAM_TEXT_CHAR_LIMIT, cfg.telegram_parse_mode);
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
            if !err
                .downcast_ref::<TelegramApiFailure>()
                .is_some_and(|error| {
                    error.code == 400
                        && (error.description.contains("message to edit not found")
                            || error.description.contains("message can't be edited"))
                })
            {
                return Err(err);
            }
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

#[cfg(test)]
pub(crate) fn should_send_reply_as_document(text: &str) -> bool {
    text.chars().count() > TELEGRAM_TEXT_CHAR_LIMIT
}

pub(crate) fn send_markdown_reply_document(
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

pub(crate) fn split_text_chunks(
    text: &str,
    max_chars: usize,
    parse_mode: TelegramParseMode,
) -> Vec<String> {
    if max_chars == 0 {
        return vec![text.to_string()];
    }
    if text.is_empty() {
        return Vec::new();
    }
    if text.chars().count() <= max_chars {
        return vec![text.to_string()];
    }

    match parse_mode {
        TelegramParseMode::Html | TelegramParseMode::MarkdownV2 => {
            split_format_aware(text, max_chars, parse_mode)
        }
        TelegramParseMode::Off => split_plain(text, max_chars),
    }
}

// ── Format-aware splitter internals ────────────────────────────────

/// Reserve room for the chunk indicator at the tail of each chunk.
const INDICATOR_RESERVE: usize = 12;

/// Build the opening fence string for a code block with an optional lang tag.
fn code_fence_open_with_lang(parse_mode: TelegramParseMode, lang: &str) -> String {
    match parse_mode {
        TelegramParseMode::MarkdownV2 => {
            if lang.is_empty() {
                "```\n".to_string()
            } else {
                format!("```{lang}\n")
            }
        }
        TelegramParseMode::Html => {
            if lang.is_empty() {
                "<pre><code>".to_string()
            } else {
                format!("<pre><code class=\"language-{lang}\">")
            }
        }
        TelegramParseMode::Off => String::new(),
    }
}

fn fence_close_str(parse_mode: TelegramParseMode) -> &'static str {
    match parse_mode {
        TelegramParseMode::MarkdownV2 => "```",
        TelegramParseMode::Html => "</code></pre>",
        TelegramParseMode::Off => "",
    }
}

fn format_indicator(parse_mode: TelegramParseMode, current: usize, total: usize) -> String {
    match parse_mode {
        TelegramParseMode::MarkdownV2 => format!("\\({current}/{total}\\)"),
        _ => format!("({current}/{total})"),
    }
}

fn split_format_aware(text: &str, max_chars: usize, parse_mode: TelegramParseMode) -> Vec<String> {
    let fence_close = fence_close_str(parse_mode);
    let is_md = matches!(parse_mode, TelegramParseMode::MarkdownV2);

    let mut chunks: Vec<String> = Vec::new();
    let mut remaining: &str = text;
    let mut carry_lang: Option<String> = None;

    while !remaining.is_empty() {
        let prefix = match &carry_lang {
            Some(lang) => code_fence_open_with_lang(parse_mode, lang),
            None => String::new(),
        };

        let prefix_len = prefix.chars().count();
        let remaining_len = remaining.chars().count();

        if prefix_len + remaining_len <= max_chars.saturating_sub(INDICATOR_RESERVE) {
            chunks.push(prefix + remaining);
            break;
        }

        let overhead = INDICATOR_RESERVE
            .saturating_add(prefix_len)
            .saturating_add(fence_close.chars().count());
        let headroom = max_chars.saturating_sub(overhead).max(1);

        // Collect exactly `headroom` chars as the candidate region.
        let region: String = remaining.chars().take(headroom).collect();
        let region_char_len = region.chars().count();

        // find_split_point returns a *char index* within `region`.
        let split_at = find_split_point_chars(&region, region_char_len, is_md);
        let split_at = split_at.max(1); // guarantee forward progress

        let chunk_body: String = remaining.chars().take(split_at).collect();
        remaining = &remaining[chunk_body.len()..];
        let trimmed = remaining.trim_start_matches('\n');
        if trimmed.len() < remaining.len() {
            remaining = trimmed;
        }

        let full_body = prefix + &chunk_body;

        let (in_code, lang) = scan_code_block_state(&full_body, parse_mode);

        let full_chunk = if in_code {
            carry_lang = Some(lang);
            full_body + fence_close
        } else {
            carry_lang = None;
            full_body
        };

        chunks.push(full_chunk);
    }

    if chunks.len() > 1 {
        let total = chunks.len();
        for (i, chunk) in chunks.iter_mut().enumerate() {
            chunk.push_str(&format_indicator(parse_mode, i + 1, total));
        }
    }
    chunks
}

/// Find a safe split point within `region`, returning a *char index*.
/// Prefers newlines, then spaces, then hard-cuts at `region_char_len`.
/// For MarkdownV2, avoids splitting inside inline backtick spans.
fn find_split_point_chars(region: &str, region_char_len: usize, is_md: bool) -> usize {
    // Walk chars to find the last newline and last space (as char indices).
    let mut last_nl: Option<usize> = None;
    let mut last_sp: Option<usize> = None;
    for (i, ch) in region.chars().enumerate() {
        if ch == '\n' {
            last_nl = Some(i);
        } else if ch == ' ' {
            last_sp = Some(i);
        }
    }

    let mut split_at = if let Some(nl) = last_nl {
        if nl >= region_char_len / 2 {
            nl
        } else {
            last_sp.unwrap_or(region_char_len)
        }
    } else {
        last_sp.unwrap_or(region_char_len)
    };

    if split_at < 1 {
        split_at = region_char_len;
    }

    // Protect inline code spans (MarkdownV2 only).
    // Count only backticks that are NOT part of code fence lines (``` …).
    if is_md {
        let candidate: String = region.chars().take(split_at).collect();
        let inline_backtick_count: usize = candidate
            .split('\n')
            .filter(|line| !line.trim().starts_with("```"))
            .map(|line| line.matches('`').count())
            .sum();
        if inline_backtick_count % 2 == 1 {
            // Find the char index of the last non-fence backtick.
            if let Some(pos) = candidate.rfind('`') {
                // Verify this backtick isn't on a fence line.
                let before = &candidate[..pos];
                let on_fence = before
                    .rfind('\n')
                    .map(|nl| candidate[nl + 1..].trim_start().starts_with("```"))
                    .unwrap_or(candidate.trim_start().starts_with("```"));
                if !on_fence {
                    let char_pos = before.chars().count();
                    if char_pos > region_char_len / 4 {
                        split_at = char_pos;
                    }
                }
            }
        }
    }

    split_at
}

/// Scan text to determine if we're inside an open code block at the end.
/// Returns (in_code_block, language_tag).
fn scan_code_block_state(text: &str, parse_mode: TelegramParseMode) -> (bool, String) {
    let mut in_code = false;
    let mut lang = String::new();

    match parse_mode {
        TelegramParseMode::MarkdownV2 => {
            for line in text.split('\n') {
                let trimmed = line.trim();
                if trimmed.starts_with("```") {
                    if in_code {
                        in_code = false;
                        lang = String::new();
                    } else {
                        in_code = true;
                        let tag = trimmed.trim_start_matches('`').trim();
                        lang = tag.split_whitespace().next().unwrap_or("").to_string();
                    }
                }
            }
        }
        TelegramParseMode::Html => {
            // Use rfind to determine the final state — whichever appears
            // last wins (handles close+open on the same line correctly).
            let last_open = text.rfind("<pre><code");
            let last_close = text.rfind("</code></pre>");
            match (last_open, last_close) {
                (Some(open_pos), Some(close_pos)) if open_pos > close_pos => {
                    in_code = true;
                    let after = &text[open_pos..];
                    if let Some(cls_start) = after.find("class=\"language-") {
                        let rest = &after[cls_start + 16..];
                        if let Some(cls_end) = rest.find('"') {
                            lang = rest[..cls_end].to_string();
                        }
                    }
                }
                (Some(open_pos), None) => {
                    in_code = true;
                    let after = &text[open_pos..];
                    if let Some(cls_start) = after.find("class=\"language-") {
                        let rest = &after[cls_start + 16..];
                        if let Some(cls_end) = rest.find('"') {
                            lang = rest[..cls_end].to_string();
                        }
                    }
                }
                _ => {
                    in_code = false;
                }
            }
        }
        TelegramParseMode::Off => {}
    }

    (in_code, lang)
}

/// Original plain-text splitter (for `TelegramParseMode::Off`).
/// Reserves room for indicators so chunks never exceed `max_chars`.
fn split_plain(text: &str, max_chars: usize) -> Vec<String> {
    // We don't know the total chunk count upfront, so we reserve a
    // generous fixed amount for indicators.  After splitting we trim
    // if the actual indicator is shorter.
    let effective_max = max_chars.saturating_sub(INDICATOR_RESERVE).max(1);

    let mut chunks = Vec::new();
    let mut current = String::new();

    for line in text.split('\n') {
        let line_len = line.chars().count();
        if line_len > effective_max {
            if !current.is_empty() {
                chunks.push(current);
                current = String::new();
            }
            let chars: Vec<char> = line.chars().collect();
            let mut start = 0usize;
            while start < chars.len() {
                let end = (start + effective_max).min(chars.len());
                chunks.push(chars[start..end].iter().collect());
                start = end;
            }
            continue;
        }

        let sep = if current.is_empty() { 0 } else { 1 };
        if current.chars().count() + sep + line_len > effective_max {
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

    if chunks.len() > 1 {
        let total = chunks.len();
        for (i, chunk) in chunks.iter_mut().enumerate() {
            chunk.push_str(&format_indicator(TelegramParseMode::Off, i + 1, total));
        }
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
    match telegram_post_form(client, cfg, &url, &params, "editMessageText") {
        Ok(_) => Ok(()),
        Err(err) if telegram_error_is_not_modified(&err) => Ok(()),
        Err(err) if should_retry_plain_text(cfg) && should_fallback_plain_for_error(&err) => {
            tracing::warn!("editMessageText markdown parse failed, retrying plain text: {err:#}");
            let retry = strip_parse_mode_param(&params);
            telegram_post_form(client, cfg, &url, &retry, "editMessageText").map(|_| ())
        }
        Err(err) => Err(err),
    }
}

pub(crate) fn telegram_delete_message(
    client: &Client,
    cfg: &RuntimeConfig,
    chat_id: &str,
    message_id: &str,
) -> Result<()> {
    let base = telegram_api_base(cfg)?;
    let session = crate::session::SessionKey::telegram(chat_id);
    let params = [
        ("chat_id", session.root_id),
        ("message_id", message_id.to_string()),
    ];
    let response = telegram_request(client, cfg, "deleteMessage", || {
        Ok(client
            .post(format!("{base}/deleteMessage"))
            .form(&params)
            .build()
            .map_err(reqwest::Error::without_url)?)
    })?;
    parse_telegram_response(response, "deleteMessage")?;
    Ok(())
}

pub(crate) fn telegram_remove_keyboard(
    client: &Client,
    cfg: &RuntimeConfig,
    chat_id: &str,
    message_id: &str,
) -> Result<()> {
    let base = telegram_api_base(cfg)?;
    let reply_markup = r#"{"inline_keyboard":[]}"#;
    let session = crate::session::SessionKey::telegram(chat_id);
    let params = [
        ("chat_id", session.root_id),
        ("message_id", message_id.to_string()),
        ("reply_markup", reply_markup.to_string()),
    ];
    let response = telegram_request(client, cfg, "editMessageReplyMarkup", || {
        Ok(client
            .post(format!("{base}/editMessageReplyMarkup"))
            .form(&params)
            .build()
            .map_err(reqwest::Error::without_url)?)
    })?;
    parse_telegram_response(response, "editMessageReplyMarkup")?;
    Ok(())
}

fn telegram_error_is_not_modified(err: &anyhow::Error) -> bool {
    err.to_string()
        .to_ascii_lowercase()
        .contains("message is not modified")
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
    let value = match telegram_post_form(client, cfg, &url, &params, "sendMessage") {
        Ok(value) => value,
        Err(err) if should_retry_plain_text(cfg) && should_fallback_plain_for_error(&err) => {
            tracing::warn!("sendMessage markdown parse failed, retrying plain text: {err:#}");
            let retry = strip_parse_mode_param(&params);
            telegram_post_form(client, cfg, &url, &retry, "sendMessage")?
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
    // Routing identifiers are validated at ingress; keep the historical helper
    // signature for regular formatting callers.
    params.extend(telegram_route_params(chat_id).expect("validated Telegram route"));
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
    cfg: &RuntimeConfig,
    url: &str,
    params: &[(String, String)],
    action: &str,
) -> Result<Value> {
    let mut params = params.to_vec();
    if let Some(index) = params.iter().position(|(key, _)| key == "chat_id") {
        let route = params.remove(index).1;
        // Text helpers may already have expanded the topic.
        let has_thread = params.iter().any(|(key, _)| key == "message_thread_id");
        for (key, value) in telegram_route_params(&route)? {
            if key != "message_thread_id" || !has_thread {
                params.push((key, value));
            }
        }
    }
    let response = telegram_request(client, cfg, action, || {
        client
            .post(url)
            .form(&params)
            .build()
            .map_err(|error| error.without_url().into())
    })?;
    parse_telegram_response(response, action)
}

fn telegram_request(
    client: &Client,
    cfg: &RuntimeConfig,
    action: &str,
    mut build: impl FnMut() -> Result<reqwest::blocking::Request>,
) -> Result<(reqwest::StatusCode, Vec<u8>)> {
    for retries in 0..=cfg.telegram_api_retry_attempts {
        if runtime_stopping() {
            bail!("runtime stopping; Telegram request interrupted");
        }
        let delay = match execute_request(client, build()?) {
            Ok(response) => {
                let status = response.0.as_u16();
                if status != 429 && !(500..600).contains(&status) {
                    return Ok(response);
                }
                let body = String::from_utf8_lossy(&response.1);
                if retries == cfg.telegram_api_retry_attempts {
                    return Ok(response);
                }
                if status == 429 {
                    Duration::from_secs(
                        telegram_retry_after_seconds(&body)
                            .unwrap_or(1)
                            .clamp(1, 60),
                    )
                } else {
                    Duration::from_millis((500u64 << retries.min(3)).min(4000))
                }
            }
            Err(error) => {
                if retries == cfg.telegram_api_retry_attempts
                    || error.downcast_ref::<reqwest::Error>().is_none()
                {
                    return Err(error).with_context(|| format!("telegram {action} request failed"));
                }
                Duration::from_millis((500u64 << retries.min(3)).min(4000))
            }
        };
        tracing::warn!(
            "telegram {action} retry {}/{}",
            retries + 1,
            cfg.telegram_api_retry_attempts
        );
        let until = Instant::now() + delay;
        while Instant::now() < until {
            if runtime_stopping() {
                bail!("runtime stopping; Telegram backoff interrupted");
            }
            thread::sleep(
                Duration::from_millis(25).min(until.saturating_duration_since(Instant::now())),
            );
        }
    }
    unreachable!("initial attempt is always included")
}

static RUNTIME_STOP: std::sync::Mutex<std::sync::Weak<AtomicBool>> =
    std::sync::Mutex::new(std::sync::Weak::new());

pub(crate) fn set_runtime_shutdown(flag: &Arc<AtomicBool>) {
    *RUNTIME_STOP.lock().expect("runtime shutdown") = Arc::downgrade(flag);
}

fn runtime_stopping() -> bool {
    RUNTIME_STOP
        .lock()
        .expect("runtime shutdown")
        .upgrade()
        .is_some_and(|flag| flag.load(Ordering::SeqCst))
}

fn execute_request(
    client: &Client,
    request: reqwest::blocking::Request,
) -> Result<(reqwest::StatusCode, Vec<u8>)> {
    let execute = |client: &Client, request| -> Result<_> {
        let response = client
            .execute(request)
            .map_err(reqwest::Error::without_url)?;
        let status = response.status();
        let bytes = response
            .bytes()
            .map_err(reqwest::Error::without_url)?
            .to_vec();
        Ok((status, bytes))
    };
    if RUNTIME_STOP
        .lock()
        .expect("runtime shutdown")
        .upgrade()
        .is_none()
    {
        return execute(client, request);
    }
    // The interruptible worker owns only HTTP and read-only upload handles.
    // SQLite/projection owners remain joined on shutdown, even if a peer stalls
    // either headers or body. The foreground process owns the request lifetime.
    let client = client.clone();
    let (tx, rx) = mpsc::sync_channel(1);
    let worker = thread::spawn(move || {
        let _ = tx.send(execute(&client, request));
    });
    loop {
        if runtime_stopping() {
            bail!("runtime stopping; Telegram request interrupted");
        }
        match rx.recv_timeout(Duration::from_millis(25)) {
            Ok(result) => {
                let _ = worker.join();
                return result;
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(_) => bail!("Telegram HTTP worker stopped unexpectedly"),
        }
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
    let response = telegram_request(client, cfg, method, || {
        let mut form = multipart::Form::new();
        for (key, value) in telegram_route_params(chat_id)? {
            form = form.text(key, value);
        }
        let form = form
            .file(field.to_string(), path)
            .context("failed to prepare multipart upload")?;
        Ok(client
            .post(format!("{base}/{method}"))
            .multipart(form)
            .build()
            .map_err(reqwest::Error::without_url)?)
    })?;
    parse_telegram_response(response, method)?;
    Ok(())
}

/// Internal session IDs never cross the Telegram boundary as chat_id.
/// All send paths, including multipart documents, expand the same route.
pub(crate) fn telegram_route_params(route: &str) -> Result<Vec<(String, String)>> {
    let session = crate::session::SessionKey::telegram(route);
    let chat_id = session
        .root_id
        .parse::<i64>()
        .ok()
        .filter(|id| *id > 0 && *id < (1i64 << 52))
        .ok_or_else(|| anyhow::anyhow!("invalid Telegram private chat route"))?;
    let mut params = vec![("chat_id".into(), chat_id.to_string())];
    if let Some(thread) = session.thread_id {
        let id = thread
            .parse::<i64>()
            .ok()
            .filter(|id| *id > 0 && *id <= i32::MAX as i64)
            .ok_or_else(|| anyhow::anyhow!("invalid Telegram topic route"))?;
        params.push(("message_thread_id".into(), id.to_string()));
    }
    Ok(params)
}

pub(crate) fn parse_telegram_response(
    response: (reqwest::StatusCode, Vec<u8>),
    action: &str,
) -> Result<Value> {
    let (status, bytes) = response;
    let body = String::from_utf8(bytes)
        .with_context(|| format!("invalid UTF-8 in telegram {action} response"))?;
    let value: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
    if !status.is_success() || value["ok"] == false {
        return Err(TelegramApiFailure {
            code: if status.is_server_error() {
                status.as_u16() as u64
            } else {
                value["error_code"]
                    .as_u64()
                    .unwrap_or(status.as_u16() as u64)
            },
            retry_after: value["parameters"]["retry_after"].as_u64(),
            description: value["description"]
                .as_str()
                .unwrap_or("unknown Telegram error")
                .to_string(),
        }
        .into());
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
#[path = "telegram_tests.rs"]
mod tests;
