//! Slack API interaction layer.
//!
//! Mirrors telegram.rs: client construction, message send/edit, file upload/download,
//! Block Kit rendering, progress updates, and Socket Mode receiving.

use std::fs;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use coconutclaw_config::{RuntimeConfig, SlackFormatFallback, SlackFormatMode};
use pulldown_cmark::{Event, Options, Parser, Tag, TagEnd};
use reqwest::blocking::Client;
use serde_json::{Value, json};

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

use crate::InputType;
use crate::TurnInput;
use crate::markers::parse_markers;
use crate::resolve_instance_path;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub(crate) struct SlackWebhookTurn {
    pub event_id: Option<String>,  // for dedup (stored in update_id column)
    pub channel_id: String,        // Slack channel ID (C01234567)
    pub thread_ts: Option<String>, // for thread replies
    pub source_user_id: Option<String>,
    pub input: TurnInput,
    pub media: Option<SlackMedia>,
}

#[derive(Debug, Clone)]
pub(crate) enum SlackMedia {
    File {
        url_private: String,
        filetype: Option<String>,
        filename: String,
        #[allow(dead_code)]
        size: Option<u64>,
    },
}

const SLACK_TEXT_CHAR_LIMIT: usize = 40_000;
const SLACK_BLOCK_TEXT_LIMIT: usize = 3000;
const SLACK_MAX_BLOCKS: usize = 50;
const INDICATOR_RESERVE: usize = 12;

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

pub(crate) fn valid_slack_token(cfg: &RuntimeConfig) -> Option<&str> {
    cfg.slack_bot_token
        .as_deref()
        .map(str::trim)
        .filter(|t| !t.is_empty() && t.starts_with("xoxb-"))
}

pub(crate) fn valid_slack_channel_id(cfg: &RuntimeConfig) -> Option<&str> {
    cfg.slack_channel_id.as_deref().map(str::trim).filter(|id| {
        !id.is_empty()
            && id
                .chars()
                .next()
                .map(|c| c.is_ascii_uppercase())
                .unwrap_or(false)
            && id.chars().all(|c| c.is_ascii_alphanumeric())
    })
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

fn slack_api_base() -> String {
    std::env::var("COCONUTCLAW_SLACK_API_BASE")
        .ok()
        .map(|value| value.trim_end_matches('/').to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "https://slack.com/api".to_string())
}

pub(crate) fn build_slack_user_client(cfg: &RuntimeConfig) -> Result<Option<Client>> {
    let token = match cfg
        .slack_user_token
        .as_deref()
        .map(str::trim)
        .filter(|t| !t.is_empty())
    {
        Some(t) => t,
        None => return Ok(None),
    };
    let timeout_secs = cfg.slack_api_timeout_secs.max(1);
    let client = Client::builder()
        .timeout(Duration::from_secs(timeout_secs))
        .default_headers(reqwest::header::HeaderMap::from_iter([(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {token}").parse().unwrap(),
        )]))
        .build()
        .context("failed to build slack user HTTP client")?;
    Ok(Some(client))
}

pub(crate) fn build_slack_client(cfg: &RuntimeConfig) -> Result<Client> {
    let token = valid_slack_token(cfg).ok_or_else(|| {
        anyhow::anyhow!("SLACK_BOT_TOKEN is missing; set it in instance config.toml")
    })?;
    let timeout_secs = cfg.slack_api_timeout_secs.max(1);
    Client::builder()
        .timeout(Duration::from_secs(timeout_secs))
        .default_headers(reqwest::header::HeaderMap::from_iter([(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {token}").parse().unwrap(),
        )]))
        .build()
        .context("failed to build slack HTTP client")
}

// ---------------------------------------------------------------------------
// API helpers
// ---------------------------------------------------------------------------

fn parse_slack_response(body: &str, context: &str) -> Result<Value> {
    let v: Value = serde_json::from_str(body)
        .with_context(|| format!("slack {context}: invalid JSON response"))?;
    if v["ok"].as_bool() == Some(true) {
        return Ok(v);
    }
    let error = v["error"].as_str().unwrap_or("unknown_error");
    bail!("slack {context}: {error}")
}

pub(crate) fn format_slack_thread_context(
    json_response: &str,
    min_ts: Option<f64>,
) -> Result<String> {
    let v = parse_slack_response(json_response, "conversations.replies/history")?;

    let messages = v["messages"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("missing or invalid messages array"))?;

    let mut lines = Vec::new();
    for msg in messages {
        let message_ts = msg["ts"]
            .as_str()
            .and_then(|value| value.parse::<f64>().ok());
        if let (Some(cutoff), Some(message_ts)) = (min_ts, message_ts)
            && message_ts <= cutoff
        {
            continue;
        }
        let user = msg["user"].as_str().unwrap_or("unknown_user");
        let text = msg["text"].as_str().unwrap_or("");
        lines.push(format!("{user}: {text}"));
    }

    Ok(lines.join("\n"))
}

pub(crate) fn slack_fetch_thread_context(
    client: &Client,
    channel: &str,
    thread_ts: Option<&str>,
    min_ts: Option<f64>,
) -> Result<String> {
    let (url, mut form) = match thread_ts {
        Some(ts) => (
            format!("{}/conversations.replies", slack_api_base()),
            vec![("channel", channel.to_string()), ("ts", ts.to_string())],
        ),
        None => (
            format!("{}/conversations.history", slack_api_base()),
            vec![("channel", channel.to_string())],
        ),
    };

    form.push(("limit", "15".to_string()));

    let resp = client
        .post(&url)
        .form(&form)
        .send()
        .context(format!("slack {} send failed", url))?;

    let body = resp.text().context(format!("slack {} read failed", url))?;
    format_slack_thread_context(&body, min_ts)
}

pub(crate) fn slack_post_message(
    client: &Client,
    channel: &str,
    text: &str,
    blocks: Option<&str>,
    thread_ts: Option<&str>,
) -> Result<String> {
    let mut form = vec![("channel", channel.to_string()), ("text", text.to_string())];
    if let Some(b) = blocks {
        form.push(("blocks", b.to_string()));
    }
    if let Some(ts) = thread_ts {
        form.push(("thread_ts", ts.to_string()));
    }
    let resp = client
        .post(format!("{}/chat.postMessage", slack_api_base()))
        .form(&form)
        .send()
        .context("slack chat.postMessage send failed")?;
    let body = resp.text().context("slack chat.postMessage read failed")?;
    let v = parse_slack_response(&body, "chat.postMessage")?;
    let ts = v["message"]["ts"]
        .as_str()
        .or_else(|| v["ts"].as_str())
        .unwrap_or("")
        .to_string();
    Ok(ts)
}

pub(crate) fn slack_update_message(
    client: &Client,
    channel: &str,
    ts: &str,
    text: &str,
    blocks: Option<&str>,
) -> Result<()> {
    let mut form = vec![
        ("channel", channel.to_string()),
        ("ts", ts.to_string()),
        ("text", text.to_string()),
    ];
    if let Some(b) = blocks {
        form.push(("blocks", b.to_string()));
    }
    let resp = client
        .post(format!("{}/chat.update", slack_api_base()))
        .form(&form)
        .send()
        .context("slack chat.update send failed")?;
    let body = resp.text().context("slack chat.update read failed")?;
    parse_slack_response(&body, "chat.update")?;
    Ok(())
}

pub(crate) fn slack_delete_message(client: &Client, channel: &str, ts: &str) -> Result<()> {
    let resp = client
        .post(format!("{}/chat.delete", slack_api_base()))
        .form(&[("channel", channel), ("ts", ts)])
        .send()
        .context("slack chat.delete send failed")?;
    let body = resp.text().context("slack chat.delete read failed")?;
    parse_slack_response(&body, "chat.delete")?;
    Ok(())
}

pub(crate) fn slack_upload_file(
    client: &Client,
    channel: &str,
    file_path: &Path,
    filename: &str,
    _title: &str,
    thread_ts: Option<&str>,
) -> Result<()> {
    if !file_path.exists() {
        bail!("file not found: {}", file_path.display());
    }
    let file_bytes =
        fs::read(file_path).with_context(|| format!("failed to read {}", file_path.display()))?;
    let length = file_bytes.len();

    // Step 1: Get external upload URL
    let resp = client
        .post(format!("{}/files.getUploadURLExternal", slack_api_base()))
        .form(&[
            ("filename", filename.to_string()),
            ("length", length.to_string()),
        ])
        .send()
        .context("slack files.getUploadURLExternal send failed")?;
    let body = resp
        .text()
        .context("slack files.getUploadURLExternal read failed")?;
    let v = parse_slack_response(&body, "files.getUploadURLExternal")?;
    let upload_url = v["upload_url"].as_str().ok_or_else(|| {
        anyhow::anyhow!("missing upload_url in files.getUploadURLExternal response")
    })?;
    let file_id = v["file_id"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("missing file_id in files.getUploadURLExternal response"))?;

    // Step 2: Upload file bytes to the external URL
    let upload_client = Client::builder()
        .timeout(Duration::from_secs(120))
        .build()
        .context("failed to build upload client")?;
    let resp = upload_client
        .post(upload_url)
        .body(file_bytes)
        .send()
        .context("slack external file upload send failed")?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().unwrap_or_default();
        bail!("slack external file upload HTTP {status}: {body}");
    }

    // Step 3: Complete the upload and share to channel
    let mut complete_params: Vec<(&str, String)> = vec![
        ("files", json!([{"id": file_id}]).to_string()),
        ("channel_id", channel.to_string()),
    ];
    if let Some(ts) = thread_ts {
        complete_params.push(("thread_ts", ts.to_string()));
    }
    let resp = client
        .post(format!("{}/files.completeUploadExternal", slack_api_base()))
        .form(&complete_params)
        .send()
        .context("slack files.completeUploadExternal send failed")?;
    let body = resp
        .text()
        .context("slack files.completeUploadExternal read failed")?;
    parse_slack_response(&body, "files.completeUploadExternal")?;
    Ok(())
}

#[allow(dead_code)]
pub(crate) fn slack_download_file(
    client: &Client,
    url_private: &str,
    out_path: &Path,
) -> Result<()> {
    if let Some(parent) = out_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let resp = client
        .get(url_private)
        .send()
        .context("slack file download failed")?;
    if !resp.status().is_success() {
        bail!("slack file download HTTP {}", resp.status().as_u16());
    }
    let bytes = resp.bytes().context("slack file download read failed")?;
    fs::write(out_path, &bytes)
        .with_context(|| format!("failed to write {}", out_path.display()))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

pub(crate) enum SlackRenderedOutput {
    Text(String),
    Blocks(String),
}

pub(crate) fn render_slack_reply_text(cfg: &RuntimeConfig, text: &str) -> SlackRenderedOutput {
    match cfg.slack_format_mode {
        SlackFormatMode::Plain => SlackRenderedOutput::Text(text.to_string()),
        SlackFormatMode::Mrkdwn => SlackRenderedOutput::Text(text.to_string()),
        SlackFormatMode::Blocks => match render_blocks_reply(text) {
            Ok(blocks_json) => SlackRenderedOutput::Blocks(blocks_json),
            Err(err) => match cfg.slack_format_fallback {
                SlackFormatFallback::None => {
                    tracing::error!("Blocks Kit rendering failed: {err:#}, no fallback configured");
                    SlackRenderedOutput::Text(text.to_string())
                }
                SlackFormatFallback::Plain => {
                    tracing::warn!(
                        "Blocks Kit rendering failed: {err:#}, falling back to plain text"
                    );
                    SlackRenderedOutput::Text(text.to_string())
                }
            },
        },
    }
}

/// Convert CommonMark text to a Slack Block Kit JSON blocks array.
fn render_blocks_reply(text: &str) -> Result<String> {
    let mut opts = Options::empty();
    opts.insert(Options::ENABLE_STRIKETHROUGH);
    let parser = Parser::new_ext(text, opts);

    let mut blocks: Vec<Value> = Vec::new();
    let mut current_text = String::new();
    let mut code_lang: Option<String> = None;

    for event in parser {
        match event {
            Event::Start(Tag::CodeBlock(kind)) => {
                flush_section(&mut blocks, &mut current_text);
                code_lang = match kind {
                    pulldown_cmark::CodeBlockKind::Fenced(lang) => {
                        if lang.is_empty() {
                            None
                        } else {
                            Some(lang.to_string())
                        }
                    }
                    _ => None,
                };
            }
            Event::End(TagEnd::CodeBlock) => {
                let prefix = code_lang
                    .as_deref()
                    .map(|l| format!("```{l}\n"))
                    .unwrap_or_else(|| "```\n".to_string());
                let code_text = format!("{prefix}{current_text}```");
                current_text.clear();
                push_section(&mut blocks, &code_text);
                code_lang = None;
            }
            Event::Start(Tag::Heading { .. }) => {
                flush_section(&mut blocks, &mut current_text);
            }
            Event::End(TagEnd::Heading(_)) => {
                let trimmed = current_text.trim();
                // Avoid compounding with existing inline formatting (e.g. *bold*)
                // by only wrapping if the text doesn't already contain formatting
                let heading =
                    if trimmed.contains('*') || trimmed.contains('_') || trimmed.contains('`') {
                        trimmed.to_string()
                    } else {
                        format!("*{trimmed}*")
                    };
                current_text.clear();
                push_section(&mut blocks, &heading);
            }
            Event::Start(Tag::Paragraph) => {}
            Event::End(TagEnd::Paragraph) => {
                flush_section(&mut blocks, &mut current_text);
            }
            Event::Start(Tag::List(_)) => {}
            Event::End(TagEnd::List(_)) => {
                flush_section(&mut blocks, &mut current_text);
            }
            Event::Start(Tag::BlockQuote(_)) => {
                current_text.push_str("> ");
            }
            Event::End(TagEnd::BlockQuote(_)) => {
                flush_section(&mut blocks, &mut current_text);
            }
            Event::Code(code) => {
                current_text.push('`');
                current_text.push_str(&code);
                current_text.push('`');
            }
            Event::Text(t) => {
                current_text.push_str(&t);
            }
            Event::SoftBreak | Event::HardBreak => {
                current_text.push('\n');
            }
            Event::Start(Tag::Strong) => current_text.push('*'),
            Event::End(TagEnd::Strong) => current_text.push('*'),
            Event::Start(Tag::Emphasis) => current_text.push('_'),
            Event::End(TagEnd::Emphasis) => current_text.push('_'),
            Event::Start(Tag::Strikethrough) => current_text.push('~'),
            Event::End(TagEnd::Strikethrough) => current_text.push('~'),
            _ => {}
        }
    }
    flush_section(&mut blocks, &mut current_text);

    if blocks.is_empty() {
        bail!("Blocks Kit rendering produced no blocks");
    }
    Ok(serde_json::to_string(&blocks)?)
}

fn flush_section(blocks: &mut Vec<Value>, text: &mut String) {
    let trimmed = text.trim();
    if !trimmed.is_empty() {
        push_section(blocks, trimmed);
    }
    text.clear();
}

fn push_section(blocks: &mut Vec<Value>, text: &str) {
    let text = if text.len() > SLACK_BLOCK_TEXT_LIMIT {
        let suffix = "...(truncated)";
        let max_content = SLACK_BLOCK_TEXT_LIMIT.saturating_sub(suffix.len());
        // Walk back to the previous char boundary to avoid panicking on multi-byte chars
        let mut cut = max_content;
        while cut > 0 && !text.is_char_boundary(cut) {
            cut -= 1;
        }
        format!("{}{suffix}", &text[..cut])
    } else {
        text.to_string()
    };
    blocks.push(json!({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": text
        }
    }));
}

// ---------------------------------------------------------------------------
// Text splitting
// ---------------------------------------------------------------------------

pub(crate) fn split_slack_text(text: &str, max_chars: usize) -> Vec<String> {
    if max_chars <= INDICATOR_RESERVE {
        return vec![text.to_string()];
    }
    if text.chars().count() <= max_chars {
        return vec![text.to_string()];
    }
    let effective_max = max_chars.saturating_sub(INDICATOR_RESERVE);
    let mut chunks = Vec::new();
    let mut remaining = text;

    while remaining.chars().count() > effective_max {
        let cut = find_split_point(remaining, effective_max);
        chunks.push(remaining[..cut].to_string());
        remaining = &remaining[cut..];
    }
    if !remaining.is_empty() {
        chunks.push(remaining.to_string());
    }

    let total = chunks.len();
    if total > 1 {
        for (i, chunk) in chunks.iter_mut().enumerate() {
            chunk.push_str(&format!(" ({}/{})", i + 1, total));
        }
    }
    chunks
}

fn find_split_point(text: &str, max_chars: usize) -> usize {
    // Convert char count to byte offset safely
    let byte_limit = text
        .char_indices()
        .nth(max_chars)
        .map(|(i, _)| i)
        .unwrap_or(text.len());

    // Try newline
    if let Some(pos) = text[..byte_limit].rfind('\n') {
        return pos + 1;
    }
    // Try space
    if let Some(pos) = text[..byte_limit].rfind(' ') {
        return pos + 1;
    }
    // Hard cut at char boundary
    byte_limit
}

pub(crate) fn split_slack_blocks(blocks_json: &str, max_blocks: usize) -> Vec<String> {
    let blocks: Vec<Value> = match serde_json::from_str(blocks_json) {
        Ok(v) => v,
        Err(_) => return vec![blocks_json.to_string()],
    };
    if blocks.len() <= max_blocks {
        return vec![blocks_json.to_string()];
    }
    let mut chunks = Vec::new();
    for chunk in blocks.chunks(max_blocks) {
        if let Ok(s) = serde_json::to_string(chunk) {
            chunks.push(s);
        }
    }
    if chunks.len() <= 1 {
        return chunks;
    }
    let total = chunks.len();
    for (i, chunk_json) in chunks.iter_mut().enumerate() {
        if i < total - 1 {
            // Append indicator to last block's text
            if let Ok(mut v) = serde_json::from_str::<Vec<Value>>(chunk_json) {
                if let Some(last) = v.last_mut()
                    && let Some(text) = last.get_mut("text").and_then(|t| t.get_mut("text"))
                    && let Some(s) = text.as_str()
                {
                    let updated = format!("{} ({}/{})", s, i + 1, total);
                    *text = Value::String(updated);
                }
                *chunk_json = serde_json::to_string(&v).unwrap_or_else(|_| chunk_json.clone());
            }
        }
    }
    chunks
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub(crate) fn dispatch_slack_output(
    client: &Client,
    cfg: &RuntimeConfig,
    channel_id: &str,
    output: &str,
    progress_message_id: Option<&str>,
    thread_ts: Option<&str>,
) -> Result<()> {
    let effects = parse_markers(output).to_effects();

    for effect in &effects {
        match effect {
            crate::markers::Effect::TelegramReply(reply) => {
                if !reply.trim().is_empty() {
                    let rendered = render_slack_reply_text(cfg, reply);

                    match &rendered {
                        SlackRenderedOutput::Text(text) => {
                            let chunks = split_slack_text(text, SLACK_TEXT_CHAR_LIMIT);
                            for (i, chunk) in chunks.iter().enumerate() {
                                if i == 0 {
                                    if let Some(ts) = progress_message_id {
                                        if let Err(err) = slack_update_message(
                                            client, channel_id, ts, chunk, None,
                                        ) {
                                            tracing::warn!(
                                                "slack chat.update failed: {err:#}, sending new message"
                                            );
                                            slack_post_message(
                                                client, channel_id, chunk, None, thread_ts,
                                            )?;
                                        }
                                    } else {
                                        slack_post_message(
                                            client, channel_id, chunk, None, thread_ts,
                                        )?;
                                    }
                                } else {
                                    slack_post_message(client, channel_id, chunk, None, thread_ts)?;
                                }
                            }
                        }
                        SlackRenderedOutput::Blocks(blocks_json) => {
                            let chunks = split_slack_blocks(blocks_json, SLACK_MAX_BLOCKS);
                            for (i, chunk) in chunks.iter().enumerate() {
                                if i == 0 {
                                    if let Some(ts) = progress_message_id {
                                        if let Err(err) = slack_update_message(
                                            client,
                                            channel_id,
                                            ts,
                                            reply,
                                            Some(chunk),
                                        ) {
                                            tracing::warn!(
                                                "slack chat.update (blocks) failed: {err:#}, sending new message"
                                            );
                                            slack_post_message(
                                                client, channel_id, reply, None, thread_ts,
                                            )?;
                                        }
                                    } else if let Err(err) = slack_post_message(
                                        client,
                                        channel_id,
                                        reply,
                                        Some(chunk),
                                        thread_ts,
                                    ) {
                                        tracing::warn!(
                                            "slack blocks failed: {err:#}, falling back to text"
                                        );
                                        slack_post_message(
                                            client, channel_id, reply, None, thread_ts,
                                        )?;
                                    }
                                } else if let Err(err) = slack_post_message(
                                    client,
                                    channel_id,
                                    reply,
                                    Some(chunk),
                                    thread_ts,
                                ) {
                                    tracing::warn!(
                                        "slack blocks failed: {err:#}, falling back to text"
                                    );
                                    slack_post_message(client, channel_id, reply, None, thread_ts)?;
                                }
                            }
                        }
                    }
                } else if let Some(ts) = progress_message_id
                    && let Err(err) = slack_delete_message(client, channel_id, ts)
                {
                    tracing::warn!("slack delete progress message failed: {err:#}");
                }
            }
            crate::markers::Effect::SendPhoto(path_str) => {
                let path = resolve_instance_path(&cfg.instance_dir, path_str.into());
                let filename = path
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_else(|| "image".to_string());
                if let Err(err) =
                    slack_upload_file(client, channel_id, &path, &filename, "Photo", thread_ts)
                {
                    tracing::warn!("slack upload photo failed: {err:#}");
                }
            }
            crate::markers::Effect::SendDocument(path_str) => {
                let path = resolve_instance_path(&cfg.instance_dir, path_str.into());
                let filename = path
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_else(|| "document".to_string());
                if let Err(err) =
                    slack_upload_file(client, channel_id, &path, &filename, &filename, thread_ts)
                {
                    tracing::warn!("slack upload document failed: {err:#}");
                }
            }
            crate::markers::Effect::SendVideo(path_str) => {
                let path = resolve_instance_path(&cfg.instance_dir, path_str.into());
                let filename = path
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_else(|| "video".to_string());
                if let Err(err) =
                    slack_upload_file(client, channel_id, &path, &filename, "Video", thread_ts)
                {
                    tracing::warn!("slack upload video failed: {err:#}");
                }
            }
            crate::markers::Effect::VoiceReply(voice_text) => {
                if !voice_text.trim().is_empty()
                    && cfg.tts_cmd_template.is_some()
                    && let Err(err) =
                        send_slack_voice_reply(client, cfg, channel_id, voice_text, thread_ts)
                {
                    tracing::warn!("slack voice reply failed: {err:#}");
                }
            }
            _ => {}
        }
    }

    Ok(())
}

fn send_slack_voice_reply(
    client: &Client,
    cfg: &RuntimeConfig,
    channel_id: &str,
    text: &str,
    thread_ts: Option<&str>,
) -> Result<()> {
    let tts_script = cfg.root_dir.join("scripts/tts.sh");
    if !tts_script.exists() {
        bail!("TTS script not found at {}", tts_script.display());
    }

    let output_path = cfg.instance_dir.join(format!(
        "tmp/reply_slack_{}.ogg",
        chrono::Utc::now().timestamp_millis()
    ));

    let mut cmd = Command::new("bash");
    cmd.arg(&tts_script)
        .arg(&output_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env(
            "INSTANCE_DIR",
            cfg.instance_dir.to_string_lossy().to_string(),
        )
        .env(
            "TTS_CMD_TEMPLATE",
            cfg.tts_cmd_template.as_deref().unwrap_or(""),
        )
        .env("VOICE_BITRATE", "32k")
        .env("TTS_MAX_CHARS", "260");

    let mut child = cmd.spawn().context("failed to spawn TTS script")?;
    if let Some(mut stdin) = child.stdin.take() {
        use std::io::Write;
        let _ = stdin.write_all(text.as_bytes());
    }
    let status = child.wait().context("failed to wait for TTS script")?;
    if !status.success() {
        bail!("TTS script exited with status {status}");
    }

    if output_path.exists() {
        let result = slack_upload_file(
            client,
            channel_id,
            &output_path,
            "voice_reply.ogg",
            "Voice Reply",
            thread_ts,
        );
        let _ = fs::remove_file(&output_path);
        result?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Progress messages
// ---------------------------------------------------------------------------

pub(crate) fn send_slack_approval_request(
    client: &Client,
    channel_id: &str,
    thread_ts: Option<&str>,
    approval_id: i64,
    prompt: &str,
) -> Result<String> {
    let text = if prompt.trim().is_empty() {
        "Approval requested.".to_string()
    } else {
        format!("Approval requested: {}", prompt.trim())
    };
    let blocks = slack_approval_blocks(approval_id, prompt);
    let blocks_str = serde_json::to_string(&blocks)?;
    slack_post_message(client, channel_id, &text, Some(&blocks_str), thread_ts)
}

fn slack_approval_blocks(approval_id: i64, prompt: &str) -> Value {
    json!([
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": format!("*Approval required*\n{}", prompt.trim())
            }
        },
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "Approve"},
                    "style": "primary",
                    "action_id": "approval_approve",
                    "value": format!("approval:{}:approve", approval_id)
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "Reject"},
                    "style": "danger",
                    "action_id": "approval_reject",
                    "value": format!("approval:{}:reject", approval_id)
                }
            ]
        }
    ])
}

pub(crate) fn send_slack_progress_message(
    client: &Client,
    channel_id: &str,
    thread_ts: Option<&str>,
) -> Result<String> {
    let blocks = json!([
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "⏳ _Thinking..._"
            }
        },
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "Cancel"
                    },
                    "action_id": "cancel",
                    "style": "danger"
                }
            ]
        }
    ]);
    let blocks_str = serde_json::to_string(&blocks)?;
    slack_post_message(
        client,
        channel_id,
        "Thinking...",
        Some(&blocks_str),
        thread_ts,
    )
}

pub(crate) fn spawn_slack_progress_updater(
    client: Client,
    channel_id: String,
    message_ts: String,
    status_rx: std::sync::mpsc::Receiver<String>,
    cancel_flag: Arc<AtomicBool>,
    interval_secs: u64,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let start = Instant::now();
        let mut last_status = String::new();
        loop {
            thread::sleep(Duration::from_secs(interval_secs));
            if cancel_flag.load(Ordering::Relaxed) {
                break;
            }
            // Drain latest status
            while let Ok(s) = status_rx.try_recv() {
                last_status = s;
            }
            let elapsed = start.elapsed().as_secs();
            let text = if last_status.is_empty() {
                format!("⏳ _Thinking... ({elapsed}s)_")
            } else {
                format!("⏳ _Thinking... ({elapsed}s) — {last_status}_")
            };
            if let Err(err) = slack_update_message(&client, &channel_id, &message_ts, &text, None) {
                tracing::warn!("slack progress update failed: {err:#}");
                break;
            }
        }
    })
}

// ---------------------------------------------------------------------------
// Socket Mode receiver (stubs — full implementation requires tokio runtime)
// ---------------------------------------------------------------------------

/// Connect to Slack Socket Mode and relay events as SlackWebhookTurn items.
/// Runs in a thread::spawn with its own tokio runtime.
/// Includes automatic reconnection with exponential backoff on disconnect.
pub(crate) fn start_slack_socket_mode(
    cfg: &RuntimeConfig,
    tx: tokio::sync::mpsc::UnboundedSender<SlackWebhookTurn>,
) -> Result<()> {
    let app_token = cfg
        .slack_app_token
        .clone()
        .ok_or_else(|| anyhow::anyhow!("SLACK_APP_TOKEN is required for Socket Mode"))?;

    let cfg_clone = cfg.clone();
    thread::spawn(move || {
        let rt = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(err) => {
                tracing::error!("slack socket mode: failed to create tokio runtime: {err:#}");
                return;
            }
        };

        let mut attempt = 0u32;
        let max_attempts: u32 = 10;
        let base_delay_secs: u64 = 2;
        let max_delay_secs: u64 = 60;

        loop {
            let result = rt.block_on(slack_socket_mode_inner(&app_token, &cfg_clone, &tx));

            if let Err(err) = result {
                attempt += 1;
                if attempt > max_attempts {
                    tracing::error!(
                        "slack socket mode: max reconnection attempts ({max_attempts}) reached, giving up: {err:#}"
                    );
                    break;
                }
                let delay = (base_delay_secs * 2u64.saturating_pow(attempt.saturating_sub(1)))
                    .min(max_delay_secs);
                tracing::warn!(
                    "slack socket mode: connection lost (attempt {attempt}/{max_attempts}), reconnecting in {delay}s: {err:#}"
                );
                thread::sleep(Duration::from_secs(delay));
            } else {
                // Clean disconnect (close frame or "disconnect" message) — reset and retry
                attempt = 0;
                tracing::info!(
                    "slack socket mode: disconnected, reconnecting in {base_delay_secs}s"
                );
                thread::sleep(Duration::from_secs(base_delay_secs));
            }
        }
    });

    Ok(())
}

/// Call Slack's `apps.connections.open` to obtain a short-lived WSS URL for Socket Mode.
fn fetch_socket_mode_url(app_token: &str, cfg: &RuntimeConfig) -> Result<String> {
    let client = Client::builder()
        .timeout(Duration::from_secs(cfg.slack_api_timeout_secs.max(1)))
        .default_headers(reqwest::header::HeaderMap::from_iter([(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {app_token}")
                .parse()
                .context("failed to parse app token header")?,
        )]))
        .build()
        .context("failed to build HTTP client for apps.connections.open")?;

    let resp = client
        .post(format!("{}/apps.connections.open", slack_api_base()))
        .header(
            reqwest::header::CONTENT_TYPE,
            "application/x-www-form-urlencoded",
        )
        .body("")
        .send()
        .context("apps.connections.open HTTP request failed")?;
    let body = resp
        .text()
        .context("apps.connections.open read body failed")?;
    let v: Value = serde_json::from_str(&body)
        .with_context(|| "apps.connections.open: invalid JSON response")?;

    if v["ok"].as_bool() != Some(true) {
        let error = v["error"].as_str().unwrap_or("unknown_error");
        bail!("apps.connections.open failed: {error}");
    }

    let url = v["url"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("apps.connections.open response missing url field"))?;
    Ok(url.to_string())
}

async fn slack_socket_mode_inner(
    app_token: &str,
    cfg: &RuntimeConfig,
    tx: &tokio::sync::mpsc::UnboundedSender<SlackWebhookTurn>,
) -> Result<()> {
    // Dynamically fetch the short-lived WSS URL via apps.connections.open.
    // Uses reqwest::blocking which is safe inside rt.block_on() (it spawns its own threads).
    let wss_url = fetch_socket_mode_url(app_token, cfg)?;

    let request = wss_url
        .into_client_request()
        .context("failed to parse Slack Socket Mode WSS URL")?;

    let (mut ws_stream, _response) = tokio_tungstenite::connect_async(request)
        .await
        .context("failed to connect to Slack Socket Mode WebSocket")?;

    tracing::info!("slack socket mode: connected to WebSocket");

    while let Some(msg_result) = ws_stream.next().await {
        let msg = match msg_result {
            Ok(m) => m,
            Err(err) => {
                tracing::warn!("slack socket mode: WebSocket read error: {err:#}");
                bail!("WebSocket read error: {err:#}");
            }
        };

        let text = match msg {
            tokio_tungstenite::tungstenite::Message::Text(t) => t,
            tokio_tungstenite::tungstenite::Message::Close(_) => {
                tracing::info!("slack socket mode: received close frame");
                return Ok(());
            }
            _ => continue,
        };

        let envelope: Value = match serde_json::from_str(&text) {
            Ok(v) => v,
            Err(err) => {
                tracing::warn!("slack socket mode: failed to parse frame JSON: {err:#}");
                continue;
            }
        };

        let msg_type = envelope["type"].as_str().unwrap_or("");

        match msg_type {
            "hello" => {
                tracing::info!("slack socket mode: received hello — connection established");
            }
            "connect" => {
                tracing::info!("slack socket mode: received connect — ready for events");
            }
            "disconnect" => {
                tracing::info!("slack socket mode: received disconnect — closing");
                return Ok(());
            }
            "events_api" => {
                let envelope_id = envelope["envelope_id"].as_str().unwrap_or("").to_string();
                if envelope_id.is_empty() {
                    tracing::warn!("slack socket mode: events_api frame missing envelope_id");
                    continue;
                }

                // Acknowledge within 3 seconds
                let ack = json!({"envelope_id": envelope_id});
                if let Err(err) = ws_stream
                    .send(tokio_tungstenite::tungstenite::Message::Text(
                        ack.to_string().into(),
                    ))
                    .await
                {
                    tracing::warn!(
                        "slack socket mode: failed to ack envelope_id={envelope_id}: {err:#}"
                    );
                }

                let payload = &envelope["payload"];
                let event = &payload["event"];
                let event_type = event["type"].as_str().unwrap_or("");

                let event_id = envelope["envelope_id"]
                    .as_str()
                    .map(|s| s.to_string())
                    .or_else(|| payload["event_id"].as_str().map(|s| s.to_string()));

                if event_type == "message" {
                    // Skip bot messages (avoid echo loops)
                    if event.get("bot_id").is_some()
                        || event.get("subtype").and_then(|v| v.as_str()) == Some("bot_message")
                    {
                        tracing::debug!("slack socket mode: skipping bot message");
                        continue;
                    }

                    let channel_id = event["channel"].as_str().unwrap_or("").to_string();
                    let user_text = event["text"].as_str().unwrap_or("").to_string();
                    let thread_ts = event["thread_ts"].as_str().map(|s| s.to_string());

                    let media = extract_slack_media(event);

                    let turn = SlackWebhookTurn {
                        event_id,
                        channel_id,
                        thread_ts,
                        source_user_id: event["user"].as_str().map(|s| s.to_string()),
                        input: TurnInput {
                            input_type: InputType::Text,
                            user_text: if user_text.trim().is_empty() {
                                "(empty message)".to_string()
                            } else {
                                user_text
                            },
                            asr_text: String::new(),
                            attachment_type: None,
                            attachment_path: None,
                            attachment_owned: false,
                            supplemental_context: None,
                            channel: "slack".to_string(),
                        },
                        media,
                    };

                    if let Err(err) = tx.send(turn) {
                        tracing::warn!(
                            "slack socket mode: failed to send turn via channel: {err:#}"
                        );
                        bail!("channel send failed: {err:#}");
                    }
                } else {
                    tracing::debug!(
                        "slack socket mode: ignoring events_api event type={event_type}"
                    );
                }
            }
            "slash_commands" => {
                let envelope_id = envelope["envelope_id"].as_str().unwrap_or("").to_string();
                if !envelope_id.is_empty() {
                    let ack = json!({"envelope_id": envelope_id});
                    let _ = ws_stream
                        .send(tokio_tungstenite::tungstenite::Message::Text(
                            ack.to_string().into(),
                        ))
                        .await;
                }

                let payload = &envelope["payload"];
                let command = payload["command"].as_str().unwrap_or("");
                let channel_id = payload["channel_id"].as_str().unwrap_or("").to_string();

                tracing::info!(
                    "slack socket mode: received slash command={command} channel={channel_id}"
                );
                let text = payload["text"].as_str().unwrap_or("").trim();
                let thread_ts = payload["thread_ts"].as_str().map(|s| s.to_string());
                let user_id = payload["user_id"].as_str().map(|s| s.to_string());
                let mut user_text = command.to_string();
                if !text.is_empty() {
                    user_text.push(' ');
                    user_text.push_str(text);
                }

                let turn = SlackWebhookTurn {
                    event_id: envelope["envelope_id"].as_str().map(|s| s.to_string()),
                    channel_id,
                    thread_ts,
                    source_user_id: user_id,
                    input: TurnInput {
                        input_type: InputType::Text,
                        user_text,
                        asr_text: String::new(),
                        attachment_type: None,
                        attachment_path: None,
                        attachment_owned: false,
                        supplemental_context: None,
                        channel: "slack".to_string(),
                    },
                    media: None,
                };

                if let Err(err) = tx.send(turn) {
                    tracing::warn!(
                        "slack socket mode: failed to send slash command via channel: {err:#}"
                    );
                    bail!("channel send failed: {err:#}");
                }
            }
            other => {
                tracing::debug!("slack socket mode: ignoring frame type={other}");
            }
        }
    }

    Ok(())
}

fn extract_slack_media(event: &Value) -> Option<SlackMedia> {
    let files = event.get("files")?.as_array()?;
    let first = files.first()?;
    let url_private = first["url_private"].as_str()?.to_string();
    let filetype = first["filetype"].as_str().map(|s| s.to_string());
    let filename = first["filename"].as_str().unwrap_or("file").to_string();
    let size = first["size"].as_u64();
    Some(SlackMedia::File {
        url_private,
        filetype,
        filename,
        size,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "slack_tests.rs"]
mod tests;
