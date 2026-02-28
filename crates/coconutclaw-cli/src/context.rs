//! Context building and memory/task persistence.
//!
//! Builds the full prompt context sent to the AI provider,
//! including SOUL.md, USER.md, MEMORY.md, TASKS/pending.md,
//! recent turn history, and user input.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;

use anyhow::{Context, Result};
use coconutclaw_config::RuntimeConfig;
use coconutclaw_config::TelegramParseMode;

use crate::markers::ParsedMarkers;
use crate::store::Store;
use crate::{QuotedMessage, TurnInput};

pub(crate) fn build_context(
    cfg: &RuntimeConfig,
    store: &Store,
    input: &TurnInput,
    ts: &str,
    quoted: &QuotedMessage,
) -> Result<String> {
    let soul = read_or_default(
        &cfg.instance_dir.join("SOUL.md"),
        "You are CoconutClaw, a calm and practical local agent.\n",
    );
    let user = read_or_default(&cfg.instance_dir.join("USER.md"), "(missing USER.md)\n");
    let memory = read_or_default(&cfg.instance_dir.join("MEMORY.md"), "# Long-Term Memory\n");
    let tasks = read_or_default(
        &cfg.instance_dir.join("TASKS/pending.md"),
        "# Pending Tasks\n",
    );

    let mut text = String::new();
    text.push_str("# CoconutClaw Runtime Context\n\n");
    text.push_str(&format!("Timestamp: {ts}\n"));
    text.push_str(&format!("Input type: {}\n", input.input_type));
    text.push_str(&format!("Agent provider: {}\n", cfg.provider.as_str()));
    text.push_str(&format!("Exec policy: {}\n", cfg.exec_policy));
    text.push_str(&format!(
        "Allowlist path: {}\n\n",
        cfg.allowlist_path.display()
    ));

    text.push_str("## SOUL.md\n");
    text.push_str(&soul);
    if !soul.ends_with('\n') {
        text.push('\n');
    }

    text.push_str("\n## USER.md\n");
    text.push_str(&user);
    if !user.ends_with('\n') {
        text.push('\n');
    }

    text.push_str("\n## MEMORY.md\n");
    text.push_str(&memory);
    if !memory.ends_with('\n') {
        text.push('\n');
    }

    text.push_str("\n## TASKS/pending.md\n");
    text.push_str(&tasks);
    if !tasks.ends_with('\n') {
        text.push('\n');
    }

    text.push_str("\n## Recent turns\n");
    for line in store.recent_turns_snippet(cfg.context_turns)? {
        text.push_str(&line);
        text.push('\n');
    }

    if let Some(reply_text) = quoted.reply_text.as_ref()
        && !reply_text.trim().is_empty()
    {
        text.push_str("\n## Quoted/replied-to message\n");
        let reply_from = quoted.reply_from.as_deref().unwrap_or("someone");
        text.push_str(&format!("REPLY_FROM: {reply_from}\n"));
        text.push_str(&format!("REPLY_TEXT: {reply_text}\n"));
        text.push_str(
            "The user is replying to the above message. Use it as context for understanding their intent.\n",
        );
    }

    text.push_str("\n## Current user input\n");
    text.push_str(&format!("USER_TEXT: {}\n", input.user_text));
    if !input.asr_text.trim().is_empty() {
        text.push_str(&format!("ASR_TEXT: {}\n", input.asr_text));
    }
    if let (Some(attachment_type), Some(attachment_path)) =
        (&input.attachment_type, &input.attachment_path)
    {
        text.push_str(&format!("ATTACHMENT_TYPE: {attachment_type}\n"));
        text.push_str(&format!("ATTACHMENT_PATH: {}\n", attachment_path.display()));
        text.push_str(&format!(
            "The user sent a {attachment_type}. The file has been downloaded to the path above. You can access and analyze it using your tools.\n"
        ));
    }

    text.push_str("\n## Output requirements\n");
    text.push_str("Return only plain text marker lines. No prose before or after markers.\n");
    text.push_str("Required first line format:\n");
    text.push_str("TELEGRAM_REPLY: <reply text>\n");
    text.push_str("Optional additional lines:\n");
    text.push_str("VOICE_REPLY: <spoken reply text>\n");
    text.push_str("SEND_PHOTO: <absolute file path>\n");
    text.push_str("SEND_DOCUMENT: <absolute file path>\n");
    text.push_str("SEND_VIDEO: <absolute file path>\n");
    text.push_str("MEMORY_APPEND: <single memory line>\n");
    text.push_str("TASK_APPEND: <single task line>\n");
    if matches!(cfg.telegram_parse_mode, TelegramParseMode::MarkdownV2) {
        text.push_str("MarkdownV2 is enabled for Telegram replies.\n");
        text.push_str("Use Telegram MarkdownV2 formatting only inside marker values.\n");
        text.push_str("Use `*bold*`, `_italic_`, and `` `code` `` syntax.\n");
        text.push_str("Do not use CommonMark syntax like `**bold**` or fenced code blocks.\n");
        text.push_str("Keep marker prefixes plain and unchanged.\n");
        text.push_str("Do not use code fences or extra prefixes.\n");
    } else {
        text.push_str("Do not use markdown, code fences, or extra prefixes.\n");
    }

    Ok(text)
}

pub(crate) fn read_or_default(path: &Path, fallback: &str) -> String {
    fs::read_to_string(path).unwrap_or_else(|_| fallback.to_string())
}

pub(crate) fn append_memory_and_tasks(
    cfg: &RuntimeConfig,
    store: &Store,
    ts: &str,
    markers: &ParsedMarkers,
) -> Result<()> {
    if !markers.memory_append.is_empty() {
        let memory_path = cfg.instance_dir.join("MEMORY.md");
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&memory_path)
            .with_context(|| format!("failed to open {}", memory_path.display()))?;

        for line in &markers.memory_append {
            writeln!(file, "- {ts} | {line}")?;
        }
    }

    if !markers.task_append.is_empty() {
        let task_path = cfg.instance_dir.join("TASKS/pending.md");
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&task_path)
            .with_context(|| format!("failed to open {}", task_path.display()))?;

        for line in &markers.task_append {
            writeln!(file, "- [ ] {line}")?;
            store.insert_task(ts, cfg.provider.as_str(), line)?;
        }
    }

    Ok(())
}
