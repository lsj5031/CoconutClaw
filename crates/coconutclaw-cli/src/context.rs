//! Context building and memory/task persistence.
//!
//! Builds the full prompt context sent to the AI provider,
//! including SOUL.md, USER.md, MEMORY.md, TASKS/pending.md,
//! recent turn history, and user input.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;

use anyhow::{Context, Result};
use coconutclaw_config::{AgentProvider, RuntimeConfig, SlackFormatMode, TelegramParseMode};

use crate::markers::ParsedMarkers;
use crate::store::{ScheduledTaskInsertResult, Store};
use crate::{QuotedMessage, TurnInput};

#[derive(Debug, Default)]
pub(crate) struct AppendOutcome {
    pub(crate) schedule_feedback: Vec<String>,
}

pub(crate) fn build_context(
    cfg: &RuntimeConfig,
    store: &Store,
    input: &TurnInput,
    ts: &str,
    chat_id: &str,
    quoted: &QuotedMessage,
) -> Result<String> {
    let channel = input.channel.as_str();
    let local_visual_mode = cfg.provider == AgentProvider::Pi && cfg.pi.no_extensions;

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
    let soul = if local_visual_mode {
        truncate_chars(&soul, 1600)
    } else {
        soul
    };
    let user = if local_visual_mode {
        truncate_chars(&user, 900)
    } else {
        user
    };
    let memory = if local_visual_mode {
        truncate_chars(&memory, 900)
    } else {
        memory
    };
    let tasks = if local_visual_mode {
        truncate_chars(&tasks, 900)
    } else {
        tasks
    };

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
    let recent_limit = if local_visual_mode {
        cfg.context_turns.min(2)
    } else {
        cfg.context_turns
    };
    for line in store.recent_turns_snippet(recent_limit, chat_id, channel)? {
        let line = if local_visual_mode {
            truncate_chars(&line, 240)
        } else {
            line
        };
        text.push_str(&line);
        text.push('\n');
    }

    let boundary_unix = store.latest_boundary_unix(chat_id, channel)?;
    let quoted_is_after_boundary = match (quoted.reply_ts, boundary_unix) {
        (Some(reply_ts), Some(boundary_ts)) => reply_ts > boundary_ts,
        _ => true,
    };

    if let Some(reply_text) = quoted.reply_text.as_ref()
        && quoted_is_after_boundary
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

    if let Some(supplemental_context) = input.supplemental_context.as_ref()
        && !supplemental_context.trim().is_empty()
    {
        text.push_str("\n## Supplemental conversation context\n");
        text.push_str(supplemental_context);
        if !supplemental_context.ends_with('\n') {
            text.push('\n');
        }
    }

    text.push_str("\n## Current user input\n");
    let user_text = if local_visual_mode {
        truncate_chars(&input.user_text, 1200)
    } else {
        input.user_text.clone()
    };
    text.push_str(&format!("USER_TEXT: {}\n", user_text));
    if !input.asr_text.trim().is_empty() {
        let asr_text = if local_visual_mode {
            truncate_chars(&input.asr_text, 1200)
        } else {
            input.asr_text.clone()
        };
        text.push_str(&format!("ASR_TEXT: {}\n", asr_text));
    }
    if let (Some(attachment_type), Some(attachment_path)) =
        (&input.attachment_type, &input.attachment_path)
    {
        text.push_str(&format!("ATTACHMENT_TYPE: {attachment_type}\n"));
        let local_visual_mode = cfg.provider == AgentProvider::Pi && cfg.pi.no_extensions;
        if local_visual_mode {
            text.push_str(&format!(
                "The user sent a {attachment_type}. It is included in your input as visual data. Analyze it directly and do not read the file with tools.\n"
            ));
        } else {
            text.push_str(&format!("ATTACHMENT_PATH: {}\n", attachment_path.display()));
            text.push_str(&format!(
                "The user sent a {attachment_type}. The file is available at the path above. Use tools to inspect or process it when needed.\n"
            ));
        }
    }

    text.push_str("\n## Output requirements\n");
    text.push_str("Return only plain text marker lines. No prose before or after markers.\n");
    text.push_str("Required first line format:\n");
    text.push_str("TELEGRAM_REPLY: <reply text>\n");
    if channel == "slack" {
        text.push_str("Use the historical TELEGRAM_REPLY marker for Slack replies too.\n");
    }
    text.push_str("Optional additional lines:\n");
    text.push_str("VOICE_REPLY: <spoken reply text>\n");
    text.push_str("SEND_PHOTO: <absolute file path>\n");
    text.push_str("SEND_DOCUMENT: <absolute file path>\n");
    text.push_str("SEND_VIDEO: <absolute file path>\n");
    text.push_str("MEMORY_APPEND: <single memory line>\n");
    text.push_str("TASK_APPEND: <single task line>\n");
    text.push_str("SCHEDULE_PROMPT: HH:MM|<prompt text> (recurring daily)\n");
    text.push_str("SCHEDULE_PROMPT: once HH:MM|<prompt text> (one-shot)\n");

    if channel == "slack" {
        match cfg.slack_format_mode {
            SlackFormatMode::Plain => {
                text.push_str("Use plain text for replies. No formatting.\n");
                text.push_str("Keep marker prefixes plain and unchanged.\n");
                text.push_str("Message limit: 40,000 characters.\n");
            }
            SlackFormatMode::Mrkdwn => {
                text.push_str("Use Slack mrkdwn formatting for replies: *bold*, _italic_, ~strikethrough~, `code`, ```code blocks```.\n");
                text.push_str("Keep marker prefixes plain and unchanged.\n");
                text.push_str("Message limit: 40,000 characters.\n");
            }
            SlackFormatMode::Blocks => {
                text.push_str("Use Slack Block Kit JSON for rich replies.\n");
                text.push_str("Wrap the Block Kit blocks array in a ```blocks_json code fence after the TELEGRAM_REPLY: marker.\n");
                text.push_str(
                    "Use section blocks with mrkdwn text for paragraphs, code blocks, and lists.\n",
                );
                text.push_str(
                    "Keep marker prefixes (TELEGRAM_REPLY:, SEND_PHOTO:, etc.) OUTSIDE the blocks JSON.\n",
                );
                text.push_str(
                    "Message limit: 50 blocks per message, ~3000 chars per block text field.\n",
                );
            }
        }
    } else {
        match cfg.telegram_parse_mode {
            TelegramParseMode::Html => {
                text.push_str("Rich formatting is enabled for Telegram replies.\n");
                text.push_str("Use standard Markdown formatting inside marker values (e.g. **bold**, *italic*, `code`, ```code blocks```, [links](url)).\n");
                text.push_str(
                    "CoconutClaw will automatically convert Markdown to the appropriate format.\n",
                );
                text.push_str("Keep marker prefixes plain and unchanged.\n");
            }
            TelegramParseMode::MarkdownV2 => {
                text.push_str("MarkdownV2 is enabled for Telegram replies.\n");
                text.push_str("Use Telegram MarkdownV2 formatting only inside marker values.\n");
                text.push_str("Use `*bold*`, `_italic_`, and `` `code` `` syntax.\n");
                text.push_str(
                    "Do not use CommonMark syntax like `**bold**` or fenced code blocks.\n",
                );
                text.push_str("Keep marker prefixes plain and unchanged.\n");
                text.push_str("Do not use code fences or extra prefixes.\n");
            }
            TelegramParseMode::Off => {
                text.push_str("Do not use markdown, code fences, or extra prefixes.\n");
            }
        }
    }

    Ok(text)
}

pub(crate) fn read_or_default(path: &Path, fallback: &str) -> String {
    fs::read_to_string(path).unwrap_or_else(|_| fallback.to_string())
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    if max_chars == 0 {
        return String::new();
    }
    let mut out = String::new();
    for (idx, ch) in value.chars().enumerate() {
        if idx >= max_chars {
            out.push_str("...");
            return out;
        }
        out.push(ch);
    }
    out
}

pub(crate) fn append_memory_and_tasks(
    cfg: &RuntimeConfig,
    store: &mut Store,
    ts: &str,
    markers: &ParsedMarkers,
) -> Result<AppendOutcome> {
    let mut outcome = AppendOutcome::default();

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
        }
        store.insert_tasks(ts, cfg.provider.as_str(), &markers.task_append)?;
    }

    if !markers.schedule_prompt.is_empty() && !cfg.scheduled_tasks_enabled {
        for line in &markers.schedule_prompt {
            outcome.schedule_feedback.push(format!(
                "Runtime confirmation: schedule not saved because scheduled tasks are disabled for this instance — {}",
                truncate_chars(line.trim(), 100)
            ));
        }
    }

    if !markers.schedule_prompt.is_empty() && cfg.scheduled_tasks_enabled {
        for line in &markers.schedule_prompt {
            if let Some((recurring, time, text)) = parse_schedule_prompt_line(line) {
                match store.insert_scheduled_task(ts, "agent", &text, &time, recurring) {
                    Ok(result) => outcome
                        .schedule_feedback
                        .push(schedule_feedback_line(cfg, result, recurring, &time, &text)),
                    Err(err) => {
                        tracing::warn!("failed to persist scheduled task: {err:#}");
                        outcome.schedule_feedback.push(format!(
                            "Runtime confirmation: failed to save {} schedule at {} ({}) — {}",
                            if recurring { "daily" } else { "one-shot" },
                            time,
                            cfg.timezone,
                            truncate_chars(&text, 100)
                        ));
                    }
                }
            } else {
                outcome.schedule_feedback.push(format!(
                    "Runtime confirmation: schedule not saved because the format was invalid — {}",
                    truncate_chars(line.trim(), 100)
                ));
            }
        }
    }

    Ok(outcome)
}

fn schedule_feedback_line(
    cfg: &RuntimeConfig,
    result: ScheduledTaskInsertResult,
    recurring: bool,
    time: &str,
    text: &str,
) -> String {
    let kind = if recurring { "daily" } else { "one-shot" };
    let action = match result {
        ScheduledTaskInsertResult::Inserted => "saved",
        ScheduledTaskInsertResult::Duplicate => "already active",
    };
    format!(
        "Runtime confirmation: {action} {kind} schedule at {time} ({}) — {}",
        cfg.timezone,
        truncate_chars(text.trim(), 100)
    )
}

/// Parse a SCHEDULE_PROMPT value into its components.
/// Format: `[once ]HH:MM|prompt text`
/// Returns normalized `(recurring, "HH:MM", "prompt text")` or `None` on parse failure.
fn parse_schedule_prompt_line(line: &str) -> Option<(bool, String, String)> {
    let (recurring, rest) = if let Some(stripped) = line.strip_prefix("once ") {
        (false, stripped.trim())
    } else {
        (true, line)
    };

    let (time, prompt) = rest.split_once('|')?;
    let time = time.trim();
    let prompt = prompt.trim();

    // Basic HH:MM validation
    let parts: Vec<&str> = time.split(':').collect();
    if parts.len() != 2 {
        return None;
    }
    let h = parts[0].parse::<u8>().ok()?;
    let m = parts[1].parse::<u8>().ok()?;
    if h > 23 || m > 59 {
        return None;
    }

    if prompt.is_empty() {
        None
    } else {
        Some((recurring, format!("{h:02}:{m:02}"), prompt.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use coconutclaw_config::RuntimeConfig;
    use std::path::PathBuf;

    #[test]
    fn truncate_chars_no_truncation() {
        assert_eq!(truncate_chars("hello", 10), "hello");
    }

    #[test]
    fn truncate_chars_truncation() {
        assert_eq!(truncate_chars("hello world", 8), "hello wo...");
    }

    #[test]
    fn truncate_chars_exact_boundary() {
        assert_eq!(truncate_chars("hello", 5), "hello");
    }

    #[test]
    fn truncate_chars_zero_max() {
        assert_eq!(truncate_chars("hello", 0), "");
    }

    #[test]
    fn truncate_chars_empty_input() {
        assert_eq!(truncate_chars("", 5), "");
    }

    #[test]
    fn truncate_chars_multibyte() {
        assert_eq!(truncate_chars("日本語テスト", 3), "日本語...");
    }

    #[test]
    fn truncate_chars_multibyte_no_truncation() {
        let s = "日本語テスト";
        assert_eq!(truncate_chars(s, 10), s);
    }

    #[test]
    fn read_or_default_missing_file() {
        let path = PathBuf::from("/nonexistent/path/file.txt");
        assert_eq!(read_or_default(&path, "fallback"), "fallback");
    }

    #[test]
    fn read_or_default_existing_file() {
        let dir = std::env::temp_dir().join("coconutclaw_test_read_or_default");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test_file.txt");
        std::fs::write(&path, "file content").unwrap();
        assert_eq!(read_or_default(&path, "fallback"), "file content");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn parse_schedule_prompt_line_normalizes_time_and_once_prefix() {
        assert_eq!(
            parse_schedule_prompt_line("once 9:05|Check backups"),
            Some((false, "09:05".to_string(), "Check backups".to_string()))
        );
    }

    #[test]
    fn append_memory_and_tasks_stores_normalized_scheduled_time() {
        let cfg = RuntimeConfig::test_config();
        let mut store = Store::open(&cfg).expect("store");
        let markers = ParsedMarkers {
            schedule_prompt: vec!["9:00|Check backups".to_string()],
            ..ParsedMarkers::default()
        };

        let outcome =
            append_memory_and_tasks(&cfg, &mut store, "2026-04-20T08:00:00+0000", &markers)
                .expect("append schedule prompt");

        let due = store
            .get_due_scheduled_tasks("10:00", "2026-04-20")
            .expect("get due scheduled tasks");
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].schedule_time, "09:00");
        assert_eq!(due[0].prompt, "Check backups");
        assert_eq!(outcome.schedule_feedback.len(), 1);
        assert!(outcome.schedule_feedback[0].contains("saved daily schedule at 09:00"));
    }

    #[test]
    fn append_memory_and_tasks_reports_duplicate_schedules() {
        let cfg = RuntimeConfig::test_config();
        let mut store = Store::open(&cfg).expect("store");
        let markers = ParsedMarkers {
            schedule_prompt: vec!["9:00|Check backups".to_string()],
            ..ParsedMarkers::default()
        };

        append_memory_and_tasks(&cfg, &mut store, "2026-04-20T08:00:00+0000", &markers)
            .expect("insert schedule");
        let duplicate =
            append_memory_and_tasks(&cfg, &mut store, "2026-04-20T08:01:00+0000", &markers)
                .expect("insert duplicate schedule");

        assert_eq!(duplicate.schedule_feedback.len(), 1);
        assert!(duplicate.schedule_feedback[0].contains("already active daily schedule"));
    }
}
