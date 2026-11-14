//! Context building and memory/task persistence.
//!
//! Builds the full prompt context sent to the AI provider,
//! including SOUL.md, USER.md, MEMORY.md, TASKS/pending.md,
//! recent turn history, and user input.

use std::fs;
use std::io::Write;
use std::path::Path;

use anyhow::{Context, Result};
use coconutclaw_config::{RuntimeConfig, TelegramParseMode};

use crate::markers::Effect;
use crate::store::Store;
use crate::{QuotedMessage, TurnInput};

static CONTEXT_FILES: std::sync::Mutex<()> = std::sync::Mutex::new(());
pub(crate) const MEMORY_MANAGED_START: &str = "<!-- COCONUTCLAW:MANAGED:MEMORY:START -->";
pub(crate) const MEMORY_MANAGED_END: &str = "<!-- COCONUTCLAW:MANAGED:MEMORY:END -->";
pub(crate) const TASKS_MANAGED_START: &str = "<!-- COCONUTCLAW:MANAGED:TASKS:START -->";
pub(crate) const TASKS_MANAGED_END: &str = "<!-- COCONUTCLAW:MANAGED:TASKS:END -->";

pub(crate) fn build_context(
    cfg: &RuntimeConfig,
    store: &Store,
    input: &TurnInput,
    ts: &str,
    chat_id: &str,
    quoted: &QuotedMessage,
) -> Result<String> {
    let channel = input.channel.as_str();

    let files = CONTEXT_FILES.lock().expect("context files");
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
    drop(files);
    let mut text = String::new();
    text.push_str("# CoconutClaw Runtime Context\n\n");
    text.push_str(&format!("Timestamp: {ts}\n"));
    text.push_str(&format!("Input type: {}\n", input.input_type));
    text.push_str(&format!("Agent provider: {}\n", "codex"));
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
    for line in store.recent_turns_snippet(cfg.context_turns, chat_id, channel)? {
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
    text.push_str(&format!("USER_TEXT: {}\n", input.user_text));
    if let (Some(attachment_type), Some(attachment_path)) =
        (&input.attachment_type, &input.attachment_path)
    {
        text.push_str(&format!("ATTACHMENT_TYPE: {attachment_type}\n"));
        text.push_str(&format!("ATTACHMENT_PATH: {}\n", attachment_path.display()));
        text.push_str("The attachment is available at this path. Inspect it using Codex tools.\n");
    }

    text.push_str("\n## Output requirements\n");
    text.push_str("Return only plain text marker lines. No prose before or after markers.\n");
    text.push_str("Required first line format:\n");
    text.push_str("TELEGRAM_REPLY: <reply text>\n");
    text.push_str("Optional additional lines:\n");
    text.push_str("SEND_PHOTO: <absolute file path>\n");
    text.push_str("SEND_DOCUMENT: <absolute file path>\n");
    text.push_str("SEND_VIDEO: <absolute file path>\n");
    text.push_str("MEMORY_APPEND: <single memory line>\n");
    text.push_str("TASK_APPEND: <single task line>\n");
    text.push_str("DELEGATE_TASK: <self-contained task prompt> (separate Codex attempt; result returns here)\n");
    text.push_str(
        "SEND_APPROVAL: <what needs approval> (pause; do not perform the action until approved)\n",
    );

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
            text.push_str("Do not use CommonMark syntax like `**bold**` or fenced code blocks.\n");
            text.push_str("Keep marker prefixes plain and unchanged.\n");
            text.push_str("Do not use code fences or extra prefixes.\n");
        }
        TelegramParseMode::Off => {
            text.push_str("Do not use markdown, code fences, or extra prefixes.\n");
        }
    }

    Ok(text)
}

pub(crate) fn read_or_default(path: &Path, fallback: &str) -> String {
    fs::read_to_string(path).unwrap_or_else(|_| fallback.to_string())
}

pub(crate) fn append_memory_and_tasks(
    cfg: &RuntimeConfig,
    store: &mut Store,
    ts: &str,
    turn_id: Option<i64>,
    effects: &[Effect],
    _session_id: Option<&str>,
    _delivery_target_json: Option<&str>,
) -> Result<()> {
    let memory_append: Vec<String> = effects
        .iter()
        .filter_map(|e| match e {
            Effect::MemoryAppend(s) => Some(s.clone()),
            _ => None,
        })
        .collect();
    let task_append: Vec<String> = effects
        .iter()
        .filter_map(|e| match e {
            Effect::TaskAppend(s) => Some(s.clone()),
            _ => None,
        })
        .collect();

    if !memory_append.is_empty() || !task_append.is_empty() {
        store.insert_memory_and_tasks(ts, "codex", turn_id, &memory_append, &task_append)?;
        sync_managed_context_files(cfg, store)?;
    }

    Ok(())
}

pub(crate) fn sync_managed_context_files(cfg: &RuntimeConfig, store: &Store) -> Result<()> {
    let _files = CONTEXT_FILES.lock().expect("context files");
    let memory_lines = store
        .managed_memory_entries_from_db()?
        .into_iter()
        .map(|(ts, content)| format!("- {ts} | {content}"))
        .collect::<Vec<_>>();
    let task_lines = store
        .managed_pending_task_entries_from_db()?
        .into_iter()
        .map(|content| format!("- [ ] {content}"))
        .collect::<Vec<_>>();

    rewrite_managed_markdown_file(
        &cfg.instance_dir.join("MEMORY.md"),
        "# Long-Term Memory\n",
        MEMORY_MANAGED_START,
        MEMORY_MANAGED_END,
        &memory_lines,
    )?;
    rewrite_managed_markdown_file(
        &cfg.instance_dir.join("TASKS/pending.md"),
        "# Pending Tasks\n",
        TASKS_MANAGED_START,
        TASKS_MANAGED_END,
        &task_lines,
    )?;
    Ok(())
}

fn rewrite_managed_markdown_file(
    path: &Path,
    fallback: &str,
    start_marker: &str,
    end_marker: &str,
    managed_lines: &[String],
) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    let existing = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => fallback.to_string(),
        Err(error) => {
            return Err(error).context("failed to read managed projection; original preserved");
        }
    };
    if existing.contains(start_marker) && !existing.contains(end_marker) {
        anyhow::bail!("incomplete managed section; original preserved for manual reconciliation");
    }
    let mut block = format!("{start_marker}\n");
    for line in managed_lines {
        block.push_str(line);
        block.push('\n');
    }
    block.push_str(end_marker);
    let rendered = if let Some(start) = existing.find(start_marker) {
        let end = existing[start..]
            .find(end_marker)
            .context("incomplete managed section")?
            + start
            + end_marker.len();
        format!("{}{}{}", &existing[..start], block, &existing[end..])
    } else {
        let mut rendered = existing;
        if !rendered.ends_with('\n') {
            rendered.push('\n');
        }
        if !rendered.ends_with("\n\n") {
            rendered.push('\n');
        }
        rendered.push_str(&block);
        rendered.push('\n');
        rendered
    };

    // Replace on the same filesystem, never truncate the live document. Resolve
    // existing symlinks so custom document locations retain their link identity.
    let target = if path.is_symlink() {
        fs::canonicalize(path)?
    } else {
        path.to_path_buf()
    };
    let parent = target.parent().context("projection has no parent")?;
    let mut staged = tempfile::NamedTempFile::new_in(parent)?;
    if let Ok(metadata) = fs::metadata(&target) {
        staged.as_file().set_permissions(metadata.permissions())?;
    }
    staged.write_all(rendered.as_bytes())?;
    staged.as_file().sync_all()?;
    #[cfg(test)]
    if std::env::var("COCONUTCLAW_TEST_PROJECTION_EXIT").as_deref() == Ok("before") {
        std::process::exit(73);
    }
    staged
        .persist(&target)
        .map_err(|error| anyhow::anyhow!("failed to replace projection: {}", error.error))?;
    #[cfg(unix)]
    fs::File::open(parent)?.sync_all()?;
    #[cfg(test)]
    if std::env::var("COCONUTCLAW_TEST_PROJECTION_EXIT").as_deref() == Ok("after") {
        std::process::exit(74);
    }
    Ok(())
}

#[cfg(test)]
#[path = "context_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "context_integration_test.rs"]
mod integration_tests;
