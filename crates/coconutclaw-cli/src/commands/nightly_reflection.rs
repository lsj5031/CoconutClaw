use anyhow::{Context, Result};
use coconutclaw_config::RuntimeConfig;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;

use crate::store::Store;
use crate::turn::process_turn;
use crate::types::{InputType, QuotedMessage, TurnInput};

pub(crate) fn nightly_reflection_marker(day: &str) -> String {
    format!("<!-- nightly-reflection:{day} -->")
}

pub(crate) fn nightly_reflection_prompt(cfg: &RuntimeConfig) -> String {
    cfg.nightly_reflection_prompt.clone().unwrap_or_else(|| {
        "Do a brief nightly reflection in first person: include today outcomes, today lessons, and the single most important thing for tomorrow in under 120 words.".to_string()
    })
}

pub(crate) fn nightly_reflection_file_path(cfg: &RuntimeConfig) -> PathBuf {
    cfg.nightly_reflection_file.clone()
}

pub(crate) fn run_nightly_reflection(cfg: &RuntimeConfig, store: &mut Store) -> Result<()> {
    let reflection_path = nightly_reflection_file_path(cfg);
    let local_day = crate::util::local_day(&cfg.timezone);
    let marker = nightly_reflection_marker(&local_day);
    let now_iso = crate::util::iso_now(&cfg.timezone);

    if let Some(parent) = reflection_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    if !reflection_path.exists() {
        fs::write(&reflection_path, "")
            .with_context(|| format!("failed to initialize {}", reflection_path.display()))?;
    }

    let existing = fs::read_to_string(&reflection_path)
        .with_context(|| format!("failed to read {}", reflection_path.display()))?;
    if existing.contains(&marker) {
        tracing::info!("nightly reflection already exists for {local_day}");
        return Ok(());
    }

    let prompt = nightly_reflection_prompt(cfg);
    let before_id = store.max_turn_id()?;
    let skip_agent = cfg.nightly_reflection_skip_agent;
    if !skip_agent {
        let output = process_turn(
            cfg,
            store,
            TurnInput {
                input_type: InputType::Text,
                user_text: prompt.clone(),
                asr_text: String::new(),
                attachment_type: None,
                attachment_path: None,
                attachment_owned: false,
                supplemental_context: None,
                channel: "telegram".to_string(),
            },
            &crate::delivery::TaskSource::Telegram,
            crate::telegram::valid_telegram_chat_id(cfg).map(ToOwned::to_owned),
            None,
            None,
            &QuotedMessage {
                reply_from: None,
                reply_text: None,
                reply_ts: None,
            },
            None,
        )?;
        let client = crate::telegram::build_telegram_client(cfg)?;
        crate::telegram::dispatch_telegram_output(
            &client,
            cfg,
            crate::telegram::valid_telegram_chat_id(cfg),
            &output,
            None,
        )?;
        super::helpers::dispatch_slack_if_configured(cfg, &output);
    }

    let (turn_ts, reflection_text, status) = if let Some((turn_ts, text, status)) =
        store.latest_turn_for_prompt_after_id(before_id, &prompt)?
    {
        if !text.trim().is_empty() {
            (turn_ts, text, status)
        } else {
            (
                "<none>".to_string(),
                "- Today outcomes:\n- Today insights:\n- Most important thing tomorrow:"
                    .to_string(),
                "template_only".to_string(),
            )
        }
    } else {
        (
            "<none>".to_string(),
            "- Today outcomes:\n- Today insights:\n- Most important thing tomorrow:".to_string(),
            "template_only".to_string(),
        )
    };

    let block = format!(
        "{marker}\n## {local_day} nightly reflection\n- generated_at: {now_iso}\n- source: coconutclaw nightly-reflection\n- turn_ts: {turn_ts}\n- status: {status}\n\n{reflection_text}\n\n"
    );
    let mut file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(&reflection_path)
        .with_context(|| format!("failed to open {}", reflection_path.display()))?;
    file.write_all(block.as_bytes())
        .with_context(|| format!("failed to write {}", reflection_path.display()))?;

    tracing::info!(
        "nightly reflection appended to {}",
        reflection_path.display()
    );
    Ok(())
}
