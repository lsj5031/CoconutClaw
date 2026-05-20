use anyhow::Result;
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use coconutclaw_config::RuntimeConfig;
use serde_json::Value;
use std::path::{Path, PathBuf};

use crate::store::Store;
use crate::types::IncomingMedia;

pub(crate) fn shorten_log_text(text: &str, max_chars: usize) -> String {
    if max_chars == 0 {
        return String::new();
    }
    let chars: Vec<char> = text.chars().collect();
    if chars.len() <= max_chars {
        return text.to_string();
    }
    let keep = max_chars.saturating_sub(3);
    let mut out: String = chars[..keep].iter().collect();
    out.push_str("...");
    out
}

pub(crate) fn extract_incoming_media(message: &Value) -> Option<IncomingMedia> {
    if let Some(file_id) = message
        .get("voice")
        .and_then(|node| node.get("file_id"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Some(IncomingMedia::Voice {
            file_id: file_id.to_string(),
        });
    }

    if let Some(photo_array) = message.get("photo").and_then(Value::as_array) {
        for item in photo_array.iter().rev() {
            if let Some(file_id) = item
                .get("file_id")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                return Some(IncomingMedia::Photo {
                    file_id: file_id.to_string(),
                });
            }
        }
    }

    if let Some(document) = message.get("document")
        && let Some(file_id) = document
            .get("file_id")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
    {
        let file_name = document
            .get("file_name")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        return Some(IncomingMedia::Document {
            file_id: file_id.to_string(),
            file_name,
        });
    }

    if let Some(file_id) = message
        .get("video")
        .and_then(|node| node.get("file_id"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Some(IncomingMedia::Video {
            file_id: file_id.to_string(),
        });
    }

    if let Some(file_id) = message
        .get("video_note")
        .and_then(|node| node.get("file_id"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Some(IncomingMedia::VideoNote {
            file_id: file_id.to_string(),
        });
    }

    None
}

pub(crate) fn is_allowed_chat(cfg: &RuntimeConfig, chat_id: Option<&str>) -> bool {
    let Some(actual) = chat_id.map(str::trim).filter(|chat_id| !chat_id.is_empty()) else {
        return false;
    };

    telegram_allowed_chat_ids(cfg)
        .iter()
        .any(|expected| expected == &actual)
}

fn telegram_allowed_chat_ids(cfg: &RuntimeConfig) -> Vec<&str> {
    let mut allowed = Vec::new();
    if let Some(chat_id) = cfg
        .telegram_chat_id
        .as_deref()
        .map(str::trim)
        .filter(|chat_id| !chat_id.is_empty() && *chat_id != "replace_me")
    {
        allowed.push(chat_id);
    }
    for chat_id in &cfg.telegram_chat_ids {
        let trimmed = chat_id.trim();
        if trimmed.is_empty() || trimmed == "replace_me" || allowed.contains(&trimmed) {
            continue;
        }
        allowed.push(trimmed);
    }
    allowed
}

pub(crate) fn configured_telegram_chat_scope(cfg: &RuntimeConfig) -> String {
    let allowed = telegram_allowed_chat_ids(cfg);
    if allowed.is_empty() {
        "<none>".to_string()
    } else {
        allowed.join(",")
    }
}

pub(crate) fn set_inflight_update(
    store: &Store,
    update_id: &str,
    payload_json: &str,
    timezone: &str,
) -> Result<()> {
    store.kv_set("inflight_update_id", update_id)?;
    store.kv_set("inflight_update_json", payload_json)?;
    store.kv_set("inflight_started_at", &iso_now(timezone))?;
    Ok(())
}

pub(crate) fn resolve_instance_path(instance_dir: &Path, raw: PathBuf) -> PathBuf {
    if raw.is_absolute() {
        raw
    } else {
        instance_dir.join(raw)
    }
}

pub(crate) fn local_day(timezone: &str) -> String {
    let now: DateTime<Utc> = Utc::now();
    if let Ok(tz) = timezone.parse::<Tz>() {
        return now.with_timezone(&tz).format("%Y-%m-%d").to_string();
    }
    now.format("%Y-%m-%d").to_string()
}

pub(crate) fn scheduled_task_slot_at(
    now: chrono::DateTime<chrono::Utc>,
    timezone: &str,
) -> (String, String) {
    if let Ok(tz) = timezone.parse::<Tz>() {
        let local = now.with_timezone(&tz);
        return (
            local.format("%H:%M").to_string(),
            local.format("%Y-%m-%d").to_string(),
        );
    }
    (
        now.format("%H:%M").to_string(),
        now.format("%Y-%m-%d").to_string(),
    )
}

pub(crate) fn scheduled_task_slot_now(timezone: &str) -> (String, String) {
    scheduled_task_slot_at(chrono::Utc::now(), timezone)
}

pub(crate) fn iso_now(timezone: &str) -> String {
    let now: DateTime<Utc> = Utc::now();
    if let Ok(tz) = timezone.parse::<Tz>() {
        return now
            .with_timezone(&tz)
            .format("%Y-%m-%dT%H:%M:%S%z")
            .to_string();
    }
    now.format("%Y-%m-%dT%H:%M:%S%z").to_string()
}

pub(crate) fn command_exists(bin: &str) -> bool {
    let candidate = Path::new(bin);
    if candidate.is_absolute() || bin.contains('/') || bin.contains('\\') {
        return candidate.is_file();
    }
    crate::service::find_on_path(bin).is_some()
}

pub(crate) fn yes_no(value: bool) -> &'static str {
    if value { "ok" } else { "missing" }
}

pub(crate) fn asr_feature_enabled(cfg: &RuntimeConfig) -> bool {
    cfg.asr_cmd_template.is_some() || cfg.asr_url.is_some()
}
