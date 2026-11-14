use crate::store::Store;
use anyhow::Result;
use coconutclaw_config::RuntimeConfig;
use reqwest::blocking::Client;
use serde_json::{Value, json};

pub(crate) mod telegram;

pub(crate) fn cleanup_attachment_path(path: &std::path::Path) {
    if let Err(err) = std::fs::remove_file(path)
        && err.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!("failed to remove attachment {}: {err:#}", path.display());
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DeliveryTarget {
    Telegram { chat_id: String },
    Stdout,
}
impl DeliveryTarget {
    pub(crate) fn display_id(&self) -> &str {
        match self {
            Self::Telegram { chat_id } => chat_id,
            Self::Stdout => "local",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TaskSource {
    Telegram,
    Local,
}
impl TaskSource {
    pub(crate) fn channel_name(&self) -> &'static str {
        match self {
            Self::Telegram => "telegram",
            Self::Local => "local",
        }
    }
}

pub(crate) fn parse_delivery_target(raw: Option<&str>) -> Option<DeliveryTarget> {
    let value: Value = serde_json::from_str(raw?).ok()?;
    match value.get("kind")?.as_str()? {
        "telegram" => Some(DeliveryTarget::Telegram {
            chat_id: value.get("chat_id")?.as_str()?.to_string(),
        }),
        "stdout" => Some(DeliveryTarget::Stdout),
        _ => None,
    }
}
pub(crate) fn serialize_delivery_target(target: &DeliveryTarget) -> String {
    match target {
        DeliveryTarget::Telegram { chat_id } => {
            json!({"kind":"telegram","chat_id":chat_id}).to_string()
        }
        DeliveryTarget::Stdout => json!({"kind":"stdout"}).to_string(),
    }
}

#[derive(Debug, Default)]
pub(crate) struct DeliveryState {
    completed_ops: Vec<String>,
    pub(crate) text_plan: Option<Value>,
}
impl DeliveryState {
    pub(crate) fn has_telegram_op(&self, op: &str) -> bool {
        self.completed_ops.iter().any(|s| s == op)
    }
    pub(crate) fn mark_telegram_op(&mut self, op: impl Into<String>) {
        let op = op.into();
        if !self.has_telegram_op(&op) {
            self.completed_ops.push(op);
        }
    }
}
#[derive(Clone, Copy)]
pub(crate) struct TaskDispatch<'a> {
    pub(crate) task_id: i64,
    pub(crate) output: &'a str,
    pub(crate) progress_message_id: Option<&'a str>,
}
pub(crate) fn persist_delivery_state(
    store: &Store,
    task_id: i64,
    state: &DeliveryState,
) -> Result<()> {
    store.conn.execute(
        "UPDATE task_payloads SET delivery_state=?2 WHERE task_run_id=?1",
        rusqlite::params![
            task_id,
            json!({"completed_ops":state.completed_ops,"text_plan":state.text_plan}).to_string()
        ],
    )?;
    Ok(())
}
pub(crate) fn dispatch_task_output(
    store: &Store,
    cfg: &RuntimeConfig,
    client: Option<&Client>,
    target: &DeliveryTarget,
    request: TaskDispatch<'_>,
    raw_state: &str,
) -> Result<bool> {
    match target {
        DeliveryTarget::Stdout => {
            println!("{}", request.output);
            Ok(true)
        }
        DeliveryTarget::Telegram { chat_id } => {
            let client = client.ok_or_else(|| anyhow::anyhow!("Telegram client missing"))?;
            let value: Value = serde_json::from_str(raw_state)?;
            let (completed_ops, text_plan) = if value.is_array() {
                (serde_json::from_value(value)?, None)
            } else {
                (
                    serde_json::from_value(
                        value.get("completed_ops").cloned().unwrap_or(json!([])),
                    )?,
                    value.get("text_plan").filter(|v| !v.is_null()).cloned(),
                )
            };
            let mut state = DeliveryState {
                completed_ops,
                text_plan,
            };
            telegram::dispatch_task_output(store, cfg, client, request, &mut state, Some(chat_id))
        }
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
