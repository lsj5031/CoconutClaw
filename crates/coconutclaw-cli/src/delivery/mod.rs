//! Delivery module — output routing to Telegram, Slack, and stdout.
//!
//! Owns the routing types (`DeliveryTarget`, `TaskSource`) plus the scheduled-task
//! delivery pipeline with idempotent retry tracking.

use anyhow::Result;
use coconutclaw_config::RuntimeConfig;
use reqwest::blocking::Client;
use serde_json::{Value, json};

use crate::store::Store;

pub(crate) mod slack;
pub(crate) mod telegram;

/// Remove an attachment file, ignoring NotFound errors.
pub(crate) fn cleanup_attachment_path(path: &std::path::Path) {
    if let Err(err) = std::fs::remove_file(path)
        && err.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!("failed to remove attachment {}: {err:#}", path.display());
    }
}

/// Where the output of a task should be delivered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DeliveryTarget {
    Telegram {
        chat_id: String,
    },
    Slack {
        channel_id: String,
        thread_ts: Option<String>,
    },
    Stdout,
}

impl DeliveryTarget {
    pub(crate) fn transport_name(&self) -> &'static str {
        match self {
            Self::Telegram { .. } => "telegram",
            Self::Slack { .. } => "slack",
            Self::Stdout => "local",
        }
    }
}

/// What kind of session caused the task to be enqueued.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TaskSource {
    Telegram,
    Slack {
        channel_id: String,
        thread_ts: Option<String>,
    },
    Scheduled,
    Local,
}

impl TaskSource {
    pub(crate) fn channel_name(&self) -> &'static str {
        match self {
            Self::Telegram => "telegram",
            Self::Slack { .. } => "slack",
            Self::Scheduled => "scheduled",
            Self::Local => "local",
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct TelegramDeliveryState {
    completed_ops: Vec<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct SlackDeliveryState {
    completed: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct ScheduledDeliveryState {
    telegram: TelegramDeliveryState,
    slack: SlackDeliveryState,
}

impl ScheduledDeliveryState {
    pub(crate) fn has_telegram_op(&self, op: &str) -> bool {
        self.telegram
            .completed_ops
            .iter()
            .any(|existing| existing == op)
    }

    pub(crate) fn mark_telegram_op(&mut self, op: impl Into<String>) {
        let op = op.into();
        if !self.has_telegram_op(&op) {
            self.telegram.completed_ops.push(op);
        }
    }

    pub(crate) fn slack_completed(&self) -> bool {
        self.slack.completed
    }

    pub(crate) fn mark_slack_completed(&mut self) {
        self.slack.completed = true;
    }

    #[cfg(test)]
    pub(crate) fn telegram_completed_ops(&self) -> &[String] {
        &self.telegram.completed_ops
    }
}

#[derive(Clone, Copy)]
pub(crate) struct ScheduledTaskDispatch<'a> {
    pub(crate) scheduled_task_id: i64,
    pub(crate) delivery_target: Option<&'a DeliveryTarget>,
    pub(crate) output: &'a str,
    pub(crate) progress_message_id: Option<&'a str>,
    pub(crate) delivery_state_raw: Option<&'a str>,
}

/// Result of attempting to deliver scheduled task output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScheduledDeliveryResult {
    /// Every expected op completed successfully.
    Delivered,
    /// No ops were possible (e.g. no targets configured) — stop retrying.
    SkippedPermanent,
    /// One or more ops failed transiently — retry on the next tick.
    RetryableFailure,
}

pub(crate) fn parse_delivery_target(raw: Option<&str>) -> Option<DeliveryTarget> {
    let raw = raw?;
    let value: Value = serde_json::from_str(raw).ok()?;
    match value.get("kind").and_then(Value::as_str)? {
        "telegram" => Some(DeliveryTarget::Telegram {
            chat_id: value
                .get("chat_id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
        }),
        "slack" => Some(DeliveryTarget::Slack {
            channel_id: value
                .get("channel_id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            thread_ts: value
                .get("thread_ts")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
        }),
        "stdout" => Some(DeliveryTarget::Stdout),
        _ => None,
    }
}

pub(crate) fn serialize_delivery_target(target: &DeliveryTarget) -> String {
    match target {
        DeliveryTarget::Telegram { chat_id } => {
            json!({"kind": "telegram", "chat_id": chat_id}).to_string()
        }
        DeliveryTarget::Slack {
            channel_id,
            thread_ts,
        } => json!({"kind": "slack", "channel_id": channel_id, "thread_ts": thread_ts}).to_string(),
        DeliveryTarget::Stdout => json!({"kind": "stdout"}).to_string(),
    }
}

pub(crate) fn parse_scheduled_delivery_state(raw: Option<&str>) -> ScheduledDeliveryState {
    let Some(raw) = raw else {
        return ScheduledDeliveryState::default();
    };
    let Ok(value) = serde_json::from_str::<Value>(raw) else {
        return ScheduledDeliveryState::default();
    };

    if value.get("version").and_then(Value::as_i64) == Some(1) {
        let telegram_completed_ops = value
            .get("targets")
            .and_then(|targets| targets.get("telegram"))
            .and_then(|telegram| telegram.get("completed_ops"))
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(Value::as_str)
                    .map(ToOwned::to_owned)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let slack_completed = value
            .get("targets")
            .and_then(|targets| targets.get("slack"))
            .and_then(|slack| slack.get("completed"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        return ScheduledDeliveryState {
            telegram: TelegramDeliveryState {
                completed_ops: telegram_completed_ops,
            },
            slack: SlackDeliveryState {
                completed: slack_completed,
            },
        };
    }

    let telegram_completed_ops = value
        .get("completed_ops")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    ScheduledDeliveryState {
        telegram: TelegramDeliveryState {
            completed_ops: telegram_completed_ops,
        },
        slack: SlackDeliveryState::default(),
    }
}

pub(crate) fn persist_scheduled_delivery_state(
    store: &Store,
    scheduled_task_id: i64,
    state: &ScheduledDeliveryState,
) -> Result<()> {
    let encoded = json!({
        "version": 1,
        "targets": {
            "telegram": {
                "completed_ops": state.telegram.completed_ops.clone(),
            },
            "slack": {
                "completed": state.slack.completed,
            }
        }
    })
    .to_string();
    store.set_scheduled_task_delivery_state(scheduled_task_id, Some(&encoded))
}

/// Dispatch scheduled task output to the configured delivery surface.
///
/// Marks ops complete only when delivery succeeded (or the op is permanently skippable).
pub(crate) fn dispatch_scheduled_task_output(
    store: &Store,
    cfg: &RuntimeConfig,
    telegram_client: &Client,
    request: ScheduledTaskDispatch<'_>,
) -> Result<ScheduledDeliveryResult> {
    let mut state = parse_scheduled_delivery_state(request.delivery_state_raw);

    match request.delivery_target {
        Some(DeliveryTarget::Telegram { chat_id }) => {
            let delivered = telegram::dispatch_scheduled_output(
                store,
                cfg,
                telegram_client,
                request,
                &mut state,
                Some(chat_id.as_str()),
            )?;
            if delivered {
                Ok(ScheduledDeliveryResult::Delivered)
            } else {
                Ok(ScheduledDeliveryResult::RetryableFailure)
            }
        }
        Some(DeliveryTarget::Slack {
            channel_id,
            thread_ts,
        }) => slack::dispatch_scheduled_output(
            store,
            cfg,
            request,
            &mut state,
            channel_id,
            thread_ts.as_deref(),
        ),
        Some(DeliveryTarget::Stdout) => {
            println!("{}", request.output);
            Ok(ScheduledDeliveryResult::Delivered)
        }
        None => {
            let delivered = telegram::dispatch_scheduled_output(
                store,
                cfg,
                telegram_client,
                request,
                &mut state,
                crate::telegram::valid_telegram_chat_id(cfg),
            )?;
            if delivered {
                Ok(ScheduledDeliveryResult::Delivered)
            } else if telegram::scheduled_delivery_has_expected_ops(
                cfg,
                request.output,
                request.progress_message_id,
            ) {
                Ok(ScheduledDeliveryResult::RetryableFailure)
            } else {
                Ok(ScheduledDeliveryResult::SkippedPermanent)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_delivery_target_telegram() {
        let target = parse_delivery_target(Some(r#"{"kind":"telegram","chat_id":"12345"}"#));
        assert!(matches!(target, Some(DeliveryTarget::Telegram { chat_id }) if chat_id == "12345"));
    }

    #[test]
    fn parse_delivery_target_slack() {
        let target = parse_delivery_target(Some(
            r#"{"kind":"slack","channel_id":"C123","thread_ts":"456"}"#,
        ));
        match target {
            Some(DeliveryTarget::Slack {
                channel_id,
                thread_ts,
            }) => {
                assert_eq!(channel_id, "C123");
                assert_eq!(thread_ts.as_deref(), Some("456"));
            }
            other => panic!("expected slack target, got {other:?}"),
        }
    }

    #[test]
    fn parse_delivery_target_stdout() {
        let target = parse_delivery_target(Some(r#"{"kind":"stdout"}"#));
        assert!(matches!(target, Some(DeliveryTarget::Stdout)));
    }

    #[test]
    fn parse_delivery_target_unknown_returns_none() {
        let target = parse_delivery_target(Some(r#"{"kind":"unknown"}"#));
        assert!(target.is_none());
    }

    #[test]
    fn parse_delivery_target_none_input() {
        assert!(parse_delivery_target(None).is_none());
    }

    #[test]
    fn serialize_roundtrip() {
        for target in [
            DeliveryTarget::Telegram {
                chat_id: "12345".into(),
            },
            DeliveryTarget::Slack {
                channel_id: "C123".into(),
                thread_ts: Some("456".into()),
            },
            DeliveryTarget::Stdout,
        ] {
            let encoded = serialize_delivery_target(&target);
            let decoded = parse_delivery_target(Some(&encoded)).expect("roundtrip");
            assert_eq!(decoded, target);
        }
    }

    #[test]
    fn parse_legacy_scheduled_delivery_state() {
        let state = parse_scheduled_delivery_state(Some(
            r#"{"completed_ops":["telegram:text","telegram:voice"]}"#,
        ));
        assert!(state.has_telegram_op("telegram:text"));
        assert!(state.has_telegram_op("telegram:voice"));
        assert!(!state.slack_completed());
    }

    #[test]
    fn persisted_scheduled_delivery_state_is_versioned() {
        let mut state = ScheduledDeliveryState::default();
        state.mark_telegram_op("telegram:text");
        state.mark_slack_completed();
        let encoded = json!({
            "version": 1,
            "targets": {
                "telegram": {
                    "completed_ops": state.telegram_completed_ops(),
                },
                "slack": {
                    "completed": state.slack_completed(),
                }
            }
        })
        .to_string();
        let parsed = parse_scheduled_delivery_state(Some(&encoded));
        assert_eq!(
            parsed.telegram_completed_ops(),
            &["telegram:text".to_string()]
        );
        assert!(parsed.slack_completed());
    }
}
