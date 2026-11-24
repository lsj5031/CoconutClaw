use coconutclaw_telegram::{PrivateRoute, private_route};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::executor_wire::SignedEvidence;
pub use crate::executor_wire::{BODY_LIMIT, CancelEvidence, Phase, TEXT_LIMIT};

pub const INSTANCE: &str = "local-fixture";
pub const LEASE_MS: i64 = 1500;
pub const START_MS: i64 = 1500;

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Device {
    Alpha,
    Beta,
}

impl Device {
    pub fn name(self) -> &'static str {
        match self {
            Self::Alpha => "alpha",
            Self::Beta => "beta",
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Agent {
    pub device: Device,
    pub at_ms: i64,
    pub sequence: i64,
    pub command: Command,
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Command {
    Hello,
    Next,
    Status {
        attempt: i64,
    },
    Claim {
        task: String,
    },
    Start {
        attempt: i64,
    },
    Started {
        attempt: i64,
    },
    Result {
        attempt: i64,
        text: String,
    },
    Cancelled {
        attempt: i64,
        evidence: CancelEvidence,
    },
    Report {
        proof: SignedEvidence,
    },
}

impl Command {
    pub fn available(&self, executor: bool) -> bool {
        match self {
            Self::Status { .. } | Self::Report { .. } => executor,
            Self::Started { .. } | Self::Result { .. } | Self::Cancelled { .. } => !executor,
            _ => true,
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Fixture {
    Cancel {
        task: String,
    },
    Disconnect {
        attempt: i64,
    },
    Send {
        task: String,
    },
    RecoverSend {
        task: String,
    },
    DeliveryStatus {
        task: String,
    },
    Schedule {
        version: i64,
        first_at: i64,
        interval_ms: i64,
        count: i64,
        message_thread_id: Option<i32>,
        prompt: String,
        pin: Option<Device>,
    },
    Disable {
        version: i64,
    },
    Wake {
        at_ms: i64,
    },
}

pub fn valid_text(text: &str) -> bool {
    !text.trim().is_empty() && text.len() <= TEXT_LIMIT
}

pub fn telegram_input(update: &Value) -> Option<(PrivateRoute, &str)> {
    let message = update.get("message")?;
    let route = private_route(message).ok()?;
    let text = message.get("text")?.as_str()?;
    if route.chat_id != 321
        || message["from"]["id"].as_i64() != Some(321)
        || message["from"]["is_bot"] == true
        || text.trim_start().starts_with('/')
        || !valid_text(text)
    {
        return None;
    }
    Some((route, text))
}
