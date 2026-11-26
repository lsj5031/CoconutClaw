use std::collections::BTreeMap;

use coconutclaw_telegram::{PrivateRoute, private_route};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::executor_wire::SignedEvidence;
pub use crate::executor_wire::{BODY_LIMIT, CancelEvidence, Phase, TEXT_LIMIT};

pub const INSTANCE: &str = "local-fixture";
pub const PLACEMENT_INSTANCE: &str = "local-placement-fixture";
pub const LEASE_MS: i64 = 1500;
pub const START_MS: i64 = 1500;

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Device {
    Alpha,
    Beta,
    Gamma,
}

impl Device {
    pub const ALL: [Self; 3] = [Self::Alpha, Self::Beta, Self::Gamma];

    pub fn name(self) -> &'static str {
        match self {
            Self::Alpha => "alpha",
            Self::Beta => "beta",
            Self::Gamma => "gamma",
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

pub struct TelegramTask<'a> {
    pub route: PrivateRoute,
    pub prompt: &'a str,
    pub pin: Option<Device>,
}

pub fn telegram_input(update: &Value) -> Option<TelegramTask<'_>> {
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
    Some(TelegramTask {
        route,
        prompt: text,
        pin: None,
    })
}

pub struct PlacementAliases(BTreeMap<String, Device>);

impl PlacementAliases {
    pub fn parse(raw: &str) -> Option<Self> {
        let canonical = BTreeMap::from([
            ("leo-claw".to_owned(), Device::Alpha),
            ("mars".to_owned(), Device::Beta),
            ("mercury".to_owned(), Device::Gamma),
        ]);
        let mut aliases = canonical.clone();
        if !raw.is_empty() {
            for entry in raw.split(',') {
                let (alias, target) = entry.split_once('=')?;
                if alias.is_empty()
                    || !alias.bytes().all(|byte| {
                        byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-'
                    })
                    || aliases
                        .insert(alias.to_owned(), *canonical.get(target)?)
                        .is_some()
                {
                    return None;
                }
            }
        }
        Some(Self(aliases))
    }

    pub fn input<'a>(&self, update: &'a Value) -> Option<TelegramTask<'a>> {
        let mut task = telegram_input(update)?;
        let message = &update["message"];
        if task.route.message_thread_id != Some(37)
            || message["chat"]["type"] != "private"
            || message["from"]["is_bot"] != false
        {
            return None;
        }
        let device = if let Some(selection) = task.prompt.trim_start().strip_prefix('@') {
            let (alias, prompt) = selection.split_once(char::is_whitespace)?;
            let prompt = prompt.trim();
            if !valid_text(prompt) || prompt.starts_with('/') {
                return None;
            }
            task.prompt = prompt;
            *self.0.get(alias)?
        } else {
            Device::Alpha
        };
        task.pin = Some(device);
        Some(task)
    }
}
