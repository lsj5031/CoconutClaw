use serde::{Deserialize, Serialize};

pub const INSTANCE: &str = "local-executor-fixture";
pub const DEVICE: &str = "alpha";
pub const BODY_LIMIT: usize = 16 * 1024;
pub const TEXT_LIMIT: usize = 4096;

#[derive(Clone, Copy, Deserialize, PartialEq, Serialize)]
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
            Self::Alpha => DEVICE,
            Self::Beta => "beta",
            Self::Gamma => "gamma",
        }
    }

    pub fn named(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|device| device.name() == name)
    }
}

pub fn fixture_instance(generation: u32) -> String {
    if generation == 1 {
        INSTANCE.to_owned()
    } else {
        format!("{INSTANCE}:{generation}")
    }
}

pub fn request_bytes(instance: &str, body: &[u8]) -> Vec<u8> {
    let mut signed = format!("coconutclaw-v1\n{instance}\nPOST\n/v1/agent\n").into_bytes();
    signed.extend_from_slice(body);
    signed
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SignedEvidence {
    pub payload: String,
    pub signature: String,
}

pub fn evidence_bytes_for(instance: &str, payload: &str) -> Vec<u8> {
    format!("coconutclaw-evidence-v1\n{instance}\n{DEVICE}\n{payload}").into_bytes()
}

pub fn enrolled_request_bytes(
    instance: &str,
    placement: &str,
    incarnation: &str,
    body: &[u8],
) -> Vec<u8> {
    let mut signed = format!(
        "coconutclaw-enrolled-request-v1\n{instance}\n{placement}\n{incarnation}\nPOST\n/v1/agent\n"
    )
    .into_bytes();
    signed.extend_from_slice(body);
    signed
}

pub fn enrolled_evidence_bytes(
    instance: &str,
    placement: &str,
    incarnation: &str,
    payload: &str,
) -> Vec<u8> {
    format!("coconutclaw-enrolled-evidence-v1\n{instance}\n{placement}\n{incarnation}\n{payload}")
        .into_bytes()
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Evidence {
    pub attempt: i64,
    pub event: Event,
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Event {
    Started,
    Unknown,
    Result { text: String },
    Cancelled { evidence: CancelEvidence },
}

impl Event {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::Unknown => "unknown",
            Self::Result { .. } => "result",
            Self::Cancelled { .. } => "cancelled",
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CancelEvidence {
    BeforeStart,
    ProcessExited,
}

#[derive(Clone, Copy, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Claimed,
    Expired,
    Authorized,
    Started,
    Unknown,
    CancelledBeforeStart,
    CancelledAfterExit,
    Completed,
}

impl Phase {
    pub fn has_grant(self) -> bool {
        matches!(self, Self::Authorized | Self::Started | Self::Unknown)
    }
}

#[derive(Deserialize, Serialize)]
pub struct Status {
    pub attempt: i64,
    pub phase: Phase,
    pub start_until: Option<i64>,
    pub cancel_requested: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority_valid_until: Option<i64>,
}

#[derive(Deserialize, Serialize)]
pub struct Grant {
    pub instance: String,
    pub attempt: i64,
    pub start_until: i64,
    pub request_sequence: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub placement: Option<Device>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority_valid_until: Option<i64>,
}
