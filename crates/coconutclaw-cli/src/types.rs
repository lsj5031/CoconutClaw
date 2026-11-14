use std::path::PathBuf;

use crate::markers::Effect;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InputType {
    Text,
    Photo,
    Video,
    Document,
    VideoNote,
}

impl InputType {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Photo => "photo",
            Self::Video => "video",
            Self::Document => "document",
            Self::VideoNote => "video_note",
        }
    }
}

impl std::fmt::Display for InputType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TurnStatus {
    Ok,
    Cancelled,
    AgentError,
    ParseRecovered,
    ParseFallback,
    AgentErrorRecovered,
}

impl TurnStatus {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Cancelled => "cancelled",
            Self::AgentError => "agent_error",
            Self::ParseRecovered => "parse_recovered",
            Self::ParseFallback => "parse_fallback",
            Self::AgentErrorRecovered => "agent_error_recovered",
        }
    }
}

impl std::fmt::Display for TurnStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TurnInput {
    pub(crate) input_type: InputType,
    pub(crate) user_text: String,
    pub(crate) attachment_type: Option<String>,
    pub(crate) attachment_path: Option<PathBuf>,
    pub(crate) attachment_owned: bool,
    pub(crate) supplemental_context: Option<String>,
    pub(crate) channel: String, // "telegram" or "local"
}

#[derive(Debug, Clone)]
pub(crate) struct QuotedMessage {
    pub(crate) reply_from: Option<String>,
    pub(crate) reply_text: Option<String>,
    pub(crate) reply_ts: Option<i64>,
}

#[derive(Debug, Clone)]
pub(crate) struct WebhookTurn {
    pub(crate) update_id: Option<String>,
    pub(crate) chat_id: String,
    pub(crate) input: TurnInput,
    pub(crate) media: Option<IncomingMedia>,
    pub(crate) quoted: QuotedMessage,
}

#[derive(Debug, Clone)]
pub(crate) enum IncomingMedia {
    Photo {
        file_id: String,
    },
    Document {
        file_id: String,
        file_name: Option<String>,
    },
    Video {
        file_id: String,
    },
    VideoNote {
        file_id: String,
    },
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct CancelSignal {
    pub(crate) callback_query_id: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) enum WebhookAction {
    Ignore {
        update_id: Option<String>,
        reason: String,
    },
    Fresh {
        update_id: Option<String>,
        chat_id: String,
    },
    Cancel {
        update_id: Option<String>,
        chat_id: String,
    },
    Turn(Box<WebhookTurn>),
}

#[derive(Debug)]
pub(crate) struct TurnResult {
    pub(crate) effects: Vec<Effect>,
    pub(crate) telegram_reply: String,
    pub(crate) status: TurnStatus,
    #[allow(dead_code)]
    pub(crate) channel: String,
}

#[derive(Debug)]
pub(crate) struct ProcessOutcome {
    #[allow(dead_code)]
    pub(crate) should_ack: bool,
    pub(crate) update_id: Option<String>,
    pub(crate) chat_id: Option<String>,
    pub(crate) output: Option<String>,
    pub(crate) cleanup_path: Option<PathBuf>,
    pub(crate) progress_message_id: Option<String>,
}
