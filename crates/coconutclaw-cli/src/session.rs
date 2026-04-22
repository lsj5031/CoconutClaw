#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum SessionPlatform {
    Telegram,
    Slack,
    Scheduled,
    Local,
}

impl SessionPlatform {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::Telegram => "telegram",
            Self::Slack => "slack",
            Self::Scheduled => "scheduled",
            Self::Local => "local",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SessionKey {
    pub(crate) platform: SessionPlatform,
    pub(crate) root_id: String,
    pub(crate) thread_id: Option<String>,
}

impl SessionKey {
    pub(crate) fn telegram(chat_id: &str) -> Self {
        Self {
            platform: SessionPlatform::Telegram,
            root_id: chat_id.to_string(),
            thread_id: None,
        }
    }

    pub(crate) fn slack(channel_id: &str, thread_ts: Option<&str>) -> Self {
        Self {
            platform: SessionPlatform::Slack,
            root_id: channel_id.to_string(),
            thread_id: thread_ts.map(ToOwned::to_owned),
        }
    }

    pub(crate) fn scheduled(source: &str) -> Self {
        Self {
            platform: SessionPlatform::Scheduled,
            root_id: source.to_string(),
            thread_id: None,
        }
    }

    pub(crate) fn local(name: &str) -> Self {
        Self {
            platform: SessionPlatform::Local,
            root_id: name.to_string(),
            thread_id: None,
        }
    }

    pub(crate) fn id(&self) -> String {
        match self.thread_id.as_deref() {
            Some(thread_id) if !thread_id.trim().is_empty() => {
                format!("{}:{}#{}", self.platform.as_str(), self.root_id, thread_id)
            }
            _ => format!("{}:{}", self.platform.as_str(), self.root_id),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::SessionKey;

    #[test]
    fn telegram_session_key_is_chat_scoped() {
        assert_eq!(SessionKey::telegram("321").id(), "telegram:321");
    }

    #[test]
    fn slack_session_key_uses_thread_when_present() {
        assert_eq!(
            SessionKey::slack("C123", Some("171.5")).id(),
            "slack:C123#171.5"
        );
    }

    #[test]
    fn slack_session_key_falls_back_to_channel_when_no_thread() {
        assert_eq!(SessionKey::slack("C123", None).id(), "slack:C123");
    }
}
