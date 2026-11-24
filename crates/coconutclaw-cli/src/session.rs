#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum SessionPlatform {
    Telegram,
    Local,
}

impl SessionPlatform {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::Telegram => "telegram",
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
        let raw = chat_id.strip_prefix("telegram:").unwrap_or(chat_id);
        let (root, thread) = raw.split_once('#').map_or((raw, None), |(root, thread)| {
            (root, Some(thread.to_string()))
        });
        Self {
            platform: SessionPlatform::Telegram,
            root_id: root.to_string(),
            thread_id: thread,
        }
    }

    /// Only transport metadata may choose a topic, never model text or a
    /// channel's direct-messages topic. Called after the chat allowlist guard.
    pub(crate) fn from_telegram_message(message: &serde_json::Value) -> anyhow::Result<Self> {
        let route = coconutclaw_telegram::private_route(message).map_err(anyhow::Error::msg)?;
        Ok(Self {
            platform: SessionPlatform::Telegram,
            root_id: route.chat_id.to_string(),
            thread_id: route.message_thread_id.map(|id| id.to_string()),
        })
    }

    pub(crate) fn telegram_route_id(&self) -> String {
        if self.thread_id.is_none() {
            self.root_id.clone()
        } else {
            self.id()
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

    /// Parse a session ID string and reconstruct a SessionKey.
    /// Format: "platform:root_id" or "platform:root_id#thread_id"
    pub(crate) fn from_id(id: String) -> Self {
        let parts: Vec<&str> = id.splitn(2, ':').collect();
        if parts.len() != 2 {
            return Self::local(&id);
        }

        let platform_str = parts[0];
        let rest = parts[1];

        let platform = match platform_str {
            "telegram" => SessionPlatform::Telegram,
            "local" => SessionPlatform::Local,
            _ => SessionPlatform::Local,
        };

        // Check for thread_id suffix
        let (root_id, thread_id) = if let Some((root, thread)) = rest.split_once('#') {
            (root.to_string(), Some(thread.to_string()))
        } else {
            (rest.to_string(), None)
        };

        Self {
            platform,
            root_id,
            thread_id,
        }
    }
}

/// Query both historical Telegram representations without rewriting old rows.
/// Task-local scopes must never alias the origin's conversation.
pub(crate) fn history_ids(id: &str, channel: &str) -> (String, String) {
    if channel == "telegram" && !id.starts_with("local:") {
        let raw = id.strip_prefix("telegram:").unwrap_or(id);
        (format!("telegram:{raw}"), raw.to_string())
    } else {
        (id.to_string(), id.to_string())
    }
}

#[cfg(test)]
#[path = "session_tests.rs"]
mod tests;
