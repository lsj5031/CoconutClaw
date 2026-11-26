use serde_json::Value;

#[derive(Debug, PartialEq, Eq)]
pub struct PrivateRoute {
    pub chat_id: i64,
    pub message_thread_id: Option<i32>,
}

pub fn private_route(message: &Value) -> Result<PrivateRoute, &'static str> {
    let chat = &message["chat"];
    let chat_id = chat["id"]
        .as_i64()
        .or_else(|| {
            chat["id"]
                .as_str()
                .filter(|raw| !raw.is_empty() && raw.bytes().all(|ch| ch.is_ascii_digit()))
                .and_then(|raw| raw.parse().ok())
        })
        .filter(|id| *id > 0 && *id < (1i64 << 52))
        .ok_or("invalid private chat id")?;
    if chat.get("type").is_some_and(|kind| kind != "private")
        || message.get("direct_messages_topic_id").is_some()
        || message.get("direct_messages_topic").is_some()
    {
        return Err("only private bot chats are supported");
    }
    let message_thread_id = message
        .get("message_thread_id")
        .map(|value| {
            value
                .as_i64()
                .and_then(|id| i32::try_from(id).ok())
                .filter(|id| *id > 0)
                .ok_or("invalid message_thread_id")
        })
        .transpose()?;
    Ok(PrivateRoute {
        chat_id,
        message_thread_id,
    })
}
