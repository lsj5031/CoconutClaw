use serde_json::Value;

#[derive(Debug, Default)]
pub(crate) struct ParsedMarkers {
    pub(crate) telegram_reply: Option<String>,
    pub(crate) voice_reply: Option<String>,
    pub(crate) send_photo: Vec<String>,
    pub(crate) send_document: Vec<String>,
    pub(crate) send_video: Vec<String>,
    pub(crate) memory_append: Vec<String>,
    pub(crate) task_append: Vec<String>,
}

pub(crate) fn render_output(
    telegram_reply: &str,
    voice_reply: &str,
    markers: &ParsedMarkers,
) -> String {
    let mut lines = Vec::new();
    lines.push(format!("TELEGRAM_REPLY: {telegram_reply}"));

    if !voice_reply.trim().is_empty() {
        lines.push(format!("VOICE_REPLY: {voice_reply}"));
    }

    for line in &markers.send_photo {
        lines.push(format!("SEND_PHOTO: {line}"));
    }
    for line in &markers.send_document {
        lines.push(format!("SEND_DOCUMENT: {line}"));
    }
    for line in &markers.send_video {
        lines.push(format!("SEND_VIDEO: {line}"));
    }
    for line in &markers.memory_append {
        lines.push(format!("MEMORY_APPEND: {line}"));
    }
    for line in &markers.task_append {
        lines.push(format!("TASK_APPEND: {line}"));
    }

    lines.join("\n") + "\n"
}

pub(crate) fn parse_markers(payload: &str) -> ParsedMarkers {
    ParsedMarkers {
        telegram_reply: first_marker_block("TELEGRAM_REPLY", payload),
        voice_reply: first_marker_block("VOICE_REPLY", payload),
        send_photo: all_markers("SEND_PHOTO", payload),
        send_document: all_markers("SEND_DOCUMENT", payload),
        send_video: all_markers("SEND_VIDEO", payload),
        memory_append: all_markers("MEMORY_APPEND", payload),
        task_append: all_markers("TASK_APPEND", payload),
    }
}

pub(crate) fn recover_unstructured_reply(raw_output: &str) -> Option<String> {
    let trimmed = raw_output.trim();
    if trimmed.is_empty() {
        return None;
    }

    if let Some(reply) = extract_assistant_text_from_json_stream(trimmed)
        && !reply.trim().is_empty()
    {
        return Some(reply);
    }

    if looks_like_json_event_stream(trimmed) {
        return None;
    }

    Some(trimmed.to_string())
}

pub(crate) fn should_retry_provider_failure(raw_output: &str) -> bool {
    let summary = extract_error_summary(raw_output)
        .unwrap_or_else(|| raw_output.to_ascii_lowercase())
        .to_ascii_lowercase();
    [
        "network failure",
        "connection reset",
        "connection refused",
        "timed out",
        "timeout",
        "temporarily unavailable",
        "service unavailable",
        "too many requests",
        "rate limit",
        "api error: json parse error",
    ]
    .iter()
    .any(|needle| summary.contains(needle))
}

pub(crate) fn extract_error_summary(payload: &str) -> Option<String> {
    let mut found: Option<String> = None;

    for line in payload.lines() {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let Some(event_type) = value.get("type").and_then(Value::as_str) else {
            continue;
        };

        let candidate = match event_type {
            "agent_end" => value
                .get("error")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            "turn_end" => value
                .get("message")
                .and_then(|node| node.get("errorMessage"))
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            _ => None,
        };

        if let Some(text) = candidate
            && !text.trim().is_empty()
        {
            found = Some(text);
        }
    }

    found
}

fn looks_like_json_event_stream(payload: &str) -> bool {
    let mut parsed_lines = 0usize;
    let mut typed_events = 0usize;

    for line in payload.lines() {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        parsed_lines += 1;
        if value.get("type").and_then(Value::as_str).is_some() {
            typed_events += 1;
        }
    }

    parsed_lines > 0 && typed_events > 0
}

pub(crate) fn extract_assistant_text_from_json_stream(payload: &str) -> Option<String> {
    let mut final_text: Option<String> = None;
    let mut parsed_json_lines = 0usize;

    for line in payload.lines() {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        parsed_json_lines += 1;
        let Some(event_type) = value.get("type").and_then(Value::as_str) else {
            continue;
        };

        let candidate = match event_type {
            "message_end" => {
                let role = value
                    .get("message")
                    .and_then(|node| node.get("role"))
                    .and_then(Value::as_str);
                if role == Some("assistant") {
                    value
                        .get("message")
                        .and_then(|node| node.get("content"))
                        .and_then(join_text_blocks)
                } else {
                    None
                }
            }
            "agent_end" => value
                .get("messages")
                .and_then(Value::as_array)
                .and_then(|messages| {
                    let mut chunks = Vec::new();
                    for message in messages {
                        let role = message.get("role").and_then(Value::as_str);
                        if role != Some("assistant") {
                            continue;
                        }
                        if let Some(content) = message.get("content").and_then(join_text_blocks) {
                            chunks.push(content);
                        }
                    }
                    if chunks.is_empty() {
                        None
                    } else {
                        Some(chunks.join("\n"))
                    }
                }),
            _ => None,
        };

        if let Some(text) = candidate
            && !text.trim().is_empty()
        {
            final_text = Some(text);
        }
    }

    if parsed_json_lines == 0 {
        None
    } else {
        final_text
    }
}

fn join_text_blocks(node: &Value) -> Option<String> {
    if let Some(text) = node.as_str() {
        let text = text.trim();
        if text.is_empty() {
            return None;
        }
        return Some(text.to_string());
    }

    let array = node.as_array()?;
    let mut chunks = Vec::new();
    for item in array {
        let item_type = item.get("type").and_then(Value::as_str);
        if item_type == Some("text")
            && let Some(text) = item.get("text").and_then(Value::as_str)
        {
            let text = text.trim();
            if !text.is_empty() {
                chunks.push(text.to_string());
            }
        }
    }

    if chunks.is_empty() {
        None
    } else {
        Some(chunks.join("\n"))
    }
}

fn first_marker_block(marker: &str, payload: &str) -> Option<String> {
    let lines: Vec<&str> = payload.lines().collect();
    for (idx, line) in lines.iter().enumerate() {
        if let Some(value) = strip_marker(marker, line) {
            let mut block = String::new();
            block.push_str(value);

            for tail in lines.iter().skip(idx + 1) {
                if is_marker_line(tail) {
                    break;
                }
                block.push('\n');
                block.push_str(tail);
            }

            return Some(block.trim_end().to_string());
        }
    }
    None
}

fn all_markers(marker: &str, payload: &str) -> Vec<String> {
    let mut out = Vec::new();
    for line in payload.lines() {
        if let Some(value) = strip_marker(marker, line)
            && !value.trim().is_empty()
        {
            out.push(value.to_string());
        }
    }
    out
}

fn strip_marker<'a>(marker: &str, line: &'a str) -> Option<&'a str> {
    let prefix = format!("{marker}:");
    let line = line.trim_start();
    if let Some(rest) = line.strip_prefix(&prefix) {
        return Some(rest.trim_start());
    }
    None
}

fn is_marker_line(line: &str) -> bool {
    [
        "TELEGRAM_REPLY",
        "VOICE_REPLY",
        "SEND_PHOTO",
        "SEND_DOCUMENT",
        "SEND_VIDEO",
        "MEMORY_APPEND",
        "TASK_APPEND",
    ]
    .iter()
    .any(|marker| strip_marker(marker, line).is_some())
}
