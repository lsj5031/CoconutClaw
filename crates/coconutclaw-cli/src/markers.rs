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
    let mut markers = ParsedMarkers::default();
    let mut telegram_blocks = Vec::new();
    let mut voice_blocks = Vec::new();

    let mut current_marker: Option<&str> = None;
    let mut current_block = String::new();

    for line in payload.lines() {
        let line_trimmed = line.trim_start();

        if let Some((marker, content)) = detect_any_marker(line_trimmed) {
            // Commit previous block
            commit_block(
                &mut markers,
                &mut telegram_blocks,
                &mut voice_blocks,
                current_marker,
                &current_block,
            );

            // Start new block
            current_marker = Some(marker);
            current_block = content.to_string();
        } else if current_marker.is_some() {
            // Append to current block
            if !current_block.is_empty() {
                current_block.push('\n');
            }
            current_block.push_str(line);
        }
        // Note: lines before any marker are ignored (not prepended to marker content)
    }

    // Final commit
    commit_block(
        &mut markers,
        &mut telegram_blocks,
        &mut voice_blocks,
        current_marker,
        &current_block,
    );

    // Merge multi-block text fields
    if !telegram_blocks.is_empty() {
        markers.telegram_reply = Some(normalize_inline_escapes(&telegram_blocks.join("\n\n")));
    }
    if !voice_blocks.is_empty() {
        markers.voice_reply = Some(normalize_inline_escapes(&voice_blocks.join("\n\n")));
    }

    markers
}

fn detect_any_marker(line: &str) -> Option<(&'static str, &str)> {
    const MARKERS: &[(&str, &str)] = &[
        ("TELEGRAM_REPLY", "TELEGRAM_REPLY:"),
        ("VOICE_REPLY", "VOICE_REPLY:"),
        ("SEND_PHOTO", "SEND_PHOTO:"),
        ("SEND_DOCUMENT", "SEND_DOCUMENT:"),
        ("SEND_VIDEO", "SEND_VIDEO:"),
        ("MEMORY_APPEND", "MEMORY_APPEND:"),
        ("TASK_APPEND", "TASK_APPEND:"),
    ];

    for &(name, prefix) in MARKERS {
        if let Some(rest) = line.strip_prefix(prefix) {
            return Some((name, rest.trim_start()));
        }
    }
    None
}

fn commit_block(
    markers: &mut ParsedMarkers,
    telegram_blocks: &mut Vec<String>,
    voice_blocks: &mut Vec<String>,
    marker: Option<&str>,
    block: &str,
) {
    let Some(m) = marker else { return };
    let content = block.trim();
    if content.is_empty() {
        return;
    }

    match m {
        "TELEGRAM_REPLY" => telegram_blocks.push(content.to_string()),
        "VOICE_REPLY" => voice_blocks.push(content.to_string()),
        "SEND_PHOTO" => markers.send_photo.push(content.to_string()),
        "SEND_DOCUMENT" => markers.send_document.push(content.to_string()),
        "SEND_VIDEO" => markers.send_video.push(content.to_string()),
        "MEMORY_APPEND" => markers.memory_append.push(content.to_string()),
        "TASK_APPEND" => markers.task_append.push(content.to_string()),
        _ => {}
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
    let needles = [
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
    ];

    // Check all JSON errors first
    let json_errors = extract_error_summaries(raw_output);
    for summary in &json_errors {
        let lower = summary.to_ascii_lowercase();
        if needles.iter().any(|needle| lower.contains(needle)) {
            return true;
        }
    }

    // If no JSON errors matched (or none were found), we only fall back to raw output
    // if there were no JSON error events found at all. This prevents us from matching
    // retryable keywords in non-error JSON output (like tool outputs) when a non-retryable
    // error caused the failure.
    if json_errors.is_empty() {
        let lower_raw = raw_output.to_ascii_lowercase();
        needles.iter().any(|needle| lower_raw.contains(needle))
    } else {
        false
    }
}

pub(crate) fn extract_error_summaries(payload: &str) -> Vec<String> {
    let mut found = Vec::new();

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
            "turn.failed" => value
                .get("error")
                .and_then(|node| {
                    node.get("message")
                        .and_then(Value::as_str)
                        .or_else(|| node.as_str())
                })
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
            found.push(text);
        }
    }

    found
}

pub(crate) fn extract_error_summary(payload: &str) -> Option<String> {
    extract_error_summaries(payload).into_iter().last()
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
            "agent_end" => {
                let text_from_messages =
                    value
                        .get("messages")
                        .and_then(Value::as_array)
                        .and_then(|messages| {
                            let mut chunks = Vec::new();
                            for message in messages {
                                let role = message.get("role").and_then(Value::as_str);
                                if role != Some("assistant") {
                                    continue;
                                }
                                if let Some(content) =
                                    message.get("content").and_then(join_text_blocks)
                                {
                                    chunks.push(content);
                                }
                            }
                            if chunks.is_empty() {
                                None
                            } else {
                                Some(chunks.join("\n"))
                            }
                        });
                text_from_messages.or_else(|| {
                    value
                        .get("error")
                        .and_then(Value::as_str)
                        .filter(|e| !e.trim().is_empty())
                        .map(|e| format!("⚠️ Agent stopped: {e}"))
                })
            }
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

fn normalize_inline_escapes(input: &str) -> String {
    if input.contains('\n')
        || (!input.contains("\\n") && !input.contains("\\r") && !input.contains("\\t"))
    {
        return input.to_string();
    }

    // Scan for escape sequences. If any exist, we need to process them.
    // Use character iteration to preserve UTF-8 multi-byte characters.
    let chars: Vec<char> = input.chars().collect();
    let mut out = String::with_capacity(input.len());
    let mut i = 0usize;

    while i < chars.len() {
        if chars[i] == '\\' && i + 1 < chars.len() {
            let prev_is_backslash = i > 0 && chars[i - 1] == '\\';
            if !prev_is_backslash {
                match chars[i + 1] {
                    'n' => {
                        out.push('\n');
                        i += 2;
                        continue;
                    }
                    'r' => {
                        out.push('\r');
                        i += 2;
                        continue;
                    }
                    't' => {
                        out.push('\t');
                        i += 2;
                        continue;
                    }
                    _ => {}
                }
            }
        }

        out.push(chars[i]);
        i += 1;
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_retry_provider_failure_plain_text() {
        assert!(should_retry_provider_failure("network failure"));
        assert!(should_retry_provider_failure("connection reset by peer"));
        assert!(should_retry_provider_failure("timed out"));
        assert!(should_retry_provider_failure("temporarily unavailable"));
        assert!(should_retry_provider_failure("rate limit exceeded"));
    }

    #[test]
    fn test_should_not_retry_plain_text() {
        assert!(!should_retry_provider_failure("invalid credentials"));
        assert!(!should_retry_provider_failure("bad request"));
        assert!(!should_retry_provider_failure("syntax error"));
    }

    #[test]
    fn test_should_retry_provider_failure_json_agent_end() {
        let json = r#"{"type": "agent_end", "error": "network failure"}"#;
        assert!(should_retry_provider_failure(json));
    }

    #[test]
    fn test_should_retry_provider_failure_json_turn_failed() {
        let json = r#"{"type": "turn.failed", "error": {"message": "connection reset"}}"#;
        assert!(should_retry_provider_failure(json));
    }

    #[test]
    fn test_should_retry_provider_failure_json_turn_end() {
        let json = r#"{"type": "turn_end", "message": {"errorMessage": "timed out"}}"#;
        assert!(should_retry_provider_failure(json));
    }

    #[test]
    fn test_should_retry_mixed_json_and_plain_text() {
        // A common failure mode: JSON stream starts, but the process crashes and outputs a plain-text error.
        let output = "{\"type\": \"progress\", \"content\": \"Thinking...\"}\nError: connection reset by peer";
        assert!(should_retry_provider_failure(output), "Should fall back to raw output and find 'connection reset' despite being a JSON stream initially");
    }

    #[test]
    fn test_should_retry_unrecognized_json_error() {
        // The provider sends a JSON error, but it's not agent_end, turn.failed, or turn_end.
        // It still contains a retryable keyword, so it should be retried based on the raw text.
        let output = r#"{"type": "fatal_error", "message": "network failure"}"#;
        assert!(should_retry_provider_failure(output), "Should fall back to raw output for unrecognized JSON error types");
    }

    #[test]
    fn test_should_retry_multiple_errors_in_stream() {
        // A retryable error followed by a non-retryable error in the same stream.
        // It should still retry because a retryable error occurred.
        let output = "{\"type\": \"turn.failed\", \"error\": {\"message\": \"connection reset\"}}\n{\"type\": \"agent_end\", \"error\": \"unknown error\"}";
        assert!(should_retry_provider_failure(output), "Should retry if ANY error in the stream is retryable");
    }

    #[test]
    fn test_extract_error_summary_agent_end() {
        let json = r#"{"type": "agent_end", "error": "network failure"}"#;
        assert_eq!(extract_error_summary(json).as_deref(), Some("network failure"));
    }

    #[test]
    fn test_extract_error_summary_turn_failed() {
        let json = r#"{"type": "turn.failed", "error": {"message": "connection reset"}}"#;
        assert_eq!(extract_error_summary(json).as_deref(), Some("connection reset"));

        let json2 = r#"{"type": "turn.failed", "error": "timeout"}"#;
        assert_eq!(extract_error_summary(json2).as_deref(), Some("timeout"));
    }

    #[test]
    fn test_extract_error_summary_turn_end() {
        let json = r#"{"type": "turn_end", "message": {"errorMessage": "service unavailable"}}"#;
        assert_eq!(extract_error_summary(json).as_deref(), Some("service unavailable"));
    }
}
