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
    let markers = parse_markers_line_anchored(payload);
    if markers.telegram_reply.is_some() || markers.voice_reply.is_some() {
        return markers;
    }

    if let Some(recovered_payload) = recover_embedded_marker_payload(payload) {
        let recovered = parse_markers_line_anchored(recovered_payload);
        if recovered.telegram_reply.is_some() || recovered.voice_reply.is_some() {
            return recovered;
        }
    }

    markers
}

fn parse_markers_line_anchored(payload: &str) -> ParsedMarkers {
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

fn recover_embedded_marker_payload(payload: &str) -> Option<&str> {
    let first_line = payload.lines().next()?;
    let first_line_trimmed = first_line.trim_start();
    if detect_any_marker(first_line_trimmed).is_some() {
        return None;
    }
    if !starts_with_wrapper(first_line_trimmed) {
        return None;
    }

    let first_line_end = payload.find('\n').unwrap_or(payload.len());
    let head = &payload[..first_line_end];
    let start = marker_prefixes()
        .iter()
        .filter_map(|prefix| head.find(prefix))
        .min()?;

    if start == 0 {
        None
    } else {
        Some(&payload[start..])
    }
}

fn starts_with_wrapper(line: &str) -> bool {
    ["\"\"\"", "'''", "```", "\"", "'", "`"]
        .iter()
        .any(|prefix| line.starts_with(prefix))
}

fn marker_prefixes() -> &'static [&'static str] {
    &[
        "TELEGRAM_REPLY:",
        "VOICE_REPLY:",
        "SEND_PHOTO:",
        "SEND_DOCUMENT:",
        "SEND_VIDEO:",
        "MEMORY_APPEND:",
        "TASK_APPEND:",
    ]
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
    let summary = if let Some(summary) = extract_error_summary(raw_output) {
        summary
    } else if looks_like_json_event_stream(raw_output) {
        String::new()
    } else {
        raw_output.to_string()
    }
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
    fn test_recover_unstructured_reply_empty() {
        assert_eq!(recover_unstructured_reply(""), None);
        assert_eq!(recover_unstructured_reply("   "), None);
        assert_eq!(recover_unstructured_reply("\n\n"), None);
    }

    #[test]
    fn test_recover_unstructured_reply_plain_text() {
        assert_eq!(
            recover_unstructured_reply("Hello, world!"),
            Some("Hello, world!".to_string())
        );
        assert_eq!(
            recover_unstructured_reply("  Hello, world!  \n"),
            Some("Hello, world!".to_string())
        );
    }

    #[test]
    fn test_recover_unstructured_reply_json_assistant() {
        let json_stream = r#"
{"type":"message_start"}
{"type":"message","message":{"role":"assistant","content":"I am an assistant"}}
{"type":"message_end","message":{"role":"assistant","content":"I am an assistant"}}
"#;
        assert_eq!(
            recover_unstructured_reply(json_stream),
            Some("I am an assistant".to_string())
        );
    }

    #[test]
    fn test_recover_unstructured_reply_json_stream_no_assistant() {
        let json_stream = r#"
{"type":"agent_start"}
{"type":"turn_start"}
{"type":"progress"}
"#;
        assert_eq!(recover_unstructured_reply(json_stream), None);
    }

    #[test]
    fn test_recover_unstructured_reply_not_json_stream() {
        let not_json_stream = r#"
This is just some text.
It has some { curly braces }
{"but": "it's not a valid json stream of typed events"}
"#;
        assert_eq!(
            recover_unstructured_reply(not_json_stream),
            Some(not_json_stream.trim().to_string())
        );
    }

    #[test]
    fn extract_error_summary_empty_payload() {
        assert_eq!(extract_error_summary(""), None);
    }

    #[test]
    fn extract_error_summary_non_json_lines() {
        let payload = "not json\nstill not json";
        assert_eq!(extract_error_summary(payload), None);
    }

    #[test]
    fn parse_markers_recovers_first_line_embedded_marker() {
        let payload = concat!(
            "\"\"\"I will research the details first.",
            "TELEGRAM_REPLY: Parsed reply line one\n",
            "line two\n",
            "MEMORY_APPEND: saved item"
        );

        let markers = parse_markers(payload);

        assert_eq!(
            markers.telegram_reply.as_deref(),
            Some("Parsed reply line one\nline two")
        );
        assert_eq!(markers.memory_append, vec!["saved item".to_string()]);
    }

    #[test]
    fn parse_markers_does_not_recover_later_inline_marker_mentions() {
        let payload =
            "This plain reply mentions TELEGRAM_REPLY: literally, but it is not structured.";

        let markers = parse_markers(payload);

        assert!(markers.telegram_reply.is_none());
        assert!(markers.memory_append.is_empty());
    }

    #[test]
    fn extract_error_summary_json_missing_type() {
        let payload = r#"{"no_type":"here"}"#;
        assert_eq!(extract_error_summary(payload), None);
    }

    #[test]
    fn extract_error_summary_agent_end() {
        let payload = r#"{"type":"agent_end","error":"something went wrong"}"#;
        assert_eq!(
            extract_error_summary(payload).as_deref(),
            Some("something went wrong")
        );
    }

    #[test]
    fn extract_error_summary_turn_failed_object() {
        let payload = r#"{"type":"turn.failed","error":{"message":"turn failed message"}}"#;
        assert_eq!(
            extract_error_summary(payload).as_deref(),
            Some("turn failed message")
        );
    }

    #[test]
    fn extract_error_summary_turn_failed_string() {
        let payload = r#"{"type":"turn.failed","error":"direct error string"}"#;
        assert_eq!(
            extract_error_summary(payload).as_deref(),
            Some("direct error string")
        );
    }

    #[test]
    fn extract_error_summary_turn_end() {
        let payload = r#"{"type":"turn_end","message":{"errorMessage":"turn end error"}}"#;
        assert_eq!(
            extract_error_summary(payload).as_deref(),
            Some("turn end error")
        );
    }

    #[test]
    fn extract_error_summary_prefers_last_valid_event() {
        let payload = r#"{"type":"turn_end","message":{"errorMessage":"first error"}}
{"type":"agent_end","error":"second error"}"#;
        assert_eq!(
            extract_error_summary(payload).as_deref(),
            Some("second error")
        );
    }

    #[test]
    fn extract_error_summary_ignores_whitespace_errors() {
        let payload = r#"{"type":"agent_end","error":"   "}"#;
        assert_eq!(extract_error_summary(payload), None);
    }

    #[test]
    fn extract_error_summary_complex_mix() {
        let payload = r#"{"type":"other"}
not json
{"type":"turn.failed","error":{"no_message":"here"}}
{"type":"turn_end","message":{"errorMessage":"actual error"}}
{"type":"agent_end","error":""}
{"type":"agent_end","no_error":"field"}"#;
        assert_eq!(
            extract_error_summary(payload).as_deref(),
            Some("actual error")
        );
    }
}
