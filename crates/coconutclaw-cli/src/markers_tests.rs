use super::*;

#[test]
fn reply_delta_is_filtered_from_render_output() {
    // ReplyDelta is a transient internal effect for streaming;
    // it should not appear in the wire-format marker output.
    let effects = vec![
        Effect::ReplyDelta("streaming chunk".to_string()),
        Effect::MemoryAppend("remember this".to_string()),
    ];
    let rendered = render_output("final reply", "", &effects);
    // The final reply appears, MemoryAppend appears, ReplyDelta does not.
    assert!(rendered.contains("TELEGRAM_REPLY: final reply"));
    assert!(rendered.contains("MEMORY_APPEND: remember this"));
    assert!(!rendered.contains("REPLY_DELTA"));
    assert!(!rendered.contains("streaming chunk"));
}

#[test]
fn reply_delta_renders_in_effects_output() {
    // When effects are rendered directly (for debugging/serialization),
    // ReplyDelta should produce a REPLAY_DELTA marker.
    let effects = vec![Effect::ReplyDelta("hello".to_string())];
    let rendered = render_effects(&effects);
    assert!(rendered.contains("REPLY_DELTA: hello"));
}

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
    let output =
        "{\"type\": \"progress\", \"content\": \"Thinking...\"}\nError: connection reset by peer";
    assert!(
        should_retry_provider_failure(output),
        "Should fall back to raw output and find 'connection reset' despite being a JSON stream initially"
    );
}

#[test]
fn test_should_retry_unrecognized_json_error() {
    let output = r#"{"type": "fatal_error", "message": "network failure"}"#;
    assert!(
        should_retry_provider_failure(output),
        "Should fall back to raw output for unrecognized JSON error types"
    );
}

#[test]
fn test_should_retry_multiple_errors_in_stream() {
    let output = "{\"type\": \"turn.failed\", \"error\": {\"message\": \"connection reset\"}}\n{\"type\": \"agent_end\", \"error\": \"unknown error\"}";
    assert!(
        should_retry_provider_failure(output),
        "Should retry if ANY error in the stream is retryable"
    );
}

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
fn parse_markers_collects_send_approval() {
    let payload = "TELEGRAM_REPLY: Ready
SEND_APPROVAL: Delete prod data";
    let markers = parse_markers(payload);
    assert_eq!(markers.send_approval, vec!["Delete prod data".to_string()]);
}

#[test]
fn parse_markers_does_not_recover_later_inline_marker_mentions() {
    let payload = "This plain reply mentions TELEGRAM_REPLY: literally, but it is not structured.";

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
