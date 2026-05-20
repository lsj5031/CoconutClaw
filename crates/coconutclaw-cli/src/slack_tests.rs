use super::*;

#[test]
fn format_slack_thread_context_parses_replies() {
    let json = r#"{
        "ok": true,
        "messages": [
            {"user": "U1", "text": "First message", "ts": "1710000001.000100"},
            {"user": "U2", "text": "Second message", "ts": "1710000002.000100"}
        ]
    }"#;

    let context = format_slack_thread_context(json, None).unwrap();
    assert_eq!(context, "U1: First message\nU2: Second message");
}

#[test]
fn format_slack_thread_context_skips_messages_before_boundary() {
    let json = r#"{
        "ok": true,
        "messages": [
            {"user": "U1", "text": "Old message", "ts": "1710000000.100000"},
            {"user": "U2", "text": "Fresh message", "ts": "1710000010.100000"}
        ]
    }"#;

    let context = format_slack_thread_context(json, Some(1710000005.0)).unwrap();
    assert_eq!(context, "U2: Fresh message");
}

#[test]
fn split_slack_text_short() {
    let chunks = split_slack_text("hello", 100);
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0], "hello");
}

#[test]
fn split_slack_text_long() {
    let text = "word ".repeat(2000); // ~10,000 chars
    let chunks = split_slack_text(&text, 4000);
    assert!(chunks.len() > 1);
    // Each chunk should have indicator
    for chunk in &chunks {
        assert!(chunk.contains("(/") || chunk.len() <= 4000 + INDICATOR_RESERVE);
    }
}

#[test]
fn render_blocks_reply_paragraph() {
    let result = render_blocks_reply("Hello world").unwrap();
    let blocks: Vec<Value> = serde_json::from_str(&result).unwrap();
    assert_eq!(blocks.len(), 1);
    assert_eq!(blocks[0]["type"], "section");
    assert_eq!(blocks[0]["text"]["text"], "Hello world");
}

#[test]
fn render_blocks_reply_code_block() {
    let input = "```rust\nfn main() {}\n```\n";
    let result = render_blocks_reply(input).unwrap();
    let blocks: Vec<Value> = serde_json::from_str(&result).unwrap();
    assert_eq!(blocks.len(), 1);
    let text = blocks[0]["text"]["text"].as_str().unwrap();
    assert!(text.contains("```rust"));
    assert!(text.contains("fn main()"));
}

#[test]
fn render_blocks_reply_heading() {
    let input = "# Title\nParagraph text\n";
    let result = render_blocks_reply(input).unwrap();
    let blocks: Vec<Value> = serde_json::from_str(&result).unwrap();
    assert!(blocks.len() >= 2);
    assert!(
        blocks[0]["text"]["text"]
            .as_str()
            .unwrap()
            .contains("*Title*")
    );
}

#[test]
fn render_blocks_reply_heading_with_formatting() {
    let input = "# Prefix **Bold Title** Suffix\nParagraph text\n";
    let result = render_blocks_reply(input).unwrap();
    let blocks: Vec<Value> = serde_json::from_str(&result).unwrap();
    assert!(blocks.len() >= 2);
    let header_text = blocks[0]["text"]["text"].as_str().unwrap();
    // Print the output and intentionally fail to see it
    assert_eq!(header_text, "Prefix *Bold Title* Suffix");
}

#[test]
fn split_slack_blocks_within_limit() {
    let blocks: Vec<Value> =
        vec![json!({"type": "section", "text": {"type": "mrkdwn", "text": "hi"}})];
    let json = serde_json::to_string(&blocks).unwrap();
    let chunks = split_slack_blocks(&json, 50);
    assert_eq!(chunks.len(), 1);
}

#[test]
fn parse_slack_response_ok() {
    let body = r#"{"ok":true,"message":{"ts":"1234.56"}}"#;
    let v = parse_slack_response(body, "test").unwrap();
    assert_eq!(v["ok"], true);
}

#[test]
fn parse_slack_response_error() {
    let body = r#"{"ok":false,"error":"channel_not_found"}"#;
    let result = parse_slack_response(body, "test");
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("channel_not_found")
    );
}

#[test]
fn approval_blocks_encode_the_approval_row_id() {
    let blocks = slack_approval_blocks(42, "deploy production");
    let elements = blocks[1]["elements"].as_array().expect("elements");

    assert_eq!(elements[0]["value"], "approval:42:approve");
    assert_eq!(elements[1]["value"], "approval:42:reject");
}
