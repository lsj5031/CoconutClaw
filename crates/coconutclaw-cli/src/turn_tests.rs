use super::*;
use crate::{InputType, TurnInput, TurnStatus};

fn text_input(channel: &str) -> TurnInput {
    TurnInput {
        input_type: InputType::Text,
        user_text: "hello".to_string(),
        asr_text: String::new(),
        attachment_type: None,
        attachment_path: None,
        attachment_owned: false,
        supplemental_context: None,
        channel: channel.to_string(),
    }
}

#[test]
fn scheduled_turn_uses_input_channel_for_context() {
    let input = text_input("slack");
    assert_eq!(
        turn_context_channel(&TaskSource::Scheduled, &input),
        "slack"
    );
}

#[test]
fn empty_input_channel_falls_back_to_source_channel() {
    let input = text_input("");
    assert_eq!(
        turn_context_channel(&TaskSource::Telegram, &input),
        "telegram"
    );
}

#[test]
fn resolve_turn_result_marks_cancelled() {
    let result = resolve_turn_result("TELEGRAM_REPLY: hi", "telegram", true, true);
    assert_eq!(result.status, TurnStatus::Cancelled);
    assert_eq!(result.telegram_reply, "❌ Cancelled.");
    assert!(result.effects.is_empty());
}

#[test]
fn resolve_turn_result_marks_parse_recovered() {
    let result = resolve_turn_result("plain reply", "telegram", true, false);
    assert_eq!(result.status, TurnStatus::ParseRecovered);
    assert_eq!(result.telegram_reply, "plain reply");
}

#[test]
fn resolve_turn_result_marks_agent_error_recovered() {
    let payload = r#"{"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":"Recovered"}]}}"#;
    let result = resolve_turn_result(payload, "telegram", false, false);
    assert_eq!(result.status, TurnStatus::AgentErrorRecovered);
    assert_eq!(result.telegram_reply, "Recovered");
}

#[test]
fn resolve_turn_result_marks_agent_error_when_unrecoverable() {
    // A JSON event stream with no assistant text — truly unrecoverable.
    let payload = r#"{"type":"system","status":"starting"}
{"type":"error","code":"internal","message":"unexpected crash"}"#;
    let result = resolve_turn_result(payload, "telegram", false, false);
    assert_eq!(result.status, TurnStatus::AgentError);
    assert!(
        result
            .telegram_reply
            .contains("Agent execution failed locally")
    );
}

#[test]
fn resolve_turn_result_uses_turn_failed_error_message() {
    let payload = r#"{"type":"thread.started","thread_id":"abc"}
{"type":"turn.failed","error":{"message":"Codex ran out of room in the model's context window."}}"#;
    let result = resolve_turn_result(payload, "telegram", false, false);
    assert_eq!(result.status, TurnStatus::AgentError);
    assert!(result.telegram_reply.contains("context window"));
    assert!(
        !result
            .telegram_reply
            .contains(r#"{"type":"thread.started""#)
    );
}

#[test]
fn resolve_turn_result_empty_raw_output() {
    let result = resolve_turn_result("", "telegram", true, false);
    assert_eq!(result.status, TurnStatus::ParseFallback);
    assert_eq!(
        result.telegram_reply,
        "I could not parse structured markers from the model output."
    );
}

#[test]
fn resolve_turn_result_think_tag_strip_after() {
    let raw_output = "<think> I am thinking about </think> TELEGRAM_REPLY: Hello";
    let result = resolve_turn_result(raw_output, "telegram", true, false);
    assert_eq!(result.status, TurnStatus::Ok);
    assert_eq!(result.telegram_reply, "Hello");
}

#[test]
fn resolve_turn_result_think_tag_multiple() {
    let raw_output = "<think> thought 1 </think> TELEGRAM_REPLY: some text \n<think> thought 2 </think> TELEGRAM_REPLY: Hello";
    let result = resolve_turn_result(raw_output, "telegram", true, false);
    assert_eq!(result.status, TurnStatus::Ok);
    assert_eq!(result.telegram_reply, "some text\n\nHello");
}

#[test]
fn resolve_turn_result_think_tag_unclosed() {
    let raw_output = "<think> thought 1 </think> some text <think> thought 2 TELEGRAM_REPLY: Hello";
    let result = resolve_turn_result(raw_output, "telegram", true, false);
    assert_eq!(result.status, TurnStatus::ParseRecovered);
    assert_eq!(result.telegram_reply, "some text");
}

#[test]
fn resolve_turn_result_valid_markers_but_provider_failed() {
    let raw_output = "TELEGRAM_REPLY: I crashed but here is a reply";
    let result = resolve_turn_result(raw_output, "telegram", false, false);
    assert_eq!(result.status, TurnStatus::AgentError);
    assert_eq!(result.telegram_reply, "I crashed but here is a reply");
}

#[test]
fn resolve_turn_result_agent_error_blank_output() {
    let result = resolve_turn_result("   \n  \t", "telegram", false, false);
    assert_eq!(result.status, TurnStatus::AgentError);
    assert!(
        result
            .telegram_reply
            .contains("Agent execution failed locally. Please check local logs and retry.")
    );
}

#[test]
fn resolve_turn_result_recovers_plain_text_from_failed_provider() {
    let raw_output = "\n   \n\nReal error message here\nMore details";
    let result = resolve_turn_result(raw_output, "telegram", false, false);
    assert_eq!(result.status, TurnStatus::AgentErrorRecovered);
    assert!(result.telegram_reply.contains("Real error message here"));
    assert!(
        !result
            .telegram_reply
            .contains("Agent execution failed locally")
    );
}

#[test]
fn resolve_turn_result_recovers_plain_text_on_provider_failure() {
    let raw = "I can see several issues with the free-migration branch. Let me update the todo and provide a comprehensive summary:";
    let result = resolve_turn_result(raw, "telegram", false, false);
    assert_eq!(result.status, TurnStatus::AgentErrorRecovered);
    assert!(result.telegram_reply.contains("free-migration"));
    assert!(
        !result
            .telegram_reply
            .contains("Agent execution failed locally")
    );
}

#[test]
fn telegram_progress_chat_id_strips_session_prefix() {
    assert_eq!(telegram_progress_chat_id("telegram:321951227"), "321951227");
}

#[test]
fn telegram_progress_chat_id_leaves_plain_chat_id_untouched() {
    assert_eq!(telegram_progress_chat_id("321951227"), "321951227");
}
