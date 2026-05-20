use super::*;

#[test]
fn test_constant_time_secret_comparison() {
    let expected = "secret123";

    // Matching
    let provided_matching = Some("secret123");
    let is_match = if let Some(p) = provided_matching {
        p.as_bytes().ct_eq(expected.as_bytes()).into()
    } else {
        false
    };
    assert!(is_match);

    // Not matching - same length
    let provided_wrong = Some("secret456");
    let is_match = if let Some(p) = provided_wrong {
        p.as_bytes().ct_eq(expected.as_bytes()).into()
    } else {
        false
    };
    assert!(!is_match);

    // Not matching - different length
    let provided_short = Some("secret");
    let is_match = if let Some(p) = provided_short {
        p.as_bytes().ct_eq(expected.as_bytes()).into()
    } else {
        false
    };
    assert!(!is_match);

    // Not matching - empty
    let provided_none: Option<&str> = None;
    let is_match = if let Some(p) = provided_none {
        p.as_bytes().ct_eq(expected.as_bytes()).into()
    } else {
        false
    };
    assert!(!is_match);
}

#[test]
fn normalize_slack_request_payload_parses_interactive_form() {
    let value = normalize_slack_request_payload(
        "application/x-www-form-urlencoded",
        "payload=%7B%22type%22%3A%22block_actions%22%2C%22actions%22%3A%5B%7B%22action_id%22%3A%22cancel%22%7D%5D%7D",
    )
    .expect("payload");

    assert_eq!(
        value.get("type").and_then(Value::as_str),
        Some("block_actions")
    );
    assert!(slack_cancel_requested(&value));
}

#[test]
fn normalize_slack_request_payload_parses_slash_command_form() {
    let value = normalize_slack_request_payload(
        "application/x-www-form-urlencoded",
        "command=%2Fcancel&channel_id=C123&text=stop",
    )
    .expect("payload");

    assert_eq!(
        value.get("type").and_then(Value::as_str),
        Some("slash_commands")
    );
    assert_eq!(
        value.get("channel_id").and_then(Value::as_str),
        Some("C123")
    );
    assert!(slack_cancel_requested(&value));
}
