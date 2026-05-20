use super::*;

#[test]
fn parse_delivery_target_telegram() {
    let target = parse_delivery_target(Some(r#"{"kind":"telegram","chat_id":"12345"}"#));
    assert!(matches!(target, Some(DeliveryTarget::Telegram { chat_id }) if chat_id == "12345"));
}

#[test]
fn parse_delivery_target_slack() {
    let target = parse_delivery_target(Some(
        r#"{"kind":"slack","channel_id":"C123","thread_ts":"456"}"#,
    ));
    match target {
        Some(DeliveryTarget::Slack {
            channel_id,
            thread_ts,
        }) => {
            assert_eq!(channel_id, "C123");
            assert_eq!(thread_ts.as_deref(), Some("456"));
        }
        other => panic!("expected slack target, got {other:?}"),
    }
}

#[test]
fn parse_delivery_target_stdout() {
    let target = parse_delivery_target(Some(r#"{"kind":"stdout"}"#));
    assert!(matches!(target, Some(DeliveryTarget::Stdout)));
}

#[test]
fn parse_delivery_target_unknown_returns_none() {
    let target = parse_delivery_target(Some(r#"{"kind":"unknown"}"#));
    assert!(target.is_none());
}

#[test]
fn parse_delivery_target_none_input() {
    assert!(parse_delivery_target(None).is_none());
}

#[test]
fn serialize_roundtrip() {
    for target in [
        DeliveryTarget::Telegram {
            chat_id: "12345".into(),
        },
        DeliveryTarget::Slack {
            channel_id: "C123".into(),
            thread_ts: Some("456".into()),
        },
        DeliveryTarget::Stdout,
    ] {
        let encoded = serialize_delivery_target(&target);
        let decoded = parse_delivery_target(Some(&encoded)).expect("roundtrip");
        assert_eq!(decoded, target);
    }
}

#[test]
fn parse_legacy_scheduled_delivery_state() {
    let state = parse_scheduled_delivery_state(Some(
        r#"{"completed_ops":["telegram:text","telegram:voice"]}"#,
    ));
    assert!(state.has_telegram_op("telegram:text"));
    assert!(state.has_telegram_op("telegram:voice"));
    assert!(!state.slack_completed());
}

#[test]
fn persisted_scheduled_delivery_state_is_versioned() {
    let mut state = ScheduledDeliveryState::default();
    state.mark_telegram_op("telegram:text");
    state.mark_slack_completed();
    let encoded = json!({
        "version": 1,
        "targets": {
            "telegram": {
                "completed_ops": state.telegram_completed_ops(),
            },
            "slack": {
                "completed": state.slack_completed(),
            }
        }
    })
    .to_string();
    let parsed = parse_scheduled_delivery_state(Some(&encoded));
    assert_eq!(
        parsed.telegram_completed_ops(),
        &["telegram:text".to_string()]
    );
    assert!(parsed.slack_completed());
}
