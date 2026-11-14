use super::*;

#[test]
fn parse_delivery_target_telegram() {
    let target = parse_delivery_target(Some(r#"{"kind":"telegram","chat_id":"12345"}"#));
    assert!(matches!(target, Some(DeliveryTarget::Telegram { chat_id }) if chat_id == "12345"));
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
        DeliveryTarget::Stdout,
    ] {
        let encoded = serialize_delivery_target(&target);
        let decoded = parse_delivery_target(Some(&encoded)).expect("roundtrip");
        assert_eq!(decoded, target);
    }
}
