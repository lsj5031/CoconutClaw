use super::SessionKey;

#[test]
fn telegram_session_key_is_chat_scoped() {
    assert_eq!(SessionKey::telegram("321").id(), "telegram:321");
}

#[test]
fn slack_session_key_uses_thread_when_present() {
    assert_eq!(
        SessionKey::slack("C123", Some("171.5")).id(),
        "slack:C123#171.5"
    );
}

#[test]
fn slack_session_key_falls_back_to_channel_when_no_thread() {
    assert_eq!(SessionKey::slack("C123", None).id(), "slack:C123");
}
