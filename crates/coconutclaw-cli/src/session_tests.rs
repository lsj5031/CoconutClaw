use super::SessionKey;

#[test]
fn telegram_session_key_is_chat_scoped() {
    assert_eq!(SessionKey::telegram("321").id(), "telegram:321");
}
