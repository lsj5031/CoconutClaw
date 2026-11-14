use super::*;
use rusqlite::{Connection, params};

#[test]
fn telegram_history_aliases_share_fresh_boundary_but_not_other_chats() {
    let cfg = coconutclaw_config::RuntimeConfig::test_config();
    let store = Store::open(&cfg).unwrap();
    for (chat, text) in [
        ("321", "legacy-raw"),
        ("telegram:321", "canonical"),
        ("322", "other-chat"),
    ] {
        store.conn.execute("INSERT INTO turns(ts,chat_id,input_type,user_text,provider_raw,telegram_reply,status,channel)
            VALUES('2026-11-15T00:00:00+0000',?1,'text',?2,'','reply','ok','telegram')", params![chat,text]).unwrap();
    }
    let before = store
        .recent_turns_snippet(8, "telegram:321", "telegram")
        .unwrap()
        .join("\n");
    assert!(before.contains("legacy-raw") && before.contains("canonical"));
    assert!(!before.contains("other-chat"));
    store
        .insert_boundary_turn(
            "2026-11-15T00:01:00+0000",
            "321",
            Some("boundary"),
            "telegram",
        )
        .unwrap();
    assert!(
        store
            .recent_turns_snippet(8, "321", "telegram")
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .recent_turns_snippet(8, "telegram:321", "telegram")
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store.latest_boundary_id("321", "telegram").unwrap(),
        store
            .latest_boundary_id("telegram:321", "telegram")
            .unwrap()
    );
    assert!(
        store
            .latest_boundary_unix("321", "telegram")
            .unwrap()
            .is_some()
    );
    assert!(store.recent_turns_snippet(8, "322", "telegram").unwrap()[0].contains("other-chat"));
    store.conn.execute("INSERT INTO turns(ts,chat_id,input_type,user_text,provider_raw,status,channel) VALUES('now','321','text','new-alias-input','','ok','telegram')", []).unwrap();
    assert!(
        store
            .recent_turns_snippet(8, "telegram:321", "telegram")
            .unwrap()[0]
            .contains("new-alias-input")
    );
}
#[test]
fn store_open_preserves_legacy_duplicates_and_relationships() {
    let cfg = coconutclaw_config::RuntimeConfig::test_config();
    let conn = Connection::open(&cfg.sqlite_db_path).expect("open legacy db");
    conn.execute_batch(
        "CREATE TABLE kv (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
         );
         INSERT INTO kv(key, value) VALUES ('schema_version', '7');
         INSERT INTO kv(key, value) VALUES ('last_update_id', '0');
         CREATE TABLE task_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            channel TEXT NOT NULL,
            source_chat_id TEXT,
            source_user_id TEXT,
            update_id TEXT,
            prompt TEXT NOT NULL,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            started_at TEXT,
            finished_at TEXT,
            cancel_requested_at TEXT,
            progress_message_id TEXT,
            last_progress TEXT,
            error_summary TEXT,
            result_summary TEXT,
            scheduled_task_id INTEGER
         );
         CREATE TABLE turns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            chat_id TEXT NOT NULL,
            input_type TEXT NOT NULL,
            user_text TEXT,
            asr_text TEXT,
            provider_raw TEXT NOT NULL,
            telegram_reply TEXT,
            voice_reply TEXT,
            status TEXT NOT NULL,
            update_id TEXT,
            duration_ms INTEGER,
            task_run_id INTEGER,
            side_effects_applied INTEGER NOT NULL DEFAULT 0,
            channel TEXT NOT NULL DEFAULT 'telegram'
         );
         CREATE TABLE approvals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_run_id INTEGER NOT NULL,
            session_id TEXT NOT NULL,
            channel TEXT NOT NULL,
            source_user_id TEXT,
            channel_id TEXT,
            thread_ts TEXT,
            prompt_text TEXT NOT NULL,
            status TEXT NOT NULL,
            request_message_ts TEXT,
            resume_payload TEXT NOT NULL,
            created_at TEXT NOT NULL,
            resolved_at TEXT,
            resolved_by_user_id TEXT
         );",
    )
    .expect("create legacy schema");
    conn.execute(
        "INSERT INTO task_runs(session_id, channel, source_chat_id, update_id, prompt, status, created_at)
         VALUES('telegram:321', 'telegram', '321', 'dup-1', 'first', 'completed', '2026-04-01T00:00:00+0000')",
        [],
    )
    .expect("insert first duplicate");
    let first_id = conn.last_insert_rowid();
    conn.execute(
        "INSERT INTO task_runs(session_id, channel, source_chat_id, update_id, prompt, status, created_at)
         VALUES('telegram:321', 'telegram', '321', 'dup-1', 'second', 'completed', '2026-04-01T00:00:01+0000')",
        [],
    )
    .expect("insert second duplicate");
    let keep_id = conn.last_insert_rowid();
    conn.execute(
        "INSERT INTO turns(ts, chat_id, input_type, user_text, asr_text, provider_raw, telegram_reply, voice_reply, status, update_id, duration_ms, task_run_id, side_effects_applied, channel)
         VALUES('2026-04-01T00:00:02+0000', 'telegram:321', 'text', 'hello', '', 'TELEGRAM_REPLY: hi', 'hi', '', 'ok', 'turn-1', 1, ?1, 1, 'telegram')",
        rusqlite::params![first_id],
    )
    .expect("insert turn linked to duplicate");
    conn.execute(
        "INSERT INTO approvals(task_run_id, session_id, channel, prompt_text, status, resume_payload, created_at)
         VALUES(?1, 'telegram:321', 'telegram', 'approve?', 'pending', '{}', '2026-04-01T00:00:03+0000')",
        rusqlite::params![first_id],
    )
    .expect("insert approval linked to duplicate");
    drop(conn);

    let error = Store::open(&cfg)
        .err()
        .expect("duplicates require manual migration");
    assert!(
        error
            .to_string()
            .contains("duplicate task update IDs preserved")
    );
    let conn = Connection::open(&cfg.sqlite_db_path).unwrap();
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM task_runs WHERE update_id='dup-1'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 2);
    for table in ["turns", "approvals"] {
        let id: i64 = conn
            .query_row(
                &format!("SELECT task_run_id FROM {table} LIMIT 1"),
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(id, first_id);
    }
    assert!(keep_id > first_id);
}
