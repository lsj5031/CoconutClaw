use super::*;
use rusqlite::Connection;

#[test]
fn store_open_deduplicates_legacy_task_run_update_ids() {
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

    let store = Store::open(&cfg).expect("open migrated store");
    let duplicate_count: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM task_runs WHERE update_id = 'dup-1'",
            [],
            |row| row.get(0),
        )
        .expect("count deduped rows");
    assert_eq!(duplicate_count, 1);
    let migrated_turn_task_run_id: i64 = store
        .conn
        .query_row("SELECT task_run_id FROM turns LIMIT 1", [], |row| {
            row.get(0)
        })
        .expect("turn task_run_id");
    assert_eq!(migrated_turn_task_run_id, keep_id);
    let migrated_approval_task_run_id: i64 = store
        .conn
        .query_row("SELECT task_run_id FROM approvals LIMIT 1", [], |row| {
            row.get(0)
        })
        .expect("approval task_run_id");
    assert_eq!(migrated_approval_task_run_id, keep_id);
    let has_unique_index: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master
             WHERE type = 'index'
               AND name = 'idx_task_runs_update_id_unique'",
            [],
            |row| row.get(0),
        )
        .expect("check unique index");
    assert_eq!(has_unique_index, 1);
    assert!(
        store
            .conn
            .execute(
                "INSERT INTO task_runs(session_id, channel, source_chat_id, update_id, prompt, status, created_at)
                 VALUES('telegram:321', 'telegram', '321', 'dup-1', 'third', 'queued', '2026-04-01T00:00:04+0000')",
                [],
            )
            .is_err()
    );
}

#[test]
fn migration_backfills_legacy_scheduled_task_routing_from_origin_turn() {
    let cfg = coconutclaw_config::RuntimeConfig::test_config();
    let store = Store::open(&cfg).expect("store");
    store
        .conn
        .execute("UPDATE kv SET value = '9' WHERE key = 'schema_version'", [])
        .expect("rewind schema version");
    store
        .conn
        .execute(
            "INSERT INTO turns(ts, chat_id, input_type, user_text, asr_text, provider_raw, telegram_reply, voice_reply, status, update_id, duration_ms, task_run_id, side_effects_applied, channel)
             VALUES('2026-04-02T09:00:00+0000', 'slack:C123#171.5', 'text', 'schedule it', '', 'TELEGRAM_REPLY: ok\nSCHEDULE_PROMPT: 09:30|Check backups\n', 'ok', '', 'ok', 'turn-2', 1, NULL, 1, 'slack')",
            [],
        )
        .expect("insert origin turn");
    store
        .conn
        .execute(
            "INSERT INTO scheduled_tasks(ts, source, prompt, schedule_time, recurring, done, pending_output, delivery_state, origin_session, delivery_target)
             VALUES('2026-04-02T09:00:00+0000', 'agent', 'Check backups', '09:30', 1, 0, NULL, NULL, NULL, NULL)",
            [],
        )
        .expect("insert legacy scheduled task");
    drop(store);

    let reopened = Store::open(&cfg).expect("reopen migrated store");
    let task = reopened
        .get_scheduled_task(1)
        .expect("load scheduled task")
        .expect("scheduled task exists");
    assert_eq!(task.origin_session.as_deref(), Some("slack:C123#171.5"));
    assert_eq!(
        task.delivery_target.as_deref(),
        Some(r#"{"channel_id":"C123","kind":"slack","thread_ts":"171.5"}"#)
    );
}
