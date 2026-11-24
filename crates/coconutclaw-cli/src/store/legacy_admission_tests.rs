use super::Store;
use crate::delivery::{DeliveryTarget, TaskSource};
use crate::loops::webhook::process_webhook_line;
use crate::scheduler::{SessionScheduler, TaskRequest};
use crate::scheduling::Timing;
use crate::session::SessionKey;
use crate::{InputType, QuotedMessage, TurnInput};
use coconutclaw_config::RuntimeConfig;
use rusqlite::{ErrorCode, Transaction, TransactionBehavior};
use std::time::Duration;

const FREEZE: &str = "INSERT OR IGNORE INTO legacy_admission_freeze(singleton) VALUES (1)";
const UNKNOWN: &str = "runtime restarted; outcome unknown, do not automatically retry";

fn fixture() -> (tempfile::TempDir, RuntimeConfig, Store) {
    let root = tempfile::tempdir().unwrap();
    let mut cfg = RuntimeConfig::test_config_at(root.path().into());
    cfg.telegram_bot_token = None;
    cfg.telegram_topics_enabled = Some(true);
    cfg.codex.bin = "/nonexistent/never-invoke-a-real-provider".into();
    let store = Store::open(&cfg).unwrap();
    (root, cfg, store)
}

#[test]
fn legacy_admission_ingress_writer_orders_with_freeze_and_restart() {
    let (_root, cfg, mut store) = fixture();
    store
        .conn
        .execute_batch(
            "INSERT INTO task_runs(id, session_id, channel, source_chat_id, source_thread_id,
                                   update_id, prompt, status, created_at)
             VALUES (23,'telegram:321#37','telegram','321','37','201','fixture','completed','fixture'),
                    (24,'telegram:321#37','telegram','321','37',NULL,'fixture','failed','fixture');
             UPDATE runtime_attempts SET task_id=23, ordinal=2, parent_run_id=23 WHERE run_id=24;
             DELETE FROM runtime_tasks WHERE task_id=24;
             UPDATE runtime_tasks SET current_run_id=24 WHERE task_id=23;",
        )
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE task_runs SET error_summary=?1 WHERE id=24",
            [UNKNOWN],
        )
        .unwrap();
    let freezer = Store::connect(&cfg).unwrap();
    let uncommitted =
        Transaction::new_unchecked(&freezer.conn, TransactionBehavior::Immediate).unwrap();
    uncommitted.execute(FREEZE, []).unwrap();
    drop(uncommitted);

    let original_cursor = freezer.kv_get("last_update_id").unwrap();
    let writer = Transaction::new_unchecked(&store.conn, TransactionBehavior::Immediate).unwrap();
    let raw = serde_json::json!({
        "update_id": 201,
        "message": {"chat": {"id": 321, "type": "private"}, "from": {"id": 321},
                    "message_thread_id": 37, "text": "fixture"}
    })
    .to_string();
    store
        .admit_telegram("201", &raw, Some("telegram:321#37"), "fixture")
        .unwrap();
    freezer.conn.busy_timeout(Duration::ZERO).unwrap();
    let blocked = freezer.conn.execute(FREEZE, []).unwrap_err();
    assert_eq!(blocked.sqlite_error_code(), Some(ErrorCode::DatabaseBusy));
    assert_eq!(freezer.kv_get("last_update_id").unwrap(), original_cursor);
    writer.commit().unwrap();
    assert_eq!(freezer.conn.execute(FREEZE, []).unwrap(), 1);

    let new = serde_json::json!({"update_id": 202}).to_string();
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    let error = process_webhook_line(&cfg, &mut store, &scheduler, &new).unwrap_err();
    assert_eq!(error.to_string(), "legacy admission is frozen");
    scheduler.shutdown().unwrap();
    drop(freezer);
    drop(store);

    let mut reopened = Store::open(&cfg).unwrap();
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    for update in [&new, &raw] {
        let error = process_webhook_line(&cfg, &mut reopened, &scheduler, update).unwrap_err();
        assert_eq!(error.to_string(), "legacy admission is frozen");
    }
    assert_eq!(reopened.conn.execute(FREEZE, []).unwrap(), 0);
    assert_eq!(
        reopened.kv_get("last_update_id").unwrap().as_deref(),
        Some("201")
    );
    let ingress: (i32, String, String, String) = reopened
        .conn
        .query_row(
            "SELECT count(*), payload, state, ack_phase FROM telegram_ingress",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(ingress, (1, raw, "unknown".into(), "unknown".into()));
    let outcome: (i64, String, String, String) = reopened
        .conn
        .query_row(
            "SELECT r.id, r.source_thread_id, r.status, r.error_summary
             FROM runtime_tasks t JOIN runtime_attempts a ON a.task_id=t.task_id AND a.run_id=t.current_run_id
             JOIN task_runs r ON r.id=a.run_id WHERE t.task_id=23",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(outcome, (24, "37".into(), "failed".into(), UNKNOWN.into()));
    scheduler.shutdown().unwrap();
}

#[test]
fn legacy_admission_calendar_preserves_due_cursor_and_creates_no_work() {
    let (_root, cfg, store) = fixture();
    let now = "2026-11-16T09:00:00Z".parse().unwrap();
    let timing = Timing::parse(None, Some("09:00"), "UTC").unwrap();
    let request = TaskRequest {
        session: SessionKey::local("freeze-fixture"),
        source: TaskSource::Local,
        input: TurnInput {
            input_type: InputType::Text,
            user_text: "fixture".into(),
            attachment_path: None,
            attachment_type: None,
            attachment_owned: false,
            supplemental_context: None,
            channel: "local".into(),
        },
        update_id: None,
        media: None,
        quoted: QuotedMessage {
            reply_from: None,
            reply_text: None,
            reply_ts: None,
        },
        delivery: DeliveryTarget::Stdout,
        source_user_id: None,
        progress_message_id: None,
        delegated: true,
    };
    store
        .create_schedule("fixture", &request, &timing, now)
        .unwrap();
    let deadline = store.next_schedule_deadline().unwrap().unwrap();
    assert!(deadline <= now.timestamp());
    let freezer = Store::connect(&cfg).unwrap();
    freezer.conn.execute(FREEZE, []).unwrap();
    drop(freezer);
    let assert_frozen = |connection: &Store| {
        let error = connection.due_schedule_events(now).unwrap_err();
        assert_eq!(error.to_string(), "legacy admission is frozen");
        assert_eq!(connection.next_schedule_deadline().unwrap(), Some(deadline));
        let work: (i32, i32, i32, i32, i32) = connection
            .conn
            .query_row(
                "SELECT (SELECT count(*) FROM schedule_occurrences),
                        (SELECT count(*) FROM runtime_inbox),
                        (SELECT count(*) FROM task_runs),
                        (SELECT count(*) FROM task_payloads),
                        (SELECT count(*) FROM runtime_schedule_runs)",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(work, (0, 0, 0, 0, 0));
    };
    assert_frozen(&store);
    drop(store);
    assert_frozen(&Store::open(&cfg).unwrap());
}
