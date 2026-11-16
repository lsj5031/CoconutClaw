use super::*;
use crate::{
    scheduling::Timing,
    store::{TaskRunStatus, inbox::EventKind},
};
use chrono::{DateTime, Utc};
use rusqlite::{Transaction, TransactionBehavior};

#[path = "calendar_wake_tests.rs"]
mod calendar_wake;

#[test]
fn calendar_cancellation_after_start_is_not_permission_for_another_occurrence() {
    let now = at("2026-11-16T09:00:00Z");
    for (key, started) in [("queued-cancel", false), ("started-cancel", true)] {
        let (_root, cfg, store) = fixture();
        store
            .create_schedule(
                key,
                &request(&cfg, "local:cancellation", "fixture"),
                &Timing::parse(None, Some("09:00"), "UTC").unwrap(),
                now,
            )
            .unwrap();
        let run = store.due_schedule_events(now).unwrap()[0].0;
        if started {
            store.update_task_run_started(run, "fixture-start").unwrap();
        }
        store
            .finish_task_run(run, TaskRunStatus::Cancelled, "fixture-cancel", None, None)
            .unwrap();
        assert_eq!(
            store
                .due_schedule_events(at("2026-11-17T09:00:00Z"))
                .unwrap()
                .len(),
            usize::from(!started),
            "only a cancellation before start is known not to have effects"
        );
    }
}

#[test]
fn calendar_unknown_notification_blocks_future_occurrences_without_resend_or_new_run() {
    let (_root, cfg, store) = fixture();
    let now = at("2026-11-16T09:00:00Z");
    store
        .create_schedule(
            "notification",
            &request(&cfg, "local:notification", "fixture"),
            &Timing::parse(None, Some("09:00"), "UTC").unwrap(),
            now,
        )
        .unwrap();
    let run = store.due_schedule_events(now).unwrap()[0].0;
    store
        .finish_task_run(
            run,
            TaskRunStatus::Completed,
            "known model outcome",
            None,
            None,
        )
        .unwrap();
    for phase in ["sending", "unknown"] {
        store
            .conn
            .execute(
                "UPDATE task_payloads SET delivery_state=?2 WHERE task_run_id=?1",
                rusqlite::params![
                    run,
                    serde_json::json!({"text_plan":{"phase":phase}}).to_string()
                ],
            )
            .unwrap();
        assert!(
            store
                .due_schedule_events(at("2026-11-17T09:00:00Z"))
                .unwrap()
                .is_empty()
        );
    }
    assert_eq!(count(&store, "task_runs"), 1);
    assert_eq!(count(&store, "schedule_occurrences"), 1);
}

#[test]
#[cfg(unix)]
fn inbox_actual_scheduler_concurrent_replays_invoke_only_once() {
    let (root, mut cfg, store) = fixture();
    let calls = fake(&mut cfg, root.path(), "TELEGRAM_REPLY: inbox once");
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    let barrier = Arc::new(std::sync::Barrier::new(8));
    let mut req = make_stdout_request(SessionKey::local("same"), "one accepted request");
    req.update_id = Some("same-update".into());
    let threads = (0..8)
        .map(|n| {
            let scheduler = scheduler.clone();
            let barrier = barrier.clone();
            let mut req = req.clone();
            req.progress_message_id = Some(format!("transport-{n}"));
            std::thread::spawn(move || {
                barrier.wait();
                scheduler.enqueue(req).unwrap()
            })
        })
        .collect::<Vec<_>>();
    let ids = threads
        .into_iter()
        .map(|t| t.join().unwrap())
        .collect::<Vec<_>>();
    assert!(ids.iter().all(|id| *id == ids[0]));
    wait_for_terminal_tasks(&cfg, &[ids[0]]);
    req.input.user_text = "different instruction with same identity".into();
    assert!(scheduler.enqueue(req).is_err());
    assert_eq!(fs::read_to_string(calls).unwrap().lines().count(), 1);
    assert_eq!(count(&store, "task_runs"), 1);
    assert_eq!(count(&store, "runtime_inbox"), 2);
    scheduler.shutdown().unwrap();
}

#[test]
#[cfg(unix)]
fn calendar_topic_outbox_is_separate_and_slow_delivery_does_not_hold_execution_lane() {
    let (root, mut cfg, store) = fixture();
    let blocked = root.path().join("delivery-blocked");
    let gate = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
    let callback_gate = gate.clone();
    let callback_blocked = blocked.clone();
    let first = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let server = FakeTelegramServer::start_with_response(move |request| {
        if request
            .lines()
            .next()
            .is_some_and(|line| line.contains("/sendRichMessage "))
            && first.swap(false, Ordering::SeqCst)
        {
            fs::write(&callback_blocked, b"blocked").unwrap();
            let (lock, wake) = &*callback_gate;
            let _ = wake
                .wait_timeout_while(lock.lock().unwrap(), Duration::from_secs(5), |released| {
                    !*released
                })
                .unwrap();
        }
        (200, r#"{"ok":true,"result":{"message_id":77}}"#.into())
    });
    cfg.telegram_bot_token = Some("123:token".into());
    cfg.telegram_chat_ids = vec!["321".into()];
    cfg.telegram_api_base = Some(server.base_url.trim_end_matches("/bot123:token").into());
    let calls = fake(&mut cfg, root.path(), "TELEGRAM_REPLY: topic fixture");
    let now = at("2026-11-16T09:00:00Z");
    for key in ["first", "second"] {
        store
            .create_schedule(
                key,
                &request(&cfg, "telegram:321#77", key),
                &Timing::parse(Some("2026-11-16T09:00:00Z"), None, "UTC").unwrap(),
                now,
            )
            .unwrap();
    }
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    assert_eq!(scheduler.wake_due(now).unwrap(), 2);
    let deadline = Instant::now() + Duration::from_secs(5);
    while !blocked.exists() {
        assert!(
            Instant::now() < deadline,
            "no delivery: tasks={:?}, {:?}; paths={:?}; calls={:?}",
            store.get_task_run(1).unwrap(),
            store.get_task_run(2).unwrap(),
            server.requests.lock().unwrap(),
            fs::read_to_string(&calls)
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    wait_for_terminal_tasks(&cfg, &[1, 2]);
    assert!(
        !*gate.0.lock().unwrap(),
        "network dispatcher must still be blocked"
    );
    assert_eq!(fs::read_to_string(calls).unwrap().lines().count(), 2);
    assert_eq!(
        store.get_task_run(2).unwrap().unwrap().session_id,
        "telegram:321#77"
    );
    assert!(
        !store
            .conn
            .query_row(
                "SELECT delivered FROM task_payloads WHERE task_run_id=1",
                [],
                |r| r.get::<_, bool>(0)
            )
            .unwrap()
    );
    *gate.0.lock().unwrap() = true;
    gate.1.notify_all();
    eventually(|| {
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM task_payloads WHERE delivered=1",
                [],
                |r| r.get::<_, i64>(0),
            )
            .unwrap()
            == 2
    });
    let messages = server.full_requests.lock().unwrap();
    let sends = messages
        .iter()
        .filter(|r| r.contains("/sendRichMessage"))
        .collect::<Vec<_>>();
    assert_eq!(sends.len(), 2);
    assert!(
        sends
            .iter()
            .all(|r| r.contains("message_thread_id") && r.contains("77"))
    );
    drop(messages);
    assert_eq!(scheduler.wake_due(now).unwrap(), 0);
    scheduler.retry_deliveries().unwrap();
    assert_eq!(
        server
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|p| p.ends_with("/sendRichMessage"))
            .count(),
        2
    );
    scheduler.shutdown().unwrap();
}

#[test]
#[cfg(unix)]
fn inbox_result_outbox_failure_recovers_known_turn_without_model_retry() {
    let (root, mut cfg, store) = fixture();
    let calls = fake(&mut cfg, root.path(), "TELEGRAM_REPLY: durable known turn");
    let now = at("2026-11-16T09:00:00Z");
    store
        .create_schedule(
            "known",
            &request(&cfg, "local:known", "fixture"),
            &Timing::parse(Some("2026-11-16T09:00:00Z"), None, "UTC").unwrap(),
            now,
        )
        .unwrap();
    store.conn.execute_batch("CREATE TRIGGER fail_result BEFORE UPDATE OF output ON task_payloads
        WHEN NEW.output IS NOT NULL BEGIN SELECT RAISE(ABORT,'injected result commit failure'); END;").unwrap();
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    assert_eq!(scheduler.wake_due(now).unwrap(), 1);
    eventually(|| {
        count(&store, "turns") == 1 && scheduler.inner.task_state.lock().unwrap().is_empty()
    });
    assert_eq!(
        count(&store, "runtime_inbox"),
        1,
        "result must roll back with its outbox"
    );
    assert!(
        store
            .conn
            .query_row("SELECT output FROM task_payloads", [], |r| r
                .get::<_, Option<String>>(0))
            .unwrap()
            .is_none()
    );
    scheduler.shutdown().unwrap();
    store
        .conn
        .execute_batch("DROP TRIGGER fail_result")
        .unwrap();
    let restarted = SessionScheduler::new(cfg, None);
    restarted.recover().unwrap();
    assert_eq!(store.get_task_run(1).unwrap().unwrap().status, "completed");
    assert_eq!(count(&store, "runtime_inbox"), 2);
    assert_eq!(fs::read_to_string(calls).unwrap().lines().count(), 1);
    restarted.shutdown().unwrap();
}

fn at(raw: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(raw)
        .unwrap()
        .with_timezone(&Utc)
}

fn fixture() -> (tempfile::TempDir, RuntimeConfig, Store) {
    let temp = tempfile::tempdir().unwrap();
    let cfg = RuntimeConfig::test_config_at(temp.path().join("runtime"));
    let store = Store::open(&cfg).unwrap();
    (temp, cfg, store)
}

fn request(cfg: &RuntimeConfig, session: &str, prompt: &str) -> TaskRequest {
    crate::commands::schedule::request(cfg, SessionKey::from_id(session.into()), prompt).unwrap()
}

fn count(store: &Store, table: &str) -> i64 {
    store
        .conn
        .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
        .unwrap()
}

fn cursor(store: &Store, id: i64) -> (i64, String, Option<i64>) {
    store
        .conn
        .query_row(
            "SELECT revision,state,next_due FROM runtime_schedules WHERE id=?1",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap()
}

#[test]
fn calendar_dst_and_quarter_hour_offsets_have_independent_expected_instants() {
    let cases = [
        (
            "Pacific/Auckland",
            "02:30",
            "2026-09-26T12:00:00Z",
            "2026-09-27T13:30:00Z",
        ),
        (
            "Pacific/Auckland",
            "02:30",
            "2026-04-04T12:00:00Z",
            "2026-04-04T13:30:00Z",
        ),
        (
            "America/New_York",
            "02:30",
            "2026-03-08T05:00:00Z",
            "2026-03-09T06:30:00Z",
        ),
        (
            "America/New_York",
            "01:30",
            "2026-11-01T04:00:00Z",
            "2026-11-01T05:30:00Z",
        ),
        (
            "Pacific/Chatham",
            "09:10",
            "2026-11-15T12:00:00Z",
            "2026-11-15T19:25:00Z",
        ),
    ];
    for (zone, time, now, expected) in cases {
        let timing = Timing::parse(None, Some(time), zone).unwrap();
        assert_eq!(timing.first(at(now)).unwrap(), at(expected), "{zone}");
        assert_eq!(Timing::decode(&timing.encode()).unwrap(), timing);
    }
    let fold = Timing::parse(None, Some("02:30"), "Pacific/Auckland").unwrap();
    assert_eq!(
        fold.next(at("2026-04-04T13:30:00Z")).unwrap().unwrap(),
        at("2026-04-05T14:30:00Z")
    );
    assert_eq!(
        fold.occurrence(at("2026-04-04T14:40:00Z")).unwrap(),
        at("2026-04-04T13:30:00Z")
    );
    for (once, daily, zone) in [
        (None, None, "UTC"),
        (Some("2026-11-16T10:00:00Z"), Some("10:00"), "UTC"),
        (Some("2026-11-16T10:00:00.1Z"), None, "UTC"),
        (Some("2026-11-16T10:00:00"), None, "UTC"),
        (None, Some("25:00"), "UTC"),
        (None, Some("08:99"), "UTC"),
        (None, Some("08:00"), "not-a-zone"),
    ] {
        assert!(Timing::parse(once, daily, zone).is_err());
    }
}

#[test]
fn calendar_occurrence_payload_cursor_and_inbox_roll_back_and_replay_together() {
    let (_temp, cfg, store) = fixture();
    let now = at("2026-11-16T09:00:00Z");
    let timing = Timing::parse(Some("2026-11-16T09:00:00Z"), None, "Pacific/Auckland").unwrap();
    let req = request(&cfg, "local:one", "harmless fixture");
    let id = store.create_schedule("key", &req, &timing, now).unwrap();
    assert_eq!(
        store.create_schedule("key", &req, &timing, now).unwrap(),
        id
    );
    let mut conflict = req.clone();
    conflict.input.user_text = "changed".into();
    assert!(
        store
            .create_schedule("key", &conflict, &timing, now)
            .is_err()
    );
    store
        .conn
        .execute_batch(
            "CREATE TRIGGER fail_payload BEFORE INSERT ON task_payloads
        BEGIN SELECT RAISE(ABORT,'injected atomic enqueue failure'); END;",
        )
        .unwrap();
    assert!(store.due_schedule_events(now).is_err());
    assert_eq!(count(&store, "runtime_inbox"), 0);
    assert_eq!(count(&store, "schedule_occurrences"), 0);
    assert_eq!(count(&store, "task_runs"), 0);
    assert_eq!(
        cursor(&store, id),
        (1, "active".into(), Some(now.timestamp()))
    );
    store
        .conn
        .execute_batch("DROP TRIGGER fail_payload")
        .unwrap();
    let accepted = store.due_schedule_events(now).unwrap();
    assert_eq!(accepted.len(), 1);
    assert_eq!(accepted[0].1.session.id(), "local:one");
    assert!(accepted[0].1.delegated);
    assert_eq!(count(&store, "runtime_inbox"), 1);
    assert_eq!(count(&store, "schedule_occurrences"), 1);
    assert_eq!(count(&store, "task_payloads"), 1);
    assert_eq!(cursor(&store, id), (1, "done".into(), None));
    assert!(
        Store::open(&cfg)
            .unwrap()
            .due_schedule_events(now)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store.create_schedule("key", &req, &timing, now).unwrap(),
        id
    );
    assert_eq!(cursor(&store, id).1, "done");
}

#[test]
fn calendar_concurrent_sqlite_wakers_allocate_exactly_one_occurrence() {
    let (_temp, cfg, store) = fixture();
    let now = at("2026-11-16T09:00:00Z");
    store
        .create_schedule(
            "once",
            &request(&cfg, "local:race", "fixture"),
            &Timing::parse(Some("2026-11-16T09:00:00Z"), None, "UTC").unwrap(),
            now,
        )
        .unwrap();
    let stores = (0..4)
        .map(|_| Store::open(&cfg).unwrap())
        .collect::<Vec<_>>();
    let barrier = Arc::new(std::sync::Barrier::new(4));
    let threads = stores
        .into_iter()
        .map(|store| {
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                store.due_schedule_events(now).unwrap().len()
            })
        })
        .collect::<Vec<_>>();
    assert_eq!(
        threads
            .into_iter()
            .map(|t| t.join().unwrap())
            .sum::<usize>(),
        1
    );
    assert_eq!(count(&store, "runtime_inbox"), 1);
    assert_eq!(count(&store, "schedule_occurrences"), 1);
    assert_eq!(count(&store, "task_runs"), 1);
}

#[test]
fn calendar_missed_runs_coalesce_and_unfinished_or_unknown_work_never_overlaps() {
    let (_temp, cfg, store) = fixture();
    let start = at("2026-01-01T08:00:00Z");
    let timing = Timing::parse(None, Some("09:00"), "UTC").unwrap();
    let id = store
        .create_schedule(
            "daily",
            &request(&cfg, "local:daily", "fixture"),
            &timing,
            start,
        )
        .unwrap();
    let now = at("2026-11-16T12:00:00Z");
    let run = store.due_schedule_events(now).unwrap()[0].0;
    let due: i64 = store
        .conn
        .query_row("SELECT due FROM schedule_occurrences", [], |r| r.get(0))
        .unwrap();
    assert_eq!(due, at("2026-11-16T09:00:00Z").timestamp());
    assert_eq!(
        cursor(&store, id).2,
        Some(at("2026-11-17T09:00:00Z").timestamp())
    );
    assert!(
        store
            .due_schedule_events(at("2026-11-20T12:00:00Z"))
            .unwrap()
            .is_empty()
    );
    store.update_task_run_started(run, "fixture-start").unwrap();
    Store::open(&cfg)
        .unwrap()
        .mark_stale_task_runs_failed("fixture-restart")
        .unwrap();
    assert_eq!(store.get_task_run(run).unwrap().unwrap().status, "failed");
    assert!(
        store
            .due_schedule_events(at("2026-12-20T12:00:00Z"))
            .unwrap()
            .is_empty()
    );
    store
        .edit_schedule("local:daily", id, "edited future", &timing, now)
        .unwrap();
    assert!(
        store
            .due_schedule_events(at("2026-12-20T12:00:00Z"))
            .unwrap()
            .is_empty()
    );
    assert_eq!(count(&store, "task_runs"), 1);
    assert_eq!(count(&store, "schedule_occurrences"), 1);
}

#[test]
fn calendar_pause_resume_cancel_edit_and_session_scoping_preserve_accepted_work() {
    let (_temp, cfg, store) = fixture();
    let now = at("2026-11-16T08:00:00Z");
    let timing = Timing::parse(None, Some("09:00"), "UTC").unwrap();
    let id = store
        .create_schedule(
            "job",
            &request(&cfg, "local:owner", "original"),
            &timing,
            now,
        )
        .unwrap();
    assert!(
        store
            .set_schedule_state("local:other", id, "cancelled")
            .is_err()
    );
    assert!(
        store
            .edit_schedule("local:other", id, "wrong", &timing, now)
            .is_err()
    );
    store
        .set_schedule_state("local:owner", id, "paused")
        .unwrap();
    store
        .set_schedule_state("local:owner", id, "paused")
        .unwrap();
    assert!(
        store
            .due_schedule_events(at("2026-11-18T12:00:00Z"))
            .unwrap()
            .is_empty()
    );
    store
        .set_schedule_state("local:owner", id, "active")
        .unwrap();
    let run = store
        .due_schedule_events(at("2026-11-18T12:00:00Z"))
        .unwrap()[0]
        .0;
    store
        .edit_schedule(
            "local:owner",
            id,
            "new revision",
            &timing,
            at("2026-11-18T12:00:00Z"),
        )
        .unwrap();
    assert_eq!(store.get_task_run(run).unwrap().unwrap().prompt, "original");
    assert!(
        store
            .due_schedule_events(at("2026-11-19T12:00:00Z"))
            .unwrap()
            .is_empty()
    );
    store
        .finish_task_run(run, TaskRunStatus::Completed, "fixture", None, None)
        .unwrap();
    let second = store
        .due_schedule_events(at("2026-11-19T12:00:00Z"))
        .unwrap();
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].1.input.user_text, "new revision");
    let revision: i64 = store
        .conn
        .query_row(
            "SELECT revision FROM schedule_occurrences WHERE event_key=?1",
            [second[0].1.update_id.as_ref().unwrap()],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(revision, 2);
    store
        .set_schedule_state("local:owner", id, "cancelled")
        .unwrap();
    store
        .set_schedule_state("local:owner", id, "cancelled")
        .unwrap();
    assert!(
        store
            .set_schedule_state("local:owner", id, "active")
            .is_err()
    );
    assert!(
        store
            .edit_schedule("local:owner", id, "resurrect", &timing, now)
            .is_err()
    );
}

#[test]
fn calendar_batch_limit_and_blocked_jobs_do_not_starve_eligible_jobs() {
    let (_temp, cfg, store) = fixture();
    let now = at("2026-11-16T09:00:00Z");
    let timing = Timing::parse(None, Some("09:00"), "UTC").unwrap();
    for n in 0..33 {
        store
            .create_schedule(
                &format!("batch-{n}"),
                &request(&cfg, &format!("local:{n}"), "fixture"),
                &timing,
                now,
            )
            .unwrap();
    }
    assert_eq!(store.due_schedule_events(now).unwrap().len(), 32);
    assert_eq!(store.due_schedule_events(now).unwrap().len(), 1);
    assert!(store.due_schedule_events(now).unwrap().is_empty());
    let later = at("2026-11-17T09:00:00Z");
    store
        .create_schedule(
            "new",
            &request(&cfg, "local:new", "fixture"),
            &timing,
            later,
        )
        .unwrap();
    assert_eq!(
        store.due_schedule_events(later).unwrap().len(),
        1,
        "33 blocked cursors must not starve a new eligible job"
    );
}

#[test]
fn inbox_duplicate_request_is_stable_and_conflicts_roll_back_without_retry() {
    let (_temp, cfg, store) = fixture();
    let mut req = request(&cfg, "local:inbox", "fixture");
    req.update_id = Some("synthetic-update".into());
    let tx = Transaction::new_unchecked(&store.conn, TransactionBehavior::Immediate).unwrap();
    let (id, new) = store
        .admit_inbox_request(EventKind::Request, Some("source-key"), &req, "fixture")
        .unwrap();
    assert!(new);
    tx.commit().unwrap();
    store.update_task_run_started(id, "start").unwrap();
    store.mark_stale_task_runs_failed("crash").unwrap();
    req.progress_message_id = Some("changed transport ID".into());
    let tx = Transaction::new_unchecked(&store.conn, TransactionBehavior::Immediate).unwrap();
    assert_eq!(
        store
            .admit_inbox_request(EventKind::Request, Some("source-key"), &req, "later")
            .unwrap(),
        (id, false)
    );
    tx.commit().unwrap();
    req.input.user_text = "conflicting work".into();
    let tx = Transaction::new_unchecked(&store.conn, TransactionBehavior::Immediate).unwrap();
    assert!(
        store
            .admit_inbox_request(EventKind::Request, Some("source-key"), &req, "later")
            .is_err()
    );
    drop(tx);
    assert_eq!(count(&store, "task_runs"), 1);
    assert_eq!(store.get_task_run(id).unwrap().unwrap().status, "failed");
}

#[test]
fn calendar_legacy_rows_stay_inert_and_never_alias_new_schedule_ownership() {
    let (_temp, cfg, store) = fixture();
    store.conn.execute("INSERT INTO scheduled_tasks(id,ts,source,prompt,schedule_time) VALUES(1,'old','telegram','legacy private','09:00')",[]).unwrap();
    let now = at("2026-11-16T09:00:00Z");
    assert!(store.due_schedule_events(now).unwrap().is_empty());
    assert_eq!(count(&store, "runtime_schedules"), 0);
    let legacy = store
        .insert_task_run(crate::store::InsertTaskRunParams {
            session_id: "local:legacy".into(),
            channel: "local".into(),
            source_chat_id: None,
            source_user_id: None,
            update_id: None,
            prompt: "legacy".into(),
            created_at: "old".into(),
            progress_message_id: None,
            scheduled_task_id: Some(1),
        })
        .unwrap();
    store.update_task_run_started(legacy, "old").unwrap();
    let new = store
        .create_schedule(
            "new",
            &request(&cfg, "local:new", "new fixture"),
            &Timing::parse(Some("2026-11-16T09:00:00Z"), None, "UTC").unwrap(),
            now,
        )
        .unwrap();
    assert_eq!(new, 1);
    assert_eq!(store.due_schedule_events(now).unwrap().len(), 1);
    let legacy_prompt: String = store
        .conn
        .query_row("SELECT prompt FROM scheduled_tasks WHERE id=1", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(legacy_prompt, "legacy private");
}

#[test]
fn calendar_actual_cli_dispatch_parses_and_edits_only_its_conversation() {
    use clap::Parser;
    let (_temp, cfg, store) = fixture();
    let now = at("2026-11-16T08:00:00Z");
    let run = |args: &[&str]| {
        let cli = crate::Cli::try_parse_from(args).unwrap();
        let crate::Commands::Schedule(args) = cli.command else {
            panic!("calendar CLI");
        };
        crate::commands::schedule::run(&cfg, &store, &args, now)
    };
    assert_eq!(
        run(&[
            "coconutclaw",
            "schedule",
            "--session",
            "local:cli",
            "create",
            "--key",
            "cli",
            "--prompt",
            "fixture",
            "--daily",
            "09:00",
            "--timezone",
            "Pacific/Auckland"
        ])
        .unwrap()["schedule"],
        1
    );
    assert_eq!(
        run(&["coconutclaw", "schedule", "--session", "local:cli", "list"])
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert!(
        run(&[
            "coconutclaw",
            "schedule",
            "--session",
            "local:other",
            "pause",
            "1"
        ])
        .is_err()
    );
    run(&[
        "coconutclaw",
        "schedule",
        "--session",
        "local:cli",
        "pause",
        "1",
    ])
    .unwrap();
    run(&[
        "coconutclaw",
        "schedule",
        "--session",
        "local:cli",
        "edit",
        "1",
        "--prompt",
        "edited",
        "--at",
        "2026-11-17T09:00:00+13:00",
        "--timezone",
        "Pacific/Auckland",
    ])
    .unwrap();
    run(&[
        "coconutclaw",
        "schedule",
        "--session",
        "local:cli",
        "resume",
        "1",
    ])
    .unwrap();
    assert_eq!(
        cursor(&store, 1),
        (
            2,
            "active".into(),
            Some(at("2026-11-16T20:00:00Z").timestamp())
        )
    );
    run(&[
        "coconutclaw",
        "schedule",
        "--session",
        "local:cli",
        "cancel",
        "1",
    ])
    .unwrap();
    assert!(
        run(&[
            "coconutclaw",
            "schedule",
            "--session",
            "local:cli",
            "resume",
            "1"
        ])
        .is_err()
    );
}

#[cfg(unix)]
fn fake(cfg: &mut RuntimeConfig, root: &std::path::Path, answer: &str) -> PathBuf {
    let script = root.join("fixture-provider.sh");
    let calls = root.join("calls");
    fs::write(
        &script,
        format!(
            r#"printf 'call\n' >> '{}'
cat >/dev/null
while [ "$#" -gt 0 ]; do
 if [ "$1" = "--output-last-message" ]; then shift; out=$1; fi
 shift
done
printf '%s\n' '{}' > "$out"
"#,
            calls.display(),
            answer
        ),
    )
    .unwrap();
    cfg.codex.bin = format!("sh '{}'", script.display());
    calls
}

fn eventually(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !condition() {
        assert!(Instant::now() < deadline, "condition did not settle");
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
#[cfg(unix)]
fn calendar_poll_entry_wakes_due_work_even_while_telegram_fetch_fails() {
    let (root, mut cfg, store) = fixture();
    let calls = fake(&mut cfg, root.path(), "TELEGRAM_REPLY: polling fixture");
    let server = FakeTelegramServer::start_with_response(|_| {
        (
            500,
            r#"{"ok":false,"description":"offline fixture"}"#.into(),
        )
    });
    cfg.telegram_api_base = Some(server.base_url.trim_end_matches("/bot123:token").into());
    cfg.telegram_api_retry_attempts = 0;
    cfg.poll_interval_seconds = 1;
    let now = chrono::Utc::now();
    store
        .create_schedule(
            "polling",
            &request(&cfg, "local:polling", "fixture"),
            &Timing::parse(Some("2026-01-01T00:00:00Z"), None, "UTC").unwrap(),
            now,
        )
        .unwrap();
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    let polling = scheduler.clone();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let poll_stop = stop.clone();
    let poll_cfg = cfg.clone();
    let worker = std::thread::spawn(move || {
        let mut store = Store::open(&poll_cfg).unwrap();
        crate::loops::run_poll_loop(
            &poll_cfg,
            &mut store,
            &polling,
            &reqwest::blocking::Client::builder()
                .timeout(Duration::from_secs(2))
                .build()
                .unwrap(),
            &poll_stop,
        )
    });
    eventually(|| count(&store, "task_runs") == 1);
    wait_for_terminal_tasks(&cfg, &[1]);
    eventually(|| !server.requests.lock().unwrap().is_empty());
    stop.store(true, Ordering::SeqCst);
    worker.join().unwrap().unwrap();
    assert_eq!(store.get_task_run(1).unwrap().unwrap().status, "completed");
    assert_eq!(fs::read_to_string(calls).unwrap().lines().count(), 1);
    assert_eq!(count(&store, "runtime_inbox"), 2);
    assert!(server.requests.lock().unwrap().iter().all(|p| {
        p.split('?')
            .next()
            .is_some_and(|path| path.ends_with("/getUpdates"))
    }));
    scheduler.shutdown().unwrap();
}

#[test]
#[cfg(unix)]
fn calendar_real_scheduler_recovers_committed_inbox_before_dispatch_and_returns_outbox() {
    let (root, mut cfg, store) = fixture();
    let calls = fake(
        &mut cfg,
        root.path(),
        "TELEGRAM_REPLY: scheduled fixture complete",
    );
    let now = at("2026-11-16T09:00:00Z");
    store
        .create_schedule(
            "one",
            &request(&cfg, "local:runtime", "fixture"),
            &Timing::parse(Some("2026-11-16T09:00:00Z"), None, "UTC").unwrap(),
            now,
        )
        .unwrap();
    // Crash cut: committed occurrence/inbox/task, core died before lane dispatch.
    let run = store.due_schedule_events(now).unwrap()[0].0;
    assert!(!calls.exists());
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    scheduler.recover().unwrap();
    wait_for_terminal_tasks(&cfg, &[run]);
    eventually(|| {
        store
            .conn
            .query_row(
                "SELECT delivered FROM task_payloads WHERE task_run_id=?1",
                [run],
                |r| r.get::<_, bool>(0),
            )
            .unwrap()
    });
    assert_eq!(fs::read_to_string(&calls).unwrap().lines().count(), 1);
    assert_eq!(
        count(&store, "runtime_inbox"),
        2,
        "request + outcome events"
    );
    scheduler.shutdown().unwrap();
    let restarted = SessionScheduler::new(cfg.clone(), None);
    restarted.recover().unwrap();
    assert_eq!(restarted.wake_due(now).unwrap(), 0);
    assert_eq!(fs::read_to_string(calls).unwrap().lines().count(), 1);
    assert_eq!(count(&store, "runtime_inbox"), 2);
    restarted.shutdown().unwrap();
}

#[test]
#[cfg(unix)]
fn calendar_waiting_approval_releases_slot_and_continuation_retains_no_overlap() {
    let (root, mut cfg, store) = fixture();
    fake(
        &mut cfg,
        root.path(),
        "TELEGRAM_REPLY: waiting\nSEND_APPROVAL: allow fixture",
    );
    let now = at("2026-11-16T09:00:00Z");
    store
        .create_schedule(
            "approval",
            &request(&cfg, "local:approval", "fixture"),
            &Timing::parse(None, Some("09:00"), "UTC").unwrap(),
            now,
        )
        .unwrap();
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    assert_eq!(scheduler.wake_due(now).unwrap(), 1);
    eventually(|| {
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM task_runs WHERE status='awaiting_approval'",
                [],
                |r| r.get::<_, i64>(0),
            )
            .unwrap()
            == 1
    });
    let approval: i64 = store
        .conn
        .query_row("SELECT id FROM approvals", [], |r| r.get(0))
        .unwrap();
    assert_eq!(scheduler.wake_due(at("2026-11-17T10:00:00Z")).unwrap(), 0);
    // Pending approvals have no execution lane. An independent event in the same
    // conversation can finish (this fake also asks approval, without blocking).
    let independent = scheduler
        .enqueue(make_stdout_request(
            SessionKey::local("approval"),
            "independent",
        ))
        .unwrap();
    eventually(|| store.get_task_run(independent).unwrap().unwrap().status == "awaiting_approval");
    scheduler.shutdown().unwrap();
    fake(
        &mut cfg,
        root.path(),
        "TELEGRAM_REPLY: approved fixture complete",
    );
    let restarted = SessionScheduler::new(cfg.clone(), None);
    restarted.recover().unwrap();
    restarted
        .resolve_approval(approval, "approve", "local:approval")
        .unwrap();
    let continuation: i64 = store
        .conn
        .query_row(
            "SELECT task_run_id FROM runtime_inbox WHERE event_key=?1",
            [format!("approval:{approval}")],
            |r| r.get(0),
        )
        .unwrap();
    let links: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM runtime_schedule_runs WHERE task_run_id=?1",
            [continuation],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(links, 1);
    wait_for_terminal_tasks(&cfg, &[continuation]);
    restarted
        .resolve_approval(approval, "approve", "local:approval")
        .unwrap();
    assert_eq!(count(&store, "schedule_occurrences"), 1);
    assert_eq!(restarted.wake_due(at("2026-11-17T10:00:00Z")).unwrap(), 1);
    restarted.shutdown().unwrap();
}

#[test]
#[cfg(unix)]
fn calendar_unknown_attempt_stays_blocked_after_runtime_restart_without_provider_call() {
    let (root, mut cfg, store) = fixture();
    let calls = fake(&mut cfg, root.path(), "TELEGRAM_REPLY: must not execute");
    let now = at("2026-11-16T09:00:00Z");
    store
        .create_schedule(
            "unknown",
            &request(&cfg, "local:unknown", "fixture"),
            &Timing::parse(None, Some("09:00"), "UTC").unwrap(),
            now,
        )
        .unwrap();
    let run = store.due_schedule_events(now).unwrap()[0].0;
    store
        .update_task_run_started(run, "crash-after-start")
        .unwrap();
    let scheduler = SessionScheduler::new(cfg, None);
    scheduler.recover().unwrap();
    assert_eq!(store.get_task_run(run).unwrap().unwrap().status, "failed");
    assert_eq!(scheduler.wake_due(at("2026-12-16T09:00:00Z")).unwrap(), 0);
    assert!(
        !calls.exists(),
        "recovery must not recreate invocation permission"
    );
    assert_eq!(count(&store, "task_runs"), 1);
    scheduler.shutdown().unwrap();
}
