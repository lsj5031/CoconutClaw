use super::*;

#[test]
fn context_notifications_initial_outcome_decision_and_outbox_share_commit() {
    let cfg = scheduler_test_config("NEVER-EXECUTE".into());
    let store = Store::open(&cfg).unwrap();
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    scheduler.shutdown().unwrap();
    let req = request(7, "waiter");
    let tx = store.conn.unchecked_transaction().unwrap();
    let (source, _) = store
        .admit_inbox_request(
            crate::store::inbox::EventKind::Request,
            Some("initial-source"),
            &request(7, "source"),
            "now",
        )
        .unwrap();
    let (run, _) = store
        .admit_inbox_request(
            crate::store::inbox::EventKind::Request,
            Some("initial-waiter"),
            &req,
            "now",
        )
        .unwrap();
    tx.commit().unwrap();
    store.conn.execute_batch("CREATE TRIGGER fixture_abort BEFORE INSERT ON runtime_notifications BEGIN SELECT RAISE(ABORT,'fixture initial decision failure'); END;").unwrap();
    let raw = format!("WAIT_FOR_TASK: {source}");
    assert!(
        scheduler
            .persist_result(&store, run, &req, &req.input, &raw, crate::TurnStatus::Ok)
            .is_err()
    );
    assert_eq!(store.get_task_run(run).unwrap().unwrap().status, "queued");
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT output FROM task_payloads WHERE task_run_id=?1",
                [run],
                |r| r.get::<_, Option<String>>(0)
            )
            .unwrap(),
        None
    );
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT count(*) FROM runtime_inbox WHERE event_key=?1",
                [format!("result:{run}")],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        0
    );
    assert_eq!(
        store
            .conn
            .query_row("SELECT count(*) FROM runtime_waits", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
    store
        .conn
        .execute_batch("DROP TRIGGER fixture_abort")
        .unwrap();
    scheduler
        .persist_result(&store, run, &req, &req.input, &raw, crate::TurnStatus::Ok)
        .unwrap();
    scheduler
        .persist_result(&store, run, &req, &req.input, &raw, crate::TurnStatus::Ok)
        .unwrap();
    assert_eq!(count(&store, "initial-outcome"), 1);
    assert_eq!(
        store.get_task_run(run).unwrap().unwrap().status,
        "waiting_event"
    );
}

#[test]
fn context_notifications_inflight_cancel_preserves_real_ack() {
    let mut cfg = scheduler_test_config("NEVER-EXECUTE".into());
    let store = Store::open(&cfg).unwrap();
    let (source, run, _) = waiting(&store, 7, true);
    failed(&store, source);
    store.observe_blockers("changed").unwrap();
    let probe = cfg.clone();
    let server = FakeTelegramServer::start_with_response(move |_| {
        let store = Store::open(&probe).unwrap();
        store
            .conn
            .execute(
                "UPDATE runtime_waits SET state='cancelled' WHERE run_id=?1",
                [run],
            )
            .unwrap();
        store
            .conn
            .execute(
                "UPDATE task_runs SET status='cancelled',cancel_requested_at='now' WHERE id=?1",
                [run],
            )
            .unwrap();
        (200, r#"{"ok":true,"result":{"message_id":123}}"#.into())
    });
    cfg.telegram_api_base = Some(server.base_url.clone());
    crate::delivery::deliver_notifications(&cfg, &store).unwrap();
    crate::delivery::deliver_notifications(&cfg, &store).unwrap();
    assert_eq!(server.requests.lock().unwrap().len(), 1);
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT disposition FROM runtime_notifications WHERE provenance='changed-blocker'",
                [],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
        "sent"
    );
    assert_eq!(
        store.get_task_run(run).unwrap().unwrap().status,
        "cancelled"
    );
}

#[test]
fn context_notifications_route_substitution_and_changed_original_projection_refuse() {
    let mut cfg = scheduler_test_config("NEVER-EXECUTE".into());
    let server = FakeTelegramServer::start();
    cfg.telegram_api_base = Some(server.base_url.clone());
    let mut store = Store::open(&cfg).unwrap();
    let (source, run, mut req) = waiting(&store, 7, true);
    failed(&store, source);
    store.observe_blockers("changed").unwrap();
    req.delivery = DeliveryTarget::Telegram {
        chat_id: "321".into(),
        thread_id: Some("8".into()),
    };
    store.conn.execute("UPDATE task_payloads SET request=?2,output='TELEGRAM_REPLY: different outcome' WHERE task_run_id=?1",
        rusqlite::params![run,build_resume_payload(&req,&req.input).unwrap()]).unwrap();
    crate::delivery::deliver_notifications(&cfg, &store).unwrap();
    assert!(server.requests.lock().unwrap().is_empty());
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT disposition FROM runtime_notifications WHERE provenance='changed-blocker'",
                [],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
        "superseded"
    );
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    scheduler.shutdown().unwrap();
    assert!(
        command(&cfg, &mut store, &scheduler, 400, 7, "/notifications")
            .contains("original projection changed")
    );
}

#[test]
fn context_notifications_exhausted_batch_does_not_starve_fresh_notice() {
    let mut cfg = scheduler_test_config("NEVER-EXECUTE".into());
    let server = FakeTelegramServer::start();
    cfg.telegram_api_base = Some(server.base_url.clone());
    let store = Store::open(&cfg).unwrap();
    let (source, _, req) = waiting(&store, 7, true);
    for n in 0..32 {
        let tx = store.conn.unchecked_transaction().unwrap();
        let (run, _) = store
            .admit_inbox_request(
                crate::store::inbox::EventKind::Request,
                Some(&format!("exhausted:{n}")),
                &req,
                "now",
            )
            .unwrap();
        store
            .wait_for_task(
                run,
                source,
                &build_resume_payload(&req, &req.input).unwrap(),
            )
            .unwrap();
        store
            .conn
            .execute(
                "UPDATE task_payloads SET output='TELEGRAM_REPLY: waiting' WHERE task_run_id=?1",
                [run],
            )
            .unwrap();
        store.record_initial_blocker(run, "initial").unwrap();
        tx.commit().unwrap();
    }
    failed(&store, source);
    store.observe_blockers("batch1").unwrap();
    store.observe_blockers("batch2").unwrap();
    store.conn.execute("UPDATE runtime_notifications SET delivery_state=?1 WHERE id IN(SELECT id FROM runtime_notifications WHERE disposition='pending' ORDER BY id LIMIT 32)",
        [json!({"completed_ops":[],"text_plan":{"kind":"rich","phase":"ready","attempts":3}}).to_string()]).unwrap();
    crate::delivery::deliver_notifications(&cfg, &store).unwrap();
    assert!(server.requests.lock().unwrap().is_empty());
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT count(*) FROM runtime_notifications WHERE disposition='suspended'",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        32
    );
    crate::delivery::deliver_notifications(&cfg, &store).unwrap();
    assert_eq!(server.requests.lock().unwrap().len(), 1);
}

fn command(
    cfg: &RuntimeConfig,
    store: &mut Store,
    scheduler: &SessionScheduler,
    update: i64,
    topic: i64,
    text: &str,
) -> String {
    let raw=json!({"update_id":update,"message":{"chat":{"id":321,"type":"private"},"from":{"id":321},"message_thread_id":topic,"text":text}}).to_string();
    crate::loops::webhook::process_webhook_line(cfg, store, scheduler, &raw).unwrap();
    store
        .conn
        .query_row(
            "SELECT coalesce(output,'') FROM telegram_ingress WHERE update_id=?1",
            [update.to_string()],
            |r| r.get(0),
        )
        .unwrap()
}

#[test]
fn context_notifications_actual_preference_command_is_not_model_work() {
    let cfg = scheduler_test_config("NEVER-EXECUTE".into());
    let mut store = Store::open(&cfg).unwrap();
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    scheduler.shutdown().unwrap();
    let reply = command(
        &cfg,
        &mut store,
        &scheduler,
        1,
        7,
        "/preferences set language Speak Finnish",
    );
    assert!(reply.contains("Preference saved"), "{reply}");
    assert!(command(&cfg, &mut store, &scheduler, 2, 7, "/preferences").contains("Speak Finnish"));
    assert!(!command(&cfg, &mut store, &scheduler, 3, 8, "/preferences").contains("Speak Finnish"));
    assert_eq!(
        store
            .conn
            .query_row("SELECT count(*) FROM task_runs", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );
}

fn request(topic: i64, prompt: &str) -> TaskRequest {
    let mut req = make_request(
        SessionKey::telegram(&format!("321#{topic}")),
        "telegram",
        prompt,
    );
    req.source_user_id = Some("321".into());
    req.delivery = DeliveryTarget::Telegram {
        chat_id: "321".into(),
        thread_id: Some(topic.to_string()),
    };
    req
}

fn waiting(store: &Store, topic: i64, initial: bool) -> (i64, i64, TaskRequest) {
    let dep = request(topic, "dependency");
    let waiter = request(topic, "waiter");
    let tx = store.conn.unchecked_transaction().unwrap();
    let (source, _) = store
        .admit_inbox_request(
            crate::store::inbox::EventKind::Request,
            Some(&format!("dependency:{topic}")),
            &dep,
            "now",
        )
        .unwrap();
    let (run, _) = store
        .admit_inbox_request(
            crate::store::inbox::EventKind::Request,
            Some(&format!("waiter:{topic}")),
            &waiter,
            "now",
        )
        .unwrap();
    store
        .wait_for_task(
            run,
            source,
            &build_resume_payload(&waiter, &waiter.input).unwrap(),
        )
        .unwrap();
    store.conn.execute("UPDATE task_payloads SET output='TELEGRAM_REPLY: Initial waiting.' WHERE task_run_id=?1",[run]).unwrap();
    if initial {
        store.record_initial_blocker(run, "now").unwrap();
    }
    tx.commit().unwrap();
    (source, run, waiter)
}

fn failed(store: &Store, source: i64) {
    store.conn.execute("UPDATE task_runs SET status='failed',error_summary='outcome unknown; no automatic retry' WHERE id=?1",[source]).unwrap();
}

fn count(store: &Store, provenance: &str) -> i64 {
    store
        .conn
        .query_row(
            "SELECT count(*) FROM runtime_notifications WHERE provenance=?1",
            [provenance],
            |r| r.get(0),
        )
        .unwrap()
}

#[test]
fn context_notifications_context_authority_and_fact_progress_separation() {
    let cfg = scheduler_test_config("NEVER-EXECUTE".into());
    let mut store = Store::open(&cfg).unwrap();
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    scheduler.shutdown().unwrap();
    command(
        &cfg,
        &mut store,
        &scheduler,
        1,
        7,
        "/preferences set language Speak Finnish",
    );
    command(
        &cfg,
        &mut store,
        &scheduler,
        2,
        7,
        "/memory remember Factual glacier evidence",
    );
    let (source, run, req) = waiting(&store, 7, true);
    failed(&store, source);
    store.observe_blockers("now").unwrap();
    let context = crate::context::build_context(
        &cfg,
        &store,
        &req.input,
        "now",
        &req.session.id(),
        &req.quoted,
    )
    .unwrap();
    let prefs = context
        .split("## Owner instructions/preferences")
        .nth(1)
        .unwrap()
        .split("## Curated memory")
        .next()
        .unwrap();
    assert!(prefs.contains("Speak Finnish"));
    assert!(!prefs.contains("Factual glacier evidence"));
    assert!(!context.contains("remains blocked")); // History is never instructions/context.
    assert!(
        store
            .curated_memory(&req.session.id())
            .unwrap()
            .contains("Factual glacier evidence")
    );
    assert!(
        !store
            .curated_memory(&req.session.id())
            .unwrap()
            .contains("Speak Finnish")
    );
    assert!(
        !store
            .task_progress_context(run)
            .unwrap()
            .contains("Speak Finnish")
    );
    assert!(
        command(&cfg, &mut store, &scheduler, 3, 7, &format!("/tasks {run}"))
            .contains("Waiting for: another task")
    );
    assert!(
        command(&cfg, &mut store, &scheduler, 4, 8, &format!("/tasks {run}")).contains("not found")
    );
    assert!(
        command(
            &cfg,
            &mut store,
            &scheduler,
            5,
            7,
            &format!("/notifications {run}")
        )
        .contains("outcome unknown")
    );
    assert!(
        command(&cfg, &mut store, &scheduler, 6, 8, "/notifications")
            .contains("No task notifications recorded in this conversation.")
    );
    let isolated = crate::context::build_context_scoped(
        &cfg,
        &store,
        &req.input,
        "now",
        "local:task-555",
        &req.quoted,
        &req.session.id(),
    )
    .unwrap();
    assert!(isolated.contains("Speak Finnish"));
    assert!(!isolated.contains("Factual glacier evidence"));
}

#[test]
fn context_notifications_preferences_replay_bounds_unset_and_actor() {
    let cfg = scheduler_test_config("NEVER-EXECUTE".into());
    let mut store = Store::open(&cfg).unwrap();
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    scheduler.shutdown().unwrap();
    for n in 0..8 {
        assert!(
            command(
                &cfg,
                &mut store,
                &scheduler,
                10 + n,
                7,
                &format!("/preferences set key{n} value{n}")
            )
            .contains("saved")
        );
    }
    let revision = store.preference_revision("telegram:321#7").unwrap();
    assert!(
        command(
            &cfg,
            &mut store,
            &scheduler,
            18,
            7,
            "/preferences set ninth no"
        )
        .contains("At most 8")
    );
    assert!(
        command(
            &cfg,
            &mut store,
            &scheduler,
            19,
            7,
            "/preferences set key0 value0"
        )
        .contains("unchanged")
    );
    assert_eq!(
        store.preference_revision("telegram:321#7").unwrap(),
        revision
    );
    assert!(
        command(
            &cfg,
            &mut store,
            &scheduler,
            20,
            7,
            "/preferences unset key0"
        )
        .contains("removed")
    );
    assert!(
        command(
            &cfg,
            &mut store,
            &scheduler,
            20,
            7,
            "/preferences unset key0"
        )
        .contains("removed")
    );
    assert!(
        !store
            .preferences_context("telegram:321#7")
            .unwrap()
            .contains("value0")
    );
    assert!(store.preference_revision("telegram:321#7").unwrap() > revision);
    let raw=json!({"update_id":21,"message":{"chat":{"id":321,"type":"private"},"from":{"id":322},"message_thread_id":7,"text":"/preferences set forged no"}}).to_string();
    crate::loops::webhook::process_webhook_line(&cfg, &mut store, &scheduler, &raw).unwrap();
    assert!(
        !store
            .preferences_context("telegram:321#7")
            .unwrap()
            .contains("forged")
    );
    assert!(
        store
            .conn
            .execute("UPDATE runtime_preferences SET value='rewritten'", [])
            .is_err()
    );
    assert_eq!(
        store
            .conn
            .query_row("SELECT count(*) FROM memory_records", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert_eq!(
        store
            .conn
            .query_row("SELECT count(*) FROM turns", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );
}

#[test]
fn context_notifications_preference_outcome_failure_rolls_back() {
    let cfg = scheduler_test_config("NEVER-EXECUTE".into());
    let mut store = Store::open(&cfg).unwrap();
    let scheduler = SessionScheduler::new(cfg.clone(), None);
    scheduler.shutdown().unwrap();
    store.conn.execute_batch("CREATE TRIGGER fixture_abort BEFORE UPDATE OF state ON telegram_ingress WHEN NEW.state='done' BEGIN SELECT RAISE(ABORT,'fixture outcome failure'); END;").unwrap();
    let raw=json!({"update_id":1,"message":{"chat":{"id":321,"type":"private"},"from":{"id":321},"message_thread_id":7,"text":"/preferences set language Finnish"}}).to_string();
    assert!(
        crate::loops::webhook::process_webhook_line(&cfg, &mut store, &scheduler, &raw).is_err()
    );
    assert_eq!(store.preference_revision("telegram:321#7").unwrap(), 0);
    assert_eq!(
        store
            .conn
            .query_row("SELECT state FROM telegram_ingress", [], |r| r
                .get::<_, String>(0))
            .unwrap(),
        "pending"
    );
    store
        .conn
        .execute_batch("DROP TRIGGER fixture_abort")
        .unwrap();
    drop(store);
    let mut store = Store::open(&cfg).unwrap();
    crate::loops::webhook::process_webhook_line(&cfg, &mut store, &scheduler, &raw).unwrap();
    assert_eq!(
        store
            .conn
            .query_row("SELECT count(*) FROM runtime_preferences", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        1
    );
}

#[test]
fn context_notifications_changed_identity_once_restart_reorder_real_http() {
    let mut cfg = scheduler_test_config("NEVER-EXECUTE".into());
    let server = FakeTelegramServer::start();
    cfg.telegram_api_base = Some(server.base_url.clone());
    let store = Store::open(&cfg).unwrap();
    let (source, run, _) = waiting(&store, 7, true);
    assert_eq!(count(&store, "initial-outcome"), 1);
    for _ in 0..3 {
        assert_eq!(store.observe_blockers("repeat").unwrap(), 0);
    }
    failed(&store, source);
    assert_eq!(store.observe_blockers("later").unwrap(), 1);
    crate::delivery::deliver_notifications(&cfg, &store).unwrap();
    drop(store);
    let store = Store::open(&cfg).unwrap();
    for _ in 0..4 {
        assert_eq!(store.observe_blockers("older-event-time").unwrap(), 0);
        crate::delivery::deliver_notifications(&cfg, &store).unwrap();
    }
    assert_eq!(server.requests.lock().unwrap().len(), 1);
    assert_eq!(count(&store, "changed-blocker"), 1);
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT output FROM task_payloads WHERE task_run_id=?1",
                [run],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
        "TELEGRAM_REPLY: Initial waiting."
    );
    assert!(server.full_requests.lock().unwrap()[0].contains("message_thread_id=7"));
    assert_eq!(
        store.get_task_run(source).unwrap().unwrap().status,
        "failed"
    );
}

#[test]
fn context_notifications_ambiguous_send_retains_unknown_without_resend() {
    let mut cfg = scheduler_test_config("NEVER-EXECUTE".into());
    let server = FakeTelegramServer::start_with_response(|_| (500, "{}".into()));
    cfg.telegram_api_base = Some(server.base_url.clone());
    let store = Store::open(&cfg).unwrap();
    let (source, _, _) = waiting(&store, 7, true);
    failed(&store, source);
    store.observe_blockers("now").unwrap();
    crate::delivery::deliver_notifications(&cfg, &store).unwrap();
    drop(store);
    let store = Store::open(&cfg).unwrap();
    for _ in 0..5 {
        store.observe_blockers("restart").unwrap();
        crate::delivery::deliver_notifications(&cfg, &store).unwrap();
    }
    assert_eq!(server.requests.lock().unwrap().len(), 1);
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT disposition FROM runtime_notifications WHERE provenance='changed-blocker'",
                [],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
        "unknown"
    );
    assert_eq!(count(&store, "changed-blocker"), 1);
}

#[test]
fn context_notifications_decision_outbox_failure_is_atomic() {
    let cfg = scheduler_test_config("NEVER-EXECUTE".into());
    let store = Store::open(&cfg).unwrap();
    let (source, _, _) = waiting(&store, 7, true);
    failed(&store, source);
    let before: String = store
        .conn
        .query_row("SELECT identity FROM runtime_task_blockers", [], |r| {
            r.get(0)
        })
        .unwrap();
    store.conn.execute_batch("CREATE TRIGGER fixture_abort BEFORE INSERT ON runtime_notifications WHEN NEW.provenance='changed-blocker' BEGIN SELECT RAISE(ABORT,'fixture decision failure'); END;").unwrap();
    assert!(store.observe_blockers("cut").is_err());
    assert_eq!(
        store
            .conn
            .query_row("SELECT identity FROM runtime_task_blockers", [], |r| r
                .get::<_, String>(
                0
            ))
            .unwrap(),
        before
    );
    assert_eq!(count(&store, "changed-blocker"), 0);
    store
        .conn
        .execute_batch("DROP TRIGGER fixture_abort")
        .unwrap();
    assert_eq!(store.observe_blockers("restart").unwrap(), 1);
    assert_eq!(store.observe_blockers("duplicate").unwrap(), 0);
    assert!(
        store
            .conn
            .execute("DELETE FROM runtime_notifications", [])
            .is_err()
    );
    assert!(
        store
            .conn
            .execute("UPDATE runtime_notifications SET output='forged'", [])
            .is_err()
    );
}

#[test]
fn context_notifications_concurrent_topics_have_independent_dedup() {
    let cfg = scheduler_test_config("NEVER-EXECUTE".into());
    let store = Store::open(&cfg).unwrap();
    let (a, _, _) = waiting(&store, 7, true);
    let (b, _, _) = waiting(&store, 8, true);
    failed(&store, a);
    failed(&store, b);
    let one = cfg.clone();
    let two = cfg.clone();
    let a = std::thread::spawn(move || Store::open(&one).unwrap().observe_blockers("one").unwrap());
    let b = std::thread::spawn(move || Store::open(&two).unwrap().observe_blockers("two").unwrap());
    assert_eq!(a.join().unwrap() + b.join().unwrap(), 2);
    assert_eq!(count(&store, "changed-blocker"), 2);
    assert_eq!(store.conn.query_row("SELECT count(DISTINCT target) FROM runtime_notifications WHERE provenance='changed-blocker'",[],|r|r.get::<_,i64>(0)).unwrap(),2);
}

#[test]
fn context_notifications_stale_cancelled_notice_never_sends() {
    let mut cfg = scheduler_test_config("NEVER-EXECUTE".into());
    let server = FakeTelegramServer::start();
    cfg.telegram_api_base = Some(server.base_url.clone());
    let store = Store::open(&cfg).unwrap();
    let (source, run, _) = waiting(&store, 7, true);
    failed(&store, source);
    store.observe_blockers("now").unwrap();
    store
        .conn
        .execute(
            "UPDATE runtime_waits SET state='cancelled' WHERE run_id=?1",
            [run],
        )
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE task_runs SET status='cancelled',cancel_requested_at='now' WHERE id=?1",
            [run],
        )
        .unwrap();
    crate::delivery::deliver_notifications(&cfg, &store).unwrap();
    assert!(server.requests.lock().unwrap().is_empty());
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT disposition FROM runtime_notifications WHERE provenance='changed-blocker'",
                [],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
        "superseded"
    );
}

#[test]
fn context_notifications_legacy_baseline_and_volatile_changes_are_silent() {
    let cfg = scheduler_test_config("NEVER-EXECUTE".into());
    let store = Store::open(&cfg).unwrap();
    let (source, _, _) = waiting(&store, 7, false);
    assert_eq!(store.observe_blockers("baseline").unwrap(), 1);
    assert_eq!(count(&store, "legacy-baseline"), 1);
    store.conn.execute("UPDATE task_runs SET status='running',started_at='later',last_progress='different wording' WHERE id=?1",[source]).unwrap();
    assert_eq!(store.observe_blockers("new-clock").unwrap(), 0);
    assert_eq!(count(&store, "changed-blocker"), 0);
    failed(&store, source);
    assert_eq!(store.observe_blockers("older-clock").unwrap(), 1);
}

#[test]
fn context_notifications_bounded_rotating_cursor_reaches_thirty_three() {
    let cfg = scheduler_test_config("NEVER-EXECUTE".into());
    let store = Store::open(&cfg).unwrap();
    let (source, _, req) = waiting(&store, 7, true);
    for n in 0..32 {
        let tx = store.conn.unchecked_transaction().unwrap();
        let (run, _) = store
            .admit_inbox_request(
                crate::store::inbox::EventKind::Request,
                Some(&format!("extra:{n}")),
                &req,
                "now",
            )
            .unwrap();
        store
            .wait_for_task(
                run,
                source,
                &build_resume_payload(&req, &req.input).unwrap(),
            )
            .unwrap();
        store
            .conn
            .execute(
                "UPDATE task_payloads SET output='TELEGRAM_REPLY: waiting' WHERE task_run_id=?1",
                [run],
            )
            .unwrap();
        store.record_initial_blocker(run, "initial").unwrap();
        tx.commit().unwrap();
    }
    failed(&store, source);
    assert_eq!(store.observe_blockers("batch1").unwrap(), 32);
    assert_eq!(store.observe_blockers("batch2").unwrap(), 1);
    assert_eq!(store.observe_blockers("batch3").unwrap(), 0);
    assert_eq!(count(&store, "changed-blocker"), 33);
}

#[test]
fn context_notifications_normal_binary_context_change_blocker_and_restart() {
    use std::sync::atomic::AtomicUsize;
    let round = Arc::new(AtomicUsize::new(0));
    let phase = round.clone();
    let server = FakeTelegramServer::start_with_response(move |raw| {
        let body = if raw.contains("/getMe ") {
            json!({"ok":true,"result":{"is_bot":true,"username":"ContextFixtureBot","has_topics_enabled":true}})
        } else if raw.contains("/getUpdates?") {
            let texts = if phase.load(Ordering::SeqCst) == 0 {
                vec![
                    (100, "/preferences set language Speak Finnish"),
                    (101, "waiter"),
                ]
            } else {
                vec![
                    (102, "/preferences unset language"),
                    (103, "second"),
                    (104, "/notifications 2"),
                ]
            };
            json!({"ok":true,"result":texts.into_iter().map(|(id,text)|json!({"update_id":id,"message":{"chat":{"id":321,"type":"private"},"from":{"id":321},"message_thread_id":7,"text":text}})).collect::<Vec<_>>()})
        } else {
            json!({"ok":true,"result":{"message_id":123}})
        };
        (200, body.to_string())
    });
    let mut cfg = scheduler_test_config("NEVER-EXECUTE".into());
    cfg.telegram_api_base = Some(server.base_url.clone());
    cfg.codex.bin = write_fake_provider_script(
        &cfg.instance_dir,
        "context-provider",
        "#!/bin/sh\nbase=$(dirname \"$0\"); printf '%s\\n' \"$*\" >> \"$base/context-args\"; out=''; while [ \"$#\" -gt 0 ]; do if [ \"$1\" = '--output-last-message' ]; then shift; out=\"$1\"; fi; shift; done; input=$(cat); echo run >> \"$base/context-calls\"; case \"$input\" in *'USER_TEXT: second'*) printf '%s' \"$input\" > \"$base/context-second\"; printf 'TELEGRAM_REPLY: second result' > \"$out\" ;; *) printf '%s' \"$input\" > \"$base/context-first\"; printf 'WAIT_FOR_TASK: 1' > \"$out\" ;; esac; printf '%s\\n' '{\"type\":\"thread.started\",\"thread_id\":\"11111111-1111-4111-8111-111111111111\"}'",
        "$a=$args; [IO.File]::AppendAllText((Join-Path $PSScriptRoot 'context-args'),(($a -join ' ')+\"`n\")); $i=[Array]::IndexOf($a,'--output-last-message'); $p=[Console]::In.ReadToEnd(); [IO.File]::AppendAllText((Join-Path $PSScriptRoot 'context-calls'),\"run`n\"); if($p.Contains('USER_TEXT: second')){[IO.File]::WriteAllText((Join-Path $PSScriptRoot 'context-second'),$p);$text='TELEGRAM_REPLY: second result'}else{[IO.File]::WriteAllText((Join-Path $PSScriptRoot 'context-first'),$p);$text='WAIT_FOR_TASK: 1'}; [IO.File]::WriteAllText($a[$i+1],$text); Write-Output '{\"type\":\"thread.started\",\"thread_id\":\"11111111-1111-4111-8111-111111111111\"}'",
    );
    struct Evidence(PathBuf);
    impl Drop for Evidence {
        fn drop(&mut self) {
            eprintln!("context fixture: {}", self.0.display());
            if let Some(base) = std::env::var_os("COCONUTCLAW_CONTEXT_NOTIFICATION_ARTIFACTS") {
                let dst = PathBuf::from(base);
                let copy = || -> std::io::Result<()> {
                    fs::create_dir_all(&dst)?;
                    for entry in fs::read_dir(&self.0)? {
                        let e = entry?;
                        if e.file_type()?.is_file() {
                            fs::copy(e.path(), dst.join(e.file_name()))?;
                        }
                    }
                    Ok(())
                };
                if let Err(error) = copy() {
                    eprintln!("fixture preservation failed: {error}");
                }
            }
        }
    }
    let _evidence = Evidence(cfg.instance_dir.clone());
    let store = Store::open(&cfg).unwrap();
    let dep = request(7, "remote dependency");
    let tx = store.conn.unchecked_transaction().unwrap();
    let (source, _) = store
        .admit_inbox_request(
            crate::store::inbox::EventKind::Request,
            Some("context-remote"),
            &dep,
            "now",
        )
        .unwrap();
    assert_eq!(source, 1);
    store.conn.execute("INSERT INTO runtime_remote_runs SELECT r.id,'fixture-origin',X'0102',r.session_id,p.request FROM task_runs r JOIN task_payloads p ON p.task_run_id=r.id WHERE r.id=?1",[source]).unwrap();
    store
        .conn
        .execute(
            "UPDATE task_runs SET status='waiting_remote' WHERE id=?1",
            [source],
        )
        .unwrap();
    tx.commit().unwrap();
    let binary = crate::util::test_binary_path();
    let hash = blake3::hash(&fs::read(&binary).unwrap());
    struct Owned(std::process::Child);
    impl Drop for Owned {
        fn drop(&mut self) {
            if self.0.try_wait().ok().flatten().is_none() {
                let _ = self.0.kill();
            }
            let _ = self.0.wait();
        }
    }
    let start = |n| {
        Owned(
            crate::util::scrubbed_command(&binary)
                .arg("--data-dir")
                .arg(&cfg.instance_dir)
                .arg("run")
                .env("HOME", &cfg.instance_dir)
                .env("FACTORY_AIRGAP_ENABLED", "1")
                .env("TELEGRAM_BOT_TOKEN", "123:token")
                .env("TELEGRAM_CHAT_ID", "321")
                .env(
                    "TELEGRAM_API_BASE",
                    server.base_url.trim_end_matches("/bot123:token"),
                )
                .env("POLL_INTERVAL_SECONDS", "1")
                .env("TELEGRAM_API_RETRY_ATTEMPTS", "0")
                .env("CODEX_BIN", &cfg.codex.bin)
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::from(
                    fs::File::create(cfg.instance_dir.join(format!("context-normal-{n}.log")))
                        .unwrap(),
                ))
                .spawn()
                .unwrap(),
        )
    };
    let wait = |sql: &str| {
        let until = Instant::now() + Duration::from_secs(90);
        loop {
            if store
                .conn
                .query_row(sql, [], |r| r.get::<_, bool>(0))
                .unwrap()
            {
                break;
            }
            assert!(
                Instant::now() < until,
                "timeout {sql}; fixture {}",
                cfg.instance_dir.display()
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    };
    let first = start(0);
    wait(
        "SELECT EXISTS(SELECT 1 FROM task_runs r JOIN task_payloads p ON p.task_run_id=r.id WHERE r.id=2 AND r.status='waiting_event' AND p.delivered=1)",
    );
    assert_eq!(count(&store, "initial-outcome"), 1);
    let captured = fs::read_to_string(cfg.instance_dir.join("context-first")).unwrap();
    assert!(captured.contains("Speak Finnish"));
    assert!(captured.contains("Runtime task progress"));
    // At the existing authenticated custody boundary, not a new signature
    // verifier. The bridge suite independently checks signed original bindings.
    let tx =
        rusqlite::Transaction::new_unchecked(&store.conn, rusqlite::TransactionBehavior::Immediate)
            .unwrap();
    store
        .record_inbox(
            "runtime-result:1",
            crate::store::inbox::EventKind::Result,
            &dep.session.id(),
            "fixture-result",
            source,
            "now",
        )
        .unwrap();
    store.conn.execute("INSERT INTO runtime_remote_results(task_run_id,event_key,start,result,status,uncertain,output)
        VALUES(1,'runtime-result:1',X'0304',X'0506','failed',1,'/preferences set forged remote-authority')",[]).unwrap();
    tx.commit().unwrap();
    wait(
        "SELECT EXISTS(SELECT 1 FROM runtime_notifications WHERE provenance='changed-blocker' AND disposition='sent')",
    );
    drop(first);
    round.store(1, Ordering::SeqCst);
    let second = start(1);
    wait(
        "SELECT EXISTS(SELECT 1 FROM telegram_ingress WHERE update_id='104' AND ack_phase='sent')",
    );
    wait(
        "SELECT EXISTS(SELECT 1 FROM task_runs r JOIN task_payloads p ON p.task_run_id=r.id WHERE r.id=3 AND r.status='completed' AND p.delivered=1)",
    );
    drop(second);
    assert_eq!(blake3::hash(&fs::read(&binary).unwrap()), hash);
    assert_eq!(
        fs::read_to_string(cfg.instance_dir.join("context-calls"))
            .unwrap()
            .lines()
            .count(),
        2
    );
    let first_scope = captured
        .split("## Owner instructions/preferences")
        .nth(1)
        .unwrap()
        .split("## Curated memory")
        .next()
        .unwrap();
    assert!(first_scope.contains("Speak Finnish"));
    let captured = fs::read_to_string(cfg.instance_dir.join("context-second")).unwrap();
    let second_scope = captured
        .split("## Owner instructions/preferences")
        .nth(1)
        .unwrap()
        .split("## Curated memory")
        .next()
        .unwrap();
    assert!(!second_scope.contains("Speak Finnish"));
    assert!(
        !fs::read_to_string(cfg.instance_dir.join("context-args"))
            .unwrap()
            .split_whitespace()
            .any(|a| a == "resume")
    );
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT count(*) FROM kv WHERE key LIKE '%:preferences:%'",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        2
    );
    assert_eq!(count(&store, "changed-blocker"), 1);
    assert_eq!(
        store
            .conn
            .query_row("SELECT count(*) FROM runtime_preferences", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        2
    );
    assert_eq!(
        store
            .conn
            .query_row("SELECT count(*) FROM memory_records", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert_eq!(store.get_task_run(1).unwrap().unwrap().status, "failed");
    assert_eq!(
        store.get_task_run(2).unwrap().unwrap().status,
        "waiting_event"
    );
    let sent = server.full_requests.lock().unwrap();
    assert_eq!(
        sent.iter()
            .filter(|r| r.contains("remains+blocked"))
            .count(),
        1
    );
    if let Some(base) = std::env::var_os("COCONUTCLAW_CONTEXT_NOTIFICATION_ARTIFACTS") {
        let dst = PathBuf::from(base);
        fs::create_dir_all(&dst).unwrap();
        fs::write(dst.join("http-requests.json"), json!(*sent).to_string()).unwrap();
    }
}
